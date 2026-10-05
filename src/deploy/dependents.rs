//! The objects that depend on a function that the plan drops: a
//! function that the plan drops and makes again (a change that CREATE
//! OR REPLACE FUNCTION cannot do), or one that only the database has.
//! PostgreSQL refuses to drop a function while an object depends on
//! it, thus the plan drops each dependent first and makes it again
//! from the project after the function.
//!
//! The snapshot's dependency edges give the dependents, transitively.
//! A whole object (a view, for example) is dropped and made again, and
//! the objects that depend on it are dependents too. A part of a table
//! or domain (a column default, a check, an index, a trigger, a
//! policy) is dropped, and the table or domain makes it again in
//! place. Each of these statements is destructive, thus without
//! `--allow-drop` all of them are withheld with the function's drop.
//!
//! The plan does not use DROP ... CASCADE: CASCADE drops each
//! dependent without a list, also one that the project does not have
//! or that the snapshot does not show. The plan must make each
//! dependent again, thus it names each one; a dependent that it does
//! not find makes the DROP fail, and PostgreSQL names it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use libpgdump::ObjectType as OT;

use super::alter::{self, Resolution};
use super::diff::{Change, Diff, ObjectKey};
use super::{drop_match_key, entry_key, entry_label};
use crate::models::{Definition, Domain, Table};
use crate::utils::quote_ident;

/// The dependents that the plan drops and makes again
#[derive(Default)]
pub(crate) struct Dependents {
    /// The DROP statements of the dependents, labeled, by the snapshot
    /// entry that they come from. The plan writes them with the drops
    /// of the removed objects, in reverse snapshot order
    pub drops: HashMap<i32, Vec<alter::Alter>>,
    /// The label of each dependent that the plan makes again, for the
    /// script header
    pub labels: Vec<String>,
    /// Each dependent that the plan cannot drop and make again, with
    /// the reason
    pub refused: Vec<String>,
}

/// The types that are dropped and made again from the project
const WHOLE: [OT; 7] = [
    OT::View,
    OT::MaterializedView,
    OT::Function,
    OT::Procedure,
    OT::Aggregate,
    OT::Operator,
    OT::Cast,
];

/// The entries that are a part of a relation, with an entry of their
/// own
const PARTS: [OT; 5] = [
    OT::Index,
    OT::Trigger,
    OT::Policy,
    OT::CheckConstraint,
    OT::Default,
];

/// A part of a table or domain to drop
enum Part<'a> {
    /// The defaults and checks of the entry itself that call these
    /// functions, as (dump id, schema, name)
    Inline(Vec<(i32, String, String)>),
    /// A part with an entry of its own
    Entry(&'a libpgdump::Entry),
}

/// Find the dependents of each function that the plan drops, and
/// change `diff` and `resolutions` so that the plan drops them and
/// makes them again
pub(crate) fn rebuild(
    project: &crate::project::Project,
    diff: &mut Diff,
    resolutions: &mut BTreeMap<usize, Resolution>,
    snapshot: &libpgdump::Dump,
    groups: &alter::IndexGroups,
    families: &alter::operator_class::Families,
    allow_drop_indexes: bool,
) -> Dependents {
    let mut dependents = Dependents::default();
    let entries = snapshot.entries();
    let by_id: HashMap<i32, &libpgdump::Entry> =
        entries.iter().map(|entry| (entry.dump_id, entry)).collect();
    let keys: BTreeMap<ObjectKey, usize> = project
        .inventory
        .iter()
        .map(|item| {
            let key = ObjectKey::new(item.desc, &item.definition);
            (drop_match_key(&key, &item.definition), item.id)
        })
        .collect();
    let removed: BTreeSet<ObjectKey> = diff
        .removed
        .iter()
        .map(|(key, definition)| drop_match_key(key, definition))
        .collect();
    let item = |entry: &libpgdump::Entry| {
        entry_key(entry).and_then(|key| keys.get(&key).copied())
    };
    let is_removed = |entry: &libpgdump::Entry| {
        entry_key(entry).is_some_and(|key| removed.contains(&key))
    };
    // the functions that the plan drops: a replaced one, or one that
    // only the database has
    let mut closure: BTreeSet<i32> = entries
        .iter()
        .filter(|entry| entry.desc == OT::Function)
        .filter(|entry| {
            // the key of a removed function can be the key of a new one
            // with other argument names
            is_removed(entry)
                || item(entry).is_some_and(|id| {
                    diff.items.get(&id) == Some(&Change::Changed)
                        && matches!(
                            resolutions.get(&id),
                            Some(Resolution::Replace)
                        )
                })
        })
        .map(|entry| entry.dump_id)
        .collect();
    let mut pending: Vec<i32> = closure.iter().copied().collect();
    let mut whole: Vec<(&libpgdump::Entry, usize)> = Vec::new();
    let mut parts: BTreeMap<i32, Vec<Part>> = BTreeMap::new();
    let mut later: Vec<(&libpgdump::Entry, i32)> = Vec::new();
    let mut seen: BTreeSet<i32> = BTreeSet::new();
    while let Some(id) = pending.pop() {
        let source = by_id[&id];
        for entry in entries {
            // the plan drops a removed object with the other removed
            // objects, in reverse snapshot order
            if !entry.dependencies.contains(&id)
                || closure.contains(&entry.dump_id)
                || is_removed(entry)
            {
                continue;
            }
            match entry.desc {
                // their object makes them again
                OT::Comment | OT::Acl | OT::SecurityLabel => {}
                _ if WHOLE.contains(&entry.desc) => {
                    if !seen.insert(entry.dump_id) {
                        continue;
                    }
                    match item(entry).filter(|id| {
                        !matches!(
                            diff.items.get(id),
                            Some(Change::Skipped | Change::Added) | None
                        )
                    }) {
                        Some(id) => {
                            closure.insert(entry.dump_id);
                            pending.push(entry.dump_id);
                            whole.push((entry, id));
                        }
                        None => dependents.refused.push(format!(
                            "{}: the project does not have it",
                            entry_label(entry)
                        )),
                    }
                }
                OT::Table | OT::Domain => {
                    if source.desc != OT::Function {
                        dependents.refused.push(format!(
                            "{}: it depends on {}, and deploy can drop \
                             and make again only its defaults and checks",
                            entry_label(entry),
                            entry_label(source)
                        ));
                        continue;
                    }
                    let Some((schema, name)) = function_name(source) else {
                        continue;
                    };
                    let called = (id, schema, name);
                    let list = parts.entry(entry.dump_id).or_default();
                    match list.iter_mut().find_map(|part| match part {
                        Part::Inline(functions) => Some(functions),
                        Part::Entry(_) => None,
                    }) {
                        Some(functions) if functions.contains(&called) => {}
                        Some(functions) => functions.push(called),
                        None => list.push(Part::Inline(vec![called])),
                    }
                }
                _ if PARTS.contains(&entry.desc) => {
                    if !seen.insert(entry.dump_id) {
                        continue;
                    }
                    match relation(entry, &by_id) {
                        // the rebuild of its relation makes it again
                        Some(owner) if closure.contains(&owner.dump_id) => {}
                        Some(owner)
                            if matches!(
                                owner.desc,
                                OT::Table | OT::Domain
                            ) =>
                        {
                            parts
                                .entry(owner.dump_id)
                                .or_default()
                                .push(Part::Entry(entry));
                        }
                        // the traversal can add its relation later
                        Some(owner) => later.push((entry, owner.dump_id)),
                        None => dependents.refused.push(format!(
                            "{}: deploy cannot drop and make it again",
                            entry_label(entry)
                        )),
                    }
                }
                _ => {
                    if seen.insert(entry.dump_id) {
                        dependents.refused.push(format!(
                            "{}: deploy cannot drop and make it again",
                            entry_label(entry)
                        ));
                    }
                }
            }
        }
    }
    for (entry, owner) in later {
        if !closure.contains(&owner) {
            dependents.refused.push(format!(
                "{}: deploy cannot drop and make it again",
                entry_label(entry)
            ));
        }
    }
    // a relation that the rebuild of a whole object drops is not
    // changed in place
    parts.retain(|id, _| !closure.contains(id));
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_postgres::LANGUAGE.into())
        .expect("the tree-sitter-postgres grammar loads");
    for (relation_id, list) in parts {
        let relation = by_id[&relation_id];
        if is_removed(relation) {
            continue;
        }
        let Some(id) = item(relation) else {
            dependents.refused.push(format!(
                "{}: the project does not have it",
                entry_label(relation)
            ));
            continue;
        };
        rebuild_parts(
            &mut parser,
            project,
            diff,
            resolutions,
            (relation, id),
            &list,
            (groups, families),
            allow_drop_indexes,
            &mut dependents,
        );
    }
    for (entry, id) in whole {
        let definition = project
            .inventory
            .iter()
            .find(|item| item.id == id)
            .map(|item| item.definition.clone())
            .expect("a project item of the inventory");
        // unchanged, so the project's copy is the database's
        diff.changed.entry(id).or_insert(definition);
        diff.items.insert(id, Change::Changed);
        let before = match resolutions.remove(&id) {
            Some(Resolution::Rebuild { before, .. }) => before,
            _ => Vec::new(),
        };
        resolutions.insert(
            id,
            Resolution::Rebuild {
                before,
                drop: String::new(),
            },
        );
        let label = entry_label(entry);
        if let Some(drop) = entry.drop_stmt.clone() {
            dependents
                .drops
                .entry(entry.dump_id)
                .or_default()
                .push(alter::Alter::destructive(drop).labeled(&label));
        }
        dependents.labels.push(label);
    }
    dependents.labels.sort();
    dependents
}

/// Drop the parts in `list` of a table or domain, and resolve the
/// table or domain again without them, so that it makes them again.
/// A part that the project does not have is only dropped, with the
/// gate of the drop that the table or domain would write for it
#[allow(clippy::too_many_arguments)]
fn rebuild_parts(
    parser: &mut tree_sitter::Parser,
    project: &crate::project::Project,
    diff: &mut Diff,
    resolutions: &mut BTreeMap<usize, Resolution>,
    (relation, id): (&libpgdump::Entry, usize),
    list: &[Part],
    (groups, families): (
        &alter::IndexGroups,
        &alter::operator_class::Families,
    ),
    allow_drop_indexes: bool,
    dependents: &mut Dependents,
) {
    let repo = &project
        .inventory
        .iter()
        .find(|item| item.id == id)
        .expect("a project item of the inventory")
        .definition;
    let label = entry_label(relation);
    if diff.items.get(&id) == Some(&Change::Undiffable) {
        dependents.refused.push(format!(
            "{label}: the project writes it as raw sql, which deploy \
             does not change in place"
        ));
        return;
    }
    let mut database = diff
        .changed
        .get(&id)
        .cloned()
        .unwrap_or_else(|| repo.clone());
    let mut drops = Vec::new();
    for part in list {
        let result = match (part, &mut database, repo) {
            (
                Part::Inline(functions),
                Definition::Table(table),
                Definition::Table(wanted),
            ) => inline_table(parser, table, wanted, functions, &mut drops),
            (
                Part::Inline(functions),
                Definition::Domain(domain),
                Definition::Domain(wanted),
            ) => inline_domain(parser, domain, wanted, functions, &mut drops),
            (Part::Entry(entry), definition, wanted) => entry_part(
                (relation, entry),
                definition,
                wanted,
                allow_drop_indexes,
                &mut drops,
            ),
            _ => Err(String::from("deploy cannot change it in place")),
        };
        if let Err(reason) = result {
            dependents.refused.push(format!("{label}: {reason}"));
            return;
        }
    }
    let old: BTreeSet<String> = match resolutions.get(&id) {
        Some(Resolution::Statements(alters)) => {
            alters.iter().map(|alter| alter.sql.clone()).collect()
        }
        _ => BTreeSet::new(),
    };
    // the rebuild of the table or domain makes the parts again
    let rebuilt = matches!(
        resolutions.get(&id),
        Some(Resolution::Replace | Resolution::Rebuild { .. })
    );
    let resolution =
        match alter::resolve_with(repo, &database, groups, families) {
            // each statement that makes a part again is gated with the
            // drop of the part
            Resolution::Statements(mut alters) => {
                for alter in &mut alters {
                    if !old.contains(&alter.sql) {
                        alter.destructive = true;
                    }
                    // a column default is set again with ALTER TABLE
                    // ONLY, as it is dropped: without ONLY, PostgreSQL
                    // sets it on each inheritance child and partition
                    for (_, drop, _) in &drops {
                        if let Some(set) = only_default(&drop.sql, &alter.sql)
                        {
                            alter.sql = set;
                        }
                    }
                }
                Resolution::Statements(alters)
            }
            resolution if rebuilt => resolution,
            _ => {
                dependents.refused.push(format!(
                    "{label}: deploy cannot make its parts again in place"
                ));
                return;
            }
        };
    for (entry, drop, again) in drops {
        let entry_id = entry.map_or(relation.dump_id, |entry| entry.dump_id);
        if again {
            dependents.labels.extend(drop.label.iter().cloned());
        }
        dependents.drops.entry(entry_id).or_default().push(drop);
    }
    diff.items.insert(id, Change::Changed);
    diff.changed.insert(id, database);
    resolutions.insert(id, resolution);
}

/// A part to drop: its entry (None for a part of the relation's own
/// entry), its labeled DROP statement, and true when the project has
/// the part, thus the plan makes it again
type Drop<'a> = (Option<&'a libpgdump::Entry>, alter::Alter, bool);

/// The drop of a part: gated with --allow-drop when the plan makes it
/// again, else a statement that the table or domain would write
fn part_drop(label: String, sql: String, again: bool) -> alter::Alter {
    if again {
        alter::Alter::destructive(sql).labeled(&label)
    } else {
        alter::Alter::new(sql).labeled(&label)
    }
}

/// Remove from `table` the column defaults and checks that call one of
/// `functions`, with the statements that drop them. Each function must
/// have a part that calls it: else the table depends on it in a way
/// that deploy cannot change (a generated column, for example)
fn inline_table<'a>(
    parser: &mut tree_sitter::Parser,
    table: &mut Table,
    wanted: &Table,
    functions: &[(i32, String, String)],
    drops: &mut Vec<Drop<'a>>,
) -> Result<(), String> {
    let name = format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    );
    let tag = format!("{}.{}", table.schema, table.name);
    // the project has a default for the column, thus the plan makes it
    // again
    let default_again = |column: &str| {
        wanted
            .columns
            .iter()
            .flatten()
            .any(|wanted| wanted.name == column && wanted.default.is_some())
            || has(&wanted.column_defaults, |wanted| wanted.column == column)
    };
    let mut found = BTreeSet::new();
    let mut calls = |expression: &str| {
        let called = called(parser, expression, functions);
        found.extend(called.iter().copied());
        !called.is_empty()
    };
    for column in table.columns.iter_mut().flatten() {
        if let Some(generated) = &column.generated
            && generated.expression.as_deref().is_some_and(&mut calls)
        {
            return Err(format!(
                "its generated column {} calls the function, and deploy \
                 cannot drop and make it again",
                column.name
            ));
        }
        if column.default.as_ref().is_some_and(|default| {
            calls(&crate::build::render_default(default))
        }) {
            column.default = None;
            drops.push((
                None,
                part_drop(
                    format!("DEFAULT {tag} {}", column.name),
                    format!(
                        "ALTER TABLE ONLY {name} ALTER COLUMN {} DROP \
                         DEFAULT;\n",
                        quote_ident(&column.name)
                    ),
                    default_again(&column.name),
                ),
                default_again(&column.name),
            ));
        }
    }
    if let Some(defaults) = &mut table.column_defaults {
        defaults.retain(|default| {
            if !calls(&crate::build::render_default(&default.default)) {
                return true;
            }
            drops.push((
                None,
                part_drop(
                    format!("DEFAULT {tag} {}", default.column),
                    format!(
                        "ALTER TABLE ONLY {name} ALTER COLUMN {} DROP \
                         DEFAULT;\n",
                        quote_ident(&default.column)
                    ),
                    default_again(&default.column),
                ),
                default_again(&default.column),
            ));
            false
        });
    }
    if let Some(checks) = &mut table.check_constraints {
        checks.retain(|check| {
            if !calls(&check.expression) {
                return true;
            }
            let again = has(&wanted.check_constraints, |wanted| {
                wanted.name == check.name
            });
            drops.push((
                None,
                part_drop(
                    format!("CHECK CONSTRAINT {tag} {}", check.name),
                    format!(
                        "ALTER TABLE {name} DROP CONSTRAINT {};\n",
                        quote_ident(&check.name)
                    ),
                    again,
                ),
                again,
            ));
            false
        });
    }
    all_found(functions, &found)
}

/// [`inline_table`] for the default and the checks of a domain. A
/// check with no name has the name that PostgreSQL gives it (see
/// [`Domain::with_check_names`])
fn inline_domain<'a>(
    parser: &mut tree_sitter::Parser,
    domain: &mut Domain,
    wanted: &Domain,
    functions: &[(i32, String, String)],
    drops: &mut Vec<Drop<'a>>,
) -> Result<(), String> {
    *domain = domain.with_check_names();
    let wanted = &wanted.with_check_names();
    let name = format!(
        "{}.{}",
        quote_ident(&domain.schema),
        quote_ident(&domain.name)
    );
    let tag = format!("{}.{}", domain.schema, domain.name);
    let mut found = BTreeSet::new();
    let mut calls = |expression: &str| {
        let called = called(parser, expression, functions);
        found.extend(called.iter().copied());
        !called.is_empty()
    };
    if domain.default.as_deref().is_some_and(&mut calls) {
        domain.default = None;
        let again = wanted.default.is_some();
        drops.push((
            None,
            part_drop(
                format!("DEFAULT {tag}"),
                format!("ALTER DOMAIN {name} DROP DEFAULT;\n"),
                again,
            ),
            again,
        ));
    }
    if let Some(checks) = &mut domain.check_constraints {
        checks.retain(|check| {
            if !check.expression.as_deref().is_some_and(&mut calls) {
                return true;
            }
            let Some(check_name) = &check.name else {
                return true;
            };
            let again = has(&wanted.check_constraints, |wanted| {
                wanted.name.as_ref() == Some(check_name)
            });
            drops.push((
                None,
                part_drop(
                    format!("CHECK CONSTRAINT {tag} {check_name}"),
                    format!(
                        "ALTER DOMAIN {name} DROP CONSTRAINT {};\n",
                        quote_ident(check_name)
                    ),
                    again,
                ),
                again,
            ));
            false
        });
    }
    all_found(functions, &found)
}

/// Each function of `functions` is in `found`
fn all_found(
    functions: &[(i32, String, String)],
    found: &BTreeSet<i32>,
) -> Result<(), String> {
    match functions.iter().find(|(id, ..)| !found.contains(id)) {
        Some((_, schema, name)) => Err(format!(
            "it depends on function {schema}.{name}, but no default or \
             check of it calls the function, and deploy cannot drop and \
             make again the part that uses it"
        )),
        None => Ok(()),
    }
}

/// Remove from `definition` the part that `entry` makes, with the
/// statement that drops it. `wanted` is the project's table or domain
fn entry_part<'a>(
    (relation, entry): (&libpgdump::Entry, &'a libpgdump::Entry),
    definition: &mut Definition,
    wanted: &Definition,
    allow_drop_indexes: bool,
    drops: &mut Vec<Drop<'a>>,
) -> Result<(), String> {
    let label = entry_label(entry);
    let tag = entry.tag.as_deref().unwrap_or_default();
    // the tag of a part other than an index starts with the name of
    // its relation
    let name = match entry.desc {
        OT::Index => Some(tag),
        _ => relation
            .tag
            .as_deref()
            .and_then(|relation| tag.strip_prefix(&format!("{relation} "))),
    };
    // true when the project has the part, thus the plan makes it again
    let again =
        match (&entry.desc, wanted, name) {
            (OT::Index, Definition::Table(table), Some(name)) => {
                has(&table.indexes, |index| index.name == name)
            }
            (OT::Trigger, Definition::Table(table), Some(name)) => {
                has(&table.triggers, |trigger| {
                    trigger.name.as_deref() == Some(name)
                })
            }
            (OT::Policy, Definition::Table(table), Some(name)) => {
                has(&table.policies, |policy| policy.name == name)
            }
            (OT::CheckConstraint, Definition::Table(table), Some(name)) => {
                has(&table.check_constraints, |check| check.name == name)
            }
            (OT::CheckConstraint, Definition::Domain(domain), Some(name)) => {
                has(&domain.check_constraints, |check| {
                    check.name.as_deref() == Some(name)
                })
            }
            (OT::Default, Definition::Table(table), Some(name)) => {
                table.columns.iter().flatten().any(|column| {
                    column.name == name && column.default.is_some()
                }) || has(&table.column_defaults, |default| {
                    default.column == name
                })
            }
            _ => false,
        };
    // the project does not keep a statement after the CREATE: the
    // enabled state of a trigger (ALTER TABLE ... DISABLE TRIGGER), or
    // ALTER TABLE ... CLUSTER ON and SET STATISTICS of an index
    if again
        && entry
            .defn
            .as_deref()
            .is_some_and(|defn| crate::ddl::split_statements(defn).len() > 1)
    {
        return Err(format!(
            "{label} has statements after its CREATE that the project \
             does not keep (for example, ALTER TABLE ... DISABLE \
             TRIGGER), and deploy would lose them"
        ));
    }
    // a part that the project does not have is dropped as the table
    // would drop it. Deploy keeps an index that only the database has
    // without --allow-drop-indexes, and then the function's drop fails
    if !again && entry.desc == OT::Index && !allow_drop_indexes {
        return Err(format!(
            "the project does not have {label}, and deploy keeps it \
             without --allow-drop-indexes"
        ));
    }
    let mut restrictive = false;
    let mut own_drop = None;
    let removed = match (&entry.desc, definition, name) {
        (OT::Index, Definition::Table(table), Some(name)) => {
            remove(&mut table.indexes, |index| index.name == name)
        }
        (OT::Trigger, Definition::Table(table), Some(name)) => {
            remove(&mut table.triggers, |trigger| {
                trigger.name.as_deref() == Some(name)
            })
        }
        (OT::Policy, Definition::Table(table), Some(name)) => {
            restrictive = has(&table.policies, |policy| {
                policy.name == name && policy.restrictive == Some(true)
            });
            remove(&mut table.policies, |policy| policy.name == name)
        }
        (OT::CheckConstraint, Definition::Table(table), Some(name)) => {
            remove(&mut table.check_constraints, |check| check.name == name)
        }
        (OT::CheckConstraint, Definition::Domain(domain), Some(name)) => {
            remove(&mut domain.check_constraints, |check| {
                check.name.as_deref() == Some(name)
            })
        }
        (OT::Default, Definition::Table(table), Some(name)) => {
            // pg_dump drops the default without ONLY, which drops the
            // default of each inheritance child and partition too
            own_drop = Some(format!(
                "ALTER TABLE ONLY {}.{} ALTER COLUMN {} DROP DEFAULT;\n",
                quote_ident(&table.schema),
                quote_ident(&table.name),
                quote_ident(name)
            ));
            let column = table.columns.iter_mut().flatten().find(|column| {
                column.name == name && column.default.is_some()
            });
            match column {
                Some(column) => {
                    column.default = None;
                    true
                }
                None => remove(&mut table.column_defaults, |default| {
                    default.column == name
                }),
            }
        }
        _ => false,
    };
    let drop = match (&entry.drop_stmt, removed) {
        (Some(drop), true) => own_drop.unwrap_or_else(|| drop.clone()),
        _ => return Err(format!("deploy cannot drop and make again {label}")),
    };
    let drop = match entry.desc {
        // the gates of the table's drop of a part that the project
        // does not have
        OT::Index if !again => {
            alter::Alter::index_removal(drop).labeled(&label)
        }
        OT::Policy if !again && restrictive => {
            alter::Alter::destructive(drop).labeled(&label)
        }
        _ => part_drop(label, drop, again),
    };
    drops.push((Some(entry), drop, again));
    Ok(())
}

/// `set` with ALTER TABLE ONLY, when it sets the column default that
/// `drop` (an ALTER TABLE ONLY ... DROP DEFAULT) drops
fn only_default(drop: &str, set: &str) -> Option<String> {
    let target = drop
        .strip_prefix("ALTER TABLE ONLY ")?
        .strip_suffix(" DROP DEFAULT;\n")?;
    let expression = set
        .strip_prefix("ALTER TABLE ")?
        .strip_prefix(target)?
        .strip_prefix(" SET DEFAULT ")?;
    Some(format!(
        "ALTER TABLE ONLY {target} SET DEFAULT {expression}"
    ))
}

/// True when `list` has an item that `matches` finds
fn has<T>(list: &Option<Vec<T>>, matches: impl Fn(&T) -> bool) -> bool {
    list.iter().flatten().any(matches)
}

/// Remove the items that `matches` finds; true when there was one
fn remove<T>(list: &mut Option<Vec<T>>, matches: impl Fn(&T) -> bool) -> bool {
    let Some(list) = list else {
        return false;
    };
    let before = list.len();
    list.retain(|item| !matches(item));
    list.len() != before
}

/// The ids of the functions of `functions` that `expression` calls
fn called(
    parser: &mut tree_sitter::Parser,
    expression: &str,
    functions: &[(i32, String, String)],
) -> Vec<i32> {
    let mut by_name: HashMap<(String, String), Vec<usize>> = HashMap::new();
    for (index, (_, schema, name)) in functions.iter().enumerate() {
        by_name
            .entry((schema.clone(), name.clone()))
            .or_default()
            .push(index);
    }
    crate::build::called_functions(parser, expression, &by_name)
        .into_iter()
        .map(|index| functions[index].0)
        .collect()
}

/// The schema and name, without the argument types, of a function
/// entry
fn function_name(entry: &libpgdump::Entry) -> Option<(String, String)> {
    let tag = entry.tag.as_deref()?;
    let name =
        crate::utils::split_signature(tag).map_or(tag, |(name, _)| name);
    Some((entry.namespace.clone()?, name.trim_end().to_string()))
}

/// The table, view, materialized view or domain entry that `entry` is
/// a part of
fn relation<'a>(
    entry: &libpgdump::Entry,
    by_id: &HashMap<i32, &'a libpgdump::Entry>,
) -> Option<&'a libpgdump::Entry> {
    entry
        .dependencies
        .iter()
        .filter_map(|id| by_id.get(id).copied())
        .find(|owner| {
            matches!(
                owner.desc,
                OT::Table
                    | OT::View
                    | OT::MaterializedView
                    | OT::ForeignTable
                    | OT::Domain
            )
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::constants::ObjectType;
    use crate::models::Item;

    fn item(id: usize, desc: ObjectType, json: serde_json::Value) -> Item {
        let definition = match desc {
            ObjectType::Function => {
                Definition::Function(serde_json::from_value(json).unwrap())
            }
            ObjectType::View => {
                Definition::View(serde_json::from_value(json).unwrap())
            }
            ObjectType::MaterializedView => Definition::MaterializedView(
                serde_json::from_value(json).unwrap(),
            ),
            ObjectType::Table => {
                Definition::Table(serde_json::from_value(json).unwrap())
            }
            ObjectType::Domain => {
                Definition::Domain(serde_json::from_value(json).unwrap())
            }
            _ => unreachable!("not used by the tests"),
        };
        Item {
            id,
            desc,
            definition,
            dependencies: BTreeSet::new(),
        }
    }

    fn function(returns: &str) -> serde_json::Value {
        serde_json::json!({
            "name": "f",
            "schema": "test",
            "owner": "postgres",
            "parameters": [
                {"mode": "IN", "name": "a", "data_type": "integer"},
            ],
            "returns": returns,
            "language": "sql",
            "definition": "SELECT a;",
        })
    }

    fn view(name: &str, query: &str) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            "schema": "test",
            "owner": "postgres",
            "query": query,
        })
    }

    fn table(default: &str) -> serde_json::Value {
        serde_json::json!({
            "name": "t",
            "schema": "test",
            "owner": "postgres",
            "columns": [
                {"name": "id", "data_type": "integer", "default": default},
            ],
            "indexes": [
                {"name": "i", "columns": [{"expression": "test.f(id)"}]},
            ],
        })
    }

    fn project(inventory: Vec<Item>) -> crate::project::Project {
        crate::project::Project {
            name: "test".to_string(),
            superuser: "postgres".to_string(),
            default_schema: "public".to_string(),
            path: std::path::PathBuf::new(),
            settings: Default::default(),
            inventory,
        }
    }

    fn entry(
        dump: &mut libpgdump::Dump,
        desc: OT,
        tag: &str,
        drop: Option<&str>,
        dependencies: &[i32],
    ) -> i32 {
        entry_with(dump, desc, tag, "CREATE ...;\n", drop, dependencies)
    }

    /// [`entry`] with the statements of the entry
    fn entry_with(
        dump: &mut libpgdump::Dump,
        desc: OT,
        tag: &str,
        defn: &str,
        drop: Option<&str>,
        dependencies: &[i32],
    ) -> i32 {
        dump.add_entry(
            desc,
            Some("test"),
            Some(tag),
            Some("postgres"),
            Some(defn),
            drop,
            None,
            dependencies,
        )
        .expect("add entry")
    }

    /// The function `f` changes its return type, thus the plan drops
    /// it; the diff has each project item unchanged but `f`
    fn diff(project: &crate::project::Project) -> Diff {
        let mut diff = Diff {
            items: BTreeMap::new(),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
        };
        for item in &project.inventory {
            diff.items.insert(item.id, Change::Unchanged);
        }
        diff.items.insert(0, Change::Changed);
        diff.changed.insert(
            0,
            Definition::Function(
                serde_json::from_value(function("bigint")).unwrap(),
            ),
        );
        diff
    }

    fn run(
        project: &crate::project::Project,
        diff: &mut Diff,
        snapshot: &libpgdump::Dump,
    ) -> (Dependents, BTreeMap<usize, Resolution>) {
        run_with(project, diff, snapshot, false)
    }

    /// [`run`] with `--allow-drop-indexes` or without it
    fn run_with(
        project: &crate::project::Project,
        diff: &mut Diff,
        snapshot: &libpgdump::Dump,
        allow_drop_indexes: bool,
    ) -> (Dependents, BTreeMap<usize, Resolution>) {
        let mut resolutions = BTreeMap::new();
        resolutions.insert(0, Resolution::Replace);
        let dependents = rebuild(
            project,
            diff,
            &mut resolutions,
            snapshot,
            &alter::IndexGroups::new(),
            &alter::operator_class::Families::new(),
            allow_drop_indexes,
        );
        (dependents, resolutions)
    }

    /// A view, a view on that view, a column default and an index
    /// expression that call the function are dropped and made again;
    /// the comment of the view comes with the view. The default is
    /// dropped and set with ALTER TABLE ONLY, which does not change the
    /// default of an inheritance child or a partition
    #[test]
    fn dependents_are_dropped_and_made_again() {
        let project = project(vec![
            item(0, ObjectType::Function, function("integer")),
            item(1, ObjectType::View, view("v", " SELECT test.f(1) AS n")),
            item(2, ObjectType::View, view("v2", " SELECT n FROM test.v")),
            item(3, ObjectType::Table, table("test.f(2)")),
        ]);
        let mut diff = diff(&project);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let f = entry(&mut snapshot, OT::Function, "f(integer)", None, &[]);
        let v = entry(
            &mut snapshot,
            OT::View,
            "v",
            Some("DROP VIEW test.v;\n"),
            &[f],
        );
        entry(&mut snapshot, OT::Comment, "VIEW v", None, &[v]);
        let v2 = entry(
            &mut snapshot,
            OT::View,
            "v2",
            Some("DROP VIEW test.v2;\n"),
            &[v],
        );
        let t = entry(
            &mut snapshot,
            OT::Table,
            "t",
            Some("DROP TABLE test.t;\n"),
            &[f, f],
        );
        let i = entry(
            &mut snapshot,
            OT::Index,
            "i",
            Some("DROP INDEX test.i;\n"),
            &[t, f],
        );
        let (dependents, resolutions) = run(&project, &mut diff, &snapshot);
        assert!(dependents.refused.is_empty(), "{:?}", dependents.refused);
        assert_eq!(
            dependents.labels,
            vec![
                "DEFAULT test.t id",
                "INDEX test.i",
                "VIEW test.v",
                "VIEW test.v2",
            ]
        );
        let drop = |id: i32| -> Vec<String> {
            dependents.drops[&id]
                .iter()
                .map(|alter| {
                    assert!(alter.destructive);
                    alter.sql.clone()
                })
                .collect()
        };
        assert_eq!(drop(v), vec!["DROP VIEW test.v;\n"]);
        assert_eq!(drop(v2), vec!["DROP VIEW test.v2;\n"]);
        assert_eq!(drop(i), vec!["DROP INDEX test.i;\n"]);
        assert_eq!(
            drop(t),
            vec!["ALTER TABLE ONLY test.t ALTER COLUMN id DROP DEFAULT;\n"]
        );
        for id in [1, 2] {
            assert_eq!(diff.items[&id], Change::Changed);
            assert!(matches!(
                &resolutions[&id],
                Resolution::Rebuild { before, drop }
                    if before.is_empty() && drop.is_empty()
            ));
        }
        // the table makes the default and the index again in place,
        // gated with their drops
        let Resolution::Statements(alters) = &resolutions[&3] else {
            panic!("the table changes in place");
        };
        let statements: Vec<(&str, bool)> = alters
            .iter()
            .map(|alter| (alter.sql.as_str(), alter.destructive))
            .collect();
        assert_eq!(
            statements,
            vec![
                (
                    "ALTER TABLE ONLY test.t ALTER COLUMN id SET DEFAULT \
                     test.f(2);\n",
                    true
                ),
                (
                    "CREATE INDEX i ON test.t USING btree ( (test.f(id)) \
                     );\n",
                    true
                ),
            ]
        );
    }

    /// An index that calls the function, on a materialized view that
    /// depends on the function only through a view, is made again by
    /// the rebuild of the materialized view. The traversal finds the
    /// index before the materialized view
    #[test]
    fn a_part_of_a_later_rebuilt_relation_is_not_refused() {
        let project = project(vec![
            item(0, ObjectType::Function, function("integer")),
            item(1, ObjectType::View, view("v", " SELECT test.f(1) AS n")),
            item(
                2,
                ObjectType::MaterializedView,
                view("m", " SELECT n FROM test.v"),
            ),
        ]);
        let mut diff = diff(&project);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let f = entry(&mut snapshot, OT::Function, "f(integer)", None, &[]);
        let v = entry(
            &mut snapshot,
            OT::View,
            "v",
            Some("DROP VIEW test.v;\n"),
            &[f],
        );
        let m = entry(
            &mut snapshot,
            OT::MaterializedView,
            "m",
            Some("DROP MATERIALIZED VIEW test.m;\n"),
            &[v],
        );
        let i = entry(
            &mut snapshot,
            OT::Index,
            "i",
            Some("DROP INDEX test.i;\n"),
            &[m, f],
        );
        let (dependents, _) = run(&project, &mut diff, &snapshot);
        assert!(dependents.refused.is_empty(), "{:?}", dependents.refused);
        assert_eq!(
            dependents.labels,
            vec!["MATERIALIZED VIEW test.m", "VIEW test.v"]
        );
        assert!(!dependents.drops.contains_key(&i));
    }

    /// A dependent that the project does not have, and a table that
    /// depends on the function by no default or check, cannot be made
    /// again. A dependent that the project removes is dropped as a
    /// removed object, not as a dependent
    #[test]
    fn dependents_that_cannot_be_made_again_are_refused() {
        let project = project(vec![
            item(0, ObjectType::Function, function("integer")),
            item(3, ObjectType::Table, table("0")),
        ]);
        let mut diff = diff(&project);
        let removed: crate::models::View =
            serde_json::from_value(view("gone", " SELECT test.f(3) AS n"))
                .unwrap();
        diff.removed.insert(
            ObjectKey::new(
                ObjectType::View,
                &Definition::View(removed.clone()),
            ),
            Definition::View(removed),
        );
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let f = entry(&mut snapshot, OT::Function, "f(integer)", None, &[]);
        entry(
            &mut snapshot,
            OT::View,
            "w",
            Some("DROP VIEW test.w;\n"),
            &[f],
        );
        entry(
            &mut snapshot,
            OT::View,
            "gone",
            Some("DROP VIEW test.gone;\n"),
            &[f],
        );
        entry(&mut snapshot, OT::Table, "t", None, &[f]);
        let (dependents, resolutions) = run(&project, &mut diff, &snapshot);
        assert_eq!(
            dependents.refused,
            vec![
                "VIEW test.w: the project does not have it",
                "TABLE test.t: it depends on function test.f, but no \
                 default or check of it calls the function, and deploy \
                 cannot drop and make again the part that uses it",
            ]
        );
        assert!(dependents.drops.is_empty());
        assert!(dependents.labels.is_empty());
        assert!(!resolutions.contains_key(&3));
    }

    /// A disabled trigger and a clustered index: the project does not
    /// keep the statements after the CREATE, thus a DROP and a CREATE
    /// would lose them
    #[test]
    fn a_part_with_more_statements_is_refused() {
        let mut json = table("0");
        json["triggers"] = serde_json::json!([{
            "name": "tg",
            "when": "BEFORE",
            "events": ["UPDATE"],
            "for_each": "ROW",
            "condition": "(test.f(new.id) > 0)",
            "function": "test.tf()",
        }]);
        let project = project(vec![
            item(0, ObjectType::Function, function("integer")),
            item(3, ObjectType::Table, json),
        ]);
        for (desc, tag, defn, drop, label) in [
            (
                OT::Trigger,
                "t tg",
                "CREATE TRIGGER tg BEFORE UPDATE ON test.t FOR EACH ROW \
                 WHEN ((test.f(new.id) > 0)) EXECUTE FUNCTION \
                 test.tf();\n\nALTER TABLE test.t DISABLE TRIGGER tg;\n",
                "DROP TRIGGER tg ON test.t;\n",
                "TRIGGER test.t tg",
            ),
            (
                OT::Index,
                "i",
                "CREATE INDEX i ON test.t USING btree (test.f(id));\n\n\
                 ALTER TABLE test.t CLUSTER ON i;\n",
                "DROP INDEX test.i;\n",
                "INDEX test.i",
            ),
        ] {
            let mut snapshot =
                libpgdump::new("test", "UTF8", "18.0").expect("new dump");
            let f =
                entry(&mut snapshot, OT::Function, "f(integer)", None, &[]);
            let t = entry(&mut snapshot, OT::Table, "t", None, &[]);
            entry_with(&mut snapshot, desc, tag, defn, Some(drop), &[t, f]);
            let (dependents, resolutions) =
                run(&project, &mut diff(&project), &snapshot);
            assert_eq!(
                dependents.refused,
                vec![format!(
                    "TABLE test.t: {label} has statements after its CREATE \
                     that the project does not keep (for example, ALTER \
                     TABLE ... DISABLE TRIGGER), and deploy would lose them"
                )]
            );
            assert!(dependents.drops.is_empty());
            assert!(dependents.labels.is_empty());
            assert!(!resolutions.contains_key(&3));
        }
    }

    /// An index that calls the function and that the project does not
    /// have is dropped as the table drops an index that only the
    /// database has: only with --allow-drop-indexes, and not as a
    /// dependent that the plan makes again
    #[test]
    fn a_part_that_the_project_removes_is_only_dropped() {
        let mut json = table("test.f(2)");
        json["indexes"] = serde_json::json!([]);
        let project = project(vec![
            item(0, ObjectType::Function, function("integer")),
            item(3, ObjectType::Table, json),
        ]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let f = entry(&mut snapshot, OT::Function, "f(integer)", None, &[]);
        let t = entry(&mut snapshot, OT::Table, "t", None, &[f]);
        let i = entry(
            &mut snapshot,
            OT::Index,
            "i",
            Some("DROP INDEX test.i;\n"),
            &[t, f],
        );
        // the database has the index
        let database = || {
            let mut database = diff(&project);
            database.items.insert(3, Change::Changed);
            database.changed.insert(
                3,
                Definition::Table(
                    serde_json::from_value(table("test.f(2)")).unwrap(),
                ),
            );
            database
        };
        let (dependents, resolutions) =
            run_with(&project, &mut database(), &snapshot, false);
        assert_eq!(
            dependents.refused,
            vec![
                "TABLE test.t: the project does not have INDEX test.i, and \
                 deploy keeps it without --allow-drop-indexes"
            ]
        );
        assert!(dependents.drops.is_empty());
        assert!(!resolutions.contains_key(&3));
        let (dependents, resolutions) =
            run_with(&project, &mut database(), &snapshot, true);
        assert!(dependents.refused.is_empty(), "{:?}", dependents.refused);
        assert_eq!(dependents.labels, vec!["DEFAULT test.t id"]);
        let drop = &dependents.drops[&i];
        assert_eq!(drop.len(), 1);
        assert_eq!(drop[0].sql, "DROP INDEX test.i;\n");
        assert!(drop[0].index_removal && !drop[0].destructive);
        // the table does not drop the index again
        let Resolution::Statements(alters) = &resolutions[&3] else {
            panic!("the table changes in place");
        };
        assert!(alters.iter().all(|alter| !alter.sql.contains("INDEX")));
    }

    /// A domain CHECK with no name that calls the function has the
    /// name that PostgreSQL gave it: the check is dropped and added
    /// again with that name, also when the domain is not changed
    #[test]
    fn an_unnamed_domain_check_is_made_again() {
        let project = project(vec![
            item(0, ObjectType::Function, function("integer")),
            item(
                4,
                ObjectType::Domain,
                serde_json::json!({
                    "name": "d",
                    "schema": "test",
                    "owner": "postgres",
                    "data_type": "integer",
                    "check_constraints": [
                        {"expression": "test.f(VALUE) > 0"},
                    ],
                }),
            ),
        ]);
        let mut diff = diff(&project);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let f = entry(&mut snapshot, OT::Function, "f(integer)", None, &[]);
        let d = entry(&mut snapshot, OT::Domain, "d", None, &[f]);
        let (dependents, resolutions) = run(&project, &mut diff, &snapshot);
        assert!(dependents.refused.is_empty(), "{:?}", dependents.refused);
        let drops: Vec<&str> = dependents.drops[&d]
            .iter()
            .map(|alter| alter.sql.as_str())
            .collect();
        assert_eq!(
            drops,
            vec!["ALTER DOMAIN test.d DROP CONSTRAINT d_check;\n"]
        );
        let Resolution::Statements(alters) = &resolutions[&4] else {
            panic!("the domain changes in place");
        };
        let alters: Vec<&str> =
            alters.iter().map(|alter| alter.sql.as_str()).collect();
        assert_eq!(
            alters,
            vec![
                "ALTER DOMAIN test.d ADD CONSTRAINT d_check CHECK \
                 ((test.f(VALUE) > 0));\n"
            ]
        );
    }
}
