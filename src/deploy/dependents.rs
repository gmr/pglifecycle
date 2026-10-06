//! The objects that depend on an object that the plan drops: an object
//! that the plan drops and makes again (a function change that CREATE
//! OR REPLACE FUNCTION cannot do, a view whose columns change, a
//! materialized view, an aggregate, an operator, a type, a domain or a
//! table that deploy cannot change in place), or a function that only
//! the database has. PostgreSQL refuses to drop an object while another
//! object depends on it, thus the plan drops each dependent first and
//! makes it again from the project after the object. A replaced object
//! with dependents is dropped with them, before the objects that the
//! plan makes, and made again at its position.
//!
//! The snapshot's dependency edges give the dependents, transitively.
//! A whole object (a view, for example) is dropped and made again, and
//! the objects that depend on it are dependents too. A part of a table
//! or domain (a column default, a check, an index, a trigger, a
//! policy) is dropped, and the table or domain makes it again in
//! place. The parts of a relation that the plan drops (its indexes and
//! constraints, for example) come back with the relation. PostgreSQL
//! drops the partitions of a table, and the sequences that its columns
//! own, with the table. Each of these statements is destructive,
//! thus without `--allow-drop` all of them are withheld with the drop
//! of the object.
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

/// The types of the objects that the plan drops and makes again (a
/// `Resolution::Replace`) and that other objects can depend on
const ROOTS: [OT; 8] = [
    OT::Function,
    OT::View,
    OT::MaterializedView,
    OT::Aggregate,
    OT::Operator,
    OT::Type,
    OT::Domain,
    OT::Table,
];

/// The types that are dropped and made again from the project
const WHOLE: [OT; 8] = [
    OT::View,
    OT::MaterializedView,
    OT::Function,
    OT::Procedure,
    OT::Aggregate,
    OT::Operator,
    OT::Cast,
    OT::Statistics,
];

/// The entries that are a part of a relation, with an entry of their
/// own, that the relation makes again in place
const PARTS: [OT; 5] = [
    OT::Index,
    OT::Trigger,
    OT::Policy,
    OT::CheckConstraint,
    OT::Default,
];

/// The entries that are a part of a relation, with an entry of their
/// own. The rebuild of the relation makes them again, or PostgreSQL
/// drops them with the relation
const CHILDREN: [OT; 11] = [
    OT::Index,
    OT::Trigger,
    OT::Policy,
    OT::CheckConstraint,
    OT::Default,
    OT::Constraint,
    OT::FkConstraint,
    OT::Rule,
    OT::IndexAttach,
    OT::TableAttach,
    OT::PublicationTable,
];

/// A part of a table or domain to drop
enum Part<'a> {
    /// The defaults and checks of the entry itself that call these
    /// functions, as (dump id, schema, name)
    Inline(Vec<(i32, String, String)>),
    /// A part with an entry of its own
    Entry(&'a libpgdump::Entry),
}

/// Find the dependents of each object that the plan drops, and change
/// `diff` and `resolutions` so that the plan drops them and makes them
/// again. A replaced object with dependents is dropped with them,
/// before all creates, and made again at its position
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
    let position: HashMap<i32, usize> = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.dump_id, index))
        .collect();
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
    // the objects that the plan drops: a replaced one, or a function
    // that only the database has
    let roots: BTreeSet<i32> = entries
        .iter()
        .filter(|entry| ROOTS.contains(&entry.desc))
        .filter(|entry| {
            // the key of a removed function can be the key of a new one
            // with other argument names
            (entry.desc == OT::Function && is_removed(entry))
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
    let mut closure = roots.clone();
    // the root that each entry of the closure comes from
    let mut origin: HashMap<i32, i32> =
        roots.iter().map(|id| (*id, *id)).collect();
    // the object of the closure that each entry of the closure is
    // dropped with: itself, or the relation of a part, or the table of
    // a partition or of a sequence that a column owns
    let mut group: HashMap<i32, i32> =
        roots.iter().map(|id| (*id, *id)).collect();
    // the roots that have dependents, thus are dropped with them
    let mut early: BTreeSet<i32> = BTreeSet::new();
    // each part of a relation of the closure, with the relation
    let mut covered: HashMap<i32, i32> = HashMap::new();
    let mut pending: Vec<i32> = closure.iter().copied().collect();
    let mut whole: Vec<(&libpgdump::Entry, usize)> = Vec::new();
    let mut parts: BTreeMap<i32, Vec<Part>> = BTreeMap::new();
    let mut later: Vec<(&libpgdump::Entry, i32)> = Vec::new();
    let mut seen: BTreeSet<i32> = BTreeSet::new();
    while let Some(id) = pending.pop() {
        let source = by_id[&id];
        let root = origin[&id];
        for entry in entries {
            // the plan drops a removed object with the other removed
            // objects, in reverse snapshot order
            if !entry.dependencies.contains(&id) || is_removed(entry) {
                continue;
            }
            // their object makes them again
            if matches!(
                entry.desc,
                OT::Comment
                    | OT::Acl
                    | OT::SecurityLabel
                    | OT::SequenceOwnedBy
                    | OT::SequenceSet
                    | OT::StatisticsData
            ) {
                continue;
            }
            let owner = relation(entry, &by_id);
            // a part of a relation that the source is dropped with, or
            // an object that PostgreSQL drops with the source table and
            // that the table makes again: a partition that is not an
            // item of its own, or the sequence of an identity column
            let own = (CHILDREN.contains(&entry.desc)
                && owner.is_some_and(|owner| {
                    group.contains_key(&owner.dump_id)
                        && group.get(&owner.dump_id) == group.get(&id)
                }))
                || (source.desc == OT::Table
                    && ((entry.desc == OT::Sequence
                        && !owned_by(entry, entries))
                        || (partition(entry, source, entries)
                            && item(entry).is_none())));
            if !own {
                early.insert(root);
            }
            if closure.contains(&entry.dump_id) {
                // an object that depends on two roots, or a root that
                // depends on another root
                if !own {
                    early.insert(origin[&entry.dump_id]);
                }
                continue;
            }
            if own && !CHILDREN.contains(&entry.desc) {
                closure.insert(entry.dump_id);
                origin.insert(entry.dump_id, root);
                group.insert(entry.dump_id, group[&id]);
                pending.push(entry.dump_id);
                continue;
            }
            let reason = match entry.desc {
                // PostgreSQL drops a partition with its table, and the
                // plan does not make again a partition that is an item
                // of its own
                OT::Table if partition(entry, source, entries) => {
                    Some(format!(
                        "it is a partition of {}, and PostgreSQL drops it \
                         with the table",
                        entry_label(source)
                    ))
                }
                // a sequence that a column of the table owns (serial, or
                // OWNED BY)
                OT::Sequence if source.desc == OT::Table => Some(format!(
                    "a column of {} owns it, thus PostgreSQL drops it with \
                     the table, and deploy cannot make it again with its \
                     value",
                    entry_label(source)
                )),
                // the plan attaches a partition that is an item of its
                // own again only when it makes the partition and its
                // table again
                OT::TableAttach
                    if attached(entry, &by_id).is_some_and(|tables| {
                        item(tables[0]).is_some()
                            && !tables
                                .iter()
                                .all(|table| roots.contains(&table.dump_id))
                    }) =>
                {
                    Some(String::from(
                        "deploy would not attach the partition again",
                    ))
                }
                OT::PublicationTable => Some(String::from(
                    "deploy would remove the table from the publication and \
                     not add it again",
                )),
                _ => None,
            };
            if let Some(reason) = reason {
                if seen.insert(entry.dump_id) {
                    dependents
                        .refused
                        .push(format!("{}: {reason}", entry_label(entry)));
                }
                continue;
            }
            match entry.desc {
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
                            origin.insert(entry.dump_id, root);
                            group.insert(entry.dump_id, entry.dump_id);
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
                _ if CHILDREN.contains(&entry.desc) => match owner {
                    // the rebuild of its relation makes it again; what
                    // depends on it depends on the relation
                    Some(owner) if closure.contains(&owner.dump_id) => {
                        if origin[&owner.dump_id] != root {
                            early.insert(origin[&owner.dump_id]);
                        }
                        closure.insert(entry.dump_id);
                        origin.insert(entry.dump_id, origin[&owner.dump_id]);
                        group.insert(entry.dump_id, group[&owner.dump_id]);
                        covered.insert(entry.dump_id, owner.dump_id);
                        pending.push(entry.dump_id);
                    }
                    Some(owner)
                        if PARTS.contains(&entry.desc)
                            && matches!(
                                owner.desc,
                                OT::Table | OT::Domain
                            ) =>
                    {
                        if seen.insert(entry.dump_id) {
                            parts
                                .entry(owner.dump_id)
                                .or_default()
                                .push(Part::Entry(entry));
                        }
                    }
                    // the traversal can add its relation later
                    Some(owner)
                        if matches!(
                            owner.desc,
                            OT::View | OT::MaterializedView
                        ) =>
                    {
                        later.push((entry, owner.dump_id));
                    }
                    _ => {
                        if seen.insert(entry.dump_id) {
                            dependents.refused.push(format!(
                                "{}: deploy cannot drop and make it again",
                                entry_label(entry)
                            ));
                        }
                    }
                },
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
    let mut refused_later = BTreeSet::new();
    for (entry, owner) in later {
        if !closure.contains(&owner) {
            if refused_later.insert(entry.dump_id) {
                dependents.refused.push(format!(
                    "{}: deploy cannot drop and make it again",
                    entry_label(entry)
                ));
            }
        } else if !closure.contains(&entry.dump_id) {
            covered.insert(entry.dump_id, owner);
            group.insert(entry.dump_id, group[&owner]);
        }
    }
    // a part of a relation that depends on an object of the closure
    // that the plan drops before the relation is dropped on its own
    // first; the relation makes it again
    for (&id, &owner) in &covered {
        let entry = by_id[&id];
        let before = entry.dependencies.iter().any(|dependency| {
            *dependency != owner
                && closure.contains(dependency)
                && group.get(dependency) != group.get(&owner)
                && position.get(dependency) > position.get(&owner)
        });
        if let Some(drop) = entry.drop_stmt.clone().filter(|_| before)
            && !drop.is_empty()
        {
            dependents.drops.entry(id).or_default().push(
                alter::Alter::destructive(drop).labeled(&entry_label(entry)),
            );
        }
    }
    // the rebuild of a relation makes its parts again from the project
    let mut lost: Vec<(i32, i32)> = covered
        .iter()
        .map(|(&id, &owner)| (id, owner))
        .filter(|(id, _)| more_statements(by_id[id]))
        .collect();
    lost.sort();
    for (id, owner) in lost {
        dependents.refused.push(format!(
            "{}: {}",
            entry_label(by_id[&owner]),
            more_statements_reason(&entry_label(by_id[&id]))
        ));
    }
    // a relation that the plan drops is not changed in place
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
    // a replaced object with dependents is dropped after them, before
    // the objects that the plan makes, and made again at its position.
    // A function that only the database has is dropped as a removed
    // object
    for &root in &early {
        let entry = by_id[&root];
        let Some(id) = item(entry).filter(|id| {
            matches!(resolutions.get(id), Some(Resolution::Replace))
        }) else {
            continue;
        };
        let Some(drop) = entry.drop_stmt.clone() else {
            continue;
        };
        resolutions.insert(
            id,
            Resolution::Rebuild {
                before: Vec::new(),
                drop: String::new(),
            },
        );
        dependents.drops.entry(root).or_default().push(
            alter::Alter::destructive(drop).labeled(&entry_label(entry)),
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
    referenced_keys(
        project,
        diff,
        resolutions,
        entries,
        (&by_id, &item, &is_removed),
        &mut dependents,
    );
    dependents.labels.sort();
    dependents
}

/// The lookups of [`rebuild`]: the entries by dump id, the project item
/// of an entry, and true when the plan drops the object of an entry as
/// a removed object
type Lookups<'a, 'b> = (
    &'b HashMap<i32, &'a libpgdump::Entry>,
    &'b dyn Fn(&libpgdump::Entry) -> Option<usize>,
    &'b dyn Fn(&libpgdump::Entry) -> bool,
);

/// The foreign keys that reference a primary key or unique constraint
/// that a table drops and adds again in place (see
/// [`alter::Alter::drops_key`]). PostgreSQL refuses to drop the
/// constraint while a foreign key references it, thus the plan drops
/// each such foreign key first, and adds it again after the statements
/// of the table, gated with the drop. When the project changes or
/// removes the foreign key, or rebuilds its table, the plan cannot do
/// this, and deploy refuses it. An object of another type that depends
/// on the constraint is refused too.
fn referenced_keys(
    project: &crate::project::Project,
    diff: &Diff,
    resolutions: &mut BTreeMap<usize, Resolution>,
    entries: &[libpgdump::Entry],
    (by_id, item, is_removed): Lookups,
    dependents: &mut Dependents,
) {
    let definition = |id: usize| {
        project
            .inventory
            .iter()
            .find(|item| item.id == id)
            .map(|item| &item.definition)
    };
    let mut seen = BTreeSet::new();
    for table in entries.iter().filter(|entry| entry.desc == OT::Table) {
        let Some(id) = item(table)
            .filter(|id| diff.items.get(id) == Some(&Change::Changed))
        else {
            continue;
        };
        let Some(Resolution::Statements(alters)) = resolutions.get(&id) else {
            continue;
        };
        let names: Vec<String> = alters
            .iter()
            .filter_map(|alter| alter.drops_key.clone())
            .collect();
        let tag = table.tag.as_deref().unwrap_or_default();
        let mut again = Vec::new();
        for name in names {
            let Some(key) = entries.iter().find(|entry| {
                entry.desc == OT::Constraint
                    && entry.namespace == table.namespace
                    && entry.tag.as_deref() == Some(&format!("{tag} {name}"))
            }) else {
                continue;
            };
            for entry in entries {
                if !entry.dependencies.contains(&key.dump_id)
                    || matches!(entry.desc, OT::Comment | OT::SecurityLabel)
                    || !seen.insert(entry.dump_id)
                {
                    continue;
                }
                let label = entry_label(entry);
                if entry.desc != OT::FkConstraint {
                    dependents.refused.push(format!(
                        "{label}: it depends on {}, which deploy drops \
                         and adds again, and deploy cannot drop and make \
                         it again",
                        entry_label(key)
                    ));
                    continue;
                }
                let Some(owner) = relation(entry, by_id) else {
                    continue;
                };
                // the plan drops the table of the foreign key, and the
                // foreign key with it, before the table changes
                if is_removed(owner) {
                    continue;
                }
                match foreign_key(
                    diff,
                    resolutions,
                    (owner, entry),
                    item,
                    definition,
                ) {
                    Ok(sql) => {
                        if let Some(drop) = entry.drop_stmt.clone() {
                            dependents
                                .drops
                                .entry(entry.dump_id)
                                .or_default()
                                .push(
                                    alter::Alter::destructive(drop)
                                        .labeled(&label),
                                );
                        }
                        again.push(
                            alter::Alter::destructive(sql).labeled(&label),
                        );
                        dependents.labels.push(label);
                    }
                    Err(reason) => dependents.refused.push(format!(
                        "{label}: it references {}, which deploy drops \
                         and adds again, and {reason}",
                        entry_label(key)
                    )),
                }
            }
        }
        if let Some(Resolution::Statements(alters)) = resolutions.get_mut(&id)
        {
            alters.extend(again);
        }
    }
}

/// The statement that adds again the foreign key `entry` of the table
/// `owner`, when the project keeps the foreign key as the database has
/// it, and the plan changes its table in place or not at all
fn foreign_key<'a>(
    diff: &Diff,
    resolutions: &BTreeMap<usize, Resolution>,
    (owner, entry): (&libpgdump::Entry, &libpgdump::Entry),
    item: &dyn Fn(&libpgdump::Entry) -> Option<usize>,
    definition: impl Fn(usize) -> Option<&'a Definition>,
) -> Result<String, String> {
    let Some(id) = item(owner) else {
        return Err(String::from("the project does not have its table"));
    };
    if diff.items.get(&id) == Some(&Change::Changed)
        && !matches!(resolutions.get(&id), Some(Resolution::Statements(_)))
    {
        return Err(String::from("deploy drops and makes its table again"));
    }
    let name = owner.tag.as_deref().and_then(|table| {
        entry
            .tag
            .as_deref()
            .and_then(|tag| tag.strip_prefix(&format!("{table} ")))
    });
    let Some(Definition::Table(repo)) = definition(id) else {
        return Err(String::from("the project does not have its table"));
    };
    let database = match diff.changed.get(&id) {
        Some(Definition::Table(table)) => table,
        _ => repo,
    };
    let find = |table: &Table| {
        table
            .foreign_keys
            .iter()
            .flatten()
            .find(|fk| Some(fk.name.as_str()) == name)
            .cloned()
    };
    match (find(repo), find(database)) {
        (Some(wanted), Some(existing)) if wanted == existing => Ok(format!(
            "ALTER TABLE {}.{} ADD CONSTRAINT {} {};\n",
            quote_ident(&repo.schema),
            quote_ident(&repo.name),
            quote_ident(&wanted.name),
            crate::build::render_foreign_key(&wanted)
        )),
        _ => Err(String::from("the project changes or removes it")),
    }
}

/// True when a SEQUENCE OWNED BY entry links the sequence `entry` to a
/// column (serial, or OWNED BY). The sequence of an identity column
/// has no such entry
fn owned_by(entry: &libpgdump::Entry, entries: &[libpgdump::Entry]) -> bool {
    entries.iter().any(|link| {
        link.desc == OT::SequenceOwnedBy
            && link.dependencies.contains(&entry.dump_id)
    })
}

/// The partition and the table of a TABLE ATTACH entry, whose tag is
/// the name of the partition
fn attached<'a>(
    entry: &libpgdump::Entry,
    by_id: &HashMap<i32, &'a libpgdump::Entry>,
) -> Option<[&'a libpgdump::Entry; 2]> {
    let tables: Vec<&libpgdump::Entry> = entry
        .dependencies
        .iter()
        .filter_map(|id| by_id.get(id).copied())
        .filter(|table| table.desc == OT::Table)
        .collect();
    let partition = tables.iter().find(|table| {
        table.namespace == entry.namespace && table.tag == entry.tag
    })?;
    let parent = tables
        .iter()
        .find(|table| table.dump_id != partition.dump_id)?;
    Some([partition, parent])
}

/// True when `entry` is a partition of the table `parent`: PostgreSQL
/// drops it with the parent
fn partition(
    entry: &libpgdump::Entry,
    parent: &libpgdump::Entry,
    entries: &[libpgdump::Entry],
) -> bool {
    entry.desc == OT::Table
        && entries.iter().any(|attach| {
            attach.desc == OT::TableAttach
                && attach.dependencies.contains(&entry.dump_id)
                && attach.dependencies.contains(&parent.dump_id)
        })
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
    let resolution =
        match alter::resolve_with(repo, &database, groups, families) {
            // each statement that makes a part again is gated with the
            // drop of the part
            Resolution::Statements(mut alters) => {
                for alter in &mut alters {
                    if !old.contains(&alter.sql) {
                        alter.destructive = true;
                    }
                }
                Resolution::Statements(alters)
            }
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
    if again && more_statements(entry) {
        return Err(more_statements_reason(&label));
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

/// True when `entry` has statements after its CREATE that the project
/// does not keep: the enabled state of a trigger (ALTER TABLE ...
/// DISABLE TRIGGER), or ALTER TABLE ... CLUSTER ON and SET STATISTICS
/// of an index. The project keeps the enabled state of a rule and the
/// replica identity of a table
fn more_statements(entry: &libpgdump::Entry) -> bool {
    entry.desc != OT::Rule
        && entry.defn.as_deref().is_some_and(|defn| {
            crate::ddl::split_statements(defn).iter().skip(1).any(
                |statement| {
                    !statement.contains(" REPLICA IDENTITY USING INDEX ")
                },
            )
        })
}

/// The reason that deploy refuses to drop and make again the part
/// `label`, which has statements after its CREATE
fn more_statements_reason(label: &str) -> String {
    format!(
        "{label} has statements after its CREATE that the project does \
         not keep (for example, ALTER TABLE ... DISABLE TRIGGER), and \
         deploy would lose them"
    )
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
/// a part of: the one whose name starts the tag of `entry` (an FK
/// constraint depends on its table and on the table that it
/// references), else the first
fn relation<'a>(
    entry: &libpgdump::Entry,
    by_id: &HashMap<i32, &'a libpgdump::Entry>,
) -> Option<&'a libpgdump::Entry> {
    let relations: Vec<&libpgdump::Entry> = entry
        .dependencies
        .iter()
        .filter_map(|id| by_id.get(id).copied())
        .filter(|owner| {
            matches!(
                owner.desc,
                OT::Table
                    | OT::View
                    | OT::MaterializedView
                    | OT::ForeignTable
                    | OT::Domain
            )
        })
        .collect();
    let tag = entry.tag.as_deref().unwrap_or_default();
    relations
        .iter()
        .find(|owner| {
            owner.namespace == entry.namespace
                && owner
                    .tag
                    .as_deref()
                    .is_some_and(|name| tag.starts_with(&format!("{name} ")))
        })
        .or(relations.first())
        .copied()
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
            ObjectType::Type => {
                Definition::Type(serde_json::from_value(json).unwrap())
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

    /// The plan replaces the items `roots`; each other item of the
    /// project is unchanged
    fn replace(
        project: &crate::project::Project,
        snapshot: &libpgdump::Dump,
        roots: &[usize],
    ) -> (Dependents, BTreeMap<usize, Resolution>, Diff) {
        let mut diff = Diff {
            items: BTreeMap::new(),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
        };
        let mut resolutions = BTreeMap::new();
        for item in &project.inventory {
            let change = if roots.contains(&item.id) {
                diff.changed.insert(item.id, item.definition.clone());
                resolutions.insert(item.id, Resolution::Replace);
                Change::Changed
            } else {
                Change::Unchanged
            };
            diff.items.insert(item.id, change);
        }
        let dependents = rebuild(
            project,
            &mut diff,
            &mut resolutions,
            snapshot,
            &alter::IndexGroups::new(),
            &alter::operator_class::Families::new(),
            false,
        );
        (dependents, resolutions, diff)
    }

    /// The SQL of the drops of the entry `id`
    fn drops(dependents: &Dependents, id: i32) -> Vec<&str> {
        dependents.drops[&id]
            .iter()
            .map(|alter| {
                assert!(alter.destructive);
                alter.sql.as_str()
            })
            .collect()
    }

    fn rebuilt(resolution: &Resolution) -> bool {
        matches!(
            resolution,
            Resolution::Rebuild { before, drop }
                if before.is_empty() && drop.is_empty()
        )
    }

    /// A table that the plan makes again: a view on it and a function
    /// on its row type are dropped first and made again. The table is
    /// dropped after them, before the objects that the plan makes. Its
    /// own constraint and index, its partition that is not an item of
    /// its own and the sequence of its identity column go with it,
    /// with no statement of their own
    #[test]
    fn a_table_rebuild_drops_its_dependents_first() {
        let project = project(vec![
            item(0, ObjectType::Table, table("0")),
            item(1, ObjectType::View, view("v", " SELECT id FROM test.t")),
            item(2, ObjectType::Function, function("integer")),
        ]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let t = entry(
            &mut snapshot,
            OT::Table,
            "t",
            Some("DROP TABLE test.t;\n"),
            &[],
        );
        // an identity column has no SEQUENCE OWNED BY entry
        let sequence = entry(
            &mut snapshot,
            OT::Sequence,
            "t_id_seq",
            Some("ALTER TABLE test.t ALTER COLUMN id DROP IDENTITY;\n"),
            &[t],
        );
        let p = entry(
            &mut snapshot,
            OT::Table,
            "p",
            Some("DROP TABLE test.p;\n"),
            &[t],
        );
        let attach = entry(&mut snapshot, OT::TableAttach, "p", None, &[p, t]);
        let f = entry(
            &mut snapshot,
            OT::Function,
            "f(integer)",
            Some("DROP FUNCTION test.f(integer);\n"),
            &[t],
        );
        let v = entry(
            &mut snapshot,
            OT::View,
            "v",
            Some("DROP VIEW test.v;\n"),
            &[t],
        );
        entry(&mut snapshot, OT::Comment, "VIEW v", None, &[v]);
        let pk = entry(
            &mut snapshot,
            OT::Constraint,
            "t t_pkey",
            Some("ALTER TABLE ONLY test.t DROP CONSTRAINT t_pkey;\n"),
            &[t],
        );
        let i = entry(
            &mut snapshot,
            OT::Index,
            "i",
            Some("DROP INDEX test.i;\n"),
            &[t],
        );
        let (dependents, resolutions, _) = replace(&project, &snapshot, &[0]);
        assert!(dependents.refused.is_empty(), "{:?}", dependents.refused);
        assert_eq!(
            dependents.labels,
            vec!["FUNCTION test.f(integer)", "VIEW test.v"]
        );
        assert_eq!(drops(&dependents, t), vec!["DROP TABLE test.t;\n"]);
        assert_eq!(
            drops(&dependents, f),
            vec!["DROP FUNCTION test.f(integer);\n"]
        );
        assert_eq!(drops(&dependents, v), vec!["DROP VIEW test.v;\n"]);
        for id in [sequence, p, attach, pk, i] {
            assert!(!dependents.drops.contains_key(&id));
        }
        for id in [0, 1, 2] {
            assert!(rebuilt(&resolutions[&id]), "{id}");
        }
    }

    /// PostgreSQL drops a partition with its table, and the plan does
    /// not make again a partition that is an item of its own (with
    /// `attached`). The plan also does not attach a partition again
    /// when it makes only the partition again
    #[test]
    fn a_table_rebuild_refuses_its_partitions() {
        let mut json = table("0");
        json["name"] = serde_json::json!("p");
        let project = project(vec![
            item(0, ObjectType::Table, table("0")),
            item(1, ObjectType::Table, json.clone()),
        ]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let t = entry(
            &mut snapshot,
            OT::Table,
            "t",
            Some("DROP TABLE test.t;\n"),
            &[],
        );
        let p = entry(
            &mut snapshot,
            OT::Table,
            "p",
            Some("DROP TABLE test.p;\n"),
            &[t],
        );
        entry(&mut snapshot, OT::TableAttach, "p", None, &[p, t]);
        let (mut dependents, _, _) = replace(&project, &snapshot, &[0]);
        dependents.refused.sort();
        assert_eq!(
            dependents.refused,
            vec![
                "TABLE ATTACH test.p: deploy would not attach the partition \
                 again",
                "TABLE test.p: it is a partition of TABLE test.t, and \
                 PostgreSQL drops it with the table",
            ]
        );
        // the plan makes the partition again, not its parent
        let project = self::project(vec![item(0, ObjectType::Table, json)]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let t = entry(&mut snapshot, OT::Table, "t", None, &[]);
        let p = entry(
            &mut snapshot,
            OT::Table,
            "p",
            Some("DROP TABLE test.p;\n"),
            &[t],
        );
        entry(&mut snapshot, OT::TableAttach, "p", None, &[p, t]);
        let (dependents, _, _) = replace(&project, &snapshot, &[0]);
        assert_eq!(
            dependents.refused,
            vec![
                "TABLE ATTACH test.p: deploy would not attach the partition \
                  again"
            ]
        );
    }

    /// PostgreSQL drops the table from each publication, and the plan
    /// does not add it again
    #[test]
    fn a_table_rebuild_refuses_its_publications() {
        let project = project(vec![item(0, ObjectType::Table, table("0"))]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let t = entry(
            &mut snapshot,
            OT::Table,
            "t",
            Some("DROP TABLE test.t;\n"),
            &[],
        );
        let publication =
            entry(&mut snapshot, OT::Publication, "pub", None, &[]);
        entry(
            &mut snapshot,
            OT::PublicationTable,
            "pub t",
            Some("ALTER PUBLICATION pub DROP TABLE ONLY test.t;\n"),
            &[t, publication],
        );
        let (dependents, _, _) = replace(&project, &snapshot, &[0]);
        assert_eq!(
            dependents.refused,
            vec![
                "PUBLICATION TABLE test.pub t: deploy would remove the table \
                 from the publication and not add it again"
            ]
        );
    }

    /// A disabled trigger of a table, and a clustered index of a
    /// materialized view, that the plan makes again: the project does
    /// not keep the statements after the CREATE, thus the rebuild would
    /// lose them
    #[test]
    fn a_rebuild_refuses_a_part_with_more_statements() {
        for (desc, json, part, defn, label) in [
            (
                ObjectType::Table,
                table("0"),
                OT::Trigger,
                "CREATE TRIGGER tg BEFORE UPDATE ON test.t FOR EACH ROW \
                 EXECUTE FUNCTION test.tf();\n\n\
                 ALTER TABLE test.t DISABLE TRIGGER tg;\n",
                "TABLE test.t: TRIGGER test.t tg",
            ),
            (
                ObjectType::MaterializedView,
                view("t", " SELECT 1 AS id"),
                OT::Index,
                "CREATE INDEX i ON test.t USING btree (id);\n\n\
                 ALTER TABLE test.t CLUSTER ON i;\n",
                "MATERIALIZED VIEW test.t: INDEX test.i",
            ),
        ] {
            let project = project(vec![item(0, desc, json)]);
            let mut snapshot =
                libpgdump::new("test", "UTF8", "18.0").expect("new dump");
            let relation = match desc {
                ObjectType::Table => OT::Table,
                _ => OT::MaterializedView,
            };
            let t = entry(&mut snapshot, relation, "t", Some("DROP;\n"), &[]);
            let tag = if part == OT::Index { "i" } else { "t tg" };
            entry_with(&mut snapshot, part, tag, defn, Some("DROP;\n"), &[t]);
            let (dependents, _, _) = replace(&project, &snapshot, &[0]);
            assert_eq!(
                dependents.refused,
                vec![format!(
                    "{label} has statements after its CREATE that the \
                     project does not keep (for example, ALTER TABLE ... \
                     DISABLE TRIGGER), and deploy would lose them"
                )]
            );
        }
        // the project keeps the replica identity of the table
        let project = project(vec![item(0, ObjectType::Table, table("0"))]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let t = entry(&mut snapshot, OT::Table, "t", Some("DROP;\n"), &[]);
        entry_with(
            &mut snapshot,
            OT::Index,
            "i",
            "CREATE UNIQUE INDEX i ON test.t USING btree (id);\n\n\
             ALTER TABLE ONLY test.t REPLICA IDENTITY USING INDEX i;\n",
            Some("DROP;\n"),
            &[t],
        );
        let (dependents, _, _) = replace(&project, &snapshot, &[0]);
        assert!(dependents.refused.is_empty(), "{:?}", dependents.refused);
    }

    /// PostgreSQL drops a sequence that a column of the table owns
    /// (serial, or OWNED BY) with the table, and the plan does not make
    /// it again with its value
    #[test]
    fn a_table_rebuild_refuses_an_owned_sequence() {
        let project = project(vec![item(0, ObjectType::Table, table("0"))]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let t = entry(
            &mut snapshot,
            OT::Table,
            "t",
            Some("DROP TABLE test.t;\n"),
            &[],
        );
        let sequence = entry(
            &mut snapshot,
            OT::Sequence,
            "t_id_seq",
            Some("DROP SEQUENCE test.t_id_seq;\n"),
            &[t],
        );
        entry(
            &mut snapshot,
            OT::SequenceOwnedBy,
            "t_id_seq",
            None,
            &[sequence],
        );
        let (dependents, _, _) = replace(&project, &snapshot, &[0]);
        assert_eq!(
            dependents.refused,
            vec![
                "SEQUENCE test.t_id_seq: a column of TABLE test.t owns it, \
                 thus PostgreSQL drops it with the table, and deploy cannot \
                 make it again with its value"
            ]
        );
    }

    /// A replaced object with no dependents is dropped and made again
    /// at its position, as before
    #[test]
    fn a_replaced_object_with_no_dependents_stays() {
        let project = project(vec![item(0, ObjectType::Table, table("0"))]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let t = entry(
            &mut snapshot,
            OT::Table,
            "t",
            Some("DROP TABLE test.t;\n"),
            &[],
        );
        entry(
            &mut snapshot,
            OT::Index,
            "i",
            Some("DROP INDEX test.i;\n"),
            &[t],
        );
        let (dependents, resolutions, _) = replace(&project, &snapshot, &[0]);
        assert!(dependents.refused.is_empty());
        assert!(dependents.drops.is_empty());
        assert!(matches!(resolutions[&0], Resolution::Replace));
    }

    /// An FK constraint of another table that references the table, and
    /// an inheritance child of the table, cannot be dropped and made
    /// again with the table
    #[test]
    fn a_table_rebuild_refuses_what_it_cannot_make_again() {
        let project = project(vec![item(0, ObjectType::Table, table("0"))]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let t = entry(
            &mut snapshot,
            OT::Table,
            "t",
            Some("DROP TABLE test.t;\n"),
            &[],
        );
        let b = entry(&mut snapshot, OT::Table, "b", None, &[]);
        entry(&mut snapshot, OT::Table, "c", None, &[t]);
        let pk = entry(
            &mut snapshot,
            OT::Constraint,
            "t t_pkey",
            Some("ALTER TABLE ONLY test.t DROP CONSTRAINT t_pkey;\n"),
            &[t],
        );
        // an FK of the table itself, on another column, is a part of it
        entry(
            &mut snapshot,
            OT::FkConstraint,
            "t t_fkey",
            Some("ALTER TABLE ONLY test.t DROP CONSTRAINT t_fkey;\n"),
            &[pk, t, t],
        );
        entry(
            &mut snapshot,
            OT::FkConstraint,
            "b b_fkey",
            Some("ALTER TABLE ONLY test.b DROP CONSTRAINT b_fkey;\n"),
            &[pk, b, t],
        );
        let (mut dependents, resolutions, _) =
            replace(&project, &snapshot, &[0]);
        dependents.refused.sort();
        assert_eq!(
            dependents.refused,
            vec![
                "FK CONSTRAINT test.b b_fkey: deploy cannot drop and make \
                 it again",
                "TABLE test.c: it depends on TABLE test.t, and deploy can \
                 drop and make again only its defaults and checks",
            ]
        );
        assert!(dependents.labels.is_empty());
        assert!(rebuilt(&resolutions[&0]));
    }

    /// A table with a foreign key on `test.k (a)`, as `name`
    fn referencing(name: &str, column: &str) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            "schema": "test",
            "owner": "postgres",
            "columns": [{"name": "x", "data_type": "integer"}],
            "foreign_keys": [{
                "name": format!("{name}_fkey"),
                "columns": ["x"],
                "references": {"name": "test.k", "columns": [column]},
            }],
        })
    }

    /// A primary key that changes in place: the foreign keys that
    /// reference it are dropped first and added again after the
    /// statements of its table, gated with the drop. A foreign key
    /// that the project changes, and a view that depends on the key,
    /// are refused
    #[test]
    fn a_key_change_drops_and_adds_its_foreign_keys() {
        let key = |column: &str| {
            serde_json::json!({
                "name": "k",
                "schema": "test",
                "owner": "postgres",
                "columns": [
                    {"name": "a", "data_type": "integer", "nullable": false},
                ],
                "primary_key": [column],
            })
        };
        let project = project(vec![
            item(0, ObjectType::Table, key("a")),
            item(1, ObjectType::Table, referencing("b", "a")),
            item(2, ObjectType::Table, referencing("c", "a")),
        ]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let k = entry(&mut snapshot, OT::Table, "k", None, &[]);
        let b = entry(&mut snapshot, OT::Table, "b", None, &[]);
        let c = entry(&mut snapshot, OT::Table, "c", None, &[]);
        let pk = entry(
            &mut snapshot,
            OT::Constraint,
            "k k_pk",
            Some("ALTER TABLE ONLY test.k DROP CONSTRAINT k_pk;\n"),
            &[k],
        );
        let b_fkey = entry(
            &mut snapshot,
            OT::FkConstraint,
            "b b_fkey",
            Some("ALTER TABLE ONLY test.b DROP CONSTRAINT b_fkey;\n"),
            &[b, k, pk],
        );
        entry(
            &mut snapshot,
            OT::FkConstraint,
            "c c_fkey",
            Some("ALTER TABLE ONLY test.c DROP CONSTRAINT c_fkey;\n"),
            &[c, k, pk],
        );
        entry(&mut snapshot, OT::View, "v", None, &[k, pk]);
        let mut database = key("a");
        database["primary_key"] =
            serde_json::json!({"name": "k_pk", "columns": ["a"]});
        let database: Definition =
            Definition::Table(serde_json::from_value(database).unwrap());
        let mut c_database = referencing("c", "a");
        c_database["foreign_keys"][0]["on_delete"] =
            serde_json::json!("CASCADE");
        let mut diff = Diff {
            items: BTreeMap::from([
                (0, Change::Changed),
                (1, Change::Unchanged),
                (2, Change::Changed),
            ]),
            changed: BTreeMap::from([
                (0, database.clone()),
                (
                    2,
                    Definition::Table(
                        serde_json::from_value(c_database).unwrap(),
                    ),
                ),
            ]),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
        };
        let mut resolutions = BTreeMap::new();
        for id in [0, 2] {
            resolutions.insert(
                id,
                alter::resolve(
                    &project.inventory[id].definition,
                    &diff.changed[&id],
                ),
            );
        }
        let mut dependents = rebuild(
            &project,
            &mut diff,
            &mut resolutions,
            &snapshot,
            &alter::IndexGroups::new(),
            &alter::operator_class::Families::new(),
            false,
        );
        assert_eq!(
            drops(&dependents, b_fkey),
            vec!["ALTER TABLE ONLY test.b DROP CONSTRAINT b_fkey;\n"]
        );
        let Resolution::Statements(alters) = &resolutions[&0] else {
            panic!("expected in-place statements");
        };
        let sql: Vec<(&str, bool)> = alters
            .iter()
            .map(|alter| (alter.sql.as_str(), alter.destructive))
            .collect();
        assert_eq!(
            sql,
            vec![
                ("ALTER TABLE test.k DROP CONSTRAINT k_pk;\n", true),
                ("ALTER TABLE test.k ADD PRIMARY KEY (a);\n", true),
                (
                    "ALTER TABLE test.b ADD CONSTRAINT b_fkey FOREIGN KEY \
                     (x) REFERENCES test.k (a);\n",
                    true
                ),
            ]
        );
        assert_eq!(dependents.labels, vec!["FK CONSTRAINT test.b b_fkey"]);
        dependents.refused.sort();
        assert_eq!(
            dependents.refused,
            vec![
                "FK CONSTRAINT test.c c_fkey: it references CONSTRAINT \
                 test.k k_pk, which deploy drops and adds again, and the \
                 project changes or removes it",
                "VIEW test.v: it depends on CONSTRAINT test.k k_pk, which \
                 deploy drops and adds again, and deploy cannot drop and \
                 make it again",
            ]
        );
    }

    /// A replaced view that depends on another replaced view: both are
    /// dropped before the objects that the plan makes, in reverse
    /// snapshot order. Each is made again at its position
    #[test]
    fn a_root_that_depends_on_another_root_is_dropped_first() {
        let project = project(vec![
            item(1, ObjectType::View, view("v", " SELECT 1 AS n")),
            item(2, ObjectType::View, view("v2", " SELECT n FROM test.v")),
        ]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let v = entry(
            &mut snapshot,
            OT::View,
            "v",
            Some("DROP VIEW test.v;\n"),
            &[],
        );
        let v2 = entry(
            &mut snapshot,
            OT::View,
            "v2",
            Some("DROP VIEW test.v2;\n"),
            &[v],
        );
        let (dependents, resolutions, _) =
            replace(&project, &snapshot, &[1, 2]);
        assert!(dependents.refused.is_empty(), "{:?}", dependents.refused);
        // a replaced object is not a dependent in the header
        assert!(dependents.labels.is_empty());
        assert_eq!(drops(&dependents, v), vec!["DROP VIEW test.v;\n"]);
        assert_eq!(drops(&dependents, v2), vec!["DROP VIEW test.v2;\n"]);
        assert!(rebuilt(&resolutions[&1]) && rebuilt(&resolutions[&2]));
    }

    /// An index of a replaced table calls a replaced function that
    /// comes after the table in the snapshot. In reverse snapshot order
    /// the function is dropped before the table, thus the index is
    /// dropped on its own first; the rebuild of the table makes it
    /// again
    #[test]
    fn a_part_that_depends_on_a_later_root_is_dropped_first() {
        let project = project(vec![
            item(0, ObjectType::Function, function("integer")),
            item(3, ObjectType::Table, table("0")),
        ]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let t = entry(
            &mut snapshot,
            OT::Table,
            "t",
            Some("DROP TABLE test.t;\n"),
            &[],
        );
        let f = entry(
            &mut snapshot,
            OT::Function,
            "f(integer)",
            Some("DROP FUNCTION test.f(integer);\n"),
            &[],
        );
        let i = entry(
            &mut snapshot,
            OT::Index,
            "i",
            Some("DROP INDEX test.i;\n"),
            &[t, f],
        );
        let (dependents, resolutions, _) =
            replace(&project, &snapshot, &[0, 3]);
        assert!(dependents.refused.is_empty(), "{:?}", dependents.refused);
        assert!(dependents.labels.is_empty());
        assert_eq!(drops(&dependents, i), vec!["DROP INDEX test.i;\n"]);
        assert_eq!(drops(&dependents, t), vec!["DROP TABLE test.t;\n"]);
        assert_eq!(
            drops(&dependents, f),
            vec!["DROP FUNCTION test.f(integer);\n"]
        );
        assert!(rebuilt(&resolutions[&0]) && rebuilt(&resolutions[&3]));
    }

    /// A replaced type: a view whose column has the type is made again,
    /// and a table whose column has the type is refused
    #[test]
    fn a_type_rebuild_refuses_a_table_column() {
        let project = project(vec![
            item(
                0,
                ObjectType::Type,
                serde_json::json!({
                    "name": "mood", "schema": "test", "owner": "postgres",
                    "type": "enum", "enum": ["a", "b"],
                }),
            ),
            item(
                1,
                ObjectType::View,
                view("v", " SELECT 'a'::test.mood AS m"),
            ),
        ]);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let mood = entry(
            &mut snapshot,
            OT::Type,
            "mood",
            Some("DROP TYPE test.mood;\n"),
            &[],
        );
        let v = entry(
            &mut snapshot,
            OT::View,
            "v",
            Some("DROP VIEW test.v;\n"),
            &[mood],
        );
        entry(&mut snapshot, OT::Table, "t", None, &[mood]);
        let (dependents, _, _) = replace(&project, &snapshot, &[0]);
        assert_eq!(
            dependents.refused,
            vec![
                "TABLE test.t: it depends on TYPE test.mood, and deploy can \
                 drop and make again only its defaults and checks"
            ]
        );
        assert_eq!(dependents.labels, vec!["VIEW test.v"]);
        assert_eq!(drops(&dependents, v), vec!["DROP VIEW test.v;\n"]);
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
