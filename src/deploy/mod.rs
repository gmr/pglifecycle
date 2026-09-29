//! The `deploy` command: compare the repo project against a live
//! database (or an existing dump) and emit the DDL needed to make the
//! database match the project (PLAN.md Phase 6).
//!
//! Output first: the script goes to stdout or `--output` and is meant
//! to be applied separately (e.g. `psql --single-transaction
//! -v ON_ERROR_STOP=1 -f deploy.sql` in a CI step). Destructive
//! statements — DROPs for objects missing from the repo, data-losing
//! column changes, and the drop+recreate fallback for changes with no
//! in-place form — are excluded unless `--allow-drop` is given.

mod alter;
mod diff;

pub(crate) use diff::identity_type;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::IsTerminal;

use crate::deploy::alter::Resolution;
use crate::deploy::diff::{Change, Diff, ObjectKey};
use crate::models::Definition;
use crate::utils::quote_ident;
use crate::{
    build, cli, constants, diagnostics, pgdump, progress, project, pull,
};

pub fn deploy(args: &cli::Deploy) -> Result<(), String> {
    if args.connection.password && !std::io::stdin().is_terminal() {
        return Err(String::from(
            "--password requires an interactive terminal; set PGPASSWORD \
             or use a pgpass file instead",
        ));
    }
    diagnostics::init(args.error_file.clone());
    let project = project::load(&args.project)?;
    let source = source_label(args);
    log::info!("Comparing {} against {source}", project.name);
    let ddl = pgdump::DumpDdl {
        no_privileges: args.no_privileges,
        exclude_tables: args.exclude_table.clone(),
        exclude_schemas: args.exclude_schema.clone(),
        exclude_extensions: args.exclude_extension.clone(),
        ..Default::default()
    };
    // deploy compares against the project's stored SQL, which pull
    // formats with the default style unless overridden; match it here
    let (assembly, snapshot) = pull::snapshot(
        args.dump.as_deref(),
        &args.connection,
        &ddl,
        None,
        libpgfmt::style::Style::PgDump,
    )?;
    let task = progress::spinner("Diffing project against database");
    let mut diff = diff::diff(&project, &assembly);
    // --no-privileges keeps the default privileges that a dump has
    if args.no_privileges {
        diff.removed.retain(|key, _| {
            key.desc != constants::ObjectType::DefaultPrivileges
        });
    }
    let groups = partition_index_groups(&project, &mut diff);
    let resolutions = resolutions(&project, &diff, &groups);
    task.finish();
    let mut output = build::assemble(&project)?;
    let task = progress::spinner("Planning changes");
    output.dump.sort_entries();
    let plan = plan(&diff, &resolutions, &output, &snapshot, args)?;
    task.finish();
    report(&diff, &plan, &assembly);
    let script = render_script(&plan, &project.name, &source);
    if let Some(path) = &args.output {
        std::fs::write(path, &script)
            .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    } else if !args.apply {
        print!("{script}");
    }
    if args.apply {
        apply(&plan, &script, args)?;
    }
    Ok(())
}

/// Execute the plan against the database via psql, refusing if
/// destructive statements were excluded
fn apply(plan: &Plan, script: &str, args: &cli::Deploy) -> Result<(), String> {
    if !plan.excluded.is_empty() {
        return Err(format!(
            "{} destructive statement(s) are pending; re-run with \
             --allow-drop to apply them, or resolve them first",
            plan.excluded.len()
        ));
    }
    if plan.included.is_empty() {
        log::info!(
            "The database already matches the project; nothing to apply"
        );
        return Ok(());
    }
    let file = tempfile::Builder::new()
        .prefix("pglifecycle-deploy-")
        .suffix(".sql")
        .tempfile()
        .map_err(|e| format!("failed to create temp file: {e}"))?;
    std::fs::write(file.path(), script).map_err(|e| {
        format!("failed to write {}: {e}", file.path().display())
    })?;
    log::info!("Applying {} statement(s)", plan.included.len());
    let task = progress::spinner("Applying to database");
    pgdump::apply(&args.connection, file.path()).map_err(|stderr| {
        format!("deploy failed (the transaction was rolled back):\n{stderr}")
    })?;
    task.finish();
    log::info!("Deploy applied successfully");
    Ok(())
}

fn as_table(definition: &Definition) -> Option<&crate::models::Table> {
    match definition {
        Definition::Table(table) => Some(table),
        _ => None,
    }
}

/// The index groups of partitioned tables that the plan rebuilds (see
/// [`alter::IndexGroups`]). A table with no other change can still have
/// indexes in a rebuilt group, so its table is marked changed: the
/// partitioned table drops and makes again the group's index, and a
/// partition makes its indexes again after the drop.
fn partition_index_groups(
    project: &project::Project,
    diff: &mut Diff,
) -> alter::IndexGroups {
    let groups = alter::rebuilt_index_groups(
        project.inventory.iter().filter_map(|item| {
            let db = diff.changed.get(&item.id)?;
            Some((as_table(&item.definition)?, as_table(db)?))
        }),
    );
    if groups.is_empty() {
        return groups;
    }
    for item in &project.inventory {
        let Some(table) = as_table(&item.definition) else {
            continue;
        };
        if diff.items.get(&item.id) != Some(&Change::Unchanged) {
            continue;
        }
        // a partitioned table whose index is in a group, or a partition
        // whose index belongs to one
        let in_group = table.indexes.iter().flatten().any(|index| {
            groups.contains(&(table.schema.clone(), index.name.clone()))
                || index.parent.as_deref().is_some_and(|parent| {
                    groups
                        .contains(&alter::parent_index(parent, &table.schema))
                })
        });
        if in_group {
            // unchanged, so the project's copy is the database's
            diff.items.insert(item.id, Change::Changed);
            diff.changed.insert(item.id, item.definition.clone());
        }
    }
    groups
}

/// Resolve each changed item into in-place statements or the
/// drop+recreate fallback
fn resolutions(
    project: &project::Project,
    diff: &Diff,
    groups: &alter::IndexGroups,
) -> BTreeMap<usize, Resolution> {
    let inventory_by_id = project
        .inventory
        .iter()
        .map(|item| (item.id, &item.definition))
        .collect::<BTreeMap<_, _>>();
    diff.changed
        .iter()
        .map(|(id, database)| {
            let repo = inventory_by_id
                .get(id)
                .expect("changed item id missing from project inventory");
            (*id, alter::resolve_with(repo, database, groups))
        })
        .collect()
}

/// One statement in the deploy plan
struct Statement {
    label: String,
    sql: String,
    /// Withholding it can leave the database allowing access that the
    /// project does not (see `alter::Alter::fails_open`)
    fails_open: bool,
}

/// The ordered script plus the destructive statements excluded from
/// it when `--allow-drop` is not given
struct Plan {
    included: Vec<Statement>,
    excluded: Vec<Statement>,
    /// Index drops left out when `--allow-drop-indexes` is not given:
    /// the database keeps these indexes that the project does not have
    kept: Vec<Statement>,
    /// How many of `included` are destructive (non-zero only with
    /// `--allow-drop`)
    included_destructive: usize,
}

/// Assemble the ordered plan: DROPs for database-only objects first
/// (reverse snapshot order), then changed default privileges (those in
/// a new schema directly after its CREATE SCHEMA), then the repo
/// archive's entries in topological order — plain CREATEs for added
/// objects, in-place ALTERs where a renderer exists, gated
/// drop+recreate otherwise
fn plan(
    diff: &Diff,
    resolutions: &BTreeMap<usize, Resolution>,
    output: &build::BuildOutput,
    snapshot: &libpgdump::Dump,
    args: &cli::Deploy,
) -> Result<Plan, String> {
    let mut included = Vec::new();
    let mut excluded = Vec::new();
    let mut included_destructive = 0usize;
    let mut kept = Vec::new();
    let mut push = |destructive: bool, statement: Statement| {
        if destructive && !args.allow_drop {
            excluded.push(statement);
        } else {
            if destructive {
                included_destructive += 1;
            }
            included.push(statement);
        }
    };
    // pg_dump archives are stored in dependency order, so dropping in
    // reverse entry order removes dependents before dependencies.
    // `entry_key` derives a function's name from the archive tag
    // (types only, no argument names), while `ObjectKey`'s own name
    // may include them, so match through `drop_match_key`'s
    // tag-shaped form rather than the removed key directly
    let wanted: std::collections::BTreeMap<ObjectKey, &ObjectKey> = diff
        .removed
        .iter()
        .map(|(key, definition)| (drop_match_key(key, definition), key))
        .collect();
    let mut emitted: std::collections::BTreeSet<&ObjectKey> =
        std::collections::BTreeSet::new();
    let ordered: Vec<&ObjectKey> = snapshot
        .entries()
        .iter()
        .rev()
        .filter_map(entry_key)
        .filter_map(|key| wanted.get(&key).copied())
        .collect();
    for key in ordered {
        if emitted.insert(key) {
            push(
                true,
                Statement {
                    label: key.to_string(),
                    sql: drop_sql(key, diff.removed.get(key)),
                    fails_open: drop_fails_open(diff.removed.get(key)),
                },
            );
        }
    }
    // database-only objects whose snapshot entry could not be keyed
    // (should not happen for modeled types) still need dropping;
    // append them after the ordered ones
    for (key, definition) in &diff.removed {
        if !emitted.contains(key) {
            push(
                true,
                Statement {
                    label: key.to_string(),
                    sql: drop_sql(key, Some(definition)),
                    fails_open: drop_fails_open(Some(definition)),
                },
            );
        }
    }
    // PostgreSQL applies default privileges only when it creates an
    // object, and the DEFAULT ACL entries of the archive come after the
    // objects. Thus emit changed default privileges before the objects
    // that this deploy creates, so that they get the defaults of the
    // project. The role of a changed item is in the database already,
    // but a schema can be new: a statement IN SCHEMA of a schema that
    // this deploy creates comes directly after its CREATE SCHEMA, and
    // the others come before the archive entries. This also covers the
    // defaults of a role with no declarations, which make no archive
    // entry
    let defaults: Vec<usize> = diff
        .changed
        .iter()
        .filter(|(_, database)| {
            matches!(database, Definition::DefaultPrivileges(_))
        })
        .map(|(id, _)| *id)
        .collect();
    let new_schemas: HashSet<&str> = output
        .dump
        .entries()
        .iter()
        .filter(|entry| entry.desc == libpgdump::ObjectType::Schema)
        .filter(|entry| {
            output
                .item_ids
                .get(&entry.dump_id)
                .and_then(|id| diff.items.get(id))
                == Some(&Change::Added)
        })
        .filter_map(|entry| entry.tag.as_deref())
        .collect();
    let mut after_schema: HashMap<&str, Vec<(bool, Statement)>> =
        HashMap::new();
    if !args.no_privileges {
        for id in &defaults {
            if let Some(Resolution::Statements(alters)) = resolutions.get(id) {
                for alter in alters {
                    let statement = Statement {
                        label: alter.label.clone().unwrap_or_default(),
                        sql: alter.sql.clone(),
                        fails_open: alter.fails_open,
                    };
                    match alter
                        .schema
                        .as_deref()
                        .and_then(|schema| new_schemas.get(schema).copied())
                    {
                        Some(schema) => after_schema
                            .entry(schema)
                            .or_default()
                            .push((alter.destructive, statement)),
                        None => push(alter.destructive, statement),
                    }
                }
            }
        }
    }
    // dump_id -> entry, for resolving a child entry's owning items
    // through intermediate non-item entries (e.g. a COMMENT ON TRIGGER
    // depends on the TRIGGER entry, which is itself a child of the table
    // rather than a modeled inventory item)
    let entries_by_id: HashMap<i32, &libpgdump::Entry> = output
        .dump
        .entries()
        .iter()
        .map(|entry| (entry.dump_id, entry))
        .collect();
    for entry in output.dump.entries() {
        if args.no_privileges
            && matches!(
                entry.desc,
                libpgdump::ObjectType::Acl | libpgdump::ObjectType::DefaultAcl
            )
        {
            continue;
        }
        let direct = output.item_ids.get(&entry.dump_id);
        // changed default privileges are emitted above
        if direct.is_some_and(|id| defaults.contains(id)) {
            continue;
        }
        let owners: Vec<usize> = match direct {
            Some(id) => vec![*id],
            None => {
                // walk the dependency graph until it reaches inventory
                // items, so comments/ACLs on child entries still map to
                // the object that owns them
                let mut items = Vec::new();
                let mut seen = HashSet::new();
                let mut stack: Vec<i32> = entry.dependencies.clone();
                while let Some(dep) = stack.pop() {
                    if !seen.insert(dep) {
                        continue;
                    }
                    if let Some(id) = output.item_ids.get(&dep) {
                        items.push(*id);
                    } else if let Some(parent) = entries_by_id.get(&dep) {
                        stack.extend(parent.dependencies.iter().copied());
                    }
                }
                items
            }
        };
        if owners.is_empty() {
            continue;
        }
        let changes: Vec<Change> = owners
            .iter()
            .filter_map(|id| diff.items.get(id).copied())
            .collect();
        let label = entry_label(entry);
        let Some(defn) = entry.defn.clone() else {
            continue;
        };
        // the connecting role owns what the script creates, so an item
        // that names an owner gets it after its CREATE
        let owner = direct
            .filter(|id| !args.no_owner && diff.owned.contains(id))
            .and_then(|_| owner_sql(entry));
        // an object that the database has with another owner gets the
        // owner in place; a rebuild sets it after its CREATE
        let reown = direct
            .filter(|id| diff.owner_changed.contains(id))
            .and(owner.clone())
            .map(|sql| Statement {
                label: label.clone(),
                sql,
                fails_open: false,
            });
        if changes.iter().all(|c| *c == Change::Added) {
            push(
                false,
                Statement {
                    label,
                    sql: format!("{defn}{}", owner.unwrap_or_default()),
                    fails_open: false,
                },
            );
            // the changed default privileges in a new schema
            if entry.desc == libpgdump::ObjectType::Schema {
                let statements = entry
                    .tag
                    .as_deref()
                    .and_then(|tag| after_schema.remove(tag));
                for (destructive, statement) in
                    statements.into_iter().flatten()
                {
                    push(destructive, statement);
                }
            }
            continue;
        }
        if !changes.contains(&Change::Changed)
            || !changes
                .iter()
                .all(|c| matches!(c, Change::Added | Change::Changed))
        {
            if let Some(statement) = reown {
                push(false, statement);
            }
            continue;
        }
        // the object's own entry: emit its in-place statements,
        // re-issue it as CREATE OR REPLACE, or lead with its DROP for
        // the drop+recreate fallback
        if let Some(id) = direct {
            let rebuilt = match resolutions.get(id) {
                Some(Resolution::Statements(alters)) => {
                    for alter in alters {
                        let statement = Statement {
                            label: alter
                                .label
                                .clone()
                                .unwrap_or_else(|| label.clone()),
                            sql: alter.sql.clone(),
                            fails_open: alter.fails_open,
                        };
                        // an index that only the database has is kept
                        // unless --allow-drop-indexes, and is not
                        // pending: --apply runs without it
                        if alter.index_removal && !args.allow_drop_indexes {
                            kept.push(statement);
                            continue;
                        }
                        push(alter.destructive, statement);
                    }
                    false
                }
                Some(Resolution::OrReplace { comment, then }) => {
                    push(
                        false,
                        Statement {
                            label: label.clone(),
                            sql: defn.replacen(
                                "CREATE ",
                                "CREATE OR REPLACE ",
                                1,
                            ),
                            fails_open: false,
                        },
                    );
                    // CREATE OR REPLACE keeps the existing comment, so a
                    // changed or removed one is reconciled separately
                    if let Some(comment) = comment {
                        push(
                            false,
                            Statement {
                                label: label.clone(),
                                sql: comment.clone(),
                                fails_open: false,
                            },
                        );
                    }
                    for alter in then {
                        push(
                            alter.destructive,
                            Statement {
                                label: alter
                                    .label
                                    .clone()
                                    .unwrap_or_else(|| label.clone()),
                                sql: alter.sql.clone(),
                                fails_open: alter.fails_open,
                            },
                        );
                    }
                    false
                }
                _ => {
                    let mut sql = String::new();
                    if let Some(drop) = &entry.drop_stmt {
                        sql.push_str(drop);
                    }
                    sql.push_str(&defn);
                    if let Some(owner) = &owner {
                        sql.push_str(owner);
                    }
                    push(
                        true,
                        Statement {
                            label,
                            sql,
                            fails_open: false,
                        },
                    );
                    true
                }
            };
            if let Some(statement) = reown.filter(|_| !rebuilt) {
                push(false, statement);
            }
            continue;
        }
        // child entries (indexes, triggers, comments, ACLs): a
        // drop+recreate parent recreates them all (gated with it); an
        // OR REPLACE parent reconciles its own comment from its
        // resolution (so removals clear, not just changes); in-place
        // ALTERs reconcile their own children
        let replaced = owners.iter().any(|id| {
            diff.items.get(id) == Some(&Change::Changed)
                && matches!(resolutions.get(id), Some(Resolution::Replace))
        });
        if replaced {
            push(
                true,
                Statement {
                    label,
                    sql: defn,
                    fails_open: false,
                },
            );
        }
    }
    Ok(Plan {
        included,
        excluded,
        kept,
        included_destructive,
    })
}

/// Log what the plan skipped or excluded so the script is honest
/// about what it does not cover
fn report(diff: &Diff, plan: &Plan, assembly: &pull::Assembly) {
    // objects the snapshot could not model are absent from the diff
    // entirely, so without this the plan is silent about schema it is
    // leaving untouched in the database
    let unmodeled = assembly.remaining.len();
    if unmodeled > 0 {
        let plural = if unmodeled == 1 { "object" } else { "objects" };
        log::warn!(
            "{unmodeled} database {plural} ({}) cannot be modeled by this \
             version and are not represented in the plan; they were left \
             untouched",
            assembly.unmodeled_descs().join(", ")
        );
    }
    let undiffable = diff
        .items
        .values()
        .filter(|c| **c == Change::Undiffable)
        .count();
    if undiffable > 0 {
        log::warn!(
            "{undiffable} object(s) exist in both the project and the \
             database but cannot be compared: their type is only checked \
             for existence, or the project writes them as raw sql; they \
             were left untouched"
        );
    }
    for statement in &plan.excluded {
        if statement.fails_open {
            log::warn!(
                "{}: change requires a destructive statement and was \
                 withheld; until it is applied, the database can allow \
                 access that the project does not. Re-run with \
                 --allow-drop to include it",
                statement.label
            );
        } else {
            log::warn!(
                "{}: change requires a destructive statement; re-run with \
                 --allow-drop to include it",
                statement.label
            );
        }
    }
    for statement in &plan.kept {
        log::warn!(
            "{}: the database has an index that the project does not; \
             it was kept. Re-run with --allow-drop-indexes to drop it: {}",
            statement.label,
            statement.sql.trim_end()
        );
    }
    log::info!(
        "Plan: {} statement(s) included, {} excluded, {} index drop(s) \
         kept out",
        plan.included.len(),
        plan.excluded.len(),
        plan.kept.len()
    );
}

/// Render the script with a self-describing header
fn render_script(plan: &Plan, project: &str, source: &str) -> String {
    let mut script = format!(
        "-- pglifecycle deploy\n-- project: {project}\n-- source: \
         {source}\n"
    );
    if !plan.excluded.is_empty() {
        script.push_str(&format!(
            "-- destructive statements: {} excluded (re-run with \
             --allow-drop)\n",
            plan.excluded.len()
        ));
        // a withheld policy change is the one exclusion that leaves the
        // database less protected than the project, so name each one
        for statement in plan.excluded.iter().filter(|s| s.fails_open) {
            script.push_str(&format!(
                "-- WARNING: {} withheld; the database can allow access \
                 the project does not\n",
                statement.label
            ));
        }
    } else if plan.included_destructive > 0 {
        script.push_str(&format!(
            "-- destructive statements: {} included\n",
            plan.included_destructive
        ));
    } else {
        script.push_str("-- destructive statements: none\n");
    }
    if !plan.kept.is_empty() {
        script.push_str(&format!(
            "-- indexes kept: {} not in the project (re-run with \
             --allow-drop-indexes to drop them)\n",
            plan.kept.len()
        ));
        // a quoted name can contain a newline, so comment out each
        // line; else a part of the drop can run
        for statement in &plan.kept {
            for line in statement.sql.lines() {
                script.push_str(&format!("--   {line}\n"));
            }
        }
    }
    // withheld statements are changes too, so the database matches only
    // when there are none; a kept index is a difference that deploy
    // does not change, so do not say that the database matches
    if plan.included.is_empty() && plan.excluded.is_empty() {
        if plan.kept.is_empty() {
            script
                .push_str("-- no changes: the database matches the project\n");
        } else {
            script.push_str(
                "-- no changes: only the kept indexes are different from \
                 the project\n",
            );
        }
    }
    for statement in &plan.included {
        script
            .push_str(&format!("\n-- {}\n{}", statement.label, statement.sql));
    }
    script
}

/// `DROP <type> IF EXISTS <name>` for a database-only object. User
/// mappings are keyed by their user but dropped per server, so they
/// render from the definition. Default privileges have no DROP: the
/// REVOKE and GRANT statements that give the role the built-in
/// privileges again take their place. Everything else needs only the
/// key.
fn drop_sql(key: &ObjectKey, definition: Option<&Definition>) -> String {
    if let Some(Definition::DefaultPrivileges(defaults)) = definition {
        return alter::default_privileges::removal(defaults)
            .into_iter()
            .map(|alter| alter.sql)
            .collect();
    }
    if let Some(Definition::UserMapping(mapping)) = definition {
        return mapping
            .servers
            .iter()
            .map(|server| {
                format!(
                    "DROP USER MAPPING IF EXISTS FOR {} SERVER {};\n",
                    quote_ident(&mapping.name),
                    quote_ident(&server.name),
                )
            })
            .collect();
    }
    // a function or a procedure is named by its quoted name and its
    // input types, which is all that DROP FUNCTION reads; a function key
    // is an identity signature, so it is not quoted whole
    let routine = match definition {
        Some(Definition::Function(f)) => Some(f.clone()),
        Some(Definition::Procedure(p)) => Some(p.as_function()),
        _ => None,
    };
    let name = match (key.desc, routine) {
        (_, Some(f)) => {
            let types: Vec<&str> = f
                .parameters
                .iter()
                .flatten()
                .filter(|p| p.mode != "OUT" && p.mode != "TABLE")
                .map(|p| p.data_type.as_str())
                .collect();
            let base = f.name.split('(').next().unwrap_or_default();
            if f.parameters.is_none() && f.name.contains('(') {
                crate::utils::quote_routine_name(&f.name)
            } else {
                format!("{}({})", quote_ident(base), types.join(", "))
            }
        }
        (constants::ObjectType::Function, _) => key.name.clone(),
        _ => quote_ident(&key.name),
    };
    let qualified = if key.schema.is_empty() {
        name
    } else {
        format!("{}.{name}", quote_ident(&key.schema))
    };
    format!("DROP {} IF EXISTS {qualified};\n", key.desc.as_str())
}

/// Withholding the drop of a database-only object can leave the
/// database allowing access that the project does not: the removal of
/// default privileges that REVOKEs a grant
fn drop_fails_open(definition: Option<&Definition>) -> bool {
    match definition {
        Some(Definition::DefaultPrivileges(defaults)) => {
            alter::default_privileges::removal(defaults)
                .iter()
                .any(|alter| alter.fails_open)
        }
        _ => false,
    }
}

/// The key a removed object is looked up under when matching archive
/// entries for drop ordering: functions use the tag-shaped signature
/// ([`diff::function_tag_name`]) so they compare equal to
/// `entry_key`'s output; everything else uses its own key unchanged (a
/// procedure key is already tag-shaped, see `diff::object_identity`)
fn drop_match_key(key: &ObjectKey, definition: &Definition) -> ObjectKey {
    match definition {
        Definition::Function(f) => ObjectKey {
            desc: key.desc,
            schema: key.schema.clone(),
            name: diff::function_tag_name(f),
        },
        _ => key.clone(),
    }
}

/// Map a snapshot entry to the diff key space (modeled types only);
/// the schema component mirrors [`ObjectKey::new`] — empty for
/// schemaless types and extensions
fn entry_key(entry: &libpgdump::Entry) -> Option<ObjectKey> {
    use libpgdump::ObjectType as OT;
    let desc = match entry.desc {
        OT::Domain => constants::ObjectType::Domain,
        OT::Extension => constants::ObjectType::Extension,
        OT::ForeignDataWrapper => constants::ObjectType::ForeignDataWrapper,
        // foreign tables key as tables (the project models them as
        // tables with a `server`), so a removed one orders and drops
        // alongside ordinary tables
        OT::ForeignTable => constants::ObjectType::Table,
        OT::Function => constants::ObjectType::Function,
        OT::MaterializedView => constants::ObjectType::MaterializedView,
        OT::ProceduralLanguage => constants::ObjectType::ProceduralLanguage,
        OT::Procedure => constants::ObjectType::Procedure,
        OT::Schema => constants::ObjectType::Schema,
        OT::Sequence => constants::ObjectType::Sequence,
        OT::ForeignServer | OT::Server => constants::ObjectType::Server,
        OT::Table => constants::ObjectType::Table,
        OT::Type => constants::ObjectType::Type,
        OT::UserMapping => constants::ObjectType::UserMapping,
        OT::View => constants::ObjectType::View,
        // a DEFAULT ACL entry is tagged by its object type; the item is
        // the role, which is the entry's owner
        OT::DefaultAcl => {
            return Some(ObjectKey {
                desc: constants::ObjectType::DefaultPrivileges,
                schema: String::new(),
                name: entry.owner.clone()?,
            });
        }
        _ => return None,
    };
    let schema = match desc {
        constants::ObjectType::Extension
        | constants::ObjectType::ForeignDataWrapper
        | constants::ObjectType::ProceduralLanguage
        | constants::ObjectType::Schema
        | constants::ObjectType::Server
        | constants::ObjectType::UserMapping => String::new(),
        _ => entry.namespace.clone().unwrap_or_default(),
    };
    Some(ObjectKey {
        desc,
        schema,
        name: entry.tag.clone()?,
    })
}

/// `DESC namespace.tag` for plan labels
fn entry_label(entry: &libpgdump::Entry) -> String {
    let tag = entry.tag.as_deref().unwrap_or_default();
    match entry.namespace.as_deref() {
        Some(namespace) if !namespace.is_empty() => {
            format!("{} {namespace}.{tag}", entry.desc.as_str())
        }
        _ => format!("{} {tag}", entry.desc.as_str()),
    }
}

/// `ALTER <object> OWNER TO <owner>` for an archive entry, as pg_restore
/// writes it (`_getObjectDescription`). The object of a type that needs
/// a signature is its DROP statement without `DROP `. An entry with no
/// owner or no DROP statement, or of a type that has no owner of its
/// own (a cast, for example), gives None
fn owner_sql(entry: &libpgdump::Entry) -> Option<String> {
    let owner = entry.owner.as_deref().filter(|owner| !owner.is_empty())?;
    let drop = entry.drop_stmt.as_deref().filter(|drop| !drop.is_empty())?;
    let desc = entry.desc.as_str();
    let object = match desc {
        "COLLATION"
        | "CONVERSION"
        | "DOMAIN"
        | "FOREIGN TABLE"
        | "MATERIALIZED VIEW"
        | "SEQUENCE"
        | "STATISTICS"
        | "TABLE"
        | "TEXT SEARCH DICTIONARY"
        | "TEXT SEARCH CONFIGURATION"
        | "TYPE"
        | "VIEW"
        | "PROCEDURAL LANGUAGE"
        | "SCHEMA"
        | "EVENT TRIGGER"
        | "FOREIGN DATA WRAPPER"
        | "SERVER"
        | "PUBLICATION"
        | "SUBSCRIPTION" => {
            let tag = quote_ident(entry.tag.as_deref()?);
            match entry.namespace.as_deref() {
                Some(namespace) if !namespace.is_empty() => {
                    format!("{desc} {}.{tag}", quote_ident(namespace))
                }
                _ => format!("{desc} {tag}"),
            }
        }
        "AGGREGATE" | "FUNCTION" | "OPERATOR" | "OPERATOR CLASS"
        | "OPERATOR FAMILY" | "PROCEDURE" => drop
            .strip_prefix("DROP ")?
            .trim_end_matches(['\n', ';'])
            .to_string(),
        _ => return None,
    };
    Some(format!("ALTER {object} OWNER TO {};\n", quote_ident(owner)))
}

/// Human-readable comparison source for the header and logs
fn source_label(args: &cli::Deploy) -> String {
    match &args.dump {
        Some(path) => format!("dump {}", path.display()),
        None => format!(
            "{}:{}/{}",
            args.connection.host,
            args.connection.port,
            args.connection.dbname.as_deref().unwrap_or_default()
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use clap::Parser;

    use super::*;
    use crate::models::Function;

    fn function(json: serde_json::Value) -> Function {
        serde_json::from_value(json).expect("function deserializes")
    }

    /// A removed function whose database-side name (with a named
    /// parameter, per `function_key_name`) diverges from the archive
    /// tag it was dropped from (types only) must still be matched by
    /// the ordered drop pass, so it drops before the function it
    /// depends on rather than falling into the unordered fallback
    /// (finding L5)
    #[test]
    fn removed_function_with_named_parameter_drops_in_dependency_order() {
        let dep_fn = function(serde_json::json!({
            "name": "dep",
            "schema": "public",
            "owner": "postgres",
            "returns": "integer",
            "language": "sql",
            "parameters": [],
        }));
        let main_fn = function(serde_json::json!({
            "name": "main",
            "schema": "public",
            "owner": "postgres",
            "returns": "integer",
            "language": "sql",
            "parameters": [
                {"mode": "IN", "data_type": "integer", "name": "x"},
            ],
        }));
        let dep_key = ObjectKey::new(
            constants::ObjectType::Function,
            &Definition::Function(dep_fn.clone()),
        );
        let main_key = ObjectKey::new(
            constants::ObjectType::Function,
            &Definition::Function(main_fn.clone()),
        );
        // the database-side identity signature includes the
        // parameter name, unlike the archive tag it must match below
        assert_eq!(main_key.name, "main(x integer)");

        let mut diff = Diff {
            items: BTreeMap::new(),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
        };
        diff.removed
            .insert(dep_key.clone(), Definition::Function(dep_fn));
        diff.removed
            .insert(main_key.clone(), Definition::Function(main_fn));

        // pg_dump archives store entries in dependency order: `dep`
        // before `main`, which depends on it; the TOC tag carries
        // only parameter types, never argument names
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let dep_id = snapshot
            .add_entry(
                libpgdump::ObjectType::Function,
                Some("public"),
                Some("dep()"),
                None,
                None,
                None,
                None,
                &[],
            )
            .expect("add dep entry");
        snapshot
            .add_entry(
                libpgdump::ObjectType::Function,
                Some("public"),
                Some("main(integer)"),
                None,
                None,
                None,
                None,
                &[dep_id],
            )
            .expect("add main entry");

        let output = build::BuildOutput {
            dump: libpgdump::new("test", "UTF8", "18.0")
                .expect("new output dump"),
            item_ids: std::collections::HashMap::new(),
        };
        let cli = cli::Cli::parse_from([
            "pglifecycle",
            "deploy",
            "--allow-drop",
            "proj",
        ]);
        let args = match cli.action {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };

        let plan = plan(&diff, &BTreeMap::new(), &output, &snapshot, &args)
            .expect("plan succeeds");

        assert!(plan.excluded.is_empty());
        assert_eq!(
            plan.included
                .iter()
                .map(|s| s.label.clone())
                .collect::<Vec<_>>(),
            vec![main_key.to_string(), dep_key.to_string()],
            "main (the dependent) must drop before dep (its \
             dependency), which requires the removed main() to be \
             matched by the ordered pass despite its named-parameter \
             key diverging from the archive tag"
        );
    }

    /// The default privileges of a role that only the database has are
    /// keyed by the owner of their DEFAULT ACL entries, and their
    /// removal is withheld without --allow-drop, with a warning
    #[test]
    fn removed_default_privileges_are_withheld_and_fail_open() {
        let defaults: crate::models::DefaultPrivileges =
            serde_json::from_value(serde_json::json!({
                "name": "app",
                "grants": [{"object_type": "TABLES", "grantee": "PUBLIC",
                            "privileges": ["SELECT"]}],
            }))
            .expect("default privileges deserialize");
        let definition = Definition::DefaultPrivileges(defaults);
        let key = ObjectKey::new(
            constants::ObjectType::DefaultPrivileges,
            &definition,
        );
        let mut diff = Diff {
            items: BTreeMap::new(),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
        };
        diff.removed.insert(key.clone(), definition);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        snapshot
            .add_entry(
                libpgdump::ObjectType::DefaultAcl,
                None,
                Some("DEFAULT PRIVILEGES FOR TABLES"),
                Some("app"),
                None,
                None,
                None,
                &[],
            )
            .expect("add default acl entry");
        let entry = snapshot
            .entries()
            .iter()
            .find(|e| e.desc == libpgdump::ObjectType::DefaultAcl)
            .expect("default acl entry");
        assert_eq!(entry_key(entry), Some(key.clone()));
        let output = build::BuildOutput {
            dump: libpgdump::new("test", "UTF8", "18.0")
                .expect("new output dump"),
            item_ids: std::collections::HashMap::new(),
        };
        let cli = cli::Cli::parse_from(["pglifecycle", "deploy", "proj"]);
        let args = match cli.action {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };
        let plan = plan(&diff, &BTreeMap::new(), &output, &snapshot, &args)
            .expect("plan succeeds");
        assert!(plan.included.is_empty());
        assert_eq!(plan.excluded.len(), 1);
        assert_eq!(plan.excluded[0].label, "DEFAULT PRIVILEGES app");
        assert!(plan.excluded[0].fails_open);
        assert_eq!(
            plan.excluded[0].sql,
            "ALTER DEFAULT PRIVILEGES FOR ROLE app REVOKE SELECT ON TABLES \
             FROM PUBLIC;\n"
        );
    }

    /// A project role with no declarations makes no archive entry. When
    /// the database has other defaults for it, the plan still gives
    /// back the built-in ones, and does not gate them
    #[test]
    fn changed_default_privileges_with_no_entry_are_planned() {
        let defaults = |value: serde_json::Value| {
            serde_json::from_value::<crate::models::DefaultPrivileges>(value)
                .expect("default privileges deserialize")
        };
        let repo = defaults(serde_json::json!({"name": "app"}));
        let db = defaults(serde_json::json!({
            "name": "app",
            "grants": [{"object_type": "TABLES", "grantee": "PUBLIC",
                        "privileges": ["SELECT"]}],
        }));
        let mut diff = Diff {
            items: BTreeMap::new(),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
        };
        diff.items.insert(0, Change::Changed);
        diff.changed
            .insert(0, Definition::DefaultPrivileges(db.clone()));
        let resolutions = BTreeMap::from([(
            0,
            alter::default_privileges::default_privileges(&repo, &db),
        )]);
        let output = build::BuildOutput {
            dump: libpgdump::new("test", "UTF8", "18.0")
                .expect("new output dump"),
            item_ids: std::collections::HashMap::new(),
        };
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let parse = |argv: &[&str]| match cli::Cli::parse_from(argv).action {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };
        let args = parse(&["pglifecycle", "deploy", "proj"]);
        let plan = plan(&diff, &resolutions, &output, &snapshot, &args)
            .expect("plan succeeds");
        assert!(plan.excluded.is_empty());
        assert_eq!(plan.included.len(), 1);
        assert_eq!(plan.included[0].label, "DEFAULT PRIVILEGES app ON TABLES");
        assert_eq!(
            plan.included[0].sql,
            "ALTER DEFAULT PRIVILEGES FOR ROLE app REVOKE SELECT ON TABLES \
             FROM PUBLIC;\n"
        );
        // --no-privileges leaves default privileges as they are
        let args = parse(&["pglifecycle", "deploy", "-x", "proj"]);
        let unchanged =
            super::plan(&diff, &resolutions, &output, &snapshot, &args)
                .expect("plan succeeds");
        assert!(unchanged.included.is_empty());
    }

    /// PostgreSQL applies default privileges only when it creates an
    /// object, so changed default privileges go before a table that
    /// the same deploy creates, and only once, not again at their
    /// DEFAULT ACL entry
    #[test]
    fn changed_default_privileges_go_before_new_objects() {
        let defaults = |value: serde_json::Value| {
            serde_json::from_value::<crate::models::DefaultPrivileges>(value)
                .expect("default privileges deserialize")
        };
        let repo = defaults(serde_json::json!({
            "name": "app",
            "grants": [{"object_type": "SEQUENCES", "grantee": "PUBLIC",
                        "privileges": ["USAGE"]}],
        }));
        let db = defaults(serde_json::json!({
            "name": "app",
            "grants": [
                {"object_type": "SEQUENCES", "grantee": "PUBLIC",
                 "privileges": ["USAGE"]},
                {"object_type": "TABLES", "grantee": "PUBLIC",
                 "privileges": ["SELECT"]},
            ],
        }));
        let mut diff = Diff {
            items: BTreeMap::new(),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
        };
        diff.items.insert(0, Change::Changed);
        diff.items.insert(1, Change::Added);
        diff.changed
            .insert(0, Definition::DefaultPrivileges(db.clone()));
        let resolutions = BTreeMap::from([(
            0,
            alter::default_privileges::default_privileges(&repo, &db),
        )]);
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new output dump");
        let table = dump
            .add_entry(
                libpgdump::ObjectType::Table,
                Some("public"),
                Some("t"),
                Some("app"),
                Some("CREATE TABLE public.t (id integer);\n"),
                None,
                None,
                &[],
            )
            .expect("add table entry");
        let acl = dump
            .add_entry(
                libpgdump::ObjectType::DefaultAcl,
                None,
                Some("DEFAULT PRIVILEGES FOR SEQUENCES"),
                Some("app"),
                Some(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE app GRANT USAGE ON \
                     SEQUENCES TO PUBLIC;\n",
                ),
                None,
                None,
                &[],
            )
            .expect("add default acl entry");
        let output = build::BuildOutput {
            dump,
            item_ids: std::collections::HashMap::from([(acl, 0), (table, 1)]),
        };
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let args = match cli::Cli::parse_from(["pglifecycle", "deploy", "p"])
            .action
        {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };
        let plan = plan(&diff, &resolutions, &output, &snapshot, &args)
            .expect("plan succeeds");
        let sql: Vec<&str> =
            plan.included.iter().map(|s| s.sql.as_str()).collect();
        assert_eq!(
            sql,
            [
                "ALTER DEFAULT PRIVILEGES FOR ROLE app REVOKE SELECT ON \
                 TABLES FROM PUBLIC;\n",
                "CREATE TABLE public.t (id integer);\n",
            ]
        );
    }

    /// ALTER DEFAULT PRIVILEGES IN SCHEMA fails when the schema does not
    /// exist. A changed statement in a schema that the same deploy
    /// creates goes directly after its CREATE SCHEMA, before the tables
    /// of that schema and only once. A statement in a schema that the
    /// database has goes before the archive entries
    #[test]
    fn changed_default_privileges_in_a_new_schema_follow_it() {
        let defaults = |value: serde_json::Value| {
            serde_json::from_value::<crate::models::DefaultPrivileges>(value)
                .expect("default privileges deserialize")
        };
        let repo = defaults(serde_json::json!({
            "name": "app",
            "grants": [{"schema": "fresh", "object_type": "TABLES",
                        "grantee": "reader", "privileges": ["SELECT"]}],
        }));
        let db = defaults(serde_json::json!({
            "name": "app",
            "grants": [{"schema": "public", "object_type": "TABLES",
                        "grantee": "PUBLIC", "privileges": ["SELECT"]}],
        }));
        let mut diff = Diff {
            items: BTreeMap::new(),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
        };
        diff.items.insert(0, Change::Changed);
        diff.items.insert(1, Change::Added);
        diff.items.insert(2, Change::Added);
        diff.changed
            .insert(0, Definition::DefaultPrivileges(db.clone()));
        let resolutions = BTreeMap::from([(
            0,
            alter::default_privileges::default_privileges(&repo, &db),
        )]);
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new output dump");
        let schema = dump
            .add_entry(
                libpgdump::ObjectType::Schema,
                None,
                Some("fresh"),
                Some("app"),
                Some("CREATE SCHEMA fresh;\n"),
                None,
                None,
                &[],
            )
            .expect("add schema entry");
        let table = dump
            .add_entry(
                libpgdump::ObjectType::Table,
                Some("fresh"),
                Some("t"),
                Some("app"),
                Some("CREATE TABLE fresh.t (id integer);\n"),
                None,
                None,
                &[schema],
            )
            .expect("add table entry");
        let acl = dump
            .add_entry(
                libpgdump::ObjectType::DefaultAcl,
                Some("fresh"),
                Some("DEFAULT PRIVILEGES FOR TABLES"),
                Some("app"),
                Some(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE app IN SCHEMA fresh \
                     GRANT SELECT ON TABLES TO reader;\n",
                ),
                None,
                None,
                &[schema],
            )
            .expect("add default acl entry");
        let output = build::BuildOutput {
            dump,
            item_ids: std::collections::HashMap::from([
                (acl, 0),
                (schema, 1),
                (table, 2),
            ]),
        };
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let args = match cli::Cli::parse_from(["pglifecycle", "deploy", "p"])
            .action
        {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };
        let plan = plan(&diff, &resolutions, &output, &snapshot, &args)
            .expect("plan succeeds");
        assert!(plan.excluded.is_empty());
        let sql: Vec<&str> =
            plan.included.iter().map(|s| s.sql.as_str()).collect();
        assert_eq!(
            sql,
            [
                "ALTER DEFAULT PRIVILEGES FOR ROLE app IN SCHEMA public \
                 REVOKE SELECT ON TABLES FROM PUBLIC;\n",
                "CREATE SCHEMA fresh;\n",
                "ALTER DEFAULT PRIVILEGES FOR ROLE app IN SCHEMA fresh \
                 GRANT SELECT ON TABLES TO reader;\n",
                "CREATE TABLE fresh.t (id integer);\n",
            ]
        );
    }

    /// A database-only procedure is keyed by its archive tag, so it
    /// drops in dependency order, and its drop quotes its names and
    /// has its input types only
    #[test]
    fn removed_procedure_drops_by_its_tag() {
        let procedure: crate::models::Procedure =
            serde_json::from_value(serde_json::json!({
                "name": "Stray Proc",
                "schema": "Quoted Schema",
                "owner": "postgres",
                "parameters": [
                    {"mode": "IN", "name": "Arg", "data_type": "integer"},
                    {"mode": "OUT", "name": "b", "data_type": "text"},
                ],
                "language": "sql",
                "definition": "SELECT 'x'",
            }))
            .expect("procedure deserializes");
        let definition = Definition::Procedure(procedure);
        let key =
            ObjectKey::new(constants::ObjectType::Procedure, &definition);
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        snapshot
            .add_entry(
                libpgdump::ObjectType::Procedure,
                Some("Quoted Schema"),
                Some("Stray Proc(integer)"),
                None,
                None,
                None,
                None,
                &[],
            )
            .expect("add procedure entry");
        assert_eq!(
            snapshot.entries().iter().rev().find_map(entry_key),
            Some(drop_match_key(&key, &definition))
        );
        assert_eq!(
            drop_sql(&key, Some(&definition)),
            "DROP PROCEDURE IF EXISTS \"Quoted Schema\".\"Stray \
             Proc\"(integer);\n"
        );
    }

    /// A kept index drop with a newline in its quoted name must stay
    /// fully commented out, so no part of it can run
    #[test]
    fn kept_index_drop_with_newline_is_fully_commented() {
        let plan = Plan {
            included: Vec::new(),
            excluded: Vec::new(),
            kept: vec![Statement {
                label: "INDEX public.bad".to_string(),
                sql: "DROP INDEX IF EXISTS public.\"a\nDROP TABLE t; \
                      --\";\n"
                    .to_string(),
                fails_open: false,
            }],
            included_destructive: 0,
        };
        let script = render_script(&plan, "test", "db");
        for line in script.lines() {
            assert!(line.starts_with("--"), "line runs as SQL: {line}");
        }
        assert!(script.contains("--   DROP TABLE t; --\";\n"));
    }

    fn owner_entry(
        desc: libpgdump::ObjectType,
        namespace: Option<&str>,
        tag: &str,
        drop: Option<&str>,
    ) -> libpgdump::Entry {
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let id = dump
            .add_entry(
                desc,
                namespace,
                Some(tag),
                Some("Gate Owner"),
                Some("CREATE ...;\n"),
                drop,
                None,
                &[],
            )
            .expect("add entry");
        dump.entries()
            .iter()
            .find(|entry| entry.dump_id == id)
            .expect("the added entry")
            .clone()
    }

    /// The owner statement has the form that pg_restore writes: the
    /// qualified, quoted name, or the DROP statement of a type that
    /// needs a signature. A type with no owner of its own, or an entry
    /// with no DROP statement, gets none
    #[test]
    fn owner_statements_have_the_pg_restore_form() {
        use libpgdump::ObjectType as OT;
        let sql = |entry: libpgdump::Entry| owner_sql(&entry);
        assert_eq!(
            sql(owner_entry(
                OT::Table,
                Some("My Schema"),
                "t",
                Some("DROP TABLE \"My Schema\".t;\n"),
            ))
            .as_deref(),
            Some("ALTER TABLE \"My Schema\".t OWNER TO \"Gate Owner\";\n")
        );
        assert_eq!(
            sql(owner_entry(
                OT::Schema,
                None,
                "app",
                Some("DROP SCHEMA app;\n"),
            ))
            .as_deref(),
            Some("ALTER SCHEMA app OWNER TO \"Gate Owner\";\n")
        );
        assert_eq!(
            sql(owner_entry(
                OT::Function,
                Some("test"),
                "f(n integer)",
                Some("DROP FUNCTION test.f(n integer);\n"),
            ))
            .as_deref(),
            Some(
                "ALTER FUNCTION test.f(n integer) OWNER TO \"Gate Owner\";\n"
            )
        );
        assert_eq!(
            sql(owner_entry(
                OT::Cast,
                None,
                "CAST (text AS integer)",
                Some("DROP CAST (text AS integer);\n"),
            )),
            None
        );
        assert_eq!(sql(owner_entry(OT::Table, Some("test"), "t", None)), None);
    }

    /// A new object gets its owner after its CREATE, and an object that
    /// the database has with another owner gets it in place, which is
    /// not destructive. With --no-owner, deploy sets no owner
    #[test]
    fn owners_are_set_after_create_and_in_place() {
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new output dump");
        let mut table = |name: &str| {
            dump.add_entry(
                libpgdump::ObjectType::Table,
                Some("public"),
                Some(name),
                Some("app"),
                Some(&format!("CREATE TABLE public.{name} ();\n")),
                Some(&format!("DROP TABLE public.{name};\n")),
                None,
                &[],
            )
            .expect("add table entry")
        };
        let added = table("added");
        let kept = table("kept");
        let output = build::BuildOutput {
            dump,
            item_ids: HashMap::from([(added, 0), (kept, 1)]),
        };
        let diff = Diff {
            items: BTreeMap::from([
                (0, Change::Added),
                (1, Change::Unchanged),
            ]),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::from([0, 1]),
            owner_changed: BTreeSet::from([1]),
        };
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let plan_with = |argv: &[&str]| {
            let args = match cli::Cli::parse_from(argv).action {
                cli::Action::Deploy(deploy) => deploy,
                _ => unreachable!("parsed the deploy subcommand"),
            };
            plan(&diff, &BTreeMap::new(), &output, &snapshot, &args)
                .expect("plan succeeds")
        };
        let owned = plan_with(&["pglifecycle", "deploy", "p"]);
        let sql: Vec<&str> =
            owned.included.iter().map(|s| s.sql.as_str()).collect();
        assert_eq!(
            sql,
            [
                "CREATE TABLE public.added ();\nALTER TABLE public.added \
                 OWNER TO app;\n",
                "ALTER TABLE public.kept OWNER TO app;\n",
            ]
        );
        assert!(owned.excluded.is_empty());
        assert_eq!(owned.included_destructive, 0);
        let unowned = plan_with(&["pglifecycle", "deploy", "-O", "p"]);
        let sql: Vec<&str> =
            unowned.included.iter().map(|s| s.sql.as_str()).collect();
        assert_eq!(sql, ["CREATE TABLE public.added ();\n"]);
    }
}
