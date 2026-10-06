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
mod database;
mod dependents;
mod diff;
mod privileges;
mod routine_body;
mod serial;

pub(crate) use serial::integer_type as serial_integer_type;

pub(crate) use diff::{
    UserTypes, canonical_casts, canonical_check, canonical_collation,
    identity_type, is_built_in, stored_null_default,
};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::IsTerminal;

use crate::ddl::{self, NodeExt};
use crate::deploy::alter::Resolution;
use crate::deploy::diff::{Change, Diff, ObjectKey};
use crate::models::{Definition, Item};
use crate::utils::{quote_ident, user_mapping_subject};
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
    let mut project = project::load(&args.project)?;
    // one item for each text search object, not for each schema
    alter::text_search::split_inventory(&mut project.inventory);
    // a NULL default in the form that PostgreSQL stores, which the
    // types of the project tell
    diff::store_null_defaults(&mut project);
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
    let conflicts =
        sequence_owner_conflicts(&project.inventory, &assembly.sequences);
    if !args.no_owner && !conflicts.is_empty() {
        log::warn!(
            "A column owns each of these sequences, and PostgreSQL needs \
             the owner of its table for it. Give each sequence the owner \
             of its table, or the plan can fail: {}",
            conflicts.join(", ")
        );
    }
    // a serial column in the form that PostgreSQL stores, with the
    // sequence that the column owns in the database
    let implied = serial::expand(&mut project, &assembly);
    let task = progress::spinner("Diffing project against database");
    let mut diff = diff::diff(&project, &assembly);
    // --no-privileges keeps the default privileges that a dump has
    if args.no_privileges {
        diff.removed.retain(|key, _| {
            key.desc != constants::ObjectType::DefaultPrivileges
        });
    }
    let groups = partition_index_groups(&project, &mut diff);
    let families = alter::operator_class::families(&project, &assembly);
    let mut resolutions = resolutions(&project, &diff, &groups, &families);
    let dependents = dependents::rebuild(
        &project,
        &mut diff,
        &mut resolutions,
        &snapshot,
        &groups,
        &families,
        args.allow_drop_indexes,
    );
    task.finish();
    if !dependents.refused.is_empty() {
        let message = format!(
            "The plan drops objects that other objects depend on, and \
             deploy cannot drop and make these dependents again: {}",
            dependents.refused.join("; ")
        );
        // without --allow-drop the drops are withheld, thus the plan
        // stays usable
        if args.allow_drop {
            return Err(message);
        }
        log::warn!("{message}");
    }
    let mut output = build::assemble(&project)?;
    without_empty_statements(&mut output);
    let task = progress::spinner("Planning changes");
    output.dump.sort_entries();
    let privileges = privileges::plan(
        &project,
        &implied,
        &diff,
        &resolutions,
        &output,
        &snapshot,
        args,
        creator(args).as_deref(),
    )?;
    let mut plan = plan(
        &diff,
        &resolutions,
        &dependents,
        &output,
        &snapshot,
        &privileges,
        args,
    )?;
    let (settings, resets) =
        database::statements(&project.settings, &assembly);
    plan.included.extend(settings);
    plan.resets = resets;
    task.finish();
    report(&diff, &plan, &assembly);
    // a --dump file has no roles, thus only a live database is checked
    if args.dump.is_none() {
        check_roles(&plan, &args.connection)?;
        check_reads(&project.inventory, &diff, &args.connection);
    }
    let script = render_script(
        &plan,
        &project.name,
        &source,
        args.connection.role.as_deref(),
    );
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

/// The role that runs the script and thus makes the new objects:
/// `--role`, or the user of the connection. A `--dump` file does not
/// say which role runs the script, thus there is none
fn creator(args: &cli::Deploy) -> Option<String> {
    // --no-privileges compares no privileges, thus no query is necessary
    if args.no_privileges {
        return None;
    }
    if args.dump.is_some() {
        log::debug!(
            "A dump does not give the role that runs the script; deploy \
             assumes that it has the built-in default privileges"
        );
        return None;
    }
    if let Some(role) = &args.connection.role {
        return Some(role.clone());
    }
    match pgdump::current_user(&args.connection) {
        Ok(user) => Some(user),
        Err(error) => {
            log::warn!(
                "Cannot read the user of the connection, thus new objects \
                 can keep the default privileges of that user: {error}"
            );
            None
        }
    }
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
    families: &alter::operator_class::Families,
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
            (*id, alter::resolve_with(repo, database, groups, families))
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
    /// The ACL, comment and security label entries that no project item
    /// owns: their object is not in the project, so the plan does not
    /// have them
    unowned: Vec<String>,
    /// The settings of the database, and of a role in it, that the
    /// plan resets, as `name of label`. A RESET is not destructive, but
    /// it can remove a setting that a DBA made
    resets: Vec<String>,
    /// The objects that the plan drops and makes again, because they
    /// depend on an object that it drops (see [`dependents`])
    dependents: Vec<String>,
}

/// Assemble the ordered plan: OWNED BY NONE of changed sequences first,
/// then DROPs for database-only objects, for the replaced objects that
/// have dependents, and for these dependents (reverse snapshot order),
/// then
/// changed default privileges (those in a new schema directly after
/// its CREATE SCHEMA), then the repo archive's entries in topological
/// order — plain CREATEs for added objects, in-place ALTERs where a
/// renderer exists, gated drop+recreate otherwise
fn plan(
    diff: &Diff,
    resolutions: &BTreeMap<usize, Resolution>,
    dependents: &dependents::Dependents,
    output: &build::BuildOutput,
    snapshot: &libpgdump::Dump,
    privileges: &privileges::Privileges,
    args: &cli::Deploy,
) -> Result<Plan, String> {
    let mut included = Vec::new();
    let mut excluded = Vec::new();
    let mut included_destructive = 0usize;
    let mut kept = Vec::new();
    let mut unowned = Vec::new();
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
    // a drop of a column or of a table drops each sequence that it
    // owns, thus a changed sequence is unlinked before all drops
    for entry in output.dump.entries() {
        let Some(Resolution::Statements(alters)) = output
            .item_ids
            .get(&entry.dump_id)
            .filter(|id| diff.items.get(id) == Some(&Change::Changed))
            .and_then(|id| resolutions.get(id))
        else {
            continue;
        };
        for alter in alters.iter().filter(|alter| alter.unlinks) {
            push(
                false,
                Statement {
                    label: entry_label(entry),
                    sql: alter.sql.clone(),
                    fails_open: false,
                },
            );
        }
    }
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
    let filtered = !(args.exclude_table.is_empty()
        && args.exclude_schema.is_empty()
        && args.exclude_extension.is_empty());
    for entry in snapshot.entries().iter().rev() {
        // a replaced object with dependents, or a dependent of an
        // object that the plan drops: the plan makes it again, or the
        // project does not have it
        for alter in dependents.drops.get(&entry.dump_id).into_iter().flatten()
        {
            let statement = Statement {
                label: alter.label.clone().unwrap_or_default(),
                sql: alter.sql.clone(),
                fails_open: alter.fails_open,
            };
            if alter.index_removal && !args.allow_drop_indexes {
                kept.push(statement);
                continue;
            }
            push(alter.destructive, statement);
        }
        let Some(key) =
            entry_key(entry).and_then(|key| wanted.get(&key).copied())
        else {
            continue;
        };
        if emitted.insert(key) {
            let definition = diff.removed.get(key);
            // a base type drops with CASCADE, which must not drop an
            // object that the project keeps. Without CASCADE the drop
            // fails, and PostgreSQL names the objects
            let kept = cascade_keeps(key, snapshot, &wanted);
            let sql = if !kept.is_empty() {
                log::warn!(
                    "{key}: drop does not cascade, because these objects \
                     that the project keeps depend on it: {}",
                    kept.join(", ")
                );
                drop_sql(key, None)
            } else if filtered && cascades(definition) {
                // the snapshot does not have the excluded objects, thus
                // CASCADE can drop an object that the plan cannot see
                log::warn!(
                    "{key}: drop does not cascade, because the snapshot \
                     excludes objects that can depend on it"
                );
                drop_sql(key, None)
            } else {
                drop_sql(key, definition)
            };
            push(
                true,
                Statement {
                    label: key.to_string(),
                    sql,
                    fails_open: drop_fails_open(definition),
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
    // the archive position of each function and sequence that this
    // deploy creates, by schema and name. A function that depends on a
    // changed table comes after the table, and so does a sequence that
    // sorts after it, but a statement of the table can call them
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_postgres::LANGUAGE.into())
        .map_err(|e| e.to_string())?;
    let functions = created_entries(
        &mut parser,
        output,
        diff,
        resolutions,
        libpgdump::ObjectType::Function,
    );
    let sequences = created_entries(
        &mut parser,
        output,
        diff,
        resolutions,
        libpgdump::ObjectType::Sequence,
    );
    let shells = new_shell_types(output, diff);
    // the statements that wait for a function or a sequence, by its
    // archive position
    let mut waiting: BTreeMap<usize, Vec<(bool, Statement)>> = BTreeMap::new();
    // OWNED BY of a changed sequence, after each owner change
    let mut links = Vec::new();
    for (position, entry) in output.dump.entries().iter().enumerate() {
        let later = waiting.split_off(&position);
        for (destructive, statement) in std::mem::replace(&mut waiting, later)
            .into_values()
            .flatten()
        {
            push(destructive, statement);
        }
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
        // a shell type has no dependencies, thus no owners. It comes
        // only with a new base type
        if entry.desc == libpgdump::ObjectType::ShellType {
            if shells.contains(&entry.dump_id)
                && let Some(defn) = &entry.defn
            {
                push(
                    false,
                    Statement {
                        label: entry_label(entry),
                        sql: defn.clone(),
                        fails_open: false,
                    },
                );
            }
            continue;
        }
        let owners = entry_owners(entry, output, &entries_by_id);
        if owners.is_empty() {
            // the statements of `database` change the comment of the
            // database
            let database_comment = entry.desc
                == libpgdump::ObjectType::Comment
                && entry
                    .tag
                    .as_deref()
                    .is_some_and(|tag| tag.starts_with("DATABASE "));
            if !database_comment
                && matches!(
                    entry.desc,
                    libpgdump::ObjectType::Acl
                        | libpgdump::ObjectType::Comment
                        | libpgdump::ObjectType::SecurityLabel
                )
            {
                unowned.push(entry_label(entry));
            }
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
            // OWNED BY of a new sequence is a link: it comes after the
            // statements that add the column or change the owner of
            // its table
            let defn = match owned_by(entry, &defn) {
                Some((create, sql, column)) => {
                    links.push((
                        rebuild_adds(&column, diff, resolutions),
                        Statement {
                            label: label.clone(),
                            sql,
                            fails_open: false,
                        },
                    ));
                    create
                }
                None => defn,
            };
            // a CREATE that needs the DROP of a database-only object,
            // and its child entries, are withheld with the DROP
            push(
                owners.iter().any(|id| diff.gated.contains(id)),
                Statement {
                    label: label.clone(),
                    sql: format!(
                        "{defn}{}{}",
                        owner.unwrap_or_default(),
                        after_create(privileges, entry)
                    ),
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
                    // a statement that calls a function that comes
                    // later waits for it, and so do the statements
                    // after it, so that they keep their order
                    let mut after = None;
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
                        if alter.unlinks {
                            continue;
                        }
                        if let Some(column) = &alter.links {
                            links.push((
                                rebuild_adds(column, diff, resolutions),
                                statement,
                            ));
                            continue;
                        }
                        after = after.max(calls_later(
                            &mut parser,
                            &alter.sql,
                            &functions,
                            &sequences,
                            position,
                        ));
                        match after {
                            Some(function) => waiting
                                .entry(function)
                                .or_default()
                                .push((alter.destructive, statement)),
                            None => push(alter.destructive, statement),
                        }
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
                resolution => {
                    let mut sql = String::new();
                    match resolution {
                        Some(Resolution::Rebuild { before, drop }) => {
                            for alter in before {
                                push(
                                    true,
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
                            sql.push_str(drop);
                        }
                        _ => {
                            if let Some(drop) = &entry.drop_stmt {
                                sql.push_str(drop);
                            }
                        }
                    }
                    sql.push_str(&defn);
                    if let Some(owner) = &owner {
                        sql.push_str(owner);
                    }
                    sql.push_str(&after_create(privileges, entry));
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
                && matches!(
                    resolutions.get(id),
                    Some(Resolution::Replace | Resolution::Rebuild { .. })
                )
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
    for (destructive, statement) in waiting.into_values().flatten() {
        push(destructive, statement);
    }
    // a link to a column that only the rebuild of its table adds is
    // withheld with the rebuild
    for (rebuilt, statement) in links {
        push(rebuilt && !args.allow_drop, statement);
    }
    // the privileges of the objects that the database has, after each
    // statement that changes an object or its owner
    for alter in &privileges.existing {
        push(
            alter.destructive,
            Statement {
                label: alter.label.clone().unwrap_or_default(),
                sql: alter.sql.clone(),
                fails_open: alter.fails_open,
            },
        );
    }
    Ok(Plan {
        included,
        excluded,
        kept,
        included_destructive,
        unowned,
        resets: Vec::new(),
        dependents: dependents.labels.clone(),
    })
}

/// The items that own an archive entry: the entry's own item, or
/// else the items that the dependency graph reaches, so that comments
/// and ACLs on child entries still map to the object that owns them
fn entry_owners(
    entry: &libpgdump::Entry,
    output: &build::BuildOutput,
    entries_by_id: &HashMap<i32, &libpgdump::Entry>,
) -> Vec<usize> {
    if let Some(id) = output.item_ids.get(&entry.dump_id) {
        return vec![*id];
    }
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

/// The privilege statements that come directly after the CREATE of an
/// archive entry, in the same plan statement
fn after_create(
    privileges: &privileges::Privileges,
    entry: &libpgdump::Entry,
) -> String {
    privileges
        .after_create
        .get(&entry.dump_id)
        .cloned()
        .unwrap_or_default()
}

/// Warn about each role that the script needs and the database does
/// not have. deploy does not make roles, thus the script fails on the
/// first statement that names one
fn check_roles(plan: &Plan, conn: &cli::Connection) -> Result<(), String> {
    let roles = match pull::cluster_roles(conn) {
        Ok(roles) => roles,
        Err(error) => {
            log::warn!(
                "Cannot read the roles of the database, thus deploy does \
                 not check the roles of the script: {error}"
            );
            return Ok(());
        }
    };
    for (role, labels) in missing_roles(&plan.included, &roles)? {
        log::warn!(
            "Role {} is not in the database, and deploy does not make \
             roles; the script fails until it exists. It is needed by: {}",
            quote_ident(&role),
            labels.join(", ")
        );
    }
    Ok(())
}

/// The sequences that a column of a project table owns, with another
/// owner than the table. PostgreSQL gives such a sequence the owner of
/// its table, and refuses an owner change of the sequence on its own.
/// When the project has no table for the column, the database
/// sequence that the same column owns gives the owner
fn sequence_owner_conflicts(
    inventory: &[Item],
    database: &[crate::models::Sequence],
) -> Vec<String> {
    use alter::names::name;
    let columns: HashMap<String, &str> = inventory
        .iter()
        .filter_map(|item| match &item.definition {
            Definition::Table(table) => Some(table),
            _ => None,
        })
        .flat_map(|table| {
            table.columns.iter().flatten().map(move |column| {
                let key = name(&format!(
                    "{}.{}.{}",
                    quote_ident(&table.schema),
                    quote_ident(&table.name),
                    quote_ident(&column.name)
                ));
                (key, table.owner.as_str())
            })
        })
        .collect();
    inventory
        .iter()
        .filter_map(|item| match &item.definition {
            Definition::Sequence(sequence) => Some(sequence),
            _ => None,
        })
        .filter(|sequence| {
            let Some(column) = sequence.owned_by.as_deref() else {
                return false;
            };
            if let Some(owner) = columns.get(&name(column)) {
                return *owner != sequence.owner;
            }
            database.iter().any(|db| {
                db.schema == sequence.schema
                    && db.name == sequence.name
                    && !db.owner.is_empty()
                    && db.owner != sequence.owner
                    && db.owned_by.as_deref().map(name) == Some(name(column))
            })
        })
        .map(|sequence| {
            format!(
                "{}.{}",
                quote_ident(&sequence.schema),
                quote_ident(&sequence.name)
            )
        })
        .collect()
}

/// Warn when the plan changes objects that the role that read the
/// database cannot read. pg_dump does not dump them, or dumps them
/// without their options, thus the plan can make again what the
/// database has already
fn check_reads(inventory: &[Item], diff: &Diff, conn: &cli::Connection) {
    let limits = match pgdump::read_limits(conn) {
        Ok(limits) => limits,
        Err(error) => {
            log::warn!(
                "Cannot read what the role of the connection can read, \
                 thus deploy does not check it: {error}"
            );
            return;
        }
    };
    let objects = unreadable(inventory, diff, &limits);
    if !objects.is_empty() {
        log::warn!(
            "Role {} cannot read all of the database, thus the plan can \
             make again objects that the database has already: {}. Read \
             the database as a role that can read them, for example a \
             superuser",
            quote_ident(&limits.role),
            objects.join(", ")
        );
    }
}

/// The labels of the added and changed items that the role of
/// `limits` cannot read: each subscription when the role is not a
/// superuser, and each user mapping whose options it cannot read
fn unreadable(
    inventory: &[Item],
    diff: &Diff,
    limits: &pgdump::ReadLimits,
) -> Vec<String> {
    let mut labels = Vec::new();
    for item in inventory {
        if !matches!(
            diff.items.get(&item.id),
            Some(Change::Added | Change::Changed)
        ) {
            continue;
        }
        match &item.definition {
            Definition::Subscription(s) if !limits.superuser => {
                labels.push(format!("SUBSCRIPTION {}", s.name));
            }
            Definition::UserMapping(mapping) => {
                // PostgreSQL reads public as PUBLIC
                let user = if mapping.name.eq_ignore_ascii_case("public") {
                    "PUBLIC"
                } else {
                    mapping.name.as_str()
                };
                for server in &mapping.servers {
                    let pair = (user.to_string(), server.name.clone());
                    if limits.hidden_user_mappings.contains(&pair) {
                        labels.push(format!(
                            "USER MAPPING {user} SERVER {}",
                            server.name
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    labels
}

/// Each role that `statements` name and `roles` does not have, with
/// the labels of the statements that name it. PUBLIC, the
/// CURRENT_USER forms and the reserved `pg_` roles are not roles that
/// a project makes, thus they are not checked
fn missing_roles<'a>(
    statements: &'a [Statement],
    roles: &BTreeSet<String>,
) -> Result<BTreeMap<String, Vec<&'a str>>, String> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_postgres::LANGUAGE.into())
        .map_err(|e| e.to_string())?;
    let mut missing: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for statement in statements {
        let Some(tree) = parser.parse(&statement.sql, None) else {
            continue;
        };
        for node in tree.root_node().find_all("RoleSpec") {
            let text = node.text(&statement.sql);
            let role = ddl::unquote(text);
            // a quoted keyword is an ordinary role name, but
            // PostgreSQL reads "public" as PUBLIC
            let keyword = !text.starts_with('"')
                && ["current_role", "current_user", "session_user"]
                    .contains(&role.as_str());
            if role == "public"
                || keyword
                || role.starts_with("pg_")
                || roles.contains(&role)
            {
                continue;
            }
            let labels = missing.entry(role).or_default();
            if labels.last() != Some(&statement.label.as_str()) {
                labels.push(&statement.label);
            }
        }
    }
    Ok(missing)
}

/// The column is in a table that the plan rebuilds, and the database
/// table does not have it: only the rebuild adds it
fn rebuild_adds(
    column: &str,
    diff: &Diff,
    resolutions: &BTreeMap<usize, Resolution>,
) -> bool {
    use alter::names::name;
    let column = name(column);
    diff.changed.iter().any(|(id, definition)| {
        let Definition::Table(table) = definition else {
            return false;
        };
        let prefix = format!(
            "{}.{}.",
            quote_ident(&table.schema),
            quote_ident(&table.name)
        );
        column.starts_with(&prefix)
            && matches!(
                resolutions.get(id),
                Some(Resolution::Replace | Resolution::Rebuild { .. })
            )
            && !table.columns.iter().flatten().any(|c| {
                name(&format!("{prefix}{}", quote_ident(&c.name))) == column
            })
    })
}

/// The CREATE SEQUENCE of a sequence entry without its OWNED BY
/// clause, the ALTER SEQUENCE that sets it, and its column. The build
/// writes the clause last
fn owned_by(
    entry: &libpgdump::Entry,
    defn: &str,
) -> Option<(String, String, String)> {
    if entry.desc != libpgdump::ObjectType::Sequence {
        return None;
    }
    // the last clause outside the double quotes of a name. A doubled
    // quote changes the state two times
    let mut quoted = false;
    let mut clause = None;
    for (index, c) in defn.char_indices() {
        match c {
            '"' => quoted = !quoted,
            _ if !quoted && defn[index..].starts_with(" OWNED BY ") => {
                clause = Some(index);
            }
            _ => {}
        }
    }
    let (create, column) = defn.split_at(clause?);
    let column = column[" OWNED BY ".len()..].strip_suffix(";\n")?;
    let name = format!(
        "{}.{}",
        quote_ident(entry.namespace.as_deref()?),
        quote_ident(entry.tag.as_deref()?)
    );
    Some((
        format!("{create};\n"),
        format!("ALTER SEQUENCE {name} OWNED BY {column};\n"),
        column.to_string(),
    ))
}

/// The archive position of each object of type `desc` that the
/// deploy creates, or drops and makes again, by schema and name. The
/// last overload of a function name gives the position
fn created_entries(
    parser: &mut tree_sitter::Parser,
    output: &build::BuildOutput,
    diff: &Diff,
    resolutions: &BTreeMap<usize, Resolution>,
    desc: libpgdump::ObjectType,
) -> HashMap<(String, String), usize> {
    let mut created = HashMap::new();
    for (position, entry) in output.dump.entries().iter().enumerate() {
        let added = output.item_ids.get(&entry.dump_id).is_some_and(|id| {
            match diff.items.get(id) {
                Some(Change::Added) => true,
                Some(Change::Changed) => matches!(
                    resolutions.get(id),
                    Some(Resolution::Replace | Resolution::Rebuild { .. })
                ),
                _ => false,
            }
        });
        if entry.desc == desc
            && added
            && let (Some(schema), Some(tag)) =
                (entry.namespace.as_deref(), entry.tag.as_deref())
        {
            // the tag of a function is its name, which can include its
            // argument types or a `(`, thus the CREATE gives the name
            let name = match desc {
                libpgdump::ObjectType::Function => entry
                    .defn
                    .as_deref()
                    .and_then(|defn| created_function(parser, defn))
                    .unwrap_or_else(|| tag.to_string()),
                _ => tag.to_string(),
            };
            created.insert((schema.to_string(), name), position);
        }
    }
    created
}

/// The name, without its schema, of the function that `defn` creates
fn created_function(
    parser: &mut tree_sitter::Parser,
    defn: &str,
) -> Option<String> {
    let tree = parser.parse(defn, None)?;
    let name = tree
        .root_node()
        .find("CreateFunctionStmt")?
        .child_of_kind("func_name")?;
    Some(ddl::any_name(&name, defn).name)
}

/// The shell type entries of the base types that this deploy creates.
/// A function that takes a new base type fails without its shell type,
/// as PostgreSQL makes a shell type only for a function that returns
/// the type
fn new_shell_types(output: &build::BuildOutput, diff: &Diff) -> HashSet<i32> {
    let shells: HashSet<i32> = output
        .dump
        .entries()
        .iter()
        .filter(|entry| entry.desc == libpgdump::ObjectType::ShellType)
        .map(|entry| entry.dump_id)
        .collect();
    output
        .dump
        .entries()
        .iter()
        .filter(|entry| {
            entry.desc == libpgdump::ObjectType::Type
                && output
                    .item_ids
                    .get(&entry.dump_id)
                    .and_then(|id| diff.items.get(id))
                    == Some(&Change::Added)
        })
        .flat_map(|entry| entry.dependencies.iter().copied())
        .filter(|dep| shells.contains(dep))
        .collect()
}

/// The last archive position after `position` of the functions in
/// `functions` that `sql` calls, and of the sequences in `sequences`
/// that it gives to `nextval`. Only a name with a schema is looked
/// up: deploy runs with an empty `search_path`
fn calls_later(
    parser: &mut tree_sitter::Parser,
    sql: &str,
    functions: &HashMap<(String, String), usize>,
    sequences: &HashMap<(String, String), usize>,
    position: usize,
) -> Option<usize> {
    if functions.is_empty() && sequences.is_empty() {
        return None;
    }
    let tree = parser.parse(sql, None)?;
    tree.root_node()
        .find_all("func_name")
        .iter()
        .filter_map(|node| {
            let name = ddl::any_name(node, sql);
            let schema = name.schema.as_deref();
            if name.name == "nextval"
                && matches!(schema, None | Some("pg_catalog"))
            {
                // the argument is a regclass literal, as in a default
                let (schema, name) =
                    build::nextval_target(node.parent()?.text(sql))?;
                return sequences.get(&(schema, name)).copied();
            }
            functions.get(&(schema?.to_string(), name.name)).copied()
        })
        .filter(|later| *later > position)
        .max()
}

/// The warnings for the entries of the snapshot that pull could not
/// model. The DATABASE PROPERTIES entry can have properties that pull
/// does not model, as CONNECTION LIMIT, but the plan still changes the
/// settings of the entry, thus it gets its own warning
fn unmodeled_warnings(assembly: &pull::Assembly) -> Vec<String> {
    let properties = "DATABASE PROPERTIES";
    let mut warnings = Vec::new();
    let unmodeled = assembly
        .remaining
        .iter()
        .filter(|entry| entry.desc != properties)
        .count();
    if unmodeled > 0 {
        let plural = if unmodeled == 1 { "object" } else { "objects" };
        let descs: Vec<String> = assembly
            .unmodeled_descs()
            .into_iter()
            .filter(|desc| desc != properties)
            .collect();
        warnings.push(format!(
            "{unmodeled} database {plural} ({}) cannot be modeled by this \
             version and are not represented in the plan; they were left \
             untouched",
            descs.join(", ")
        ));
    }
    if assembly
        .remaining
        .iter()
        .any(|entry| entry.desc == properties)
    {
        warnings.push(format!(
            "{properties}: the database has properties, as CONNECTION \
             LIMIT, that this version cannot model; they were left \
             untouched, but the plan changes the settings of the database \
             and of its roles as the project gives them"
        ));
    }
    warnings
}

/// Log what the plan skipped or excluded so the script is honest
/// about what it does not cover
fn report(diff: &Diff, plan: &Plan, assembly: &pull::Assembly) {
    // objects the snapshot could not model are absent from the diff
    // entirely, so without this the plan is silent about schema it is
    // leaving untouched in the database
    for warning in unmodeled_warnings(assembly) {
        log::warn!("{warning}");
    }
    let undiffable = diff
        .items
        .values()
        .filter(|c| **c == Change::Undiffable)
        .count();
    if undiffable > 0 {
        log::warn!(
            "{undiffable} object(s) exist in both the project and the \
             database but cannot be compared: the project writes them as \
             raw sql, which deploy does not compare; they were left \
             untouched"
        );
    }
    if !plan.dependents.is_empty() {
        log::warn!(
            "These objects depend on an object that the plan drops, \
             thus the plan drops them and makes them again: {}",
            plan.dependents.join(", ")
        );
    }
    for reset in &plan.resets {
        log::warn!(
            "Setting {reset}: the project does not have it, thus deploy \
             resets it"
        );
    }
    for label in &plan.unowned {
        log::warn!(
            "{label}: the project does not have its object, thus the plan \
             does not include it"
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
    // a withheld drop keeps the subscription and its slot, so give the
    // note only when the plan includes the drop
    for (key, definition) in &diff.removed {
        if let Definition::Subscription(subscription) = definition
            && let Some(slot) = subscription.slot_name()
            && plan.included.iter().any(|s| s.label == key.to_string())
        {
            log::warn!(
                "{key}: the drop does not drop the replication slot {slot} \
                 on the publisher, as that cannot run in a transaction \
                 block. If the publisher has the slot, it keeps WAL there \
                 until you drop it on the publisher: SELECT \
                 pg_drop_replication_slot({})",
                crate::utils::postgres_value(&slot.into())
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

/// Render the script with a self-describing header.
///
/// The statements run with the session settings of pg_restore that
/// can change the result of DDL, so that the build's SQL runs as it
/// does in a restore of the build, and not by the settings of the
/// session:
///
/// - `client_encoding` is UTF8, the encoding of the script.
/// - `standard_conforming_strings` is on, so a backslash in a string
///   literal is not an escape.
/// - `role` is `--role`, when it is given, so that role makes the new
///   objects, as the plan of the privileges and owners expects. psql
///   does not have `--role`, thus the script sets it, as pg_restore
///   does; a script that runs by hand then also runs as the role.
/// - `search_path` is empty, so a name resolves as it does in a
///   restore.
/// - `check_function_bodies` is off, so a function can refer to an
///   object that the script makes after it.
/// - `xmloption` is content, so an xml constant that is not a
///   document is valid.
///
/// The script does not set the other settings of pg_restore (the
/// timeouts, `client_min_messages` and `row_security`): they do not
/// change the objects that the DDL makes. A timeout can stop the
/// script, but on a live database it is a limit that the operator
/// sets on purpose.
///
/// The settings are not local to the transaction, as pg_restore sets
/// them: a local setting has no effect on the statements that follow
/// it when the script runs outside a transaction block. In a
/// transaction block, a rollback also undoes the settings.
fn render_script(
    plan: &Plan,
    project: &str,
    source: &str,
    role: Option<&str>,
) -> String {
    let mut script = format!(
        "-- pglifecycle deploy\n-- project: {}\n-- source: {}\n",
        one_line(project),
        one_line(source)
    );
    if !plan.included.is_empty() {
        script.push_str(&format!(
            "-- session settings, as pg_restore sets them: \
             client_encoding, standard_conforming_strings, {}search_path, \
             check_function_bodies, xmloption\n",
            if role.is_some() { "role, " } else { "" }
        ));
    }
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
                one_line(&statement.label)
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
    if !plan.dependents.is_empty() {
        script.push_str(&format!(
            "-- dependents rebuilt with a replaced object: {} ({})\n",
            plan.dependents.len(),
            one_line(&plan.dependents.join(", "))
        ));
    }
    if !plan.resets.is_empty() {
        script.push_str(&format!(
            "-- settings reset: {} ({})\n",
            plan.resets.len(),
            one_line(&plan.resets.join(", "))
        ));
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
    if !plan.included.is_empty() {
        script.push_str(
            "\nSET client_encoding = 'UTF8';\n\
             SET standard_conforming_strings = on;\n",
        );
        if let Some(role) = role {
            script.push_str(&format!("SET ROLE {};\n", quote_ident(role)));
        }
        script.push_str(
            "SELECT pg_catalog.set_config('search_path', '', false);\n\
             SET check_function_bodies = false;\n\
             SET xmloption = content;\n",
        );
    }
    for statement in &plan.included {
        script.push_str(&format!(
            "\n-- {}\n{}",
            one_line(&statement.label),
            statement.sql
        ));
    }
    script
}

/// `label` with each control character as its escape, so that the
/// label stays in its comment. A name, as of a role or of the project,
/// can contain a line break, and the text after the break can run as
/// SQL
fn one_line(label: &str) -> String {
    let mut line = String::new();
    for c in label.chars() {
        if c.is_control() {
            line.extend(c.escape_default());
        } else {
            line.push(c);
        }
    }
    line
}

/// `DROP <type> IF EXISTS <name>` for a database-only object. User
/// mappings are keyed by their user but dropped per server, so they
/// render from the definition. Default privileges have no DROP: the
/// REVOKE and GRANT statements that give the role the built-in
/// privileges again take their place. Everything else needs only the
/// key.
///
/// DROP SUBSCRIPTION cannot run in a transaction block when the
/// subscription has a replication slot, because it drops the slot on
/// the publisher. Deploy runs in one, so it disables the subscription
/// and removes the slot name first. The publisher keeps the slot, and
/// [`report`] says so.
fn drop_sql(key: &ObjectKey, definition: Option<&Definition>) -> String {
    if let Some(Definition::DefaultPrivileges(defaults)) = definition {
        return alter::default_privileges::removal(defaults)
            .into_iter()
            .map(|alter| alter.sql)
            .collect();
    }
    if let Some(Definition::Subscription(subscription)) = definition
        && let Some(slot) = subscription.slot_name()
    {
        let name = quote_ident(&subscription.name);
        // PostgreSQL permits only a-z, 0-9 and _ in a slot name, so it
        // cannot end the comment
        return format!(
            "-- the publisher keeps the replication slot {slot}\n\
             ALTER SUBSCRIPTION {name} DISABLE;\n\
             ALTER SUBSCRIPTION {name} SET (slot_name = NONE);\n\
             DROP SUBSCRIPTION IF EXISTS {name};\n"
        );
    }
    // these types are named by more than their key
    match definition {
        Some(Definition::Aggregate(a)) => return alter::aggregate::drop(a),
        Some(Definition::Cast(c)) => return alter::cast::drop(c),
        Some(Definition::Operator(o)) => return alter::operator::drop(o),
        Some(Definition::OperatorClass(c)) => {
            return alter::operator_class::drop_class(c);
        }
        Some(Definition::OperatorFamily(f)) => {
            return alter::operator_class::drop_family(f);
        }
        Some(Definition::Transform(t)) => return alter::transform::drop(t),
        _ => {}
    }
    if cascades(definition) {
        return format!(
            "DROP TYPE IF EXISTS {}.{} CASCADE;\n",
            quote_ident(&key.schema),
            quote_ident(&key.name)
        );
    }
    if key.desc == constants::ObjectType::TextSearch
        && let Some(sql) = alter::text_search::drop_sql(&key.schema, &key.name)
    {
        return sql;
    }
    if let Some(Definition::UserMapping(mapping)) = definition {
        return mapping
            .servers
            .iter()
            .map(|server| {
                format!(
                    "DROP USER MAPPING IF EXISTS FOR {} SERVER {};\n",
                    user_mapping_subject(&mapping.name),
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
            let base =
                crate::project::routine_base_name(&f.name, &f.parameters);
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

/// A base type and its I/O functions depend on each other, thus the
/// type drops with CASCADE, as pg_dump --clean writes it. The DROP
/// FUNCTION of each I/O function then does nothing
fn cascades(definition: Option<&Definition>) -> bool {
    matches!(
        definition,
        Some(Definition::Type(user_type))
            if user_type.type_kind.as_deref() == Some("base")
    )
}

/// The snapshot objects that depend on the database-only type `key`,
/// directly or through other objects, and that the plan does not drop
/// (`removed` holds the keys of the drops). DROP TYPE ... CASCADE drops
/// them too. The I/O functions of a base type depend on its shell type,
/// thus the walk starts at both
fn cascade_keeps(
    key: &ObjectKey,
    snapshot: &libpgdump::Dump,
    removed: &BTreeMap<ObjectKey, &ObjectKey>,
) -> Vec<String> {
    if key.desc != constants::ObjectType::Type {
        return Vec::new();
    }
    let entries = snapshot.entries();
    let Some(user_type) = entries
        .iter()
        .find(|entry| entry_key(entry).as_ref() == Some(key))
    else {
        return Vec::new();
    };
    // the type depends on its I/O functions, not on its shell type,
    // thus find the shell type by its name
    let mut seen: HashSet<i32> = entries
        .iter()
        .filter(|entry| {
            entry.desc == libpgdump::ObjectType::ShellType
                && entry.namespace == user_type.namespace
                && entry.tag == user_type.tag
        })
        .map(|entry| entry.dump_id)
        .chain([user_type.dump_id])
        .collect();
    let mut pending: Vec<i32> = seen.iter().copied().collect();
    let mut kept = BTreeSet::new();
    while let Some(id) = pending.pop() {
        for entry in entries {
            if entry.dependencies.contains(&id) && seen.insert(entry.dump_id) {
                pending.push(entry.dump_id);
                // an entry with no key of its own (an index or a
                // constraint, for example) is a part of its relation
                let dependent = entry_key(entry).or_else(|| {
                    entry.dependencies.iter().find_map(|dep| {
                        entries
                            .iter()
                            .find(|owner| owner.dump_id == *dep)
                            .and_then(entry_key)
                            .filter(|owner| {
                                matches!(
                                    owner.desc,
                                    constants::ObjectType::Table
                                        | constants::ObjectType::View
                                        | constants::ObjectType::MaterializedView
                                )
                            })
                    })
                });
                if let Some(dependent) = dependent
                    && dependent != *key
                    && !removed.contains_key(&dependent)
                {
                    kept.insert(dependent.to_string());
                }
            }
        }
    }
    kept.into_iter().collect()
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
        // the tag of an ordered-set aggregate has no ORDER BY
        Definition::Aggregate(a) => ObjectKey {
            desc: key.desc,
            schema: key.schema.clone(),
            name: alter::aggregate::tag_name(a),
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
        OT::AccessMethod => constants::ObjectType::AccessMethod,
        OT::Aggregate => constants::ObjectType::Aggregate,
        OT::Cast => constants::ObjectType::Cast,
        OT::Collation => constants::ObjectType::Collation,
        OT::Conversion => constants::ObjectType::Conversion,
        OT::Domain => constants::ObjectType::Domain,
        OT::EventTrigger => constants::ObjectType::EventTrigger,
        OT::Extension => constants::ObjectType::Extension,
        OT::ForeignDataWrapper => constants::ObjectType::ForeignDataWrapper,
        // foreign tables key as tables (the project models them as
        // tables with a `server`), so a removed one orders and drops
        // alongside ordinary tables
        OT::ForeignTable => constants::ObjectType::Table,
        OT::Function => constants::ObjectType::Function,
        OT::MaterializedView => constants::ObjectType::MaterializedView,
        OT::Operator => constants::ObjectType::Operator,
        OT::OperatorClass => constants::ObjectType::OperatorClass,
        OT::OperatorFamily => constants::ObjectType::OperatorFamily,
        OT::ProceduralLanguage => constants::ObjectType::ProceduralLanguage,
        OT::Procedure => constants::ObjectType::Procedure,
        OT::Publication => constants::ObjectType::Publication,
        OT::Schema => constants::ObjectType::Schema,
        OT::Sequence => constants::ObjectType::Sequence,
        OT::Statistics => constants::ObjectType::Statistics,
        OT::ForeignServer | OT::Server => constants::ObjectType::Server,
        OT::Subscription => constants::ObjectType::Subscription,
        OT::Table => constants::ObjectType::Table,
        OT::Transform => constants::ObjectType::Transform,
        OT::Type => constants::ObjectType::Type,
        OT::UserMapping => constants::ObjectType::UserMapping,
        OT::View => constants::ObjectType::View,
        // a text search entry is tagged by its object name; the item is
        // one object, keyed by its kind and name
        OT::TextSearchParser
        | OT::TextSearchTemplate
        | OT::TextSearchDictionary
        | OT::TextSearchConfiguration => {
            return Some(ObjectKey {
                desc: constants::ObjectType::TextSearch,
                schema: entry.namespace.clone().unwrap_or_default(),
                name: alter::text_search::entry_key_name(
                    entry.desc.as_str(),
                    entry.tag.as_deref()?,
                )?,
            });
        }
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
        constants::ObjectType::AccessMethod
        | constants::ObjectType::Cast
        | constants::ObjectType::EventTrigger
        | constants::ObjectType::Extension
        | constants::ObjectType::ForeignDataWrapper
        | constants::ObjectType::ProceduralLanguage
        | constants::ObjectType::Publication
        | constants::ObjectType::Schema
        | constants::ObjectType::Server
        | constants::ObjectType::Subscription
        | constants::ObjectType::Transform
        | constants::ObjectType::UserMapping => String::new(),
        _ => entry.namespace.clone().unwrap_or_default(),
    };
    // the tags of these types do not have the whole identity
    let name = match desc {
        constants::ObjectType::Cast => alter::cast::entry_name(entry)?,
        constants::ObjectType::Operator => alter::operator::entry_name(entry)?,
        constants::ObjectType::OperatorClass
        | constants::ObjectType::OperatorFamily => {
            alter::operator_class::entry_name(entry)?
        }
        constants::ObjectType::Transform => {
            alter::transform::entry_name(entry)?
        }
        _ => entry.tag.clone()?,
    };
    Some(ObjectKey { desc, schema, name })
}

/// The build archive without the empty statement at the end of each
/// COMMENT entry. The build writes `;` after the comment, and `;`
/// again after each entry, as the Python build did. pg_restore runs
/// the empty statement; the script does not carry it.
fn without_empty_statements(output: &mut build::BuildOutput) {
    let changed: Vec<(i32, String)> = output
        .dump
        .entries()
        .iter()
        .filter(|entry| entry.desc == libpgdump::ObjectType::Comment)
        .filter_map(|entry| {
            // the comment is a dollar-quoted string, thus the `;`
            // after it ends the statement
            let defn = entry.defn.as_deref()?;
            let statement = defn.strip_suffix(";\n")?;
            statement
                .ends_with("$;\n")
                .then(|| (entry.dump_id, statement.to_string()))
        })
        .collect();
    for (dump_id, defn) in changed {
        if let Some(entry) = output.dump.get_entry_mut(dump_id) {
            entry.defn = Some(defn);
        }
    }
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

/// Human-readable comparison source for the header and logs. The
/// label of a connection has no password (see [`pgdump::label`]).
fn source_label(args: &cli::Deploy) -> String {
    match &args.dump {
        Some(path) => format!("dump {}", path.display()),
        None => pgdump::label(&args.connection),
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
            gated: BTreeSet::new(),
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

        let plan = plan(
            &diff,
            &BTreeMap::new(),
            &dependents::Dependents::default(),
            &output,
            &snapshot,
            &privileges::Privileges::default(),
            &args,
        )
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

    /// A removed subscription with a slot drops in a transaction: it
    /// loses its slot name first, so DROP SUBSCRIPTION does not go to
    /// the publisher. One with no slot drops at once.
    #[test]
    fn removed_subscription_drops_in_a_transaction() {
        let subscription = |slot: Option<&str>| {
            let mut json = serde_json::json!({
                "name": "Sub",
                "connection": "dbname=elsewhere",
                "publications": ["pub"],
            });
            if let Some(slot) = slot {
                json["parameters"] = serde_json::json!({"slot_name": slot});
            }
            Definition::Subscription(
                serde_json::from_value(json).expect("subscription"),
            )
        };
        let key = |definition: &Definition| {
            ObjectKey::new(constants::ObjectType::Subscription, definition)
        };
        let slotted = subscription(None);
        assert_eq!(
            drop_sql(&key(&slotted), Some(&slotted)),
            "-- the publisher keeps the replication slot Sub\n\
             ALTER SUBSCRIPTION \"Sub\" DISABLE;\n\
             ALTER SUBSCRIPTION \"Sub\" SET (slot_name = NONE);\n\
             DROP SUBSCRIPTION IF EXISTS \"Sub\";\n"
        );
        let slotless = subscription(Some("NONE"));
        assert_eq!(
            drop_sql(&key(&slotless), Some(&slotless)),
            "DROP SUBSCRIPTION IF EXISTS \"Sub\";\n"
        );
    }

    /// Publication and subscription entries key as their models do,
    /// with no schema, so a removed one drops in dependency order
    #[test]
    fn publication_and_subscription_entries_have_keys() {
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        for (desc, tag) in [
            (libpgdump::ObjectType::Publication, "pub"),
            (libpgdump::ObjectType::Subscription, "sub"),
        ] {
            snapshot
                .add_entry(desc, None, Some(tag), None, None, None, None, &[])
                .expect("add entry");
        }
        let keys: Vec<String> = snapshot
            .entries()
            .iter()
            .filter_map(entry_key)
            .map(|key| key.to_string())
            .collect();
        assert_eq!(keys, vec!["PUBLICATION pub", "SUBSCRIPTION sub"]);
    }

    /// Statistics, event trigger, collation, conversion and access
    /// method entries key as their models do: pg_dump tags each with
    /// its bare name, and an event trigger or an access method has no
    /// schema. So a removed one drops in dependency order, with its
    /// name quoted
    #[test]
    fn objects_with_no_data_have_keys_and_drops() {
        use constants::ObjectType as O;
        use libpgdump::ObjectType as OT;
        use serde_json::{from_value, json};
        let name = "Stray One";
        let schema = "Quoted Schema";
        let owned = |fields: serde_json::Value| {
            let mut value = json!({
                "name": name, "schema": schema, "owner": "postgres",
            });
            value
                .as_object_mut()
                .expect("an object")
                .extend(fields.as_object().expect("an object").clone());
            value
        };
        let objects = [
            (
                OT::AccessMethod,
                O::AccessMethod,
                Definition::AccessMethod(
                    from_value(json!({"name": name, "type": "TABLE",
                        "handler": "heap_tableam_handler"}))
                    .expect("access method"),
                ),
                "DROP ACCESS METHOD IF EXISTS \"Stray One\";\n",
            ),
            (
                OT::Collation,
                O::Collation,
                Definition::Collation(
                    from_value(owned(json!({"locale": "C"})))
                        .expect("collation"),
                ),
                "DROP COLLATION IF EXISTS \"Quoted Schema\".\"Stray \
                 One\";\n",
            ),
            (
                OT::Conversion,
                O::Conversion,
                Definition::Conversion(
                    from_value(owned(json!({"encoding_from": "LATIN3",
                        "encoding_to": "UTF8",
                        "function": "iso8859_to_utf8"})))
                    .expect("conversion"),
                ),
                "DROP CONVERSION IF EXISTS \"Quoted Schema\".\"Stray \
                 One\";\n",
            ),
            (
                OT::EventTrigger,
                O::EventTrigger,
                Definition::EventTrigger(
                    from_value(json!({"name": name, "event": "sql_drop",
                        "function": "test.note_ddl()"}))
                    .expect("event trigger"),
                ),
                "DROP EVENT TRIGGER IF EXISTS \"Stray One\";\n",
            ),
            (
                OT::Statistics,
                O::Statistics,
                Definition::Statistics(
                    from_value(owned(json!({"table": "test.t",
                        "elements": ["a", "b"]})))
                    .expect("statistics"),
                ),
                "DROP STATISTICS IF EXISTS \"Quoted Schema\".\"Stray \
                 One\";\n",
            ),
        ];
        for (entry_desc, desc, definition, drop) in objects {
            let key = ObjectKey::new(desc, &definition);
            let namespace = (!key.schema.is_empty()).then_some(schema);
            let mut snapshot =
                libpgdump::new("test", "UTF8", "18.0").expect("new dump");
            snapshot
                .add_entry(
                    entry_desc,
                    namespace,
                    Some(name),
                    None,
                    None,
                    None,
                    None,
                    &[],
                )
                .expect("add entry");
            assert_eq!(
                snapshot.entries().iter().find_map(entry_key),
                Some(drop_match_key(&key, &definition)),
                "{}",
                desc.as_str()
            );
            assert_eq!(drop_sql(&key, Some(&definition)), drop);
        }
    }

    /// A base type that only the database has drops with CASCADE, as
    /// its I/O functions and the type depend on each other. When an
    /// object that the project keeps depends on the type, the drop does
    /// not cascade. Other types do not cascade
    #[test]
    fn removed_base_type_drops_with_cascade() {
        use libpgdump::ObjectType as OT;
        use serde_json::{from_value, json};
        let base: Definition = Definition::Type(
            from_value(json!({"name": "gate_shell", "schema": "test",
                "owner": "postgres", "type": "base",
                "input": "test.gate_shell_read",
                "output": "test.gate_shell_emit"}))
            .expect("base type"),
        );
        let enumerated: Definition = Definition::Type(
            from_value(json!({"name": "mood", "schema": "test",
                "owner": "postgres", "type": "enum",
                "enum": ["happy", "sad"]}))
            .expect("enum type"),
        );
        let io = |name: &str, data_type: &str, returns: &str| {
            Definition::Function(function(json!({
                "name": name, "schema": "test", "owner": "postgres",
                "parameters": [{"mode": "IN", "data_type": data_type}],
                "returns": returns, "language": "internal",
                "definition": "int4in",
            })))
        };
        let read = io("gate_shell_read", "cstring", "test.gate_shell");
        let emit = io("gate_shell_emit", "test.gate_shell", "cstring");
        let table = Definition::Table(
            from_value(json!({"name": "keeper", "schema": "test",
                "owner": "postgres"}))
            .expect("table"),
        );
        let type_key = ObjectKey::new(constants::ObjectType::Type, &base);
        assert_eq!(
            drop_sql(&type_key, Some(&base)),
            "DROP TYPE IF EXISTS test.gate_shell CASCADE;\n"
        );
        assert_eq!(
            drop_sql(
                &ObjectKey::new(constants::ObjectType::Type, &enumerated),
                Some(&enumerated)
            ),
            "DROP TYPE IF EXISTS test.mood;\n"
        );

        // pg_dump makes the I/O functions depend on the shell type, and
        // the type depend on its I/O functions
        let mut snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let mut add = |desc, tag: &str, deps: &[i32]| {
            snapshot
                .add_entry(
                    desc,
                    Some("test"),
                    Some(tag),
                    None,
                    None,
                    None,
                    None,
                    deps,
                )
                .expect("add entry")
        };
        let shell = add(OT::ShellType, "gate_shell", &[]);
        let read_id = add(OT::Function, "gate_shell_read(cstring)", &[shell]);
        let emit_id =
            add(OT::Function, "gate_shell_emit(test.gate_shell)", &[shell]);
        let type_id = add(OT::Type, "gate_shell", &[read_id, emit_id]);
        add(OT::Table, "keeper", &[type_id]);
        // a CHECK constraint and an index are entries of their own, and
        // CASCADE drops them from the table that holds them
        let checked = add(OT::Table, "checked", &[]);
        add(
            OT::CheckConstraint,
            "checked shell_check",
            &[checked, type_id],
        );
        let indexed = add(OT::Table, "indexed", &[]);
        add(OT::Index, "indexed_shell_idx", &[indexed, type_id]);

        let parse = |options: &[&str]| {
            let cli = cli::Cli::parse_from(
                ["pglifecycle", "deploy", "--allow-drop"]
                    .iter()
                    .chain(options)
                    .chain(&["proj"]),
            );
            let cli::Action::Deploy(args) = cli.action else {
                unreachable!("parsed the deploy subcommand")
            };
            args
        };
        let args = parse(&[]);
        let output = build::BuildOutput {
            dump: libpgdump::new("test", "UTF8", "18.0")
                .expect("new output dump"),
            item_ids: std::collections::HashMap::new(),
        };
        let type_drop =
            |removed: Vec<(constants::ObjectType, &Definition)>,
             args: &cli::Deploy| {
                let diff = Diff {
                    items: BTreeMap::new(),
                    changed: BTreeMap::new(),
                    removed: removed
                        .into_iter()
                        .map(|(desc, definition)| {
                            (
                                ObjectKey::new(desc, definition),
                                definition.clone(),
                            )
                        })
                        .collect(),
                    owned: BTreeSet::new(),
                    owner_changed: BTreeSet::new(),
                    gated: BTreeSet::new(),
                };
                plan(
                    &diff,
                    &BTreeMap::new(),
                    &dependents::Dependents::default(),
                    &output,
                    &snapshot,
                    &privileges::Privileges::default(),
                    args,
                )
                .expect("plan succeeds")
                .included
                .into_iter()
                .find(|statement| statement.label == type_key.to_string())
                .expect("the type drops")
                .sql
            };
        let relation = |name: &str| {
            Definition::Table(
                from_value(json!({"name": name, "schema": "test",
                    "owner": "postgres"}))
                .expect("table"),
            )
        };
        let checked = relation("checked");
        let indexed = relation("indexed");
        use constants::ObjectType as O;
        let all = vec![
            (O::Type, &base),
            (O::Function, &read),
            (O::Function, &emit),
            (O::Table, &table),
            (O::Table, &checked),
            (O::Table, &indexed),
        ];
        assert_eq!(
            type_drop(all.clone(), &args),
            "DROP TYPE IF EXISTS test.gate_shell CASCADE;\n"
        );
        // the project keeps a table, or an I/O function
        for kept in 1..all.len() {
            let mut removed = all.clone();
            removed.remove(kept);
            assert_eq!(
                type_drop(removed, &args),
                "DROP TYPE IF EXISTS test.gate_shell;\n"
            );
        }
        // the snapshot does not have the excluded objects, thus their
        // dependency on the type is not known
        for option in [
            ["--exclude-table", "other"],
            ["--exclude-schema", "other"],
            ["--exclude-extension", "other"],
        ] {
            assert_eq!(
                type_drop(all.clone(), &parse(&option)),
                "DROP TYPE IF EXISTS test.gate_shell;\n"
            );
        }
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
            gated: BTreeSet::new(),
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
        let plan = plan(
            &diff,
            &BTreeMap::new(),
            &dependents::Dependents::default(),
            &output,
            &snapshot,
            &privileges::Privileges::default(),
            &args,
        )
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
            gated: BTreeSet::new(),
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
        let plan = plan(
            &diff,
            &resolutions,
            &dependents::Dependents::default(),
            &output,
            &snapshot,
            &privileges::Privileges::default(),
            &args,
        )
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
        let unchanged = super::plan(
            &diff,
            &resolutions,
            &dependents::Dependents::default(),
            &output,
            &snapshot,
            &privileges::Privileges::default(),
            &args,
        )
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
            gated: BTreeSet::new(),
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
        let plan = plan(
            &diff,
            &resolutions,
            &dependents::Dependents::default(),
            &output,
            &snapshot,
            &privileges::Privileges::default(),
            &args,
        )
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
            gated: BTreeSet::new(),
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
        let plan = plan(
            &diff,
            &resolutions,
            &dependents::Dependents::default(),
            &output,
            &snapshot,
            &privileges::Privileges::default(),
            &args,
        )
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
            unowned: Vec::new(),
            resets: Vec::new(),
            dependents: Vec::new(),
        };
        let script = render_script(&plan, "test", "db", None);
        for line in script.lines() {
            assert!(line.starts_with("--"), "line runs as SQL: {line}");
        }
        assert!(script.contains("--   DROP TABLE t; --\";\n"));
    }

    /// A label with a newline, as from a role name, must stay in its
    /// comment, so no part of it can run
    #[test]
    fn label_with_newline_stays_in_its_comment() {
        let statement = || Statement {
            label: "ROLE x\nDROP TABLE t; -- IN DATABASE d\r\nDROP TABLE u;\t"
                .to_string(),
            sql: "ALTER ROLE \"a\" RESET work_mem;\n".to_string(),
            fails_open: true,
        };
        let plan = Plan {
            included: vec![statement()],
            excluded: vec![statement()],
            kept: Vec::new(),
            included_destructive: 0,
            unowned: Vec::new(),
            resets: Vec::new(),
            dependents: Vec::new(),
        };
        let script = render_script(&plan, "test", "db", None);
        assert!(!script.contains("\nDROP TABLE"), "{script}");
        assert!(!script.contains(['\r', '\t']), "{script}");
        let label = "ROLE x\\nDROP TABLE t; -- IN DATABASE d\\r\\n\
                     DROP TABLE u;\\t";
        assert!(script.contains(&format!("\n-- {label}\n")), "{script}");
        assert!(
            script.contains(&format!("-- WARNING: {label} withheld")),
            "{script}"
        );
    }

    /// A RESET is not destructive, but it can remove a setting that a
    /// DBA made, thus the header names each setting that the plan
    /// resets
    #[test]
    fn script_names_the_settings_it_resets() {
        let plan = Plan {
            included: vec![Statement {
                label: "DATABASE app".to_string(),
                sql: "ALTER DATABASE app RESET work_mem;\n".to_string(),
                fails_open: false,
            }],
            excluded: Vec::new(),
            kept: Vec::new(),
            included_destructive: 0,
            unowned: Vec::new(),
            resets: vec![
                "work_mem of DATABASE app".to_string(),
                "a\nb of ROLE x IN DATABASE app".to_string(),
            ],
            dependents: Vec::new(),
        };
        let script = render_script(&plan, "test", "db", None);
        assert!(
            script.contains(
                "-- destructive statements: none\n\
                 -- settings reset: 2 (work_mem of DATABASE app, a\\nb of \
                 ROLE x IN DATABASE app)\n"
            ),
            "{script}"
        );
    }

    /// The header names each object that the plan drops and makes
    /// again because it depends on an object that the plan drops
    #[test]
    fn script_names_the_rebuilt_dependents() {
        let statement = |label: &str| Statement {
            label: label.to_string(),
            sql: "DROP VIEW test.v;\n".to_string(),
            fails_open: false,
        };
        let plan = Plan {
            included: Vec::new(),
            excluded: vec![statement("VIEW test.v"), statement("VIEW x")],
            kept: Vec::new(),
            included_destructive: 0,
            unowned: Vec::new(),
            resets: Vec::new(),
            dependents: vec![
                "VIEW test.v".to_string(),
                "VIEW a\nb".to_string(),
            ],
        };
        let script = render_script(&plan, "test", "db", None);
        assert!(
            script.contains(
                "-- destructive statements: 2 excluded (re-run with \
                 --allow-drop)\n\
                 -- dependents rebuilt with a replaced object: 2 (VIEW \
                 test.v, VIEW a\\nb)\n"
            ),
            "{script}"
        );
    }

    /// deploy changes the settings of a DATABASE PROPERTIES entry that
    /// pull could not model, thus the warning does not say that the
    /// entry was left untouched
    #[test]
    fn unmodeled_database_properties_warning_is_true() {
        let remaining = |desc: &str| pull::Remaining {
            desc: desc.to_string(),
            namespace: None,
            tag: None,
            defn: None,
        };
        let mut assembly = pull::Assembly::default();
        assembly.remaining.push(remaining("DATABASE PROPERTIES"));
        let warnings = unmodeled_warnings(&assembly);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].starts_with("DATABASE PROPERTIES: "));
        assert!(warnings[0].contains("the plan changes the settings"));
        assembly.remaining.push(remaining("EVENT TRIGGER"));
        let warnings = unmodeled_warnings(&assembly);
        assert_eq!(
            warnings[0],
            "1 database object (EVENT TRIGGER) cannot be modeled by this \
             version and are not represented in the plan; they were left \
             untouched"
        );
        assert_eq!(warnings.len(), 2);
        assert!(unmodeled_warnings(&pull::Assembly::default()).is_empty());
    }

    /// A project name, as from project.yaml, and a source with a line
    /// break must stay in their header comments
    #[test]
    fn header_with_newline_stays_in_its_comment() {
        let plan = Plan {
            included: Vec::new(),
            excluded: Vec::new(),
            kept: Vec::new(),
            included_destructive: 0,
            unowned: Vec::new(),
            resets: Vec::new(),
            dependents: Vec::new(),
        };
        let script = render_script(
            &plan,
            "p\nDROP TABLE t;",
            "s\r\nDROP TABLE u;",
            None,
        );
        for line in script.lines() {
            assert!(line.starts_with("--"), "line runs as SQL: {line}");
        }
        assert!(
            script.starts_with(
                "-- pglifecycle deploy\n-- project: p\\nDROP TABLE t;\n\
                 -- source: s\\r\\nDROP TABLE u;\n"
            ),
            "{script}"
        );
    }

    /// The statements run with the session settings of pg_restore that
    /// can change the result of DDL, so that they run as they do in a
    /// restore of the build. The header says which settings.
    #[test]
    fn script_runs_with_the_session_settings_of_pg_restore() {
        let plan = Plan {
            included: vec![Statement {
                label: "SCHEMA app".to_string(),
                sql: "CREATE SCHEMA app;\n".to_string(),
                fails_open: false,
            }],
            excluded: Vec::new(),
            kept: Vec::new(),
            included_destructive: 0,
            unowned: Vec::new(),
            resets: Vec::new(),
            dependents: Vec::new(),
        };
        let script = render_script(&plan, "test", "db", None);
        assert_eq!(
            script,
            "-- pglifecycle deploy\n-- project: test\n-- source: db\n\
             -- session settings, as pg_restore sets them: \
             client_encoding, standard_conforming_strings, search_path, \
             check_function_bodies, xmloption\n\
             -- destructive statements: none\n\
             \nSET client_encoding = 'UTF8';\n\
             SET standard_conforming_strings = on;\n\
             SELECT pg_catalog.set_config('search_path', '', false);\n\
             SET check_function_bodies = false;\n\
             SET xmloption = content;\n\
             \n-- SCHEMA app\nCREATE SCHEMA app;\n"
        );
    }

    /// psql does not have `--role`, thus the script sets the role, at
    /// the position where pg_restore --role sets it
    #[test]
    fn script_sets_the_role_as_pg_restore_does() {
        let plan = Plan {
            included: vec![Statement {
                label: "SCHEMA app".to_string(),
                sql: "CREATE SCHEMA app;\n".to_string(),
                fails_open: false,
            }],
            excluded: Vec::new(),
            kept: Vec::new(),
            included_destructive: 0,
            unowned: Vec::new(),
            resets: Vec::new(),
            dependents: Vec::new(),
        };
        let script = render_script(&plan, "test", "db", Some("App Owner"));
        assert!(
            script.contains(
                "-- session settings, as pg_restore sets them: \
                 client_encoding, standard_conforming_strings, role, \
                 search_path, check_function_bodies, xmloption\n"
            ),
            "{script}"
        );
        assert!(
            script.contains(
                "SET standard_conforming_strings = on;\n\
                 SET ROLE \"App Owner\";\n\
                 SELECT pg_catalog.set_config('search_path', '', false);\n"
            ),
            "{script}"
        );
        // a script with no statements does not set the role
        let plan = Plan {
            included: Vec::new(),
            ..plan
        };
        let script = render_script(&plan, "test", "db", Some("App Owner"));
        assert!(!script.contains("SET ROLE"), "{script}");
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
            gated: BTreeSet::new(),
        };
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let plan_with = |argv: &[&str]| {
            let args = match cli::Cli::parse_from(argv).action {
                cli::Action::Deploy(deploy) => deploy,
                _ => unreachable!("parsed the deploy subcommand"),
            };
            plan(
                &diff,
                &BTreeMap::new(),
                &dependents::Dependents::default(),
                &output,
                &snapshot,
                &privileges::Privileges::default(),
                &args,
            )
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

    /// OWNED BY needs a sequence with the owner of the table, thus a
    /// sequence that the project links to a column is linked after
    /// each owner change. A sequence that the database links is
    /// unlinked before all other statements, also before the statements
    /// of a table that comes first: a drop of its old column drops it.
    /// It keeps the owner of its table until OWNED BY NONE, thus it
    /// gets its owner after
    #[test]
    fn sequence_owner_order_follows_its_link() {
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new output dump");
        let mut add = |desc, name: &str| {
            let kind = match desc {
                libpgdump::ObjectType::Sequence => "SEQUENCE",
                _ => "TABLE",
            };
            dump.add_entry(
                desc,
                Some("public"),
                Some(name),
                Some("app"),
                Some(&format!("CREATE {kind} public.{name};\n")),
                Some(&format!("DROP {kind} public.{name};\n")),
                None,
                &[],
            )
            .expect("add entry")
        };
        let table = add(libpgdump::ObjectType::Table, "t");
        let sequence = add(libpgdump::ObjectType::Sequence, "s");
        let output = build::BuildOutput {
            dump,
            item_ids: HashMap::from([(sequence, 0), (table, 1)]),
        };
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let args = match cli::Cli::parse_from(["pglifecycle", "deploy", "p"])
            .action
        {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };
        let definition = |owned_by: Option<&str>| {
            Definition::Sequence(
                serde_json::from_value(serde_json::json!({
                    "name": "s", "schema": "public", "owner": "app",
                    "owned_by": owned_by,
                }))
                .unwrap(),
            )
        };
        let owner = "ALTER SEQUENCE public.s OWNER TO app;\n";
        let table_owner = "ALTER TABLE public.t OWNER TO app;\n";
        let unlink = "ALTER SEQUENCE public.s OWNED BY NONE;\n";
        let link = "ALTER SEQUENCE public.s OWNED BY public.t.id;\n";
        for (repo, db, expected) in [
            (Some("public.t.id"), None, vec![table_owner, owner, link]),
            (None, Some("public.t.id"), vec![unlink, table_owner, owner]),
            (
                Some("public.t.id"),
                Some("public.t.old"),
                vec![unlink, table_owner, owner, link],
            ),
        ] {
            let diff = Diff {
                items: BTreeMap::from([
                    (0, Change::Changed),
                    (1, Change::Unchanged),
                ]),
                changed: BTreeMap::from([(0, definition(db))]),
                removed: BTreeMap::new(),
                owned: BTreeSet::from([0, 1]),
                owner_changed: BTreeSet::from([0, 1]),
                gated: BTreeSet::new(),
            };
            let resolutions = BTreeMap::from([(
                0,
                alter::resolve(&definition(repo), &definition(db)),
            )]);
            let plan = plan(
                &diff,
                &resolutions,
                &dependents::Dependents::default(),
                &output,
                &snapshot,
                &privileges::Privileges::default(),
                &args,
            )
            .expect("plan succeeds");
            let sql: Vec<&str> =
                plan.included.iter().map(|s| s.sql.as_str()).collect();
            assert_eq!(sql, expected);
        }
    }

    /// A link to a column that only the rebuild of its table adds is
    /// withheld with the rebuild. A link to a column that the database
    /// has is not
    #[test]
    fn sequence_link_waits_for_the_rebuild_of_its_table() {
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new output dump");
        let mut add = |desc, name: &str| {
            let kind = match desc {
                libpgdump::ObjectType::Sequence => "SEQUENCE",
                _ => "TABLE",
            };
            dump.add_entry(
                desc,
                Some("public"),
                Some(name),
                Some("app"),
                Some(&format!("CREATE {kind} public.{name};\n")),
                Some(&format!("DROP {kind} public.{name};\n")),
                None,
                &[],
            )
            .expect("add entry")
        };
        let table = add(libpgdump::ObjectType::Table, "t");
        let sequence = add(libpgdump::ObjectType::Sequence, "s");
        let output = build::BuildOutput {
            dump,
            item_ids: HashMap::from([(sequence, 0), (table, 1)]),
        };
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let sequence = |owned_by: Option<&str>| {
            Definition::Sequence(
                serde_json::from_value(serde_json::json!({
                    "name": "s", "schema": "public", "owner": "app",
                    "owned_by": owned_by,
                }))
                .unwrap(),
            )
        };
        let table = Definition::Table(
            serde_json::from_value(serde_json::json!({
                "name": "t", "schema": "public", "owner": "app",
                "columns": [{"name": "id", "data_type": "integer"}],
            }))
            .unwrap(),
        );
        let rebuild = "DROP TABLE public.t;\nCREATE TABLE public.t;\n";
        for (column, allow_drop, included, excluded) in [
            ("public.t.new", false, vec![], vec![rebuild, "new"]),
            ("public.t.new", true, vec![rebuild, "new"], vec![]),
            ("public.t.id", false, vec!["id"], vec![rebuild]),
        ] {
            let link = |sql: Vec<&str>| -> Vec<String> {
                sql.into_iter()
                    .map(|s| match s {
                        "new" | "id" => format!(
                            "ALTER SEQUENCE public.s OWNED BY public.t.{s};\n"
                        ),
                        _ => s.to_string(),
                    })
                    .collect()
            };
            let mut argv = vec!["pglifecycle", "deploy", "p"];
            if allow_drop {
                argv.push("--allow-drop");
            }
            let args = match cli::Cli::parse_from(argv).action {
                cli::Action::Deploy(deploy) => deploy,
                _ => unreachable!("parsed the deploy subcommand"),
            };
            let diff = Diff {
                items: BTreeMap::from([
                    (0, Change::Changed),
                    (1, Change::Changed),
                ]),
                changed: BTreeMap::from([
                    (0, sequence(None)),
                    (1, table.clone()),
                ]),
                removed: BTreeMap::new(),
                owned: BTreeSet::new(),
                owner_changed: BTreeSet::new(),
                gated: BTreeSet::new(),
            };
            let resolutions = BTreeMap::from([
                (0, alter::resolve(&sequence(Some(column)), &sequence(None))),
                (1, Resolution::Replace),
            ]);
            let plan = plan(
                &diff,
                &resolutions,
                &dependents::Dependents::default(),
                &output,
                &snapshot,
                &privileges::Privileges::default(),
                &args,
            )
            .expect("plan succeeds");
            let sql = |statements: &[Statement]| -> Vec<String> {
                statements.iter().map(|s| s.sql.clone()).collect()
            };
            assert_eq!(sql(&plan.included), link(included), "{column}");
            assert_eq!(sql(&plan.excluded), link(excluded), "{column}");
        }
    }

    /// A new base type gets its shell type before its I/O function,
    /// which takes the type. A base type that the database has gets
    /// no shell type, also for a new function that takes it
    #[test]
    fn shell_type_comes_only_with_a_new_base_type() {
        let mut dump = libpgdump::new("test", "UTF8", "18.0").expect("dump");
        let mut entry = |desc, tag: &str, defn: &str, deps: &[i32]| {
            dump.add_entry(
                desc,
                Some("test"),
                Some(tag),
                None,
                Some(defn),
                None,
                None,
                deps,
            )
            .expect("add entry")
        };
        let shell = entry(
            libpgdump::ObjectType::ShellType,
            "b",
            "CREATE TYPE test.b;\n",
            &[],
        );
        let function = entry(
            libpgdump::ObjectType::Function,
            "b_out(test.b)",
            "CREATE FUNCTION test.b_out(test.b);\n",
            &[shell],
        );
        let base = entry(
            libpgdump::ObjectType::Type,
            "b",
            "CREATE TYPE test.b (INPUT = test.b_in, OUTPUT = test.b_out);\n",
            &[shell, function],
        );
        let output = build::BuildOutput {
            dump,
            item_ids: HashMap::from([(base, 0), (function, 1)]),
        };
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let args = match cli::Cli::parse_from(["pglifecycle", "deploy", "p"])
            .action
        {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };
        let plan_for = |base: Change| {
            let diff = Diff {
                items: BTreeMap::from([(0, base), (1, Change::Added)]),
                changed: BTreeMap::new(),
                removed: BTreeMap::new(),
                owned: BTreeSet::new(),
                owner_changed: BTreeSet::new(),
                gated: BTreeSet::new(),
            };
            plan(
                &diff,
                &BTreeMap::new(),
                &dependents::Dependents::default(),
                &output,
                &snapshot,
                &privileges::Privileges::default(),
                &args,
            )
            .expect("plan succeeds")
            .included
            .into_iter()
            .map(|statement| statement.sql)
            .collect::<Vec<_>>()
        };
        assert_eq!(
            plan_for(Change::Added),
            [
                "CREATE TYPE test.b;\n",
                "CREATE FUNCTION test.b_out(test.b);\n",
                "CREATE TYPE test.b (INPUT = test.b_in, OUTPUT = \
                 test.b_out);\n",
            ]
        );
        assert_eq!(
            plan_for(Change::Unchanged),
            ["CREATE FUNCTION test.b_out(test.b);\n"]
        );
    }

    /// A plan for the changed table `test.t` (item 0), the new
    /// functions `test.f` (item 1) and `test.g` (item 2), and the new
    /// sequences `test.s` (item 3), `test.o` (item 4), which is owned
    /// by `test.t.n`, `test."s(v)"` (item 5, named `p`), and the new
    /// function `test."f(x)"` (item 6, named `x`), in this
    /// archive order: the entries of `before`, the table, then the
    /// entries of `after`
    fn function_order_plan(
        before: &[&str],
        after: &[&str],
        alters: &[&str],
    ) -> Vec<String> {
        let mut dump = libpgdump::new("test", "UTF8", "18.0").expect("dump");
        let mut item_ids = HashMap::new();
        let mut add = |dump: &mut libpgdump::Dump, name: &str| {
            let (id, desc, tag, defn) = match name {
                "s" => (
                    3,
                    libpgdump::ObjectType::Sequence,
                    "s".to_string(),
                    "CREATE SEQUENCE test.s;\n".to_string(),
                ),
                "o" => (
                    4,
                    libpgdump::ObjectType::Sequence,
                    "o".to_string(),
                    "CREATE SEQUENCE test.o OWNED BY test.t.n;\n".to_string(),
                ),
                "p" => (
                    5,
                    libpgdump::ObjectType::Sequence,
                    "s(v)".to_string(),
                    "CREATE SEQUENCE test.\"s(v)\";\n".to_string(),
                ),
                // a build tag is the name, with no argument types
                "x" => (
                    6,
                    libpgdump::ObjectType::Function,
                    "f(x)".to_string(),
                    "CREATE FUNCTION test.\"f(x)\"(IN integer);\n".to_string(),
                ),
                _ => (
                    if name == "f" { 1 } else { 2 },
                    libpgdump::ObjectType::Function,
                    format!("{name}()"),
                    format!("CREATE FUNCTION test.{name}();\n"),
                ),
            };
            let dump_id = dump
                .add_entry(
                    desc,
                    Some("test"),
                    Some(&tag),
                    None,
                    Some(&defn),
                    None,
                    None,
                    &[],
                )
                .expect("add entry");
            item_ids.insert(dump_id, id);
        };
        for name in before {
            add(&mut dump, name);
        }
        let table = dump
            .add_entry(
                libpgdump::ObjectType::Table,
                Some("test"),
                Some("t"),
                None,
                Some("CREATE TABLE test.t (id integer);\n"),
                None,
                None,
                &[],
            )
            .expect("add table entry");
        for name in after {
            add(&mut dump, name);
        }
        item_ids.insert(table, 0);
        let output = build::BuildOutput { dump, item_ids };
        let mut diff = Diff {
            items: BTreeMap::from([
                (0, Change::Changed),
                (1, Change::Added),
                (2, Change::Added),
                (3, Change::Added),
                (4, Change::Added),
                (5, Change::Added),
                (6, Change::Added),
            ]),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
            gated: BTreeSet::new(),
        };
        diff.changed.insert(
            0,
            serde_json::from_value(serde_json::json!({
                "name": "t", "schema": "test", "owner": "postgres",
            }))
            .map(Definition::Table)
            .expect("table deserializes"),
        );
        let alters = alters
            .iter()
            .map(|sql| alter::Alter {
                sql: (*sql).to_string(),
                destructive: false,
                label: None,
                fails_open: false,
                index_removal: false,
                schema: None,
                links: None,
                unlinks: false,
                drops_key: None,
            })
            .collect();
        let resolutions =
            BTreeMap::from([(0, Resolution::Statements(alters))]);
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let args = match cli::Cli::parse_from(["pglifecycle", "deploy", "p"])
            .action
        {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };
        plan(
            &diff,
            &resolutions,
            &dependents::Dependents::default(),
            &output,
            &snapshot,
            &privileges::Privileges::default(),
            &args,
        )
        .expect("plan succeeds")
        .included
        .into_iter()
        .map(|statement| statement.sql)
        .collect()
    }

    /// A function that depends on a changed table comes after it in
    /// the archive. A statement of the table that calls the function
    /// waits for its CREATE, and so do the statements after it, which
    /// keep their order. The statements before it stay at the table
    #[test]
    fn changed_table_statements_wait_for_new_functions() {
        let sql = function_order_plan(
            &[],
            &["f", "g"],
            &[
                "ALTER TABLE test.t ADD COLUMN v integer;\n",
                "ALTER TABLE test.t ALTER COLUMN id SET DEFAULT test.f();\n",
                "ALTER TABLE test.t ADD COLUMN w integer;\n",
                "CREATE INDEX t_idx ON test.t ((test.g(id)));\n",
                "CREATE TRIGGER t_trg BEFORE INSERT ON test.t FOR EACH ROW \
                 EXECUTE FUNCTION test.f();\n",
            ],
        );
        assert_eq!(
            sql,
            [
                "ALTER TABLE test.t ADD COLUMN v integer;\n",
                "CREATE FUNCTION test.f();\n",
                "ALTER TABLE test.t ALTER COLUMN id SET DEFAULT test.f();\n",
                "ALTER TABLE test.t ADD COLUMN w integer;\n",
                "CREATE FUNCTION test.g();\n",
                "CREATE INDEX t_idx ON test.t ((test.g(id)));\n",
                "CREATE TRIGGER t_trg BEFORE INSERT ON test.t FOR EACH ROW \
                 EXECUTE FUNCTION test.f();\n",
            ]
        );
        // a check waits only for the function that it calls
        let sql = function_order_plan(
            &[],
            &["f", "g"],
            &["ALTER TABLE test.t ADD CONSTRAINT t_check \
               CHECK ((test.f() > 0));\n"],
        );
        assert_eq!(
            sql,
            [
                "CREATE FUNCTION test.f();\n",
                "ALTER TABLE test.t ADD CONSTRAINT t_check \
                 CHECK ((test.f() > 0));\n",
                "CREATE FUNCTION test.g();\n",
            ]
        );
    }

    /// The statements of a changed table stay at the table when the
    /// functions that they call come before it, or when the name has
    /// no schema, which is then a `pg_catalog` function
    #[test]
    fn changed_table_statements_keep_their_order() {
        let alters = [
            "ALTER TABLE test.t ALTER COLUMN id SET DEFAULT test.f();\n",
            "ALTER TABLE test.t ALTER COLUMN v SET DEFAULT g();\n",
        ];
        let sql = function_order_plan(&["f"], &["g"], &alters);
        assert_eq!(
            sql,
            [
                "CREATE FUNCTION test.f();\n",
                alters[0],
                alters[1],
                "CREATE FUNCTION test.g();\n",
            ]
        );
    }

    /// A sequence that sorts after a changed table comes after it in
    /// the archive. A statement of the table that gives the sequence
    /// to `nextval` waits for its CREATE. A waiting statement can add
    /// the column that owns the sequence, thus its OWNED BY comes after
    /// the waiting statements
    #[test]
    fn changed_table_statements_wait_for_new_sequences() {
        let sql = function_order_plan(
            &[],
            &["s", "f", "o"],
            &[
                "ALTER TABLE test.t ALTER COLUMN id SET DEFAULT \
                 nextval('test.s'::regclass);\n",
                "ALTER TABLE test.t ADD COLUMN n integer NOT NULL DEFAULT \
                 nextval('test.o'::regclass);\n",
            ],
        );
        assert_eq!(
            sql,
            [
                "CREATE SEQUENCE test.s;\n",
                "ALTER TABLE test.t ALTER COLUMN id SET DEFAULT \
                 nextval('test.s'::regclass);\n",
                "CREATE FUNCTION test.f();\n",
                "CREATE SEQUENCE test.o;\n",
                "ALTER TABLE test.t ADD COLUMN n integer NOT NULL DEFAULT \
                 nextval('test.o'::regclass);\n",
                "ALTER SEQUENCE test.o OWNED BY test.t.n;\n",
            ]
        );
    }

    /// A new sequence gets its OWNED BY last, also when no statement
    /// waits for it, and a statement stays at the table when the
    /// sequence that it gives to `nextval` comes before it. A string
    /// that is not the argument of `nextval` is not a sequence
    #[test]
    fn changed_table_statements_keep_their_sequence_order() {
        let alters = [
            "ALTER TABLE test.t ALTER COLUMN id SET DEFAULT \
             nextval('test.s'::regclass);\n",
            "COMMENT ON TABLE test.t IS 'test.o';\n",
        ];
        let sql = function_order_plan(&["s"], &["o"], &alters);
        assert_eq!(
            sql,
            [
                "CREATE SEQUENCE test.s;\n",
                alters[0],
                alters[1],
                "CREATE SEQUENCE test.o;\n",
                "ALTER SEQUENCE test.o OWNED BY test.t.n;\n",
            ]
        );
    }

    /// An OWNED BY waits for the statements of a changed table that
    /// wait for another object, as one of them can add the column that
    /// owns the sequence
    #[test]
    fn owned_by_waits_for_delayed_columns() {
        let alters = [
            "ALTER TABLE test.t ALTER COLUMN id SET DEFAULT \
             nextval('test.s'::regclass);\n",
            "ALTER TABLE test.t ADD COLUMN n integer;\n",
        ];
        let sql = function_order_plan(&[], &["o", "s"], &alters);
        assert_eq!(
            sql,
            [
                "CREATE SEQUENCE test.o;\n",
                "CREATE SEQUENCE test.s;\n",
                alters[0],
                alters[1],
                "ALTER SEQUENCE test.o OWNED BY test.t.n;\n",
            ]
        );
    }

    /// A sequence keeps the parentheses of its name, which only the
    /// name of a function removes
    #[test]
    fn sequence_names_keep_their_parentheses() {
        let alters = ["ALTER TABLE test.t ALTER COLUMN id SET DEFAULT \
                       nextval('test.\"s(v)\"'::regclass);\n"];
        let sql = function_order_plan(&[], &["p"], &alters);
        assert_eq!(sql, ["CREATE SEQUENCE test.\"s(v)\";\n", alters[0]]);
    }

    /// A function name keeps the parentheses of its name. The tag of
    /// a built function is its name, which has no argument types
    #[test]
    fn function_names_keep_their_parentheses() {
        let alters = ["ALTER TABLE test.t ALTER COLUMN id SET DEFAULT \
                       test.\"f(x)\"(1);\n"];
        let sql = function_order_plan(&[], &["x"], &alters);
        assert_eq!(
            sql,
            ["CREATE FUNCTION test.\"f(x)\"(IN integer);\n", alters[0]]
        );
    }

    /// A sequence that a column owns has the owner of its table in
    /// PostgreSQL, thus another owner in the project is a conflict
    #[test]
    fn sequence_owners_that_differ_from_their_table() {
        use constants::ObjectType as O;
        let item = |id, desc, value: serde_json::Value| Item {
            id,
            desc,
            definition: match desc {
                O::Sequence => Definition::Sequence(
                    serde_json::from_value(value).unwrap(),
                ),
                _ => Definition::Table(serde_json::from_value(value).unwrap()),
            },
            dependencies: BTreeSet::new(),
        };
        let sequence = |id, name: &str, owner: &str, owned_by: &str| {
            item(
                id,
                O::Sequence,
                serde_json::json!({
                    "name": name, "schema": "t", "owner": owner,
                    "owned_by": owned_by,
                }),
            )
        };
        let inventory = [
            item(
                0,
                O::Table,
                serde_json::json!({
                    "name": "Yy", "schema": "t", "owner": "o",
                    "columns": [{"name": "id", "data_type": "integer"}],
                }),
            ),
            sequence(1, "same", "o", "t.\"Yy\".id"),
            sequence(2, "other", "p", "t.\"Yy\".ID"),
            sequence(3, "no table", "p", "t.zz.id"),
            sequence(4, "db other", "p", "t.zz.id"),
            sequence(5, "db unlinked", "p", "t.zz.id"),
        ];
        // with no project table, the database sequence gives the owner
        let database: Vec<crate::models::Sequence> = [
            ("no table", "p", Some("t.zz.id")),
            ("db other", "q", Some("t.zz.ID")),
            ("db unlinked", "q", None),
        ]
        .into_iter()
        .map(|(name, owner, owned_by)| {
            serde_json::from_value(serde_json::json!({
                "name": name, "schema": "t", "owner": owner,
                "owned_by": owned_by,
            }))
            .unwrap()
        })
        .collect();
        assert_eq!(
            sequence_owner_conflicts(&inventory, &database),
            ["t.other", "t.\"db other\""]
        );
    }

    /// OWNED BY is found outside the double quotes of a name
    #[test]
    fn owned_by_skips_quoted_names() {
        let defn = "CREATE SEQUENCE test.o OWNED BY test.t.\"n OWNED BY \
                    x\";\n";
        let mut dump = libpgdump::new("test", "UTF8", "18.0").expect("dump");
        let dump_id = dump
            .add_entry(
                libpgdump::ObjectType::Sequence,
                Some("test"),
                Some("o"),
                None,
                Some(defn),
                None,
                None,
                &[],
            )
            .expect("add entry");
        let entry = dump
            .entries()
            .iter()
            .find(|entry| entry.dump_id == dump_id)
            .expect("sequence entry");
        assert_eq!(
            owned_by(entry, defn),
            Some((
                "CREATE SEQUENCE test.o;\n".to_string(),
                "ALTER SEQUENCE test.o OWNED BY test.t.\"n OWNED BY x\";\n"
                    .to_string(),
                "test.t.\"n OWNED BY x\"".to_string(),
            ))
        );
    }

    /// The plan does not have an ACL entry on an object that is not in
    /// the project, for example a grant on a catalog function. The plan
    /// gives its label, so that deploy can warn about it
    #[test]
    fn acl_on_an_object_not_in_the_project_is_unowned() {
        let diff = Diff {
            items: BTreeMap::new(),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
            gated: BTreeSet::new(),
        };
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        dump.add_entry(
            libpgdump::ObjectType::Acl,
            Some("pg_catalog"),
            Some("FUNCTION pg_reload_conf()"),
            None,
            Some(
                "GRANT ALL ON FUNCTION pg_catalog.pg_reload_conf() TO \
                 reader;\n",
            ),
            None,
            None,
            &[],
        )
        .expect("add acl entry");
        let output = build::BuildOutput {
            dump,
            item_ids: std::collections::HashMap::new(),
        };
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new snapshot");
        let cli = cli::Cli::parse_from(["pglifecycle", "deploy", "proj"]);
        let args = match cli.action {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };
        let plan = plan(
            &diff,
            &BTreeMap::new(),
            &dependents::Dependents::default(),
            &output,
            &snapshot,
            &privileges::Privileges::default(),
            &args,
        )
        .expect("plan succeeds");
        assert!(plan.included.is_empty());
        assert_eq!(
            plan.unowned,
            vec!["ACL pg_catalog.FUNCTION pg_reload_conf()".to_string()]
        );
    }

    /// The comment of the database is in the build, but no item owns
    /// it. The statements of `database` change it, thus the plan does
    /// not have the entry, and the entry is not unowned
    #[test]
    fn database_comment_is_not_unowned() {
        let diff = Diff {
            items: BTreeMap::new(),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
            gated: BTreeSet::new(),
        };
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        dump.add_entry(
            libpgdump::ObjectType::Comment,
            Some(""),
            Some("DATABASE test"),
            Some("postgres"),
            Some("COMMENT ON DATABASE test IS $$c$$;\n"),
            None,
            None,
            &[],
        )
        .expect("add comment entry");
        let output = build::BuildOutput {
            dump,
            item_ids: std::collections::HashMap::new(),
        };
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new snapshot");
        let cli = cli::Cli::parse_from(["pglifecycle", "deploy", "proj"]);
        let args = match cli.action {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };
        let plan = plan(
            &diff,
            &BTreeMap::new(),
            &dependents::Dependents::default(),
            &output,
            &snapshot,
            &privileges::Privileges::default(),
            &args,
        )
        .expect("plan succeeds");
        assert!(plan.included.is_empty());
        assert!(plan.unowned.is_empty(), "{:?}", plan.unowned);
    }

    /// Each role that a planned statement names and the database does
    /// not have, with the labels of the statements that name it. PUBLIC,
    /// the CURRENT_USER forms and the reserved pg_ roles are not checked
    #[test]
    fn missing_roles_names_each_role_and_its_statements() {
        let statement = |label: &str, sql: &str| Statement {
            label: label.to_string(),
            sql: sql.to_string(),
            fails_open: false,
        };
        let statements = vec![
            statement(
                "TABLE public.t",
                "CREATE TABLE public.t (id integer);\n\
                 ALTER TABLE public.t OWNER TO \"Table Owner\";\n",
            ),
            statement(
                "ACL public.TABLE t",
                "GRANT SELECT ON TABLE public.t TO reader;\n\
                 GRANT SELECT ON TABLE public.t TO PUBLIC;\n\
                 GRANT SELECT ON TABLE public.t TO pg_read_all_data;\n\
                 REVOKE ALL ON TABLE public.t FROM postgres;\n",
            ),
            statement(
                "POLICY public.t p",
                "CREATE POLICY p ON public.t TO app, reader USING (true);\n",
            ),
            statement(
                "DEFAULT PRIVILEGES",
                "ALTER DEFAULT PRIVILEGES FOR ROLE creator GRANT SELECT \
                 ON TABLES TO viewer;\n",
            ),
            statement(
                "USER MAPPING mapped SERVER s",
                "CREATE USER MAPPING FOR mapped SERVER s;\n\
                 CREATE USER MAPPING FOR CURRENT_USER SERVER s;\n\
                 CREATE USER MAPPING FOR PUBLIC SERVER s;\n",
            ),
        ];
        let roles = BTreeSet::from([String::from("postgres")]);
        let missing = missing_roles(&statements, &roles).expect("parses");
        assert_eq!(
            missing,
            BTreeMap::from([
                (String::from("Table Owner"), vec!["TABLE public.t"]),
                (String::from("app"), vec!["POLICY public.t p"]),
                (String::from("creator"), vec!["DEFAULT PRIVILEGES"]),
                (String::from("mapped"), vec!["USER MAPPING mapped SERVER s"]),
                (
                    String::from("reader"),
                    vec!["ACL public.TABLE t", "POLICY public.t p"]
                ),
                (String::from("viewer"), vec!["DEFAULT PRIVILEGES"]),
            ])
        );
    }

    /// A quoted keyword is an ordinary role name. Only `"public"`
    /// stays the pseudo-role, because PostgreSQL reads it as PUBLIC
    #[test]
    fn missing_roles_checks_quoted_keywords() {
        let statements = vec![Statement {
            label: String::from("ACL public.TABLE t"),
            sql: String::from(
                "GRANT SELECT ON TABLE public.t TO \"current_user\";\n\
                 GRANT SELECT ON TABLE public.t TO \"PUBLIC\";\n\
                 GRANT SELECT ON TABLE public.t TO \"public\";\n",
            ),
            fails_open: false,
        }];
        let missing =
            missing_roles(&statements, &BTreeSet::new()).expect("parses");
        assert_eq!(
            missing.keys().collect::<Vec<_>>(),
            vec!["PUBLIC", "current_user"]
        );
    }

    /// A role that is not a superuser cannot read the subscriptions or
    /// the options of some user mappings. Only the added and changed
    /// items that it cannot read are named
    #[test]
    fn unreadable_names_what_the_role_cannot_read() {
        use constants::ObjectType as OT;
        let item = |id, desc, value: serde_json::Value| Item {
            id,
            desc,
            definition: match desc {
                OT::Subscription => Definition::Subscription(
                    serde_json::from_value(value).expect("subscription"),
                ),
                _ => Definition::UserMapping(
                    serde_json::from_value(value).expect("user mapping"),
                ),
            },
            dependencies: BTreeSet::new(),
        };
        let inventory = vec![
            item(
                0,
                OT::Subscription,
                serde_json::json!({
                    "name": "sub",
                    "connection": "dbname=x",
                    "publications": ["pub"],
                }),
            ),
            item(
                1,
                OT::UserMapping,
                serde_json::json!({
                    "name": "postgres",
                    "servers": [{"name": "srv", "options": {"user": "x"}},
                                {"name": "own", "options": {"user": "y"}}],
                }),
            ),
            item(
                2,
                OT::UserMapping,
                serde_json::json!({
                    "name": "public",
                    "servers": [{"name": "srv", "options": {"user": "z"}}],
                }),
            ),
            item(
                3,
                OT::UserMapping,
                serde_json::json!({
                    "name": "app",
                    "servers": [{"name": "srv", "options": {"user": "a"}}],
                }),
            ),
        ];
        let diff = Diff {
            items: BTreeMap::from([
                (0, Change::Added),
                (1, Change::Changed),
                (2, Change::Changed),
                (3, Change::Unchanged),
            ]),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
            gated: BTreeSet::new(),
        };
        let mut limits = pgdump::ReadLimits {
            role: "Gate Reader".into(),
            superuser: false,
            hidden_user_mappings: vec![
                ("postgres".into(), "srv".into()),
                ("PUBLIC".into(), "srv".into()),
                ("app".into(), "srv".into()),
            ],
        };
        assert_eq!(
            unreadable(&inventory, &diff, &limits),
            vec![
                "SUBSCRIPTION sub",
                "USER MAPPING postgres SERVER srv",
                "USER MAPPING PUBLIC SERVER srv",
            ]
        );
        limits.superuser = true;
        limits.hidden_user_mappings.clear();
        assert!(unreadable(&inventory, &diff, &limits).is_empty());
    }

    /// A new table makes its index in a tablespace of the project.
    /// deploy does not manage tablespaces, thus the index entry has
    /// only the table as its item
    #[test]
    fn index_in_a_tablespace_of_the_project_is_created() {
        use constants::ObjectType as OT;
        let inventory = vec![
            Item {
                id: 0,
                desc: OT::Tablespace,
                definition: Definition::Tablespace(
                    serde_json::from_value(serde_json::json!({
                        "name": "fast",
                        "owner": "postgres",
                        "location": "/srv/fast",
                    }))
                    .expect("tablespace"),
                ),
                dependencies: BTreeSet::new(),
            },
            Item {
                id: 1,
                desc: OT::Table,
                definition: Definition::Table(
                    serde_json::from_value(serde_json::json!({
                        "name": "t",
                        "schema": "public",
                        "owner": "postgres",
                        "columns": [{"name": "n", "data_type": "integer"}],
                        "indexes": [{
                            "name": "t_n",
                            "columns": [{"name": "n"}],
                            "tablespace": "fast",
                        }],
                    }))
                    .expect("table"),
                ),
                dependencies: BTreeSet::new(),
            },
        ];
        let project = project::Project {
            name: "tablespace".into(),
            superuser: "postgres".into(),
            default_schema: "public".into(),
            path: std::path::PathBuf::new(),
            settings: Default::default(),
            inventory,
        };
        let diff = Diff {
            items: BTreeMap::from([(0, Change::Skipped), (1, Change::Added)]),
            changed: BTreeMap::new(),
            removed: BTreeMap::new(),
            owned: BTreeSet::new(),
            owner_changed: BTreeSet::new(),
            gated: BTreeSet::new(),
        };
        let mut output = build::assemble(&project).expect("assemble");
        output.dump.sort_entries();
        let snapshot =
            libpgdump::new("test", "UTF8", "18.0").expect("new snapshot");
        let cli = cli::Cli::parse_from(["pglifecycle", "deploy", "proj"]);
        let args = match cli.action {
            cli::Action::Deploy(deploy) => deploy,
            _ => unreachable!("parsed the deploy subcommand"),
        };
        let plan = plan(
            &diff,
            &BTreeMap::new(),
            &dependents::Dependents::default(),
            &output,
            &snapshot,
            &privileges::Privileges::default(),
            &args,
        )
        .expect("plan succeeds");
        let sql: Vec<&str> =
            plan.included.iter().map(|s| s.sql.as_str()).collect();
        assert!(
            sql.iter().any(|sql| sql.contains("CREATE INDEX t_n")),
            "the index is not in the plan: {sql:?}"
        );
    }

    /// A COMMENT entry of the build loses its empty statement; other
    /// entries do not change
    #[test]
    fn comment_entries_lose_the_empty_statement() {
        let mut output = build::BuildOutput {
            dump: libpgdump::new("test", "UTF8", "18.0")
                .expect("new output dump"),
            item_ids: std::collections::HashMap::new(),
        };
        let comment = || libpgdump::ObjectType::Comment;
        let cases = [
            (
                comment(),
                "COMMENT ON SUBSCRIPTION s IS $$c$$;\n;\n",
                "COMMENT ON SUBSCRIPTION s IS $$c$$;\n",
            ),
            (
                comment(),
                "COMMENT ON TABLE test.t IS $_$a $$ b$_$;\n;\n",
                "COMMENT ON TABLE test.t IS $_$a $$ b$_$;\n",
            ),
            (
                comment(),
                "COMMENT ON TABLE test.t IS NULL;\n",
                "COMMENT ON TABLE test.t IS NULL;\n",
            ),
            (
                libpgdump::ObjectType::View,
                "CREATE VIEW test.w AS SELECT 1 -- $;\n;\n",
                "CREATE VIEW test.w AS SELECT 1 -- $;\n;\n",
            ),
        ];
        let ids: Vec<i32> = cases
            .iter()
            .map(|(desc, defn, _)| {
                output
                    .dump
                    .add_entry(
                        desc.clone(),
                        None,
                        Some("x"),
                        None,
                        Some(defn),
                        None,
                        None,
                        &[],
                    )
                    .expect("add entry")
            })
            .collect();
        without_empty_statements(&mut output);
        for (id, (_, _, expected)) in ids.iter().zip(cases) {
            let entry = output.dump.get_entry_mut(*id).expect("entry");
            assert_eq!(entry.defn.as_deref(), Some(expected));
        }
    }

    /// In quotes, `PUBLIC` is the name of a role, so the drop of a
    /// mapping for PUBLIC writes it as the keyword
    #[test]
    fn removed_public_user_mapping_drops_for_the_keyword() {
        let definition = Definition::UserMapping(
            serde_json::from_value(serde_json::json!({
                "name": "PUBLIC",
                "servers": [{"name": "srv"}],
            }))
            .expect("user mapping"),
        );
        let key =
            ObjectKey::new(constants::ObjectType::UserMapping, &definition);
        assert_eq!(
            drop_sql(&key, Some(&definition)),
            "DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER srv;\n"
        );
    }
}
