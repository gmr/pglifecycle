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
    let diff = diff::diff(&project, &assembly);
    let resolutions = resolutions(&project, &diff);
    task.finish();
    let mut output = build::assemble(&project)?;
    let task = progress::spinner("Planning changes");
    output.dump.sort_entries();
    let plan = plan(&diff, &resolutions, &output, &snapshot, args)?;
    task.finish();
    report(&diff, &plan);
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

/// Resolve each changed item into in-place statements or the
/// drop+recreate fallback
fn resolutions(
    project: &project::Project,
    diff: &Diff,
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
            (*id, alter::resolve(repo, database))
        })
        .collect()
}

/// One statement in the deploy plan
struct Statement {
    label: String,
    sql: String,
}

/// The ordered script plus the destructive statements excluded from
/// it when `--allow-drop` is not given
struct Plan {
    included: Vec<Statement>,
    excluded: Vec<Statement>,
    /// How many of `included` are destructive (non-zero only with
    /// `--allow-drop`)
    included_destructive: usize,
}

/// Assemble the ordered plan: DROPs for database-only objects first
/// (reverse snapshot order), then the repo archive's entries in
/// topological order — plain CREATEs for added objects, in-place
/// ALTERs where a renderer exists, gated drop+recreate otherwise
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
                },
            );
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
        if args.no_privileges && entry.desc == libpgdump::ObjectType::Acl {
            continue;
        }
        let direct = output.item_ids.get(&entry.dump_id);
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
        if changes.iter().all(|c| *c == Change::Added) {
            push(false, Statement { label, sql: defn });
            continue;
        }
        if !changes.contains(&Change::Changed)
            || !changes
                .iter()
                .all(|c| matches!(c, Change::Added | Change::Changed))
        {
            continue;
        }
        // the object's own entry: emit its in-place statements,
        // re-issue it as CREATE OR REPLACE, or lead with its DROP for
        // the drop+recreate fallback
        if let Some(id) = direct {
            match resolutions.get(id) {
                Some(Resolution::Statements(alters)) => {
                    for alter in alters {
                        push(
                            alter.destructive,
                            Statement {
                                label: label.clone(),
                                sql: alter.sql.clone(),
                            },
                        );
                    }
                }
                Some(Resolution::OrReplace { comment }) => {
                    push(
                        false,
                        Statement {
                            label: label.clone(),
                            sql: defn.replacen(
                                "CREATE ",
                                "CREATE OR REPLACE ",
                                1,
                            ),
                        },
                    );
                    // CREATE OR REPLACE keeps the existing comment, so a
                    // changed or removed one is reconciled separately
                    if let Some(comment) = comment {
                        push(
                            false,
                            Statement {
                                label,
                                sql: comment.clone(),
                            },
                        );
                    }
                }
                _ => {
                    let mut sql = String::new();
                    if let Some(drop) = &entry.drop_stmt {
                        sql.push_str(drop);
                    }
                    sql.push_str(&defn);
                    push(true, Statement { label, sql });
                }
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
            push(true, Statement { label, sql: defn });
        }
    }
    Ok(Plan {
        included,
        excluded,
        included_destructive,
    })
}

/// Log what the plan skipped or excluded so the script is honest
/// about what it does not cover
fn report(diff: &Diff, plan: &Plan) {
    let undiffable = diff
        .items
        .values()
        .filter(|c| **c == Change::Undiffable)
        .count();
    if undiffable > 0 {
        log::warn!(
            "{undiffable} object(s) exist in both the project and the \
             database but their types cannot be compared yet; they were \
             left untouched"
        );
    }
    for statement in &plan.excluded {
        log::warn!(
            "{}: change requires a destructive statement; re-run with \
             --allow-drop to include it",
            statement.label
        );
    }
    log::info!(
        "Plan: {} statement(s) included, {} excluded",
        plan.included.len(),
        plan.excluded.len()
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
    } else if plan.included_destructive > 0 {
        script.push_str(&format!(
            "-- destructive statements: {} included\n",
            plan.included_destructive
        ));
    } else {
        script.push_str("-- destructive statements: none\n");
    }
    if plan.included.is_empty() {
        script.push_str("-- no changes: the database matches the project\n");
    }
    for statement in &plan.included {
        script
            .push_str(&format!("\n-- {}\n{}", statement.label, statement.sql));
    }
    script
}

/// `DROP <type> IF EXISTS <name>` for a database-only object. User
/// mappings are keyed by their user but dropped per server, so they
/// render from the definition; everything else needs only the key.
fn drop_sql(key: &ObjectKey, definition: Option<&Definition>) -> String {
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
    // function keys are identity signatures and must not be quoted
    // wholesale; everything else gets identifier quoting
    let name = match key.desc {
        constants::ObjectType::Function => key.name.clone(),
        _ => quote_ident(&key.name),
    };
    let qualified = if key.schema.is_empty() {
        name
    } else {
        format!("{}.{name}", quote_ident(&key.schema))
    };
    format!("DROP {} IF EXISTS {qualified};\n", key.desc.as_str())
}

/// The key a removed object is looked up under when matching archive
/// entries for drop ordering: functions use the tag-shaped signature
/// ([`diff::function_tag_name`]) so they compare equal to
/// `entry_key`'s output; everything else uses its own key unchanged
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
        OT::Schema => constants::ObjectType::Schema,
        OT::Sequence => constants::ObjectType::Sequence,
        OT::ForeignServer | OT::Server => constants::ObjectType::Server,
        OT::Table => constants::ObjectType::Table,
        OT::Type => constants::ObjectType::Type,
        OT::UserMapping => constants::ObjectType::UserMapping,
        OT::View => constants::ObjectType::View,
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
    use std::collections::BTreeMap;

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
}
