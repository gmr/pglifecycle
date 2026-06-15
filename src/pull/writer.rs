//! YAML project emission for `pull` (ports the storage half of
//! generate_project.py / storage.py)

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value, json};

use crate::constants::PROJECT_DIRS;
use crate::pull::{Assembly, RoleState};
use crate::{cli, models, progress, yamlio};

/// Render the project files for an assembly as `relative path →
/// YAML text`, honoring the `--ignore` file
pub fn render(
    assembly: &Assembly,
    args: &cli::Pull,
) -> Result<BTreeMap<PathBuf, String>, String> {
    let mut writer = Writer {
        ignore: read_ignore(args.ignore.as_deref())?,
        files: BTreeMap::new(),
        mode_headers: args.include_mode_headers,
    };
    writer.write_project_file(assembly)?;
    for schema in &assembly.schemas {
        writer.save(
            Path::new("schemata").join(format!("{}.yaml", schema.name)),
            schema,
        )?;
    }
    for domain in &assembly.domains {
        writer
            .save(nested("domains", &domain.schema, &domain.name), domain)?;
    }
    for sequence in &assembly.sequences {
        writer.save(
            nested("sequences", &sequence.schema, &sequence.name),
            sequence,
        )?;
    }
    for table in &assembly.tables {
        let mut value = serialize(table)?;
        if let (Some(map), Some(dependencies)) =
            (value.as_object_mut(), table_dependencies(table))
        {
            map.insert(String::from("dependencies"), dependencies);
        }
        writer.save_value(
            nested("tables", &table.schema, &table.name),
            &value,
        )?;
    }
    for view in &assembly.views {
        writer.save(nested("views", &view.schema, &view.name), view)?;
    }
    for view in &assembly.materialized_views {
        writer.save(
            nested("materialized_views", &view.schema, &view.name),
            view,
        )?;
    }
    writer.write_functions(assembly)?;
    writer.write_types(assembly)?;
    writer.write_roles(assembly)?;
    if args.save_remaining && !assembly.remaining.is_empty() {
        let entries: Vec<Value> = assembly
            .remaining
            .iter()
            .map(|r| {
                json!({
                    "desc": r.desc,
                    "namespace": r.namespace,
                    "tag": r.tag,
                    "defn": r.defn,
                })
            })
            .collect();
        writer.save_value(
            PathBuf::from("remaining.yaml"),
            &Value::Array(entries),
        )?;
    }
    Ok(writer.files)
}

/// Write the full rendered file set into a new project tree (bootstrap
/// mode)
pub fn write_bootstrap(
    files: &BTreeMap<PathBuf, String>,
    args: &cli::Pull,
) -> Result<(), String> {
    log::info!("Writing project to {}", args.destination.display());
    create_directories(&args.destination, args.gitkeep)?;
    let task = progress::spinner("Writing files");
    for (relative, content) in files {
        task.set_message(format!("Writing {}", relative.display()));
        let path = args.destination.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!("failed to create {}: {e}", parent.display())
            })?;
        }
        std::fs::write(&path, content)
            .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    }
    task.finish();
    if args.gitkeep {
        remove_unneeded_gitkeeps(&args.destination)?;
    }
    if args.remove_empty_dirs {
        remove_empty_directories(&args.destination)?;
    }
    Ok(())
}

struct Writer {
    ignore: BTreeSet<String>,
    files: BTreeMap<PathBuf, String>,
    mode_headers: bool,
}

fn create_directories(root: &Path, gitkeep: bool) -> Result<(), String> {
    for dir in PROJECT_DIRS {
        let path = root.join(dir);
        std::fs::create_dir_all(&path).map_err(|e| {
            format!("failed to create {}: {e}", path.display())
        })?;
        if gitkeep {
            let path = path.join(".gitkeep");
            std::fs::write(&path, "").map_err(|e| {
                format!("failed to create {}: {e}", path.display())
            })?;
        }
    }
    Ok(())
}

impl Writer {
    fn write_project_file(
        &mut self,
        assembly: &Assembly,
    ) -> Result<(), String> {
        let mut project = Map::new();
        project.insert(
            String::from("name"),
            Value::String(assembly.dbname.clone()),
        );
        if let Some(encoding) = &assembly.encoding {
            project.insert(
                String::from("encoding"),
                Value::String(encoding.clone()),
            );
        }
        if let Some(stdstrings) = assembly.stdstrings {
            project
                .insert(String::from("stdstrings"), Value::Bool(stdstrings));
        }
        if !assembly.extensions.is_empty() {
            project.insert(
                String::from("extensions"),
                serialize(&assembly.extensions)?,
            );
        }
        if !assembly.languages.is_empty() {
            project.insert(
                String::from("languages"),
                serialize(&assembly.languages)?,
            );
        }
        self.save_value(PathBuf::from("project.yaml"), &Value::Object(project))
    }

    /// Function files are named by function; overloads get a numeric
    /// suffix (generate_project.py _function_filename, simplified)
    fn write_functions(&mut self, assembly: &Assembly) -> Result<(), String> {
        let mut used: BTreeSet<(String, String)> = BTreeSet::new();
        for function in &assembly.functions {
            let mut filename = function.name.clone();
            let mut counter = 1;
            while used.contains(&(function.schema.clone(), filename.clone())) {
                filename = format!("{}_{counter}", function.name);
                counter += 1;
            }
            used.insert((function.schema.clone(), filename.clone()));
            self.save(
                nested("functions", &function.schema, &filename),
                function,
            )?;
        }
        Ok(())
    }

    /// Types are written as per-schema container files (types.yml)
    fn write_types(&mut self, assembly: &Assembly) -> Result<(), String> {
        let mut schemas: Vec<&str> =
            assembly.types.iter().map(|t| t.schema.as_str()).collect();
        schemas.sort_unstable();
        schemas.dedup();
        for schema in schemas {
            let types: Vec<&models::Type> = assembly
                .types
                .iter()
                .filter(|t| t.schema == schema)
                .collect();
            let container = json!({"schema": schema, "types": types});
            self.save_value(
                Path::new("types").join(format!("{schema}.yaml")),
                &container,
            )?;
        }
        Ok(())
    }

    /// Classify accumulated role state into user and role files via
    /// [`crate::pull::classify_role`] (LOGIN → user, else role; `pg_*`
    /// excluded)
    fn write_roles(&mut self, assembly: &Assembly) -> Result<(), String> {
        for (name, state) in &assembly.roles {
            let kind = match crate::pull::classify_role(name, state) {
                Some(kind) => kind,
                None => continue,
            };
            let settings = role_settings(state);
            if kind == crate::pull::RoleKind::User {
                let user = models::User {
                    name: name.clone(),
                    comment: None,
                    environments: None,
                    password: state.password.clone(),
                    valid_until: state.valid_until.clone(),
                    grants: state.grants.to_acls(),
                    revocations: state.revocations.to_acls(),
                    options: user_options(state),
                    settings,
                };
                self.save(
                    Path::new("users").join(format!("{name}.yaml")),
                    &user,
                )?;
            } else {
                let role = models::Role {
                    name: name.clone(),
                    comment: None,
                    create: (!state.created).then_some(false),
                    environments: None,
                    grants: state.grants.to_acls(),
                    revocations: state.revocations.to_acls(),
                    options: (state.options != models::RoleOptions::default())
                        .then(|| state.options.clone()),
                    settings,
                };
                self.save(
                    Path::new("roles").join(format!("{name}.yaml")),
                    &role,
                )?;
            }
        }
        Ok(())
    }

    fn save<T: serde::Serialize>(
        &mut self,
        relative: PathBuf,
        value: &T,
    ) -> Result<(), String> {
        let value = serialize(value)?;
        self.save_value(relative, &value)
    }

    fn save_value(
        &mut self,
        relative: PathBuf,
        value: &Value,
    ) -> Result<(), String> {
        let key = relative.to_string_lossy().to_string();
        if self.ignore.contains(&key) {
            log::debug!("Skipping ignored file {key}");
            return Ok(());
        }
        let body = yamlio::dump(value);
        let header = self.mode_headers.then(|| kind(&relative));
        self.files.insert(relative, yamlio::document(header, &body));
        Ok(())
    }
}

/// The object-type noun for the modeline comment, derived from the
/// destination path (the same noun the directory/schema uses)
fn kind(relative: &Path) -> &'static str {
    if relative == Path::new("project.yaml") {
        return "project";
    }
    match relative
        .components()
        .next()
        .and_then(|c| c.as_os_str().to_str())
    {
        Some("schemata") => "schema",
        Some("domains") => "domain",
        Some("sequences") => "sequence",
        Some("tables") => "table",
        Some("views") => "view",
        Some("materialized_views") => "materialized_view",
        Some("functions") => "function",
        Some("procedures") => "procedure",
        Some("types") => "type",
        Some("aggregates") => "aggregate",
        Some("casts") => "cast",
        Some("collations") => "collation",
        Some("conversions") => "conversion",
        Some("operators") => "operator",
        Some("publications") => "publication",
        Some("subscriptions") => "subscription",
        Some("servers") => "server",
        Some("tablespaces") => "tablespace",
        Some("text_search") => "text_search",
        Some("user_mappings") => "user_mapping",
        Some("event_triggers") => "event_trigger",
        Some("roles") => "role",
        Some("users") => "user",
        Some("groups") => "group",
        // remaining.yaml and any future top-level file
        _ => "object",
    }
}

/// Foreign keys order table creation: emit a `dependencies` key so
/// the build's topological sort restores referenced tables first
fn table_dependencies(table: &models::Table) -> Option<Value> {
    let this = format!("{}.{}", table.schema, table.name);
    let mut references: Vec<String> = table
        .foreign_keys
        .iter()
        .flatten()
        .map(|fk| fk.references.name.clone())
        .filter(|name| *name != this)
        .collect();
    references.sort();
    references.dedup();
    (!references.is_empty()).then(|| json!({"tables": references}))
}

/// `user.yml` does not allow the `login` option (login is implied)
fn user_options(state: &RoleState) -> Option<models::RoleOptions> {
    let mut options = state.options.clone();
    options.login = None;
    (options != models::RoleOptions::default()).then_some(options)
}

/// Convert accumulated `ALTER ROLE ... SET` state (a `name → value`
/// map) into the schema's `settings` array of single-key objects, one
/// per parameter, ordered by name for a deterministic diff
fn role_settings(state: &RoleState) -> Option<Vec<Map<String, Value>>> {
    if state.settings.is_empty() {
        return None;
    }
    let mut names: Vec<&String> = state.settings.keys().collect();
    names.sort();
    Some(
        names
            .into_iter()
            .map(|name| {
                let mut object = Map::new();
                object.insert(name.clone(), state.settings[name].clone());
                object
            })
            .collect(),
    )
}

fn serialize<T: serde::Serialize>(value: &T) -> Result<Value, String> {
    serde_json::to_value(value)
        .map_err(|e| format!("failed to serialize: {e}"))
}

fn nested(directory: &str, schema: &str, name: &str) -> PathBuf {
    Path::new(directory)
        .join(schema)
        .join(format!("{name}.yaml"))
}

pub(crate) fn read_ignore(
    path: Option<&Path>,
) -> Result<BTreeSet<String>, String> {
    let Some(path) = path else {
        return Ok(BTreeSet::new());
    };
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect())
}

/// Remove `.gitkeep` files from directories that received other files
fn remove_unneeded_gitkeeps(root: &Path) -> Result<(), String> {
    for dir in walk_directories(root)? {
        let gitkeep = dir.join(".gitkeep");
        if !gitkeep.exists() {
            continue;
        }
        let others = std::fs::read_dir(&dir)
            .map_err(|e| format!("failed to read {}: {e}", dir.display()))?
            .filter_map(|e| e.ok())
            .any(|e| e.file_name() != ".gitkeep");
        if others {
            std::fs::remove_file(&gitkeep).map_err(|e| {
                format!("failed to remove {}: {e}", gitkeep.display())
            })?;
        }
    }
    Ok(())
}

/// Remove empty directories under the project root, deepest first
fn remove_empty_directories(root: &Path) -> Result<(), String> {
    let mut directories = walk_directories(root)?;
    directories.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
    for dir in directories {
        let empty = std::fs::read_dir(&dir)
            .map_err(|e| format!("failed to read {}: {e}", dir.display()))?
            .next()
            .is_none();
        if empty {
            std::fs::remove_dir(&dir).map_err(|e| {
                format!("failed to remove {}: {e}", dir.display())
            })?;
        }
    }
    Ok(())
}

/// All directories below `root`, excluding `root` itself
fn walk_directories(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut results = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir)
            .map_err(|e| format!("failed to read {}: {e}", dir.display()))?
        {
            let entry =
                entry.map_err(|e| format!("failed to read entry: {e}"))?;
            let path = entry.path();
            if path.is_dir() {
                results.push(path.clone());
                pending.push(path);
            }
        }
    }
    Ok(results)
}
