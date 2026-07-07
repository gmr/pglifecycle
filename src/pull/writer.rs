//! YAML project emission for `pull` (ports the storage half of
//! generate_project.py / storage.py)

use std::collections::{BTreeMap, BTreeSet, HashSet};
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
        include_password_hashes: args.include_password_hashes,
    };
    writer.write_project_file(assembly)?;
    for schema in &assembly.schemas {
        writer.save(top_level("schemata", &schema.name)?, schema)?;
    }
    for domain in &assembly.domains {
        writer
            .save(nested("domains", &domain.schema, &domain.name)?, domain)?;
    }
    for sequence in &assembly.sequences {
        writer.save(
            nested("sequences", &sequence.schema, &sequence.name)?,
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
            nested("tables", &table.schema, &table.name)?,
            &value,
        )?;
    }
    let relation_inventory = relation_inventory(assembly);
    for view in &assembly.views {
        let mut value = serialize(view)?;
        if let (Some(map), Some(dependencies)) = (
            value.as_object_mut(),
            view_dependencies(
                &view.schema,
                &view.name,
                view.query.as_deref(),
                &relation_inventory,
            ),
        ) {
            map.insert(String::from("dependencies"), dependencies);
        }
        writer
            .save_value(nested("views", &view.schema, &view.name)?, &value)?;
    }
    for view in &assembly.materialized_views {
        let mut value = serialize(view)?;
        if let (Some(map), Some(dependencies)) = (
            value.as_object_mut(),
            view_dependencies(
                &view.schema,
                &view.name,
                view.query.as_deref(),
                &relation_inventory,
            ),
        ) {
            map.insert(String::from("dependencies"), dependencies);
        }
        writer.save_value(
            nested("materialized_views", &view.schema, &view.name)?,
            &value,
        )?;
    }
    writer.write_functions(assembly)?;
    writer.write_types(assembly)?;
    for server in &assembly.servers {
        writer.save(top_level("servers", &server.name)?, server)?;
    }
    writer.write_user_mappings(assembly)?;
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
        remove_empty_directories(std::slice::from_ref(&args.destination))?;
    }
    Ok(())
}

struct Writer {
    ignore: BTreeSet<String>,
    files: BTreeMap<PathBuf, String>,
    mode_headers: bool,
    include_password_hashes: bool,
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
        if !assembly.foreign_data_wrappers.is_empty() {
            project.insert(
                String::from("foreign_data_wrappers"),
                serialize(&assembly.foreign_data_wrappers)?,
            );
        }
        self.save_value(PathBuf::from("project.yaml"), &Value::Object(project))
    }

    /// Function files are named by function; overloads get a numeric
    /// suffix (generate_project.py _function_filename, simplified). A
    /// counter-suffixed candidate is skipped when it collides with
    /// another function's real name (e.g. `fn`, `fn`, `fn_1`), so the
    /// real `fn_1` is never displaced into `fn_1_1.yaml`
    fn write_functions(&mut self, assembly: &Assembly) -> Result<(), String> {
        let reserved: HashSet<(String, String)> = assembly
            .functions
            .iter()
            .map(|f| (f.schema.clone(), f.name.clone()))
            .collect();
        let mut used: BTreeSet<(String, String)> = BTreeSet::new();
        for function in &assembly.functions {
            let mut filename = function.name.clone();
            let mut counter = 1;
            while used.contains(&(function.schema.clone(), filename.clone()))
                || (filename != function.name
                    && reserved.contains(&(
                        function.schema.clone(),
                        filename.clone(),
                    )))
            {
                filename = format!("{}_{counter}", function.name);
                counter += 1;
            }
            used.insert((function.schema.clone(), filename.clone()));
            self.save(
                nested("functions", &function.schema, &filename)?,
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
            self.save_value(top_level("types", schema)?, &container)?;
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
                    comment: state.comment.clone(),
                    environments: None,
                    password: state.password.clone(),
                    valid_until: state.valid_until.clone(),
                    grants: state.grants.to_acls(),
                    revocations: state.revocations.to_acls(),
                    options: user_options(state),
                    settings,
                };
                self.save(top_level("users", name)?, &user)?;
            } else {
                let role = models::Role {
                    name: name.clone(),
                    comment: state.comment.clone(),
                    create: (!state.created).then_some(false),
                    environments: None,
                    grants: state.grants.to_acls(),
                    revocations: state.revocations.to_acls(),
                    options: (state.options != models::RoleOptions::default())
                        .then(|| state.options.clone()),
                    settings,
                };
                self.save(top_level("roles", name)?, &role)?;
            }
        }
        Ok(())
    }

    /// Write user mappings, dropping the `password` option unless
    /// password hashes were explicitly requested (it is a secret)
    fn write_user_mappings(
        &mut self,
        assembly: &Assembly,
    ) -> Result<(), String> {
        for mapping in &assembly.user_mappings {
            let mut mapping = mapping.clone();
            if !self.include_password_hashes {
                for server in &mut mapping.servers {
                    if let Some(options) = &mut server.options {
                        options.remove("password");
                        if options.is_empty() {
                            server.options = None;
                        }
                    }
                }
            }
            self.save(top_level("user_mappings", &mapping.name)?, &mapping)?;
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

/// A `schema.name` → dependency plural key ("tables", "views",
/// "materialized_views") map of every relation the pulled inventory
/// knows about, used to resolve a view query's relation references to
/// real, managed objects
fn relation_inventory(
    assembly: &Assembly,
) -> BTreeMap<(String, String), &'static str> {
    let mut inventory = BTreeMap::new();
    for table in &assembly.tables {
        inventory.insert((table.schema.clone(), table.name.clone()), "tables");
    }
    for view in &assembly.views {
        inventory.insert((view.schema.clone(), view.name.clone()), "views");
    }
    for view in &assembly.materialized_views {
        inventory.insert(
            (view.schema.clone(), view.name.clone()),
            "materialized_views",
        );
    }
    inventory
}

/// Views carry no foreign keys, so pg_dump's default same-priority
/// alphabetical sort is the only thing ordering them relative to the
/// tables/views they query; emit a `dependencies` key (mirroring
/// `table_dependencies`) so `build`/`load` order the referenced
/// relations first. Referenced relations are found by re-parsing the
/// view's `query` text and walking its CST for schema-qualified
/// `relation_expr` nodes; unqualified references (CTE names, or
/// anything the dump didn't schema-qualify) and references to objects
/// outside the pulled inventory are conservatively skipped rather than
/// guessed at.
fn view_dependencies(
    schema: &str,
    name: &str,
    query: Option<&str>,
    inventory: &BTreeMap<(String, String), &'static str>,
) -> Option<Value> {
    let mut tables = BTreeSet::new();
    let mut views = BTreeSet::new();
    let mut materialized_views = BTreeSet::new();
    for (rel_schema, rel_name) in referenced_relations(query?) {
        if rel_schema == schema && rel_name == name {
            continue; // no self edges
        }
        let full = format!("{rel_schema}.{rel_name}");
        match inventory.get(&(rel_schema, rel_name)) {
            Some(&"tables") => {
                tables.insert(full);
            }
            Some(&"views") => {
                views.insert(full);
            }
            Some(&"materialized_views") => {
                materialized_views.insert(full);
            }
            _ => {}
        }
    }
    if tables.is_empty() && views.is_empty() && materialized_views.is_empty() {
        return None;
    }
    let mut object = Map::new();
    if !tables.is_empty() {
        object.insert(String::from("tables"), json!(tables));
    }
    if !views.is_empty() {
        object.insert(String::from("views"), json!(views));
    }
    if !materialized_views.is_empty() {
        object.insert(
            String::from("materialized_views"),
            json!(materialized_views),
        );
    }
    Some(Value::Object(object))
}

/// Parse a view/materialized view's query text and return every
/// schema-qualified relation it references (`schema`, `name`), in
/// document order with duplicates kept (the caller dedupes via a
/// `BTreeSet`). Unqualified references (bare CTE names, or anything
/// pg_dump didn't schema-qualify) are skipped rather than guessed at.
/// Returns an empty list if the query fails to parse (should not
/// happen for text pg_dump itself emitted, but re-parsing is
/// defensive here rather than load-bearing).
fn referenced_relations(query: &str) -> Vec<(String, String)> {
    use crate::ddl::{NodeExt, qualified_name};

    let mut relations = Vec::new();
    let mut parser = tree_sitter::Parser::new();
    if parser
        .set_language(&tree_sitter_postgres::LANGUAGE.into())
        .is_err()
    {
        return relations;
    }
    // The stored `query` is a bare SelectStmt (no trailing
    // semicolon); append one to reparse it as a standalone statement,
    // independent of the CREATE VIEW statement it came from.
    let sql = format!("{query};");
    let Some(tree) = parser.parse(&sql, None) else {
        return relations;
    };
    let root = tree.root_node();
    if root.has_error() {
        return relations;
    }
    for stmt in root.find_all("stmt") {
        let Some(node) = stmt.named_child(0) else {
            continue;
        };
        for relation in node.find_all("relation_expr") {
            let Some(qn) = relation.find("qualified_name") else {
                continue;
            };
            let Ok(name) = qualified_name(&qn, &sql) else {
                continue;
            };
            if let Some(schema) = name.schema {
                relations.push((schema, name.name));
            }
        }
    }
    relations
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

fn nested(
    directory: &str,
    schema: &str,
    name: &str,
) -> Result<PathBuf, String> {
    Ok(Path::new(directory)
        .join(safe_component(schema)?)
        .join(format!("{}.yaml", safe_component(name)?)))
}

/// A `directory/name.yaml` path with the name validated as a safe path
/// component
fn top_level(directory: &str, name: &str) -> Result<PathBuf, String> {
    Ok(Path::new(directory).join(format!("{}.yaml", safe_component(name)?)))
}

/// Reject a database identifier that would escape or redirect the
/// destination when used as a filesystem path segment. PostgreSQL
/// quoted identifiers may contain `/`, `\`, or `.` sequences, and
/// `Path::join` treats an absolute or `..` component as a traversal.
fn safe_component(component: &str) -> Result<&str, String> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component.contains('/')
        || component.contains('\\')
    {
        return Err(format!(
            "refusing to write to a path derived from the unsafe database \
             identifier {component:?}"
        ));
    }
    Ok(component)
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

/// Remove empty directories below each of `tops`, deepest first;
/// `tops` themselves are never removed. Shared by the fresh-write path
/// (a single `tops = [project root]`) and `--update --prune` (`tops =`
/// the managed directories), so there is one reaper instead of two
/// with diverging symlink handling
pub(crate) fn remove_empty_directories(
    tops: &[PathBuf],
) -> Result<(), String> {
    for top in tops {
        // `symlink_metadata` does not follow symlinks: a top that is a
        // symlink to a directory outside the project must be skipped so
        // `--update --prune` cannot reap empty directories under its
        // target.
        let metadata = match std::fs::symlink_metadata(top) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }
            Err(error) => {
                return Err(format!(
                    "failed to read type for {}: {error}",
                    top.display()
                ));
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            continue;
        }
        let mut directories = walk_directories(top)?;
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
    }
    Ok(())
}

/// All directories below `root`, excluding `root` itself. Symlinked
/// directories are not followed: their targets may live outside the
/// project (or loop), so treating them as ordinary subdirectories
/// could remove or wander into unrelated paths
fn walk_directories(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut results = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir)
            .map_err(|e| format!("failed to read {}: {e}", dir.display()))?
        {
            let entry =
                entry.map_err(|e| format!("failed to read entry: {e}"))?;
            let file_type = entry.file_type().map_err(|e| {
                format!(
                    "failed to read type for {}: {e}",
                    entry.path().display()
                )
            })?;
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                let path = entry.path();
                results.push(path.clone());
                pending.push(path);
            }
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_component_accepts_normal_identifiers() {
        assert_eq!(safe_component("public").unwrap(), "public");
        assert_eq!(safe_component("My Table").unwrap(), "My Table");
        assert_eq!(safe_component("weird.name").unwrap(), "weird.name");
    }

    #[test]
    fn safe_component_rejects_traversal() {
        for hostile in ["", ".", "..", "/etc", "../../tmp/x", "a/b", "a\\b"] {
            assert!(
                safe_component(hostile).is_err(),
                "expected {hostile:?} to be rejected"
            );
        }
    }

    #[test]
    fn nested_rejects_hostile_schema_or_name() {
        assert!(nested("tables", "../../../tmp", "x").is_err());
        assert!(nested("tables", "public", "..").is_err());
        assert!(nested("tables", "/etc", "x").is_err());
        assert_eq!(
            nested("tables", "public", "orders").unwrap(),
            Path::new("tables").join("public").join("orders.yaml")
        );
    }

    #[test]
    fn top_level_rejects_hostile_name() {
        assert!(top_level("roles", "../../etc/passwd").is_err());
        assert_eq!(
            top_level("roles", "admin").unwrap(),
            Path::new("roles").join("admin.yaml")
        );
    }

    fn function(schema: &str, name: &str) -> models::Function {
        models::Function {
            name: name.to_string(),
            schema: schema.to_string(),
            owner: String::from("postgres"),
            sql: None,
            parameters: None,
            returns: None,
            language: None,
            transform_types: None,
            window: None,
            immutable: None,
            stable: None,
            volatile: None,
            leak_proof: None,
            called_on_null_input: None,
            strict: None,
            security: None,
            parallel: None,
            cost: None,
            rows: None,
            support: None,
            configuration: None,
            definition: None,
            object_file: None,
            link_symbol: None,
            comment: None,
        }
    }

    /// A real `fn_1` must never be displaced into `fn_1_1.yaml` by the
    /// numeric suffix used to disambiguate `fn` overloads (L9)
    #[test]
    fn write_functions_does_not_displace_real_numbered_name() {
        let assembly = Assembly {
            functions: vec![
                function("public", "fn"),
                function("public", "fn"),
                function("public", "fn_1"),
            ],
            ..Assembly::default()
        };
        let mut writer = Writer {
            ignore: BTreeSet::new(),
            files: BTreeMap::new(),
            mode_headers: false,
            include_password_hashes: false,
        };
        writer.write_functions(&assembly).unwrap();
        let paths: Vec<&PathBuf> = writer.files.keys().collect();
        let fn_1_path =
            Path::new("functions").join("public").join("fn_1.yaml");
        assert!(
            paths.contains(&&fn_1_path),
            "expected the real fn_1 to keep its own filename: {paths:?}"
        );
        // the fn_1 file must contain the real fn_1, not an overload
        let body = &writer.files[&fn_1_path];
        assert!(body.contains("name: fn_1"), "unexpected contents: {body}");
    }

    #[test]
    fn referenced_relations_finds_schema_qualified_table() {
        let relations = referenced_relations("SELECT id FROM test.users");
        assert_eq!(
            relations,
            vec![(String::from("test"), String::from("users"))]
        );
    }

    #[test]
    fn referenced_relations_skips_unqualified_names() {
        // a bare name could be a CTE, a same-schema table pg_dump
        // chose not to qualify, or a system relation; conservatively
        // skip anything that isn't schema-qualified
        assert_eq!(referenced_relations("SELECT id FROM users"), Vec::new());
    }

    #[test]
    fn referenced_relations_skips_cte_names() {
        let relations = referenced_relations(
            "WITH recent AS (SELECT id FROM test.orders) \
             SELECT id FROM recent",
        );
        assert_eq!(
            relations,
            vec![(String::from("test"), String::from("orders"))]
        );
    }

    fn inventory(
        entries: &[(&str, &str, &'static str)],
    ) -> BTreeMap<(String, String), &'static str> {
        entries
            .iter()
            .map(|(schema, name, kind)| {
                ((schema.to_string(), name.to_string()), *kind)
            })
            .collect()
    }

    #[test]
    fn view_dependencies_emits_edge_for_known_table() {
        let inv = inventory(&[("test", "users", "tables")]);
        let deps = view_dependencies(
            "test",
            "us_users",
            Some("SELECT id FROM test.users"),
            &inv,
        );
        assert_eq!(deps, Some(json!({"tables": ["test.users"]})));
    }

    #[test]
    fn view_dependencies_emits_edge_for_known_view() {
        let inv = inventory(&[("test", "base_view", "views")]);
        let deps = view_dependencies(
            "test",
            "wrapper",
            Some("SELECT id FROM test.base_view"),
            &inv,
        );
        assert_eq!(deps, Some(json!({"views": ["test.base_view"]})));
    }

    #[test]
    fn view_dependencies_skips_unknown_relation() {
        // referenced relation is schema-qualified but absent from the
        // pulled inventory (a CTE the parser mis-detected, a system
        // catalog, or an object outside the project) — must not
        // fabricate an edge that would fail to load
        let inv = inventory(&[]);
        let deps = view_dependencies(
            "test",
            "v",
            Some("SELECT id FROM test.unknown"),
            &inv,
        );
        assert_eq!(deps, None);
    }

    #[test]
    fn view_dependencies_guards_self_reference() {
        // even if the view itself is (erroneously) present in the
        // inventory map, referencing its own qualified name must not
        // produce a self-edge
        let inv = inventory(&[("test", "v", "views")]);
        let deps = view_dependencies(
            "test",
            "v",
            Some("SELECT id FROM test.v"),
            &inv,
        );
        assert_eq!(deps, None);
    }

    #[test]
    fn view_dependencies_none_without_query() {
        let inv = inventory(&[("test", "users", "tables")]);
        assert_eq!(view_dependencies("test", "v", None, &inv), None);
    }
}
