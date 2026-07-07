//! The project directory loader (ports the load half of project.py)

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::constants::{DEPENDENCIES, ObjectType, READ_ORDER};
use crate::models::{Definition, Item};
use crate::project::{Project, validate};
use crate::yamlio;

/// A dependency recorded during the read pass and resolved once the
/// whole inventory is in memory (project.py _ItemDependency)
struct CachedDependency {
    desc: ObjectType,
    namespace: Option<String>,
    tag: String,
    parent_desc: ObjectType,
    parent_namespace: String,
    parent_tag: String,
}

pub struct Loader {
    project: Project,
    cached_dependencies: Vec<CachedDependency>,
    /// File path each inventory item was loaded from, parallel to
    /// `project.inventory`, kept for duplicate-object diagnostics
    paths: Vec<Option<PathBuf>>,
    errors: usize,
    /// `(desc, namespace, tag)` -> inventory index, maintained
    /// incrementally as items are added so both the duplicate-object
    /// guard in `add_definition` and dependency resolution in
    /// `apply_cached_dependencies` can look items up in O(1) instead of
    /// scanning the inventory. Schemaless types are always keyed with a
    /// `None` namespace, matching `lookup_item`'s old comparison which
    /// ignored namespace for them.
    index: HashMap<(ObjectType, Option<String>, String), usize>,
}

impl Loader {
    pub fn new(path: &Path) -> Self {
        Self {
            project: Project {
                name: String::from("postgres"),
                encoding: String::from("UTF8"),
                stdstrings: true,
                superuser: String::from("postgres"),
                default_schema: String::from("public"),
                path: path.to_path_buf(),
                inventory: Vec::new(),
            },
            cached_dependencies: Vec::new(),
            paths: Vec::new(),
            errors: 0,
            index: HashMap::new(),
        }
    }

    pub fn load(mut self) -> Result<Project, String> {
        self.read_project_file()?;
        for ot in READ_ORDER {
            if ot.is_per_schema_file() {
                self.read_container_files(*ot)?;
            } else {
                self.read_object_files(*ot)?;
            }
        }
        for ot in [
            ObjectType::Group,
            ObjectType::Role,
            ObjectType::User,
            ObjectType::UserMapping,
        ] {
            self.read_object_files(ot)?;
        }
        self.apply_cached_dependencies()?;
        if self.errors > 0 {
            log::error!("Project load failed with {} errors", self.errors);
            return Err(String::from("Project load failure"));
        }
        log::info!("Project loaded");
        Ok(self.project)
    }

    fn read_project_file(&mut self) -> Result<(), String> {
        log::info!("Loading project from {}", self.project.path.display());
        let path = self.project.path.join("project.yaml");
        if !path.exists() {
            return Err(String::from("Missing project file"));
        }
        let project = yamlio::load(&path)?;
        if !validate::validate_object("project", "project.yaml", &project) {
            self.errors += 1;
        }
        if let Some(name) = project["name"].as_str() {
            self.project.name = name.to_string();
        }
        if let Some(encoding) = project["encoding"].as_str() {
            self.project.encoding = encoding.to_string();
        }
        for entry in array_field(&project, "extensions") {
            self.add_definition(ObjectType::Extension, entry, Some(&path));
        }
        for mut entry in array_field(&project, "foreign_data_wrappers") {
            inject(&mut entry, "owner", &self.project.superuser);
            self.add_definition(
                ObjectType::ForeignDataWrapper,
                entry,
                Some(&path),
            );
        }
        for entry in array_field(&project, "languages") {
            self.add_definition(
                ObjectType::ProceduralLanguage,
                entry,
                Some(&path),
            );
        }
        Ok(())
    }

    /// Read regular one-object-per-file definitions
    fn read_object_files(&mut self, ot: ObjectType) -> Result<(), String> {
        log::debug!("Reading {} definitions", ot.as_str());
        for (mut defn, path) in self.iterate_files(ot)? {
            let name = match object_name(&defn) {
                Ok(name) => name,
                Err(_) => {
                    self.errors += 1;
                    continue;
                }
            };
            if !validate::validate_object(&ot.schema_file(), &name, &defn) {
                self.errors += 1;
                continue;
            }
            self.cache_and_remove_dependencies(ot, &mut defn);
            self.add_definition(ot, defn, Some(&path));
        }
        Ok(())
    }

    /// Read per-schema container files: casts, conversions, operators,
    /// text search, and types (project.py _read_objects_files)
    fn read_container_files(&mut self, ot: ObjectType) -> Result<(), String> {
        log::debug!("Reading {} objects", ot.as_str());
        let key = ot.plural_key();
        for (mut container, path) in self.iterate_files(ot)? {
            let container_schema =
                container["schema"].as_str().unwrap_or_default().to_string();
            if !validate::validate_object(key, &container_schema, &container) {
                self.errors += 1;
                continue;
            }
            if ot == ObjectType::TextSearch {
                self.add_definition(ot, container, Some(&path));
                continue;
            }
            let owner =
                container["owner"].as_str().unwrap_or_default().to_string();
            let Value::Array(entries) = container[key].take() else {
                continue;
            };
            for mut entry in entries {
                inject(&mut entry, "owner", &owner);
                inject(&mut entry, "schema", &container_schema);
                let name = if ot == ObjectType::Cast {
                    format!(
                        "({} AS {})",
                        entry["source_type"].as_str().unwrap_or_default(),
                        entry["target_type"].as_str().unwrap_or_default()
                    )
                } else {
                    match object_name(&entry) {
                        Ok(name) => name,
                        Err(_) => {
                            self.errors += 1;
                            continue;
                        }
                    }
                };
                if !validate::validate_object(&ot.schema_file(), &name, &entry)
                {
                    self.errors += 1;
                    continue;
                }
                self.cache_and_remove_dependencies(ot, &mut entry);
                self.add_definition(ot, entry, Some(&path));
            }
        }
        Ok(())
    }

    /// Yield preprocessed definitions for every YAML file of an object
    /// type, in sorted path order (project.py _iterate_files)
    fn iterate_files(
        &mut self,
        ot: ObjectType,
    ) -> Result<Vec<(Value, PathBuf)>, String> {
        let Some(subdir) = ot.path() else {
            return Ok(Vec::new());
        };
        let path = self.project.path.join(subdir);
        if !path.exists() {
            log::warn!("No {} file found in project", ot.as_str());
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        for child in sorted_dir(&path)? {
            if child.is_dir() {
                for s_child in sorted_dir(&child)? {
                    if yamlio::is_yaml(&s_child) {
                        let defn = self.preprocess_definition(
                            ot,
                            &dir_name(&child),
                            Some(&file_stem(&s_child)),
                            yamlio::load(&s_child)?,
                        );
                        results.push((defn, s_child));
                    }
                }
            } else if yamlio::is_yaml(&child) {
                let defn = self.preprocess_definition(
                    ot,
                    &file_stem(&child),
                    None,
                    yamlio::load(&child)?,
                );
                results.push((defn, child));
            }
        }
        Ok(results)
    }

    /// Inject schema, name, and owner defaults derived from the file
    /// location (project.py _preprocess_definition)
    fn preprocess_definition(
        &self,
        ot: ObjectType,
        schema: &str,
        name: Option<&str>,
        mut defn: Value,
    ) -> Value {
        if !ot.is_schemaless() {
            inject(&mut defn, "schema", schema);
        }
        if let Some(name) = name {
            inject(&mut defn, "name", name);
        }
        if !ot.is_ownerless() {
            inject(&mut defn, "owner", &self.project.superuser);
        }
        defn
    }

    /// Record `dependencies` for post-load resolution and strip the key
    /// from the definition (project.py _cache_and_remove_dependencies)
    fn cache_and_remove_dependencies(
        &mut self,
        desc: ObjectType,
        defn: &mut Value,
    ) {
        let namespace = defn["schema"].as_str().map(str::to_string);
        let tag = if desc == ObjectType::Cast {
            // casts have no `name` key; their identity is derived from
            // source/target types (see Definition::name())
            format!(
                "({} AS {})",
                defn["source_type"].as_str().unwrap_or_default(),
                defn["target_type"].as_str().unwrap_or_default()
            )
        } else {
            defn["name"].as_str().unwrap_or_default().to_string()
        };
        if let Value::Object(deps) = defn[DEPENDENCIES].take() {
            for (key, names) in &deps {
                let Some(parent_desc) = ObjectType::from_plural_key(key)
                else {
                    log::error!("Unknown dependency type {key:?}");
                    self.errors += 1;
                    continue;
                };
                for name in names.as_array().into_iter().flatten() {
                    let name = name.as_str().unwrap_or_default();
                    let (parent_namespace, parent_tag) = split_name(name);
                    self.cached_dependencies.push(CachedDependency {
                        desc,
                        namespace: namespace.clone(),
                        tag: tag.clone(),
                        parent_desc,
                        parent_namespace,
                        parent_tag,
                    });
                }
            }
        }
        if let Value::Object(map) = defn {
            map.remove(DEPENDENCIES);
        }
    }

    fn apply_cached_dependencies(&mut self) -> Result<(), String> {
        for dep in &self.cached_dependencies {
            let Some(item) = lookup_item(
                &self.index,
                dep.desc,
                dep.namespace.as_deref(),
                &dep.tag,
            ) else {
                log::warn!(
                    "Skipping dependency from {} {}.{} on missing {} {}.{}",
                    dep.desc.as_str(),
                    dep.namespace.as_deref().unwrap_or_default(),
                    dep.tag,
                    dep.parent_desc.as_str(),
                    dep.parent_namespace,
                    dep.parent_tag,
                );
                continue;
            };
            // a dependency may point at an object the project does not
            // manage (e.g. a foreign key or inheritance parent owned by
            // an extension); skip the edge rather than failing the load
            let Some(parent) = lookup_item(
                &self.index,
                dep.parent_desc,
                Some(&dep.parent_namespace),
                &dep.parent_tag,
            ) else {
                log::warn!(
                    "Skipping dependency from {} {}.{} on unmanaged {} {}.{}",
                    dep.desc.as_str(),
                    dep.namespace.as_deref().unwrap_or_default(),
                    dep.tag,
                    dep.parent_desc.as_str(),
                    dep.parent_namespace,
                    dep.parent_tag,
                );
                continue;
            };
            self.project.inventory[item].dependencies.insert(parent);
        }
        Ok(())
    }

    /// Deserialize a definition into its model and add it to the
    /// inventory, verifying the model round-trips to the same value
    fn add_definition(
        &mut self,
        ot: ObjectType,
        value: Value,
        path: Option<&Path>,
    ) {
        let definition = match to_definition(ot, value.clone()) {
            Ok(definition) => definition,
            Err(error) => {
                log::error!(
                    "Failed to load {} definition: {error}",
                    ot.as_str()
                );
                self.errors += 1;
                return;
            }
        };
        let round_trip = serde_json::to_value(&definition)
            .expect("model serialization cannot fail");
        let normalized = strip_nulls(value);
        if round_trip != normalized {
            log::error!(
                "{} {} did not round-trip: {normalized} != {round_trip}",
                ot.as_str(),
                definition.name(),
            );
            self.errors += 1;
            return;
        }
        if let Some(existing) = lookup_item(
            &self.index,
            ot,
            definition.schema(),
            &definition.name(),
        ) {
            let existing_path = self
                .paths
                .get(existing)
                .and_then(Option::as_ref)
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| String::from("<unknown>"));
            log::error!(
                "Duplicate {} {} defined in {} (already defined in {})",
                ot.as_str(),
                definition.name(),
                path.map(|p| p.display().to_string())
                    .unwrap_or_else(|| String::from("<unknown>")),
                existing_path,
            );
            self.errors += 1;
            return;
        }
        let id = self.project.inventory.len();
        let key = index_key(ot, definition.schema(), &definition.name());
        self.index.insert(key, id);
        self.project.inventory.push(Item {
            id,
            desc: ot,
            definition,
            dependencies: BTreeSet::new(),
        });
        self.paths.push(path.map(Path::to_path_buf));
    }
}

/// Build the `index` key for a `(desc, namespace, tag)` triple,
/// normalizing the namespace to `None` for schemaless types so lookups
/// and insertions always agree regardless of what the caller passed
/// (project.py _lookup_item ignored namespace entirely in that case)
fn index_key(
    desc: ObjectType,
    namespace: Option<&str>,
    tag: &str,
) -> (ObjectType, Option<String>, String) {
    let namespace = if desc.is_schemaless() {
        None
    } else {
        namespace.map(str::to_string)
    };
    (desc, namespace, tag.to_string())
}

/// Find an inventory item by type, schema, and name via the loader's
/// `(desc, namespace, tag)` index (project.py _lookup_item)
fn lookup_item(
    index: &HashMap<(ObjectType, Option<String>, String), usize>,
    desc: ObjectType,
    namespace: Option<&str>,
    tag: &str,
) -> Option<usize> {
    index.get(&index_key(desc, namespace, tag)).copied()
}

fn to_definition(
    ot: ObjectType,
    value: Value,
) -> Result<Definition, serde_json::Error> {
    use serde_json::from_value as from;
    Ok(match ot {
        ObjectType::Aggregate => Definition::Aggregate(from(value)?),
        ObjectType::Cast => Definition::Cast(from(value)?),
        ObjectType::Collation => Definition::Collation(from(value)?),
        ObjectType::Conversion => Definition::Conversion(from(value)?),
        ObjectType::Domain => Definition::Domain(from(value)?),
        ObjectType::EventTrigger => Definition::EventTrigger(from(value)?),
        ObjectType::Extension => Definition::Extension(from(value)?),
        ObjectType::ForeignDataWrapper => {
            Definition::ForeignDataWrapper(from(value)?)
        }
        ObjectType::Function => Definition::Function(from(value)?),
        ObjectType::Group => Definition::Group(from(value)?),
        ObjectType::MaterializedView => {
            Definition::MaterializedView(from(value)?)
        }
        ObjectType::Operator => Definition::Operator(from(value)?),
        ObjectType::ProceduralLanguage => Definition::Language(from(value)?),
        ObjectType::Publication => Definition::Publication(from(value)?),
        ObjectType::Role => Definition::Role(from(value)?),
        ObjectType::Schema => Definition::Schema(from(value)?),
        ObjectType::Sequence => Definition::Sequence(from(value)?),
        ObjectType::Server => Definition::Server(from(value)?),
        ObjectType::Subscription => Definition::Subscription(from(value)?),
        ObjectType::Table => Definition::Table(from(value)?),
        ObjectType::Tablespace => Definition::Tablespace(from(value)?),
        ObjectType::TextSearch => Definition::TextSearch(from(value)?),
        ObjectType::Type => Definition::Type(from(value)?),
        ObjectType::User => Definition::User(from(value)?),
        ObjectType::UserMapping => Definition::UserMapping(from(value)?),
        ObjectType::View => Definition::View(from(value)?),
    })
}

/// `schema.name` for logging (project.py _object_name); name is required
fn object_name(defn: &Value) -> Result<String, String> {
    let Some(name) = defn["name"].as_str() else {
        log::error!("name missing from definition: {defn}");
        return Err(String::from("Missing object name"));
    };
    match defn["schema"].as_str() {
        Some(schema) => Ok(format!("{schema}.{name}")),
        None => Ok(name.to_string()),
    }
}

/// Set a string key on a mapping unless it is already present
fn inject(defn: &mut Value, key: &str, value: &str) {
    if let Value::Object(map) = defn
        && !map.contains_key(key)
    {
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
}

fn array_field(value: &Value, key: &str) -> Vec<Value> {
    value[key].as_array().cloned().unwrap_or_default()
}

/// `schema.name` → (schema, name); unqualified names get an empty
/// namespace (utils.split_name)
fn split_name(value: &str) -> (String, String) {
    match value.split_once('.') {
        Some((namespace, tag)) => (namespace.to_string(), tag.to_string()),
        None => (String::new(), value.to_string()),
    }
}

/// Drop null-valued keys so explicit YAML nulls compare equal to
/// omitted optional fields in round-trip verification
fn strip_nulls(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k, strip_nulls(v)))
                .collect(),
        ),
        Value::Array(items) => {
            Value::Array(items.into_iter().map(strip_nulls).collect())
        }
        other => other,
    }
}

fn sorted_dir(path: &Path) -> Result<Vec<PathBuf>, String> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    Ok(entries)
}

fn file_stem(path: &Path) -> String {
    path.file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.split('.').next())
        .unwrap_or_default()
        .to_string()
}

fn dir_name(path: &Path) -> String {
    path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// M6: a second object with the same (desc, schema, name) is
    /// rejected as an error instead of silently duplicating the item
    #[test]
    fn duplicate_objects_are_flagged_as_errors() {
        let dir = tempfile::tempdir().unwrap();
        let users = dir.path().join("users");
        std::fs::create_dir_all(&users).unwrap();
        std::fs::write(users.join("a.yaml"), "name: dup\n").unwrap();
        std::fs::write(users.join("b.yaml"), "name: dup\n").unwrap();

        let mut loader = Loader::new(dir.path());
        loader.read_object_files(ObjectType::User).unwrap();

        assert_eq!(loader.errors, 1);
        assert_eq!(loader.project.inventory.len(), 1);
    }

    /// L7: a cast's `dependencies` block is cached under the same tag
    /// `Definition::name()` computes for it, so the edge resolves
    /// instead of being dropped as "missing"
    #[test]
    fn cast_dependency_edge_resolves() {
        let mut loader = Loader::new(Path::new("."));
        loader.project.inventory.push(Item {
            id: 0,
            desc: ObjectType::Extension,
            definition: Definition::Extension(crate::models::Extension {
                name: String::from("myext"),
                schema: Some(String::from("public")),
                version: None,
                cascade: None,
                comment: None,
            }),
            dependencies: BTreeSet::new(),
        });
        loader.index.insert(
            index_key(ObjectType::Extension, Some("public"), "myext"),
            0,
        );

        let mut entry = json!({
            "source_type": "int4",
            "target_type": "text",
            "schema": "test",
            "owner": "postgres",
            "function": "f",
            "dependencies": {"extensions": ["public.myext"]},
        });
        loader.cache_and_remove_dependencies(ObjectType::Cast, &mut entry);
        assert_eq!(entry.get("dependencies"), None);
        loader.add_definition(ObjectType::Cast, entry, None);
        loader.apply_cached_dependencies().unwrap();

        assert_eq!(loader.project.inventory[1].dependencies, [0].into());
    }

    /// L10: a container entry missing `name` is skipped as an error
    /// rather than aborting the read of the rest of the file
    #[test]
    fn missing_name_skips_entry_without_aborting() {
        let dir = tempfile::tempdir().unwrap();
        let operators = dir.path().join("operators");
        std::fs::create_dir_all(&operators).unwrap();
        std::fs::write(
            operators.join("t.yaml"),
            "operators:\n  \
             - function: f1\n    \
               left_arg: int4\n    \
               right_arg: int4\n  \
             - name: valid_op\n    \
               function: f2\n    \
               left_arg: int4\n    \
               right_arg: int4\n",
        )
        .unwrap();

        let mut loader = Loader::new(dir.path());
        loader.read_container_files(ObjectType::Operator).unwrap();

        assert_eq!(loader.errors, 1);
        assert_eq!(loader.project.inventory.len(), 1);
        assert_eq!(loader.project.inventory[0].definition.name(), "valid_op");
    }

    /// L8: dependency keys for object types omitted from
    /// `from_plural_key` (e.g. servers) are recognized instead of
    /// being rejected as "Unknown dependency type"
    #[test]
    fn previously_unsupported_dependency_type_is_recognized() {
        let mut loader = Loader::new(Path::new("."));
        let mut defn = json!({
            "name": "child",
            "schema": "test",
            "dependencies": {"servers": ["myserver"]},
        });
        loader.cache_and_remove_dependencies(ObjectType::Table, &mut defn);

        assert_eq!(loader.errors, 0);
        assert_eq!(loader.cached_dependencies.len(), 1);
        let dep = &loader.cached_dependencies[0];
        assert_eq!(dep.parent_desc, ObjectType::Server);
        assert_eq!(dep.parent_tag, "myserver");
    }
}
