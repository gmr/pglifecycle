//! The project directory loader (ports the load half of project.py)

use std::collections::BTreeSet;
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
    errors: usize,
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
            errors: 0,
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
            self.add_definition(ObjectType::Extension, entry);
        }
        for mut entry in array_field(&project, "foreign_data_wrappers") {
            inject(&mut entry, "owner", &self.project.superuser);
            self.add_definition(ObjectType::ForeignDataWrapper, entry);
        }
        for entry in array_field(&project, "languages") {
            self.add_definition(ObjectType::ProceduralLanguage, entry);
        }
        Ok(())
    }

    /// Read regular one-object-per-file definitions
    fn read_object_files(&mut self, ot: ObjectType) -> Result<(), String> {
        log::debug!("Reading {} definitions", ot.as_str());
        for (mut defn, _) in self.iterate_files(ot)? {
            let name = object_name(&defn)?;
            if !validate::validate_object(&ot.schema_file(), &name, &defn) {
                self.errors += 1;
                continue;
            }
            self.cache_and_remove_dependencies(ot, &mut defn);
            self.add_definition(ot, defn);
        }
        Ok(())
    }

    /// Read per-schema container files: casts, conversions, operators,
    /// text search, and types (project.py _read_objects_files)
    fn read_container_files(&mut self, ot: ObjectType) -> Result<(), String> {
        log::debug!("Reading {} objects", ot.as_str());
        let key = ot.plural_key();
        for (mut container, _) in self.iterate_files(ot)? {
            let container_schema =
                container["schema"].as_str().unwrap_or_default().to_string();
            if !validate::validate_object(key, &container_schema, &container) {
                self.errors += 1;
                continue;
            }
            if ot == ObjectType::TextSearch {
                self.add_definition(ot, container);
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
                    object_name(&entry)?
                };
                if !validate::validate_object(&ot.schema_file(), &name, &entry)
                {
                    self.errors += 1;
                    continue;
                }
                self.cache_and_remove_dependencies(ot, &mut entry);
                self.add_definition(ot, entry);
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
        let tag = defn["name"].as_str().unwrap_or_default().to_string();
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
            let item = lookup_item(
                &self.project.inventory,
                dep.desc,
                dep.namespace.as_deref(),
                &dep.tag,
            )
            .ok_or_else(|| {
                format!(
                    "Failed to find {} {}.{} for {} {}.{}",
                    dep.desc.as_str(),
                    dep.namespace.as_deref().unwrap_or_default(),
                    dep.tag,
                    dep.parent_desc.as_str(),
                    dep.parent_namespace,
                    dep.parent_tag,
                )
            })?;
            let parent = lookup_item(
                &self.project.inventory,
                dep.parent_desc,
                Some(&dep.parent_namespace),
                &dep.parent_tag,
            )
            .ok_or_else(|| {
                format!(
                    "Failed to find parent {} {}.{} for {} {}.{}",
                    dep.parent_desc.as_str(),
                    dep.parent_namespace,
                    dep.parent_tag,
                    dep.desc.as_str(),
                    dep.namespace.as_deref().unwrap_or_default(),
                    dep.tag,
                )
            })?;
            self.project.inventory[item].dependencies.insert(parent);
        }
        Ok(())
    }

    /// Deserialize a definition into its model and add it to the
    /// inventory, verifying the model round-trips to the same value
    fn add_definition(&mut self, ot: ObjectType, value: Value) {
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
        self.project.inventory.push(Item {
            id: self.project.inventory.len(),
            desc: ot,
            definition,
            dependencies: BTreeSet::new(),
        });
    }
}

/// Find an inventory item by type, schema, and name
/// (project.py _lookup_item)
fn lookup_item(
    inventory: &[Item],
    desc: ObjectType,
    namespace: Option<&str>,
    tag: &str,
) -> Option<usize> {
    inventory
        .iter()
        .find(|item| {
            item.desc == desc
                && item.definition.name() == tag
                && (desc.is_schemaless()
                    || item.definition.schema() == namespace)
        })
        .map(|item| item.id)
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
    if let Value::Object(map) = defn {
        if !map.contains_key(key) {
            map.insert(key.to_string(), Value::String(value.to_string()));
        }
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
