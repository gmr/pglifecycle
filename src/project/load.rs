//! The project directory loader (ports the load half of project.py)

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::constants::{DEPENDENCIES, ObjectType, READ_ORDER};
use crate::deploy::identity_type;
use crate::models::{Definition, Item};
use crate::project::{Project, validate};
use crate::yamlio;

/// A dependency recorded during the read pass and resolved once the
/// whole inventory is in memory (project.py _ItemDependency). The
/// dependent object is held as its inventory id rather than its name,
/// so an overloaded function's edges land on the overload that
/// declared them
struct CachedDependency {
    item: usize,
    parent_desc: ObjectType,
    parent_namespace: String,
    parent_tag: String,
}

/// A parent reference read from a definition's `dependencies` block,
/// waiting for [`Loader::add_definition`] to assign the dependent
/// object its inventory id
struct PendingDependency {
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
    /// `(desc, namespace, tag)` -> inventory indexes, maintained
    /// incrementally as items are added so both the duplicate-object
    /// guard in `add_definition` and dependency resolution in
    /// `apply_cached_dependencies` can look items up in O(1) instead of
    /// scanning the inventory. Schemaless types are always keyed with a
    /// `None` namespace, matching `lookup_item`'s old comparison which
    /// ignored namespace for them. The key is the bare name, so
    /// overloaded functions share one entry: a name is what a
    /// `dependencies` block can reference, while identity (the
    /// signature) is what makes two definitions duplicates.
    index: HashMap<(ObjectType, Option<String>, String), Vec<usize>>,
    /// Parent references read from the definition currently being
    /// loaded, drained by `add_definition` once the item has an id
    pending_dependencies: Vec<PendingDependency>,
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
            pending_dependencies: Vec::new(),
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
        self.apply_structural_dependencies();
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
        for entry in array_field(&project, "access_methods") {
            self.add_definition(ObjectType::AccessMethod, entry, Some(&path));
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
            self.cache_and_remove_dependencies(&mut defn);
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
                if !ot.is_ownerless() {
                    inject(&mut entry, "owner", &owner);
                }
                inject(&mut entry, "schema", &container_schema);
                let name = if ot == ObjectType::Cast {
                    format!(
                        "({} AS {})",
                        entry["source_type"].as_str().unwrap_or_default(),
                        entry["target_type"].as_str().unwrap_or_default()
                    )
                } else if ot == ObjectType::Transform {
                    format!(
                        "FOR {} LANGUAGE {}",
                        entry["type"].as_str().unwrap_or_default(),
                        entry["language"].as_str().unwrap_or_default()
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
                self.cache_and_remove_dependencies(&mut entry);
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
    /// from the definition (project.py _cache_and_remove_dependencies).
    /// The parents are held in `pending_dependencies` until
    /// `add_definition` pairs them with the dependent object's id
    fn cache_and_remove_dependencies(&mut self, defn: &mut Value) {
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
                    self.pending_dependencies.push(PendingDependency {
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

    /// Order each object after the objects its own definition names,
    /// which have to exist before it is created: a table's `INHERITS`
    /// parents and `LIKE` source; the functions an aggregate, cast,
    /// conversion or event trigger calls; the types an aggregate or
    /// cast uses; and a publication's tables and schemas. Text search
    /// objects are ordered one by one in the build instead, because a
    /// container stands for a whole schema and is too coarse to order.
    ///
    /// A pulled project carries only the INHERITS edge in its
    /// `dependencies` block, and never has a `LIKE`, since pg_dump
    /// expands one into explicit columns. Nothing required a
    /// hand-written project to declare any of these as well, so the
    /// build was free to sort an object ahead of what it uses, and the
    /// restore failed. The definition already states the relationship,
    /// so it is read from there. A name the project does not manage,
    /// such as a `pg_catalog` function, orders nothing.
    fn apply_structural_dependencies(&mut self) {
        let mut edges = Vec::new();
        for (id, item) in self.project.inventory.iter().enumerate() {
            let own_schema = item.definition.schema().unwrap_or_default();
            let mut references: Vec<(ObjectType, String)> = Vec::new();
            let functions = |names: &[&Option<String>]| {
                names
                    .iter()
                    .filter_map(|name| name.as_deref())
                    .map(|name| (ObjectType::Function, name.to_string()))
                    .collect::<Vec<_>>()
            };
            match &item.definition {
                Definition::Table(table) => {
                    let sources =
                        table.parents.iter().flatten().chain(
                            table.like_table.iter().map(|like| &like.name),
                        );
                    for source in sources {
                        references.push((ObjectType::Table, source.clone()));
                    }
                }
                Definition::Aggregate(a) => {
                    references.extend(functions(&[
                        &Some(a.sfunc.clone()),
                        &a.ffunc,
                        &a.combinefunc,
                        &a.serialfunc,
                        &a.deserialfunc,
                        &a.msfunc,
                        &a.minvfunc,
                        &a.mffunc,
                    ]));
                    let types = a
                        .arguments
                        .iter()
                        .chain(a.order_by.iter().flatten())
                        .map(|arg| arg.data_type.clone())
                        .chain([a.state_data_type.clone()])
                        .chain(a.mstate_data_type.clone());
                    references.extend(types.flat_map(type_references));
                }
                Definition::Cast(c) => {
                    references.extend(functions(&[&c.function]));
                    references.extend(
                        [&c.source_type, &c.target_type]
                            .into_iter()
                            .flatten()
                            .cloned()
                            .flat_map(type_references),
                    );
                }
                Definition::Conversion(c) => {
                    references.extend(functions(&[&c.function]));
                }
                Definition::Transform(t) => {
                    references.extend(functions(&[&t.from_sql, &t.to_sql]));
                    references.extend(type_references(t.data_type.clone()));
                    references.push((
                        ObjectType::ProceduralLanguage,
                        t.language.clone(),
                    ));
                }
                Definition::Statistics(s) => {
                    references.push((ObjectType::Table, s.table.clone()));
                    references
                        .push((ObjectType::MaterializedView, s.table.clone()));
                }
                Definition::EventTrigger(t) => {
                    references.extend(functions(&[&t.function]));
                }
                Definition::AccessMethod(m) => {
                    references.extend(functions(&[&Some(m.handler.clone())]));
                }
                Definition::OperatorFamily(f) => {
                    references
                        .push((ObjectType::AccessMethod, f.method.clone()));
                    references.extend(operator_class_members(
                        f.operators.as_deref(),
                        f.functions.as_deref(),
                    ));
                }
                Definition::OperatorClass(c) => {
                    references
                        .push((ObjectType::AccessMethod, c.method.clone()));
                    if let Some(family) = &c.family {
                        references.push((
                            ObjectType::OperatorFamily,
                            family.clone(),
                        ));
                    }
                    references.extend(
                        [Some(c.data_type.clone()), c.storage.clone()]
                            .into_iter()
                            .flatten()
                            .flat_map(type_references),
                    );
                    references.extend(operator_class_members(
                        c.operators.as_deref(),
                        c.functions.as_deref(),
                    ));
                }
                Definition::Type(t) => {
                    references.extend(functions(&[
                        &t.input,
                        &t.output,
                        &t.receive,
                        &t.send,
                        &t.typmod_in,
                        &t.typmod_out,
                        &t.analyze,
                    ]));
                }
                Definition::Language(l) => {
                    references.extend(functions(&[
                        &l.handler,
                        &l.inline_handler,
                        &l.validator,
                    ]));
                }
                Definition::Function(f) => {
                    references.extend(f.language.iter().map(|language| {
                        (ObjectType::ProceduralLanguage, language.clone())
                    }));
                }
                Definition::Procedure(p) => {
                    references.extend(p.language.iter().map(|language| {
                        (ObjectType::ProceduralLanguage, language.clone())
                    }));
                }
                Definition::Publication(p) => {
                    for table in p.tables.iter().flatten() {
                        references.push((
                            ObjectType::Table,
                            table.name().to_string(),
                        ));
                    }
                    for schema in p.schemas.iter().flatten() {
                        references.push((ObjectType::Schema, schema.clone()));
                    }
                }
                _ => {}
            }
            for (desc, reference) in references {
                // a function reference may carry its argument list
                let reference =
                    reference.split('(').next().unwrap_or_default();
                let (namespace, tag) = split_sql_name(reference);
                let (namespace, tag) = match desc {
                    ObjectType::Schema | ObjectType::ProceduralLanguage => {
                        (String::new(), reference.to_string())
                    }
                    _ if namespace.is_empty() => (own_schema.to_string(), tag),
                    _ => (namespace, tag),
                };
                // a language has no schema, and the index keys it so
                let namespace = (desc != ObjectType::ProceduralLanguage)
                    .then_some(namespace.as_str());
                let found = lookup_items(&self.index, desc, namespace, &tag)
                    .into_iter()
                    .chain(if desc == ObjectType::Type {
                        lookup_items(
                            &self.index,
                            ObjectType::Domain,
                            namespace,
                            &tag,
                        )
                    } else {
                        Vec::new()
                    });
                for parent in found {
                    if parent != id {
                        edges.push((id, parent));
                    }
                }
            }
        }
        // an attached partition is a table of its own, after its
        // partitioned table, so that deploy makes the partitioned
        // table's indexes before it attaches a partition's to them
        for (id, item) in self.project.inventory.iter().enumerate() {
            let Definition::Table(table) = &item.definition else {
                continue;
            };
            for partition in table.partitions.iter().flatten() {
                if partition.attached != Some(true) {
                    continue;
                }
                for child in lookup_items(
                    &self.index,
                    ObjectType::Table,
                    Some(&partition.schema),
                    &partition.name,
                ) {
                    edges.push((child, id));
                }
            }
        }
        for (id, parent) in edges {
            self.project.inventory[id].dependencies.insert(parent);
        }
    }

    fn apply_cached_dependencies(&mut self) -> Result<(), String> {
        for dep in &self.cached_dependencies {
            if self.is_stale_foreign_key_edge(dep) {
                continue;
            }
            // a dependency may point at an object the project does not
            // manage (e.g. an inheritance parent owned by an
            // extension); skip the edge rather than failing the load
            let parents = lookup_items(
                &self.index,
                dep.parent_desc,
                Some(&dep.parent_namespace),
                &dep.parent_tag,
            );
            if parents.is_empty() {
                let item = &self.project.inventory[dep.item];
                log::warn!(
                    "Skipping dependency from {} {}.{} on unmanaged {} {}.{}",
                    item.desc.as_str(),
                    item.definition.schema().unwrap_or_default(),
                    item.definition.name(),
                    dep.parent_desc.as_str(),
                    dep.parent_namespace,
                    dep.parent_tag,
                );
                continue;
            }
            // a `dependencies` entry names an object, and a name is
            // ambiguous across overloads, so every overload of that
            // name is ordered ahead of the dependent object. A missing
            // edge fails a restore outright, where a redundant one at
            // worst costs ordering, so the ambiguity resolves wide
            for parent in parents {
                if parent != dep.item {
                    self.project.inventory[dep.item]
                        .dependencies
                        .insert(parent);
                }
            }
        }
        Ok(())
    }

    /// Whether `dep` is a table-to-table edge a project pulled before
    /// build deviation 14 recorded for a foreign key.
    ///
    /// Two relations between tables order their creation, and both
    /// name the other table in the dependent table's own definition:
    /// INHERITS through `parents`, and LIKE through `like_table`. An
    /// edge either of those backs is kept. A foreign key used to add
    /// an edge too, so that an inline `FOREIGN KEY` clause would find
    /// its referenced table. The build now emits every foreign key as
    /// its own post-data entry, which already sorts after every table,
    /// and the old edge became harmful: two tables that reference each
    /// other make it a cycle, and libpgdump breaks a cycle by hoisting
    /// its members ahead of everything else in the archive, including
    /// the CREATE SCHEMA they need (gmr/libpgdump#14). Such a project
    /// builds an archive that no longer restores, so drop the edge
    /// instead of keeping faith with it, and name it so the operator
    /// knows to pull again.
    fn is_stale_foreign_key_edge(&self, dep: &CachedDependency) -> bool {
        if dep.parent_desc != ObjectType::Table {
            return false;
        }
        let item = &self.project.inventory[dep.item];
        let Definition::Table(table) = &item.definition else {
            return false;
        };
        let names_the_parent = |name: &str| {
            let (namespace, tag) = split_name(name);
            tag == dep.parent_tag
                && (namespace == dep.parent_namespace || namespace.is_empty())
        };
        let ordered = table
            .parents
            .iter()
            .flatten()
            .any(|parent| names_the_parent(parent))
            || table
                .like_table
                .as_ref()
                .is_some_and(|like| names_the_parent(&like.name));
        if ordered {
            return false;
        }
        log::warn!(
            "Ignoring the foreign-key dependency of table {}.{} on \
             table {}.{}. Pull the project again to remove it.",
            table.schema,
            table.name,
            dep.parent_namespace,
            dep.parent_tag,
        );
        true
    }

    /// Deserialize a definition into its model and add it to the
    /// inventory, verifying the model round-trips to the same value
    fn add_definition(
        &mut self,
        ot: ObjectType,
        value: Value,
        path: Option<&Path>,
    ) {
        let pending = std::mem::take(&mut self.pending_dependencies);
        let mut definition = match to_definition(ot, value.clone()) {
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
        // after the round-trip check, which compares what was read
        match &mut definition {
            Definition::Table(table) => {
                normalize_table(table);
                index_expressions(table.indexes.as_mut());
            }
            Definition::MaterializedView(view) => {
                index_expressions(view.indexes.as_mut());
            }
            _ => {}
        }
        // two definitions collide only when they share an identity:
        // overloaded functions share a name but not a signature
        let key_identity = identity(&definition);
        let key = index_key(ot, definition.schema(), &definition.name());
        if let Some(existing) = self
            .index
            .get(&key)
            .into_iter()
            .flatten()
            .copied()
            .find(|&i| {
                identity(&self.project.inventory[i].definition) == key_identity
            })
        {
            let existing_path = self
                .paths
                .get(existing)
                .and_then(Option::as_ref)
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| String::from("<unknown>"));
            log::error!(
                "Duplicate {} {} defined in {} (already defined in {})",
                ot.as_str(),
                key_identity,
                path.map(|p| p.display().to_string())
                    .unwrap_or_else(|| String::from("<unknown>")),
                existing_path,
            );
            self.errors += 1;
            return;
        }
        let id = self.project.inventory.len();
        self.index.entry(key).or_default().push(id);
        self.project.inventory.push(Item {
            id,
            desc: ot,
            definition,
            dependencies: BTreeSet::new(),
        });
        self.paths.push(path.map(Path::to_path_buf));
        self.cached_dependencies
            .extend(pending.into_iter().map(|parent| CachedDependency {
                item: id,
                parent_desc: parent.parent_desc,
                parent_namespace: parent.parent_namespace,
                parent_tag: parent.parent_tag,
            }));
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

/// Find every inventory item of a type, schema, and name via the
/// loader's `(desc, namespace, tag)` index (project.py _lookup_item).
/// More than one is returned only for overloaded functions
fn lookup_items(
    index: &HashMap<(ObjectType, Option<String>, String), Vec<usize>>,
    desc: ObjectType,
    namespace: Option<&str>,
    tag: &str,
) -> Vec<usize> {
    index
        .get(&index_key(desc, namespace, tag))
        .cloned()
        .unwrap_or_default()
}

/// What makes two definitions the same object. A function is
/// identified by its signature, not its name, so `f(integer)` and
/// `f(text)` are distinct objects living in `f.yaml` and `f_1.yaml`
/// (the same identity deploy keys functions by)
fn identity(definition: &Definition) -> String {
    match definition {
        Definition::Function(f) => f.identity(),
        Definition::Procedure(p) => p.identity(),
        // overloads share a name, and differ in their argument types.
        // Type aliases are made canonical and typmods are removed, so
        // `int4` and `integer` are the same argument type, and
        // `varchar(10)` and `character varying` are too
        Definition::Operator(o) => format!(
            "{}({}, {})",
            o.name,
            identity_type(o.left_arg.as_deref().unwrap_or("NONE")),
            identity_type(o.right_arg.as_deref().unwrap_or("NONE"))
        ),
        // PostgreSQL identifies an ordered-set aggregate by its direct
        // and ORDER BY argument types together, so `a(integer, bigint)`
        // and `a(integer ORDER BY bigint)` are the same object
        Definition::Aggregate(a) => format!(
            "{}({})",
            a.name,
            a.arguments
                .iter()
                .chain(a.order_by.iter().flatten())
                .map(|a| identity_type(&a.data_type))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        // one name can be used once for each index method
        Definition::OperatorClass(c) => {
            format!("{} USING {}", c.name, c.method)
        }
        Definition::OperatorFamily(f) => {
            format!("{} USING {}", f.name, f.method)
        }
        _ => definition.name(),
    }
}

fn to_definition(
    ot: ObjectType,
    value: Value,
) -> Result<Definition, serde_json::Error> {
    use serde_json::from_value as from;
    Ok(match ot {
        ObjectType::AccessMethod => Definition::AccessMethod(from(value)?),
        ObjectType::Aggregate => Definition::Aggregate(from(value)?),
        ObjectType::Cast => Definition::Cast(from(value)?),
        ObjectType::Transform => Definition::Transform(from(value)?),
        ObjectType::Collation => Definition::Collation(from(value)?),
        ObjectType::Conversion => Definition::Conversion(from(value)?),
        ObjectType::DefaultPrivileges => {
            Definition::DefaultPrivileges(from(value)?)
        }
        ObjectType::Domain => Definition::Domain(from(value)?),
        ObjectType::EventTrigger => Definition::EventTrigger(from(value)?),
        ObjectType::Extension => Definition::Extension(from(value)?),
        ObjectType::ForeignDataWrapper => {
            Definition::ForeignDataWrapper(from(value)?)
        }
        ObjectType::Function => Definition::Function(from(value)?),
        ObjectType::Procedure => Definition::Procedure(from(value)?),
        ObjectType::Group => Definition::Group(from(value)?),
        ObjectType::MaterializedView => {
            Definition::MaterializedView(from(value)?)
        }
        ObjectType::Operator => Definition::Operator(from(value)?),
        ObjectType::OperatorClass => Definition::OperatorClass(from(value)?),
        ObjectType::OperatorFamily => Definition::OperatorFamily(from(value)?),
        ObjectType::ProceduralLanguage => Definition::Language(from(value)?),
        ObjectType::Publication => Definition::Publication(from(value)?),
        ObjectType::Role => Definition::Role(from(value)?),
        ObjectType::Schema => Definition::Schema(from(value)?),
        ObjectType::Sequence => Definition::Sequence(from(value)?),
        ObjectType::Server => Definition::Server(from(value)?),
        ObjectType::Statistics => Definition::Statistics(from(value)?),
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
/// Write each index expression as pull writes it, without the
/// parentheses that enclose all of it, so that `((a)::text)` and
/// `(a)::text` compare the same in deploy
fn index_expressions(indexes: Option<&mut Vec<crate::models::Index>>) {
    for index in indexes.into_iter().flatten() {
        for column in index.columns.iter_mut().flatten() {
            if let Some(expression) = &mut column.expression {
                *expression =
                    crate::utils::strip_outer_parens(expression).to_string();
            }
        }
    }
}

/// Write the parts of a table that the project can give in more than
/// one form in the form that pull writes, so that deploy compares them
/// as equal:
///
/// - a key given as one column name is a list of one column
/// - a default given as a number is its text; pull writes a boolean
///   default as a boolean
/// - a foreign key with no name gets the name PostgreSQL generates
fn normalize_table(table: &mut crate::models::Table) {
    use crate::models::ConstraintColumns;
    let keys = table
        .primary_key
        .iter_mut()
        .chain(table.unique_constraints.iter_mut().flatten());
    for key in keys {
        if let ConstraintColumns::Name(column) = key {
            *key = ConstraintColumns::Columns(vec![std::mem::take(column)]);
        }
    }
    for column in table.columns.iter_mut().flatten() {
        if let Some(default @ Value::Number(_)) = &mut column.default {
            *default = Value::String(default.to_string());
        }
    }
    for fk in table.foreign_keys.iter_mut().flatten() {
        if fk.name.is_empty() {
            fk.name =
                generated_name(&table.name, &fk.columns.join("_"), "fkey");
        }
    }
}

/// The constraint name PostgreSQL generates, `<table>_<columns>_<label>`
/// cut to 63 bytes by `makeObjectName`: it takes a byte from the longer
/// of the two names until the name fits, then cuts each name back to a
/// character boundary. PostgreSQL adds a number when the name is in
/// use; that case is not known here, so such a key needs its name in
/// the project.
pub(crate) fn generated_name(
    table: &str,
    columns: &str,
    label: &str,
) -> String {
    const MAX: usize = 63;
    let available = MAX - label.len() - 2;
    let (mut table_len, mut columns_len) = (table.len(), columns.len());
    while table_len + columns_len > available {
        if table_len > columns_len {
            table_len -= 1;
        } else {
            columns_len -= 1;
        }
    }
    format!(
        "{}_{}_{label}",
        clip(table, table_len),
        clip(columns, columns_len)
    )
}

/// The longest start of `name` that is not more than `len` bytes and
/// ends on a character boundary, as `pg_mbcliplen` gives
fn clip(name: &str, mut len: usize) -> &str {
    while !name.is_char_boundary(len) {
        len -= 1;
    }
    &name[..len]
}

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

/// Split a `schema.table` SQL reference, as INHERITS and LIKE state
/// it, into the names the index keys on. A quoted part loses its
/// quotes and may contain a dot; an unquoted part is kept as written,
/// as [`split_name`] keeps it.
pub(crate) fn split_sql_name(value: &str) -> (String, String) {
    let mut parts = vec![String::new()];
    let mut quoted = false;
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if quoted && chars.peek() == Some(&'"') => {
                chars.next();
                parts.last_mut().unwrap().push('"');
            }
            '"' => quoted = !quoted,
            '.' if !quoted => parts.push(String::new()),
            _ => parts.last_mut().unwrap().push(c),
        }
    }
    let tag = parts.pop().unwrap_or_default();
    (parts.pop().unwrap_or_default(), tag)
}

/// The operators, functions, sort families and types that the members
/// of an operator class or family name
fn operator_class_members(
    operators: Option<&[crate::models::OperatorClassOperator]>,
    functions: Option<&[crate::models::OperatorClassFunction]>,
) -> Vec<(ObjectType, String)> {
    let operators = operators.unwrap_or_default();
    let functions = functions.unwrap_or_default();
    let types = operators
        .iter()
        .flat_map(|o| o.arguments.iter().flatten())
        .chain(functions.iter().flat_map(|f| f.types.iter().flatten()))
        .cloned()
        .flat_map(type_references);
    operators
        .iter()
        .map(|o| (ObjectType::Operator, o.name.clone()))
        .chain(
            functions
                .iter()
                .map(|f| (ObjectType::Function, f.function.clone())),
        )
        // the sort family of a FOR ORDER BY operator
        .chain(
            operators
                .iter()
                .filter_map(|o| o.order_by.clone())
                .map(|family| (ObjectType::OperatorFamily, family)),
        )
        .chain(types)
        .collect()
}

/// The type a type name refers to, for ordering: its name without an
/// array suffix or modifier. Built-in types are not in the project, so
/// the lookup finds nothing for them.
fn type_references(data_type: String) -> Option<(ObjectType, String)> {
    let name = data_type
        .split(['(', '['])
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();
    (!name.is_empty()).then_some((ObjectType::Type, name))
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

    /// Operator and aggregate overloads share a name, but not an
    /// identity. A type alias gives the same identity as its canonical
    /// name, and a typmod does not change it. An ordered-set aggregate
    /// has the identity of the aggregate with the same argument types in
    /// one list
    #[test]
    fn operator_and_aggregate_overloads_have_their_own_identity() {
        let operator = |right: &str| {
            to_definition(
                ObjectType::Operator,
                json!({"name": "!!!", "schema": "s", "owner": "o",
                       "function": "f", "right_arg": right}),
            )
            .unwrap()
        };
        assert_ne!(
            identity(&operator("integer")),
            identity(&operator("bigint"))
        );
        assert_eq!(
            identity(&operator("int4")),
            identity(&operator("integer"))
        );
        let aggregate = |data_type: &str| {
            to_definition(
                ObjectType::Aggregate,
                json!({"name": "agg", "schema": "s", "owner": "o",
                       "sfunc": "f", "state_data_type": data_type,
                       "arguments": [{"data_type": data_type}]}),
            )
            .unwrap()
        };
        assert_ne!(
            identity(&aggregate("integer")),
            identity(&aggregate("bigint"))
        );
        assert_eq!(
            identity(&aggregate("int4")),
            identity(&aggregate("integer"))
        );
        // a typmod is not part of the argument type
        assert_eq!(
            identity(&operator("varchar(10)")),
            identity(&operator("character varying"))
        );
        assert_eq!(
            identity(&aggregate("numeric(10,2)")),
            identity(&aggregate("numeric"))
        );
        let ordered_set = to_definition(
            ObjectType::Aggregate,
            json!({"name": "agg", "schema": "s", "owner": "o",
                   "sfunc": "f", "state_data_type": "integer",
                   "arguments": [{"data_type": "integer"}],
                   "order_by": [{"data_type": "bigint"}]}),
        )
        .unwrap();
        let ordinary = to_definition(
            ObjectType::Aggregate,
            json!({"name": "agg", "schema": "s", "owner": "o",
                   "sfunc": "f", "state_data_type": "integer",
                   "arguments": [{"data_type": "integer"},
                                 {"data_type": "bigint"}]}),
        )
        .unwrap();
        assert_eq!(identity(&ordered_set), identity(&ordinary));
    }

    /// M6: a second object with the same (desc, schema, name) is
    /// rejected as an error instead of silently duplicating the item
    #[test]
    fn generated_foreign_key_name_matches_postgres() {
        assert_eq!(
            generated_name("addresses", "user_id", "fkey"),
            "addresses_user_id_fkey"
        );
        // cut to 63 bytes, a character at a time from the longer part:
        // PostgreSQL 18 names the foreign key of a table of 40 `a` on a
        // column of 40 `b` the same
        let name = generated_name(&"a".repeat(40), &"b".repeat(40), "fkey");
        assert_eq!(name.len(), 63);
        assert_eq!(
            name,
            format!("{}_{}_fkey", "a".repeat(29), "b".repeat(28))
        );
        // PostgreSQL balances the byte lengths first, then cuts each
        // name back to a character boundary: 29 bytes of the table
        // give 14 two-byte characters
        let name =
            generated_name(&"\u{e9}".repeat(20), &"x".repeat(35), "fkey");
        assert_eq!(
            name,
            format!("{}_{}_fkey", "\u{e9}".repeat(14), "x".repeat(28))
        );
    }

    #[test]
    fn normalizes_the_short_forms_of_a_table() {
        let mut table: crate::models::Table = serde_json::from_value(json!({
            "name": "t",
            "schema": "s",
            "owner": "o",
            "columns": [{"name": "n", "data_type": "integer", "default": 0}],
            "primary_key": "n",
            "foreign_keys": [{
                "columns": ["n"],
                "references": {"name": "s.u", "columns": ["id"]},
            }],
        }))
        .unwrap();
        normalize_table(&mut table);
        let value = serde_json::to_value(&table).unwrap();
        assert_eq!(value["primary_key"], json!(["n"]));
        assert_eq!(value["columns"][0]["default"], json!("0"));
        assert_eq!(value["foreign_keys"][0]["name"], json!("t_n_fkey"));
    }

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

    /// A value written at its default loads as written. The model
    /// used to read it as absent, which the round-trip check then
    /// rejected, so `forced: false` or `command: ALL` failed the load
    #[test]
    fn written_defaults_load() {
        let dir = tempfile::tempdir().unwrap();
        let tables = dir.path().join("tables").join("public");
        std::fs::create_dir_all(&tables).unwrap();
        std::fs::write(
            tables.join("t.yaml"),
            "owner: postgres\n\
             columns:\n  - name: id\n    data_type: integer\n\
             check_constraints:\n  - name: c\n    expression: id > 0\n\
             \x20   not_valid: false\n\
             row_level_security:\n  enabled: true\n  forced: false\n\
             policies:\n  - name: p\n    restrictive: false\n\
             \x20   command: ALL\n    roles: [public]\n",
        )
        .unwrap();
        let mut loader = Loader::new(dir.path());
        loader.read_object_files(ObjectType::Table).unwrap();
        assert_eq!(loader.errors, 0);
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
            vec![0],
        );

        let mut entry = json!({
            "source_type": "int4",
            "target_type": "text",
            "schema": "test",
            "owner": "postgres",
            "function": "f",
            "dependencies": {"extensions": ["public.myext"]},
        });
        loader.cache_and_remove_dependencies(&mut entry);
        assert_eq!(entry.get("dependencies"), None);
        loader.add_definition(ObjectType::Cast, entry, None);
        loader.apply_cached_dependencies().unwrap();

        assert_eq!(loader.project.inventory[1].dependencies, [0].into());
    }

    /// A project pulled before build deviation 14 records a
    /// table-to-table edge for every foreign key. Keeping it would
    /// recreate the cycle that hoists both tables to the front of the
    /// archive, so the load drops it. INHERITS and LIKE both order
    /// table creation, so an edge either one backs is kept.
    #[test]
    fn stale_foreign_key_edge_is_dropped_and_ordering_kept() {
        let mut loader = Loader::new(Path::new("."));
        // the foreign-key target, an object in its own right so an
        // edge on it would resolve and be visible if it were kept
        loader.add_definition(
            ObjectType::Table,
            json!({
                "name": "other",
                "schema": "test",
                "owner": "postgres",
                "columns": [{"name": "id", "data_type": "integer"}],
            }),
            None,
        );
        // "test.other" stands for the foreign-key edge, which nothing
        // in any of these definitions backs
        let deps = json!({"tables": ["test.other", "test.parent"]});
        for (name, ordering) in [
            ("parent", None),
            ("child", Some(json!({"parents": ["test.parent"]}))),
            ("copy", Some(json!({"like_table": {"name": "test.parent"}}))),
        ] {
            let mut entry = json!({
                "name": name,
                "schema": "test",
                "owner": "postgres",
                "dependencies": deps,
            });
            match ordering {
                // a LIKE table copies its columns, so it declares none
                Some(Value::Object(fields)) => {
                    for (key, value) in fields {
                        entry[key] = value;
                    }
                }
                _ => {
                    entry["columns"] =
                        json!([{"name": "id", "data_type": "integer"}]);
                }
            }
            loader.cache_and_remove_dependencies(&mut entry);
            loader.add_definition(ObjectType::Table, entry, None);
        }
        loader.index.insert(
            index_key(ObjectType::Table, Some("test"), "other"),
            vec![0],
        );
        loader.index.insert(
            index_key(ObjectType::Table, Some("test"), "parent"),
            vec![1],
        );
        loader.apply_cached_dependencies().unwrap();

        // `parent` neither inherits nor copies, so both edges are stale
        assert!(loader.project.inventory[1].dependencies.is_empty());
        // `child` keeps only the INHERITS edge on `test.parent`
        assert_eq!(loader.project.inventory[2].dependencies, [1].into());
        // `copy` keeps only the LIKE edge on `test.parent`
        assert_eq!(loader.project.inventory[3].dependencies, [1].into());
    }

    /// A hand-written project that states INHERITS or LIKE and declares
    /// no `dependencies` block is ordered from the definition itself; a
    /// source outside the project orders nothing
    #[test]
    fn structural_sources_order_their_tables() {
        let mut loader = Loader::new(Path::new("."));
        let table = |name: &str, extra: Value| {
            let mut entry = json!({
                "name": name, "schema": "test", "owner": "postgres",
            });
            for (key, value) in extra.as_object().unwrap() {
                entry[key] = value.clone();
            }
            entry
        };
        let columns = json!({"columns": [{"name": "id", "data_type": "int"}]});
        for entry in [
            table("source", columns.clone()),
            table(
                "child",
                json!({"parents": ["test.source"],
                                  "columns": columns["columns"]}),
            ),
            table("copy", json!({"like_table": {"name": "test.source"}})),
            table("elsewhere", json!({"like_table": {"name": "ext.table"}})),
            table(
                "quoted",
                json!({"like_table": {"name": "\"test\".\"Source\""}}),
            ),
        ] {
            loader.add_definition(ObjectType::Table, entry, None);
        }
        loader.index.insert(
            index_key(ObjectType::Table, Some("test"), "source"),
            vec![0],
        );
        loader.index.insert(
            index_key(ObjectType::Table, Some("test"), "Source"),
            vec![0],
        );
        loader.apply_structural_dependencies();

        assert!(loader.project.inventory[0].dependencies.is_empty());
        assert_eq!(loader.project.inventory[1].dependencies, [0].into());
        assert_eq!(loader.project.inventory[2].dependencies, [0].into());
        assert!(loader.project.inventory[3].dependencies.is_empty());
        // a quoted reference resolves to the unquoted name
        assert_eq!(loader.project.inventory[4].dependencies, [0].into());
    }

    /// Overloads are distinct objects: pull writes them to `f.yaml`
    /// and `f_1.yaml`, and keying the inventory by bare name made the
    /// second file a duplicate, failing the whole load
    #[test]
    fn function_overloads_are_not_duplicates() {
        let mut loader = Loader::new(Path::new("."));
        for (file, data_type) in [("f.yaml", "integer"), ("f_1.yaml", "text")]
        {
            loader.add_definition(
                ObjectType::Function,
                json!({
                    "name": "f",
                    "schema": "test",
                    "owner": "postgres",
                    "parameters": [{"mode": "IN", "data_type": data_type}],
                }),
                Some(Path::new(file)),
            );
        }
        assert_eq!(loader.errors, 0);
        assert_eq!(loader.project.inventory.len(), 2);
    }

    /// The same signature twice is still a duplicate
    #[test]
    fn identical_function_signatures_are_duplicates() {
        let mut loader = Loader::new(Path::new("."));
        for file in ["f.yaml", "copy.yaml"] {
            loader.add_definition(
                ObjectType::Function,
                json!({
                    "name": "f",
                    "schema": "test",
                    "owner": "postgres",
                    "parameters": [{"mode": "IN", "data_type": "integer"}],
                }),
                Some(Path::new(file)),
            );
        }
        assert_eq!(loader.errors, 1);
        assert_eq!(loader.project.inventory.len(), 1);
    }

    /// A dependency block belongs to the object it was read with, not
    /// to whichever overload happens to share its name
    #[test]
    fn dependencies_stay_with_the_overload_that_declared_them() {
        let mut loader = Loader::new(Path::new("."));
        loader.add_definition(
            ObjectType::Schema,
            json!({"name": "test", "owner": "postgres"}),
            None,
        );
        for (data_type, dependencies) in [
            ("integer", json!({})),
            ("text", json!({"schemata": ["test"]})),
        ] {
            let mut defn = json!({
                "name": "f",
                "schema": "test",
                "owner": "postgres",
                "parameters": [{"mode": "IN", "data_type": data_type}],
                "dependencies": dependencies,
            });
            loader.cache_and_remove_dependencies(&mut defn);
            loader.add_definition(ObjectType::Function, defn, None);
        }
        loader.apply_cached_dependencies().unwrap();

        assert_eq!(loader.errors, 0);
        // item 0 is the schema, 1 is f(integer), 2 is f(text)
        assert!(loader.project.inventory[1].dependencies.is_empty());
        assert_eq!(loader.project.inventory[2].dependencies, [0].into());
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
        loader.cache_and_remove_dependencies(&mut defn);

        assert_eq!(loader.errors, 0);
        assert_eq!(loader.pending_dependencies.len(), 1);
        let dep = &loader.pending_dependencies[0];
        assert_eq!(dep.parent_desc, ObjectType::Server);
        assert_eq!(dep.parent_tag, "myserver");
    }
}
