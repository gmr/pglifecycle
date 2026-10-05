//! The project directory loader (ports the load half of project.py)

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::constants::{DEPENDENCIES, ObjectType, READ_ORDER};
use crate::deploy::identity_type;
use crate::models::{
    Aggregate, Definition, FunctionParameter, Item, Operator, Table,
};
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
                superuser: String::from("postgres"),
                default_schema: String::from("public"),
                path: path.to_path_buf(),
                settings: Default::default(),
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
        self.check_type_names();
        self.apply_cached_dependencies()?;
        self.apply_structural_dependencies();
        if self.errors > 0 {
            log::error!("Project load failed with {} errors", self.errors);
            return Err(String::from("Project load failure"));
        }
        log::info!("Project loaded");
        Ok(self.project)
    }

    /// Refuse each type name with no schema that is not a built-in
    /// type and not a type or a domain of the project in the schema of
    /// the object that uses it: the restore and the deploy script run
    /// with an empty search_path, and PostgreSQL does not find it. A
    /// type of an extension, such as citext, is not in the project, so
    /// a project with an extension gets a warning instead.
    fn check_type_names(&mut self) {
        let extensions = self
            .project
            .inventory
            .iter()
            .any(|item| item.desc == ObjectType::Extension);
        for message in super::type_names::unresolved_type_names(
            &self.project.inventory,
            &self.paths,
        ) {
            if extensions {
                log::warn!(
                    "{message}. The project has extensions, and the type \
                     can be a type of one of them, thus the load continues"
                );
            } else {
                log::error!("{message}");
                self.errors += 1;
            }
        }
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
        // the text of a project is UTF-8, with standard conforming
        // strings, and build always writes the archive so. The fields
        // are obsolete, and load only with these values.
        if let Some(encoding) = project["encoding"].as_str()
            && !crate::pull::is_utf8(encoding)
        {
            return Err(format!(
                "{}: encoding {encoding} is not supported. The text of a \
                 project is UTF-8, and build always writes the archive in \
                 UTF8; pg_restore converts it to the encoding of the \
                 database. Remove the obsolete encoding field",
                path.display()
            ));
        }
        if project["stdstrings"].as_bool() == Some(false) {
            return Err(format!(
                "{}: stdstrings false is not supported. The SQL of a \
                 project is written with standard_conforming_strings on, \
                 and build always writes the archive so. Remove the \
                 obsolete stdstrings field",
                path.display()
            ));
        }
        self.project.settings.comment =
            project["comment"].as_str().map(String::from);
        if let Ok(settings) =
            serde_json::from_value(project["settings"].clone())
        {
            self.project.settings.database = settings;
        }
        if let Ok(roles) =
            serde_json::from_value(project["role_settings"].clone())
        {
            self.project.settings.roles = roles;
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
            // the error names the file, as the author edits it
            let label = format!("{name} in {}", path.display());
            if !validate::validate_object(&ot.schema_file(), &label, &defn) {
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
    /// parents, `LIKE` source and the relations of its columns' row
    /// types; the functions an aggregate, cast, conversion, operator or
    /// event trigger calls; the types an aggregate or cast uses; and a
    /// publication's tables and schemas. A function field names one
    /// overload where PostgreSQL fixes its argument types (see
    /// [`Loader::resolve_function`]), because an edge to each overload
    /// of the name can make a dependency loop with an overload that
    /// uses the object. Text search objects are ordered one by one in
    /// the build instead, because a container stands for a whole schema
    /// and is too coarse to order.
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
            // the functions that the definition names, each with the
            // argument lists that PostgreSQL calls it with
            let mut calls: Vec<(&str, Vec<Vec<String>>)> = Vec::new();
            match &item.definition {
                Definition::Table(table) => {
                    let sources =
                        table.parents.iter().flatten().chain(
                            table.like_table.iter().map(|like| &like.name),
                        );
                    for source in sources {
                        references.push((ObjectType::Table, source.clone()));
                    }
                    // a column of the row type of a relation
                    for name in row_type_names(table) {
                        for desc in [
                            ObjectType::Table,
                            ObjectType::View,
                            ObjectType::MaterializedView,
                        ] {
                            references.push((desc, name.clone()));
                        }
                    }
                }
                Definition::Aggregate(a) => {
                    calls.extend(aggregate_calls(a));
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
                    if let Some(function) = &c.function {
                        // the source value, then the typmod and whether
                        // the cast is explicit, when the function takes
                        // them
                        let candidates = c.source_type.iter().flat_map(|s| {
                            [
                                vec![s.clone()],
                                vec![s.clone(), "integer".into()],
                                vec![
                                    s.clone(),
                                    "integer".into(),
                                    "boolean".into(),
                                ],
                            ]
                        });
                        calls.push((function, candidates.collect()));
                    }
                    references.extend(
                        [&c.source_type, &c.target_type]
                            .into_iter()
                            .flatten()
                            .cloned()
                            .flat_map(type_references),
                    );
                }
                Definition::Conversion(c) => {
                    if let Some(function) = &c.function {
                        // PostgreSQL 14 added the last argument
                        let arguments = ["integer", "integer", "cstring"]
                            .into_iter()
                            .chain(["internal", "integer"])
                            .map(String::from)
                            .collect::<Vec<_>>();
                        let mut with_flag = arguments.clone();
                        with_flag.push("boolean".into());
                        calls.push((function, vec![with_flag, arguments]));
                    }
                }
                Definition::Transform(t) => {
                    for function in
                        [&t.from_sql, &t.to_sql].into_iter().flatten()
                    {
                        calls.push((function, signatures(&[&["internal"]])));
                    }
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
                    if let Some(function) = &t.function {
                        calls.push((function, vec![Vec::new()]));
                    }
                }
                Definition::AccessMethod(m) => {
                    calls.push((&m.handler, signatures(&[&["internal"]])));
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
                Definition::Type(t) => calls.extend(type_calls(t)),
                // a function that uses the operator in a SQL-standard
                // body can put the operator before the functions that
                // it names in the sort, so these need edges too
                Definition::Operator(o) => calls.extend(operator_calls(o)),
                Definition::Language(l) => {
                    let fields: [(&Option<String>, &[&str]); 3] = [
                        (&l.handler, &[]),
                        (&l.inline_handler, &["internal"]),
                        (&l.validator, &["oid"]),
                    ];
                    for (function, arguments) in fields {
                        if let Some(function) = function {
                            calls.push((function, signatures(&[arguments])));
                        }
                    }
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
            for (function, candidates) in &calls {
                for parent in
                    self.resolve_function(function, own_schema, candidates)
                {
                    if parent != id {
                        edges.push((id, parent));
                    }
                }
            }
            for (desc, reference) in references {
                if desc == ObjectType::Function {
                    for parent in
                        self.resolve_function(&reference, own_schema, &[])
                    {
                        if parent != id {
                            edges.push((id, parent));
                        }
                    }
                    continue;
                }
                let (namespace, tag) = split_sql_name(&reference);
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

    /// The overloads that a function field names. `reference` is a
    /// function name, with or without a schema, and with or without its
    /// argument types. A name without a schema is in `own_schema`.
    ///
    /// An argument list in the reference names the overload that has
    /// those argument types. Otherwise the first of `candidates` that
    /// an overload has names it: the argument types that PostgreSQL
    /// calls the function with, such as the state type and the
    /// arguments of an aggregate's state function. When no overload
    /// matches, the reference names each overload that has the name,
    /// because a missing edge fails a restore, and an edge too many at
    /// worst orders an object later than necessary.
    fn resolve_function(
        &self,
        reference: &str,
        own_schema: &str,
        candidates: &[Vec<String>],
    ) -> Vec<usize> {
        let (name, arguments) = match tag_signature(reference) {
            Some((name, arguments)) => (name, Some(arguments)),
            None => (reference, None),
        };
        let (namespace, tag) = split_sql_name(name);
        let namespace = if namespace.is_empty() {
            own_schema.to_string()
        } else {
            namespace
        };
        let overloads = lookup_items(
            &self.index,
            ObjectType::Function,
            Some(&namespace),
            &tag,
        );
        let candidates: Vec<Vec<String>> = match arguments {
            Some(arguments) => vec![arguments],
            None => candidates
                .iter()
                .map(|types| types.iter().map(|t| identity_type(t)).collect())
                .collect(),
        };
        for signature in candidates {
            let found: Vec<usize> = overloads
                .iter()
                .copied()
                .filter(|&i| {
                    routine_signature(&self.project.inventory[i].definition)
                        == signature
                })
                .collect();
            if !found.is_empty() {
                return found;
            }
        }
        overloads
    }

    fn apply_cached_dependencies(&mut self) -> Result<(), String> {
        for dep in &self.cached_dependencies {
            if self.is_stale_foreign_key_edge(dep) {
                continue;
            }
            let item = &self.project.inventory[dep.item];
            let dependent = format!(
                "{} {}.{}",
                item.desc.as_str(),
                item.definition.schema().unwrap_or_default(),
                identity(&item.definition),
            );
            let parents = match resolve_dependency(
                &self.index,
                &self.project.inventory,
                dep,
            ) {
                Ok(parents) => parents,
                Err(error) => {
                    log::error!(
                        "The dependency of {dependent} on {} {}.{} {error}",
                        dep.parent_desc.as_str(),
                        dep.parent_namespace,
                        dep.parent_tag,
                    );
                    self.errors += 1;
                    continue;
                }
            };
            // a dependency may point at an object the project does not
            // manage (e.g. an inheritance parent owned by an
            // extension); skip the edge rather than failing the load
            if parents.is_empty() {
                log::warn!(
                    "Skipping dependency from {dependent} on unmanaged \
                     {} {}.{}",
                    dep.parent_desc.as_str(),
                    dep.parent_namespace,
                    dep.parent_tag,
                );
                continue;
            }
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
    /// Three relations between tables order their creation, and each
    /// names the other table in the dependent table's own definition:
    /// INHERITS through `parents`, LIKE through `like_table`, and a
    /// column of the other table's row type through its `data_type`.
    /// An edge one of those backs is kept. A foreign key used to add
    /// an edge too, so that an inline `FOREIGN KEY` clause would find
    /// its referenced table. The build now emits every foreign key as
    /// its own post-data entry, which already sorts after every table,
    /// and the old edge became harmful: two tables that reference each
    /// other make it a cycle, and libpgdump breaks a cycle by hoisting
    /// its members ahead of everything else in the archive, including
    /// the CREATE SCHEMA they need (gmr/libpgdump#14). Such a project
    /// builds an archive that no longer restores, so drop the edge
    /// instead of keeping faith with it, and tell the operator to
    /// remove it. A person can also write the edge by hand, so a new
    /// pull does not always remove it.
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
                .is_some_and(|like| names_the_parent(&like.name))
            || row_type_names(table)
                .iter()
                .any(|name| names_the_row_type(name, &table.schema, dep));
        if ordered {
            return false;
        }
        log::warn!(
            "Ignoring the dependency of table {}.{} on table {}.{}: a \
             table dependency orders only INHERITS, LIKE and a column of \
             the other table's row type, and the build adds each foreign \
             key after all tables. Remove the entry from the \
             dependencies of {}.{}.",
            table.schema,
            table.name,
            dep.parent_namespace,
            dep.parent_tag,
            table.schema,
            table.name,
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
                for message in nullable_errors(table) {
                    log::error!("{message}");
                    self.errors += 1;
                }
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
        if let Some(warning) = kept_argument_list(ot, &definition) {
            log::warn!("{warning}");
        }
        // a routine whose name includes its argument types, as
        // test-project/functions writes it, is also an overload of its
        // name without them
        if let Some(bare) = routine_bare_name(ot, &definition) {
            self.index
                .entry(index_key(ot, definition.schema(), bare))
                .or_default()
                .push(id);
        }
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

/// Find the items that a `dependencies` entry names. An empty list
/// means that the project does not manage the object.
///
/// Overloads of a function, procedure or aggregate share a name, so
/// an entry for one of these gives its argument types, as pg_dump tags
/// it: `test.f(integer, text)`, or `test.agg(*)` for an aggregate with
/// no arguments. The types compare as PostgreSQL resolves them (see
/// [`routine_signature`]). An entry with no argument list names the
/// one overload that has the name. An entry that names no single
/// overload is an error, because the load cannot know which overload
/// the author meant.
fn resolve_dependency(
    index: &HashMap<(ObjectType, Option<String>, String), Vec<usize>>,
    inventory: &[Item],
    dep: &CachedDependency,
) -> Result<Vec<usize>, String> {
    let desc = dep.parent_desc;
    let namespace = Some(dep.parent_namespace.as_str());
    if !matches!(
        desc,
        ObjectType::Function
            | ObjectType::Procedure
            | ObjectType::Aggregate
            | ObjectType::Operator
    ) {
        return Ok(lookup_items(index, desc, namespace, &dep.parent_tag));
    }
    // a routine can also give its argument types in its name, with no
    // parameters, as test-project/functions does
    if dep.parent_tag.contains('(') {
        let exact = lookup_items(index, desc, namespace, &dep.parent_tag);
        if !exact.is_empty() {
            return Ok(exact);
        }
    }
    let (name, arguments) = match tag_signature(&dep.parent_tag) {
        Some((name, arguments)) => (name, Some(arguments)),
        None => (dep.parent_tag.as_str(), None),
    };
    let overloads = lookup_items(index, desc, namespace, name);
    let signatures = || {
        overloads
            .iter()
            .map(|&i| {
                format!(
                    "{}.{name}({})",
                    dep.parent_namespace,
                    routine_signature(&inventory[i].definition).join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    match arguments {
        Some(arguments) => {
            let found: Vec<usize> = overloads
                .iter()
                .copied()
                .filter(|&i| {
                    routine_signature(&inventory[i].definition) == arguments
                })
                .collect();
            if found.is_empty() && !overloads.is_empty() {
                return Err(format!(
                    "matches no overload. The overloads are: {}",
                    signatures()
                ));
            }
            Ok(found)
        }
        None if overloads.len() > 1 => Err(format!(
            "is ambiguous: {} overloads have that name. Give the \
             argument types of one: {}",
            overloads.len(),
            signatures()
        )),
        None => Ok(overloads),
    }
}

/// The argument types of an argument list without its closing
/// parenthesis, split at each comma that is not in parentheses or
/// quotes, so `numeric(10,2)` stays one type. `*`, the argument list of
/// an aggregate with no arguments, is no argument.
fn split_arguments(arguments: &str) -> Vec<String> {
    let arguments = arguments.trim_end();
    let arguments = arguments.strip_suffix(')').unwrap_or(arguments);
    let mut result = vec![String::new()];
    let (mut quoted, mut depth) = (false, 0usize);
    for c in arguments.chars() {
        match c {
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => depth = depth.saturating_sub(1),
            ',' if !quoted && depth == 0 => {
                result.push(String::new());
                continue;
            }
            _ => {}
        }
        result.last_mut().unwrap().push(c);
    }
    result
        .into_iter()
        .map(|argument| argument.trim().to_string())
        .filter(|argument| !argument.is_empty() && argument != "*")
        .collect()
}

/// The name and the argument types of a routine tag with an argument
/// list, such as `f(integer, text)`. The types are as
/// [`routine_signature`] writes them. A tag with no argument list
/// gives `None`.
pub(crate) fn tag_signature(tag: &str) -> Option<(&str, Vec<String>)> {
    let (name, arguments) = crate::utils::split_signature(tag)?;
    let arguments = split_arguments(arguments)
        .iter()
        .map(|a| identity_type(a))
        .collect();
    Some((name.trim_end(), arguments))
}

/// The argument types that PostgreSQL resolves a routine by, as
/// [`identity_type`] writes them: an alias is its canonical name, a
/// built-in type has no `pg_catalog` schema, a name that is not quoted
/// is in lowercase, and a typmod is removed. An OUT or TABLE parameter
/// is not an argument. The arguments of an ordered-set aggregate are
/// its direct arguments, then its ORDER BY arguments, as pg_dump lists
/// them.
///
/// A routine with no parameters whose name includes its argument
/// types, as test-project/functions writes it, has the argument types
/// of its name. The signature of an operator is its left and its right
/// argument type, `NONE` for no argument.
fn routine_signature(definition: &Definition) -> Vec<String> {
    match definition {
        Definition::Function(f) if f.parameters.is_none() => {
            name_signature(&f.name)
        }
        Definition::Procedure(p) if p.parameters.is_none() => {
            name_signature(&p.name)
        }
        Definition::Function(f) => parameter_signature(&f.parameters),
        Definition::Procedure(p) => parameter_signature(&p.parameters),
        Definition::Aggregate(a) => aggregate_signature(a),
        Definition::Operator(o) => operator_signature(o),
        _ => Vec::new(),
    }
}

/// The name of a function or procedure without the argument types
/// that its name can include, as test-project/functions writes it.
/// When the routine has parameters, the list at the end of the name
/// is its argument types only if it has the types of the parameters:
/// the name of `f(x)` with an `integer` parameter is `f(x)`. A mode in
/// the list is not part of the type, and an `OUT` argument is not an
/// argument type, as in [`parameter_signature`].
pub(crate) fn routine_base_name<'a>(
    name: &'a str,
    parameters: &Option<Vec<FunctionParameter>>,
) -> &'a str {
    match tag_signature(name) {
        Some((base, _))
            if parameters.is_none()
                || parameter_signature(parameters) == input_types(name) =>
        {
            base
        }
        _ => name,
    }
}

/// The input types of the argument list at the end of a routine name,
/// without their modes
fn input_types(name: &str) -> Vec<String> {
    let Some((_, arguments)) = crate::utils::split_signature(name) else {
        return Vec::new();
    };
    split_arguments(arguments)
        .iter()
        .filter_map(|argument| {
            let (mode, data_type) =
                argument.split_once(char::is_whitespace).unwrap_or_default();
            match mode.to_ascii_uppercase().as_str() {
                "OUT" => None,
                "IN" | "INOUT" | "VARIADIC" => Some(data_type.trim_start()),
                _ => Some(argument.as_str()),
            }
        })
        .map(identity_type)
        .collect()
}

/// A warning for a function or procedure with parameters whose name
/// has a list at its end that is not the types of the parameters. The
/// list is then part of the name, as for `f(x)`, but it can be a
/// mistake, such as `f(varchar)` for a `text` parameter.
fn kept_argument_list(
    ot: ObjectType,
    definition: &Definition,
) -> Option<String> {
    let (name, parameters) = match (ot, definition) {
        (ObjectType::Function, Definition::Function(f)) => {
            (&f.name, &f.parameters)
        }
        (ObjectType::Procedure, Definition::Procedure(p)) => {
            (&p.name, &p.parameters)
        }
        _ => return None,
    };
    if parameters.is_none()
        || tag_signature(name).is_none()
        || routine_base_name(name, parameters) != name
    {
        return None;
    }
    Some(format!(
        "{} {}.{name}: the list at the end of the name is part of the \
         name, because it is not the types of the parameters ({})",
        ot.as_str(),
        definition.schema().unwrap_or_default(),
        parameter_signature(parameters).join(", "),
    ))
}

/// The argument types in a routine name, as [`tag_signature`] reads
/// them, or none for a name with no argument list
fn name_signature(name: &str) -> Vec<String> {
    tag_signature(name)
        .map(|(_, arguments)| arguments)
        .unwrap_or_default()
}

/// The name without its argument types of a function or procedure
/// whose name includes them, as test-project/functions writes it
fn routine_bare_name(ot: ObjectType, definition: &Definition) -> Option<&str> {
    let (name, parameters) = match (ot, definition) {
        (ObjectType::Function, Definition::Function(f)) => {
            (&f.name, &f.parameters)
        }
        (ObjectType::Procedure, Definition::Procedure(p)) => {
            (&p.name, &p.parameters)
        }
        _ => return None,
    };
    let bare = routine_base_name(name, parameters);
    (bare != name).then_some(bare)
}

/// The [`routine_signature`] of an operator
pub(crate) fn operator_signature(operator: &Operator) -> Vec<String> {
    [&operator.left_arg, &operator.right_arg]
        .into_iter()
        .map(|arg| identity_type(arg.as_deref().unwrap_or("NONE")))
        .collect()
}

/// The [`routine_signature`] of a function or procedure
pub(crate) fn parameter_signature(
    parameters: &Option<Vec<FunctionParameter>>,
) -> Vec<String> {
    parameters
        .iter()
        .flatten()
        .filter(|p| !matches!(p.mode.as_str(), "OUT" | "TABLE"))
        .map(|p| identity_type(&p.data_type))
        .collect()
}

/// The [`routine_signature`] of an aggregate
pub(crate) fn aggregate_signature(aggregate: &Aggregate) -> Vec<String> {
    aggregate
        .arguments
        .iter()
        .chain(aggregate.order_by.iter().flatten())
        .map(|a| identity_type(&a.data_type))
        .collect()
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
            fk.name = crate::utils::make_object_name(
                &table.name,
                Some(&fk.columns.join("_")),
                "fkey",
            );
        }
    }
}

/// An error for each column with `nullable: true` that PostgreSQL makes
/// NOT NULL (see [`Table::is_always_not_null`]). PostgreSQL refuses
/// `NULL` on an identity column, and makes a column of the primary key
/// NOT NULL, so the database never has what the project says.
fn nullable_errors(table: &Table) -> Vec<String> {
    table
        .columns
        .iter()
        .flatten()
        .filter(|c| c.nullable == Some(true) && table.is_always_not_null(c))
        .map(|c| {
            format!(
                "TABLE {}.{}: column {} has nullable: true, but it is in \
                 the primary key or is an identity column, which \
                 PostgreSQL makes NOT NULL. Remove nullable: true",
                table.schema, table.name, c.name
            )
        })
        .collect()
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

/// The names of the types of a table's columns, without an array
/// suffix or a modifier. A name that is a relation's is its row type,
/// which the relation has to exist for.
fn row_type_names(table: &Table) -> Vec<String> {
    table
        .columns
        .iter()
        .flatten()
        .filter_map(|column| type_references(column.data_type.clone()))
        .map(|(_, name)| name)
        .collect()
}

/// Whether a column type `name` of a table in `schema` is the row type
/// of the table that `dep` names. A type name without a schema is in
/// the schema of the table, as `apply_structural_dependencies` reads
/// it.
fn names_the_row_type(
    name: &str,
    schema: &str,
    dep: &CachedDependency,
) -> bool {
    let (namespace, tag) = split_sql_name(name);
    let namespace = if namespace.is_empty() {
        schema.to_string()
    } else {
        namespace
    };
    tag == dep.parent_tag && namespace == dep.parent_namespace
}

/// Argument lists of type names, as `apply_structural_dependencies`
/// gives them for a function field
fn signatures(lists: &[&[&str]]) -> Vec<Vec<String>> {
    lists
        .iter()
        .map(|list| list.iter().map(|t| t.to_string()).collect())
        .collect()
}

/// The support functions of an aggregate, each with the argument types
/// that PostgreSQL calls it with (pg_aggregate.c). `S` is the state
/// type, `A` the arguments and, for an ordered-set aggregate, `D` the
/// direct arguments and `O` the ORDER BY arguments:
///
/// - the state function takes `(S, A)`, or `(S, O)`
/// - the final function takes `(S)`, or `(S, D)`; with
///   `FINALFUNC_EXTRA` it takes `(S, A)`, or `(S, D, O)`
/// - the combine function takes `(S, S)`, the serial function
///   `(internal)`, and the deserial function `(bytea, internal)`
/// - the moving-aggregate functions take the moving state type in
///   place of `S`
fn aggregate_calls(a: &Aggregate) -> Vec<(&str, Vec<Vec<String>>)> {
    let types = |arguments: &[crate::models::Argument]| {
        arguments
            .iter()
            .map(|a| a.data_type.clone())
            .collect::<Vec<_>>()
    };
    let direct = types(&a.arguments);
    let ordered = a.order_by.as_deref().map(types);
    let with = |state: &str, arguments: &[&[String]]| {
        let mut list = vec![state.to_string()];
        for part in arguments {
            list.extend(part.iter().cloned());
        }
        vec![list]
    };
    let state = a.state_data_type.as_str();
    let mstate = a.mstate_data_type.as_deref().unwrap_or(state);
    let (transition, extra): (&[String], Vec<&[String]>) = match &ordered {
        Some(ordered) => (ordered, vec![&direct, ordered]),
        None => (&direct, vec![&direct]),
    };
    let finals = |state: &str, extra_arguments: Option<bool>| match (
        extra_arguments == Some(true),
        &ordered,
    ) {
        (true, _) => with(state, &extra),
        (false, Some(_)) => with(state, &[&direct]),
        (false, None) => with(state, &[]),
    };
    let mut calls = vec![(a.sfunc.as_str(), with(state, &[transition]))];
    let fields = [
        (&a.ffunc, finals(state, a.finalfunc_extra)),
        (&a.combinefunc, with(state, &[&[state.to_string()]])),
        (&a.serialfunc, signatures(&[&["internal"]])),
        (&a.deserialfunc, signatures(&[&["bytea", "internal"]])),
        (&a.msfunc, with(mstate, &[&direct])),
        (&a.minvfunc, with(mstate, &[&direct])),
        (&a.mffunc, finals(mstate, a.mfinalfunc_extra)),
    ];
    for (function, candidates) in fields {
        if let Some(function) = function {
            calls.push((function.as_str(), candidates));
        }
    }
    calls
}

/// The functions of an operator, each with the argument types that
/// PostgreSQL calls it with (CREATE OPERATOR): the operator function
/// takes the argument types of the operator, the restriction estimator
/// `(internal, oid, internal, integer)`, and the join estimator
/// `(internal, oid, internal, smallint, internal)` or
/// `(internal, oid, internal, smallint)`
fn operator_calls(o: &Operator) -> Vec<(&str, Vec<Vec<String>>)> {
    let arguments = [&o.left_arg, &o.right_arg]
        .into_iter()
        .flatten()
        .filter(|arg| !arg.eq_ignore_ascii_case("NONE"))
        .cloned()
        .collect();
    let mut calls = vec![(o.function.as_str(), vec![arguments])];
    if let Some(restrict) = &o.restrict {
        calls.push((
            restrict,
            signatures(&[&["internal", "oid", "internal", "integer"]]),
        ));
    }
    if let Some(join) = &o.join {
        calls.push((
            join,
            signatures(&[
                &["internal", "oid", "internal", "smallint", "internal"],
                &["internal", "oid", "internal", "smallint"],
            ]),
        ));
    }
    calls
}

/// The I/O functions of a base type, each with the argument types that
/// PostgreSQL accepts for it (CREATE TYPE)
fn type_calls(t: &crate::models::Type) -> Vec<(&str, Vec<Vec<String>>)> {
    let own = format!("{}.{}", t.schema, t.name);
    let fields = [
        (
            &t.input,
            signatures(&[&["cstring"], &["cstring", "oid", "integer"]]),
        ),
        (&t.output, vec![vec![own.clone()]]),
        (
            &t.receive,
            signatures(&[&["internal"], &["internal", "oid", "integer"]]),
        ),
        (&t.send, vec![vec![own]]),
        (&t.typmod_in, signatures(&[&["cstring[]"]])),
        (&t.typmod_out, signatures(&[&["integer"]])),
        (&t.analyze, signatures(&[&["internal"]])),
    ];
    fields
        .into_iter()
        .filter_map(|(function, candidates)| {
            function.as_deref().map(|function| (function, candidates))
        })
        .collect()
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

    /// A list at the end of a name is the argument types when the
    /// routine has no parameters, or when it has their types
    #[test]
    fn routine_base_name_keeps_a_name_with_parentheses() {
        let parameters = |types: &[&str]| {
            Some(
                types
                    .iter()
                    .map(|t| {
                        serde_json::from_value(
                            json!({"mode": "IN", "data_type": t}),
                        )
                        .unwrap()
                    })
                    .collect::<Vec<FunctionParameter>>(),
            )
        };
        let integer = parameters(&["INTEGER"]);
        assert_eq!(routine_base_name("f(x)", &integer), "f(x)");
        assert_eq!(routine_base_name("f(x)(integer)", &integer), "f(x)");
        assert_eq!(routine_base_name("f(integer)", &integer), "f");
        assert_eq!(routine_base_name("f", &integer), "f");
        assert_eq!(routine_base_name("f(integer)", &None), "f");
        assert_eq!(
            routine_base_name("g\"(y", &parameters(&["text"])),
            "g\"(y"
        );
        // a mode in the name is not part of the type, and an OUT
        // argument is not an argument type, as in parameter_signature
        assert_eq!(routine_base_name("f(IN integer)", &integer), "f");
        assert_eq!(routine_base_name("f(inout int4)", &integer), "f");
        assert_eq!(routine_base_name("f(integer, OUT text)", &integer), "f");
        assert_eq!(
            routine_base_name(
                "f(VARIADIC integer[])",
                &parameters(&["integer[]"])
            ),
            "f"
        );
    }

    /// The loader warns when a list at the end of a routine name is
    /// part of the name, because it does not have the types of the
    /// parameters
    #[test]
    fn a_kept_argument_list_gives_a_warning() {
        let function = |name: &str, data_type: &str| {
            to_definition(
                ObjectType::Function,
                json!({"name": name, "schema": "test", "owner": "o",
                       "parameters": [{"mode": "IN", "data_type": data_type}],
                       "returns": "integer", "language": "sql",
                       "definition": "SELECT 1"}),
            )
            .unwrap()
        };
        let warning = kept_argument_list(
            ObjectType::Function,
            &function("f(varchar)", "text"),
        )
        .expect("a warning");
        assert!(warning.contains("test.f(varchar)"), "{warning}");
        assert!(warning.contains("(text)"), "{warning}");
        assert_eq!(
            kept_argument_list(
                ObjectType::Function,
                &function("f(integer)", "int4")
            ),
            None
        );
        assert_eq!(
            kept_argument_list(ObjectType::Function, &function("f", "text")),
            None
        );
    }

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

    /// PostgreSQL makes a column of the primary key and an identity
    /// column NOT NULL, so `nullable: true` on one is an error
    #[test]
    fn nullable_primary_key_or_identity_column_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let tables = dir.path().join("tables").join("public");
        std::fs::create_dir_all(&tables).unwrap();
        std::fs::write(
            tables.join("t.yaml"),
            "owner: postgres\n\
             columns:\n  - name: id\n    data_type: integer\n\
             \x20   nullable: true\n\
             \x20 - name: i\n    data_type: integer\n    nullable: true\n\
             \x20   generated:\n      sequence_behavior: ALWAYS\n\
             \x20 - name: v\n    data_type: text\n    nullable: true\n\
             primary_key: id\n",
        )
        .unwrap();
        let mut loader = Loader::new(dir.path());
        loader.read_object_files(ObjectType::Table).unwrap();
        assert_eq!(loader.errors, 2);
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

    /// Load `routines` as (type, name, argument types), then one
    /// function `test.dependent()` whose `dependencies` block names
    /// `references` under `key`, and resolve the edges. The dependent
    /// function is the last item
    fn load_routine_dependency(
        routines: &[(ObjectType, &str, &[&str])],
        key: &str,
        references: &[&str],
    ) -> Loader {
        let mut loader = Loader::new(Path::new("."));
        for (desc, name, types) in routines {
            let entry = if *desc == ObjectType::Aggregate {
                let arguments: Vec<Value> = types
                    .iter()
                    .map(|data_type| json!({"data_type": data_type}))
                    .collect();
                json!({"name": name, "schema": "test", "owner": "postgres",
                       "sfunc": "f", "state_data_type": "integer",
                       "arguments": arguments})
            } else {
                let parameters: Vec<Value> = types
                    .iter()
                    .map(|data_type| match data_type.strip_prefix("OUT ") {
                        Some(data_type) => {
                            json!({"mode": "OUT", "data_type": data_type})
                        }
                        None => json!({"mode": "IN", "data_type": data_type}),
                    })
                    .collect();
                json!({"name": name, "schema": "test", "owner": "postgres",
                       "parameters": parameters})
            };
            loader.add_definition(*desc, entry, None);
        }
        let mut defn = json!({
            "name": "dependent",
            "schema": "test",
            "owner": "postgres",
            "dependencies": {key: references},
        });
        loader.cache_and_remove_dependencies(&mut defn);
        loader.add_definition(ObjectType::Function, defn, None);
        loader.apply_cached_dependencies().unwrap();
        loader
    }

    /// A function reference with an argument list names one overload.
    /// The argument types compare as PostgreSQL resolves them: an
    /// alias is its canonical name, case and a `pg_catalog` schema do
    /// not change a built-in type, and an OUT parameter is not an
    /// argument. pg_dump writes the argument types in the same form in
    /// its archive tag, which pull writes to `dependencies`
    #[test]
    fn routine_dependency_resolves_by_signature() {
        let loader = load_routine_dependency(
            &[
                (ObjectType::Function, "f", &["integer"]),
                (ObjectType::Function, "f", &["text", "OUT integer"]),
                (ObjectType::Function, "g", &["character varying"]),
            ],
            "functions",
            &["test.f(int4)", "test.f(pg_catalog.TEXT)", "test.g(varchar)"],
        );
        assert_eq!(loader.errors, 0);
        assert_eq!(loader.project.inventory[3].dependencies, [0, 1, 2].into());

        let loader = load_routine_dependency(
            &[
                (ObjectType::Function, "f", &["integer"]),
                (ObjectType::Function, "f", &["text"]),
            ],
            "functions",
            &["test.f(int)"],
        );
        assert_eq!(loader.errors, 0);
        assert_eq!(loader.project.inventory[2].dependencies, [0].into());
    }

    /// An aggregate reference resolves by signature too, and `(*)` is
    /// the argument list of an aggregate with no arguments, as pg_dump
    /// writes it
    #[test]
    fn aggregate_dependency_resolves_by_signature() {
        let loader = load_routine_dependency(
            &[
                (ObjectType::Aggregate, "agg", &["integer"]),
                (ObjectType::Aggregate, "agg", &["bigint"]),
                (ObjectType::Aggregate, "agg", &[]),
            ],
            "aggregates",
            &["test.agg(int8)", "test.agg(*)"],
        );
        assert_eq!(loader.errors, 0);
        assert_eq!(loader.project.inventory[3].dependencies, [1, 2].into());
    }

    /// A reference with no argument list names the one overload that
    /// has the name
    #[test]
    fn bare_routine_dependency_resolves_one_overload() {
        let loader = load_routine_dependency(
            &[
                (ObjectType::Function, "z_principal_id", &[]),
                (ObjectType::Function, "other", &["integer"]),
            ],
            "functions",
            &["test.z_principal_id"],
        );
        assert_eq!(loader.errors, 0);
        assert_eq!(loader.project.inventory[2].dependencies, [0].into());
    }

    /// A reference with no argument list is an error when more than one
    /// overload has the name, as is an argument list that no overload
    /// has: either one names no single object, and the load cannot
    /// know which overload the author meant
    #[test]
    fn unresolved_routine_dependency_is_an_error() {
        let overloads: &[(ObjectType, &str, &[&str])] = &[
            (ObjectType::Function, "f", &["integer"]),
            (ObjectType::Function, "f", &["text"]),
        ];
        for reference in ["test.f", "test.f(bigint)"] {
            let loader =
                load_routine_dependency(overloads, "functions", &[reference]);
            assert_eq!(loader.errors, 1, "{reference}");
            assert!(loader.project.inventory[2].dependencies.is_empty());
        }
        // a name that the project does not have is not managed, and
        // orders nothing, with or without an argument list
        for reference in ["test.missing", "test.missing(integer)"] {
            let loader =
                load_routine_dependency(overloads, "functions", &[reference]);
            assert_eq!(loader.errors, 0, "{reference}");
            assert!(loader.project.inventory[2].dependencies.is_empty());
        }
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

    /// Add `entry` as an object of type `ot`, and give its id
    fn add(loader: &mut Loader, ot: ObjectType, mut entry: Value) -> usize {
        loader.cache_and_remove_dependencies(&mut entry);
        loader.add_definition(ot, entry, None);
        loader.project.inventory.len() - 1
    }

    /// A function `test.<name>` with `types` as its IN parameters
    fn function_entry(name: &str, types: &[&str]) -> Value {
        let parameters: Vec<Value> = types
            .iter()
            .map(|data_type| json!({"mode": "IN", "data_type": data_type}))
            .collect();
        json!({"name": name, "schema": "test", "owner": "postgres",
               "parameters": parameters})
    }

    /// A function field names the overload that PostgreSQL calls: an
    /// aggregate's state function takes the state type and the
    /// arguments, or the ORDER BY arguments of an ordered-set
    /// aggregate, and a language validator takes an oid. An edge to
    /// the other overload makes a dependency loop when that overload
    /// uses the object. A name with argument types names that
    /// overload, and a name that no candidate matches names each
    /// overload, as before
    #[test]
    fn function_fields_resolve_the_overload_that_is_called() {
        let mut loader = Loader::new(Path::new("."));
        let step = add(
            &mut loader,
            ObjectType::Function,
            function_entry("step", &["int4", "integer"]),
        );
        let other = add(
            &mut loader,
            ObjectType::Function,
            function_entry("step", &["text", "integer"]),
        );
        let aggregate = add(
            &mut loader,
            ObjectType::Aggregate,
            json!({"name": "agg", "schema": "test", "owner": "postgres",
                   "sfunc": "test.step", "state_data_type": "integer",
                   "arguments": [{"data_type": "integer"}]}),
        );
        let ordered = add(
            &mut loader,
            ObjectType::Aggregate,
            json!({"name": "sorted", "schema": "test", "owner": "postgres",
                   "sfunc": "step", "state_data_type": "text",
                   "arguments": [{"data_type": "bigint"}],
                   "order_by": [{"data_type": "integer"}]}),
        );
        let explicit = add(
            &mut loader,
            ObjectType::Cast,
            json!({"schema": "test", "owner": "postgres",
                   "source_type": "integer", "target_type": "text",
                   "function": "test.step(text, int4)"}),
        );
        let unmatched = add(
            &mut loader,
            ObjectType::Aggregate,
            json!({"name": "wide", "schema": "test", "owner": "postgres",
                   "sfunc": "test.step", "state_data_type": "bigint",
                   "arguments": [{"data_type": "bigint"}]}),
        );
        let validator = add(
            &mut loader,
            ObjectType::Function,
            function_entry("check", &["oid"]),
        );
        add(
            &mut loader,
            ObjectType::Function,
            json!({"name": "check", "schema": "test", "owner": "postgres",
                   "language": "copy",
                   "parameters": [{"mode": "IN", "data_type": "integer"}]}),
        );
        let language = add(
            &mut loader,
            ObjectType::ProceduralLanguage,
            json!({"name": "copy", "validator": "test.check"}),
        );
        loader.apply_structural_dependencies();
        let deps =
            |id: usize| loader.project.inventory[id].dependencies.clone();
        assert_eq!(deps(aggregate), [step].into());
        assert_eq!(deps(ordered), [other].into());
        assert_eq!(deps(explicit), [other].into());
        assert_eq!(deps(unmatched), [step, other].into());
        assert_eq!(deps(language), [validator].into());
    }

    /// A function field also finds a function whose name includes its
    /// argument types, as test-project/functions writes it, by the name
    /// without them
    #[test]
    fn function_fields_find_a_name_with_argument_types() {
        let mut loader = Loader::new(Path::new("."));
        let trigger_function = add(
            &mut loader,
            ObjectType::Function,
            json!({"name": "on_ddl()", "schema": "test", "owner": "postgres",
                   "returns": "event_trigger"}),
        );
        let conversion_function = add(
            &mut loader,
            ObjectType::Function,
            json!({"name": "convert(integer, integer, cstring, internal, \
                            integer)",
                   "schema": "test", "owner": "postgres"}),
        );
        let trigger = add(
            &mut loader,
            ObjectType::EventTrigger,
            json!({"name": "ddl", "event": "ddl_command_start",
                   "function": "test.on_ddl()"}),
        );
        let conversion = add(
            &mut loader,
            ObjectType::Conversion,
            json!({"name": "c", "schema": "test", "owner": "postgres",
                   "encoding_from": "UTF8", "encoding_to": "LATIN1",
                   "function": "convert"}),
        );
        loader.apply_structural_dependencies();
        let deps =
            |id: usize| loader.project.inventory[id].dependencies.clone();
        assert_eq!(deps(trigger), [trigger_function].into());
        assert_eq!(deps(conversion), [conversion_function].into());
    }

    /// An operator comes after the overload of its function that takes
    /// its argument types
    #[test]
    fn operators_order_their_function() {
        let mut loader = Loader::new(Path::new("."));
        let binary = add(
            &mut loader,
            ObjectType::Function,
            function_entry("same", &["integer", "integer"]),
        );
        let prefix = add(
            &mut loader,
            ObjectType::Function,
            function_entry("same", &["integer"]),
        );
        let operator = add(
            &mut loader,
            ObjectType::Operator,
            json!({"name": "=~=", "schema": "test", "owner": "postgres",
                   "function": "test.same", "left_arg": "integer",
                   "right_arg": "integer"}),
        );
        let negation = add(
            &mut loader,
            ObjectType::Operator,
            json!({"name": "!!!", "schema": "test", "owner": "postgres",
                   "function": "test.same", "left_arg": "NONE",
                   "right_arg": "int4"}),
        );
        loader.apply_structural_dependencies();
        let deps =
            |id: usize| loader.project.inventory[id].dependencies.clone();
        assert_eq!(deps(operator), [binary].into());
        assert_eq!(deps(negation), [prefix].into());
    }

    /// PostgreSQL 18 calls a type input or receive function with one
    /// or three arguments (findTypeInputFunction and
    /// findTypeReceiveFunction in typecmds.c). A type does not come
    /// after an overload with two arguments
    #[test]
    fn type_io_functions_take_one_or_three_arguments() {
        let mut loader = Loader::new(Path::new("."));
        add(
            &mut loader,
            ObjectType::Function,
            function_entry("t_in", &["cstring", "oid"]),
        );
        let input = add(
            &mut loader,
            ObjectType::Function,
            function_entry("t_in", &["cstring", "oid", "integer"]),
        );
        add(
            &mut loader,
            ObjectType::Function,
            function_entry("t_recv", &["internal", "oid"]),
        );
        let receive = add(
            &mut loader,
            ObjectType::Function,
            function_entry("t_recv", &["internal", "oid", "integer"]),
        );
        let base = add(
            &mut loader,
            ObjectType::Type,
            json!({"name": "t", "schema": "test", "owner": "postgres",
                   "input": "test.t_in", "receive": "test.t_recv"}),
        );
        loader.apply_structural_dependencies();
        assert_eq!(
            loader.project.inventory[base].dependencies,
            [input, receive].into()
        );
    }

    /// PostgreSQL 18 also calls a join estimator with four arguments,
    /// `(internal, oid, internal, smallint)` (ValidateJoinEstimator in
    /// operatorcmds.c). An operator comes after that overload only
    #[test]
    fn operators_order_a_four_argument_join_estimator() {
        let mut loader = Loader::new(Path::new("."));
        let function = add(
            &mut loader,
            ObjectType::Function,
            function_entry("same", &["integer", "integer"]),
        );
        let join = add(
            &mut loader,
            ObjectType::Function,
            function_entry(
                "same_join",
                &["internal", "oid", "internal", "smallint"],
            ),
        );
        add(
            &mut loader,
            ObjectType::Function,
            function_entry("same_join", &["internal", "oid", "internal"]),
        );
        let operator = add(
            &mut loader,
            ObjectType::Operator,
            json!({"name": "=~=", "schema": "test", "owner": "postgres",
                   "function": "test.same", "left_arg": "integer",
                   "right_arg": "integer", "join": "test.same_join"}),
        );
        loader.apply_structural_dependencies();
        assert_eq!(
            loader.project.inventory[operator].dependencies,
            [function, join].into()
        );
    }

    /// A column of a relation's row type, also as an array, orders the
    /// relation first. A `dependencies` entry that names that table is
    /// kept, as one that INHERITS backs is, and an entry for a foreign
    /// key is still dropped
    #[test]
    fn row_type_columns_order_their_relation() {
        let mut loader = Loader::new(Path::new("."));
        let points = add(
            &mut loader,
            ObjectType::Table,
            json!({"name": "z_points", "schema": "test", "owner": "postgres",
                   "columns": [{"name": "x", "data_type": "integer"}]}),
        );
        let other = add(
            &mut loader,
            ObjectType::Table,
            json!({"name": "other", "schema": "test", "owner": "postgres",
                   "columns": [{"name": "x", "data_type": "integer"}]}),
        );
        let view = add(
            &mut loader,
            ObjectType::View,
            json!({"name": "v", "schema": "test", "owner": "postgres",
                   "query": "SELECT 1 AS n"}),
        );
        let segments = add(
            &mut loader,
            ObjectType::Table,
            json!({"name": "a_segments", "schema": "test",
                   "owner": "postgres",
                   "columns": [
                       {"name": "start_at", "data_type": "test.z_points"},
                       {"name": "stops", "data_type": "test.z_points[]"},
                       {"name": "row", "data_type": "v"},
                   ],
                   "dependencies": {"tables": ["test.z_points",
                                               "test.other"]}}),
        );
        loader.apply_cached_dependencies().unwrap();
        assert_eq!(
            loader.project.inventory[segments].dependencies,
            [points].into(),
            "the row-type entry is kept and the foreign-key entry dropped"
        );
        loader.apply_structural_dependencies();
        assert_eq!(
            loader.project.inventory[segments].dependencies,
            [points, view].into()
        );
        assert!(
            !loader.project.inventory[segments]
                .dependencies
                .contains(&other)
        );
    }

    /// An operator entry names one overload by its left and right
    /// argument types, `NONE` for no argument, as pull writes it
    #[test]
    fn operator_dependency_resolves_by_signature() {
        let mut loader = Loader::new(Path::new("."));
        let operator = |left: &str, right: &str| {
            json!({"name": "!!!", "schema": "test", "owner": "postgres",
                   "function": "f", "left_arg": left, "right_arg": right})
        };
        add(
            &mut loader,
            ObjectType::Operator,
            operator("NONE", "integer"),
        );
        let bigint = add(
            &mut loader,
            ObjectType::Operator,
            operator("NONE", "bigint"),
        );
        let binary = add(
            &mut loader,
            ObjectType::Operator,
            json!({"name": "=~=", "schema": "test", "owner": "postgres",
                   "function": "f", "left_arg": "integer",
                   "right_arg": "integer"}),
        );
        let dependent = add(
            &mut loader,
            ObjectType::Function,
            json!({"name": "uses", "schema": "test", "owner": "postgres",
                   "dependencies": {"operators": [
                       "test.!!!(NONE, int8)", "test.=~="]}}),
        );
        loader.apply_cached_dependencies().unwrap();
        assert_eq!(loader.errors, 0);
        assert_eq!(
            loader.project.inventory[dependent].dependencies,
            [bigint, binary].into()
        );
        // two overloads have the name `!!!`
        let ambiguous = add(
            &mut loader,
            ObjectType::Function,
            json!({"name": "uses_bare", "schema": "test", "owner": "postgres",
                   "dependencies": {"operators": ["test.!!!"]}}),
        );
        loader
            .cached_dependencies
            .retain(|dep| dep.item == ambiguous);
        loader.apply_cached_dependencies().unwrap();
        assert_eq!(loader.errors, 1);
    }
}
