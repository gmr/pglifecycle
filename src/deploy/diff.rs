//! Model-level comparison between a loaded project and a database
//! snapshot. Both sides hold the same `models::` structs (the project
//! via [`crate::project::load`], the database via
//! [`crate::pull::Assembly`]), so definitions compare as normalized
//! JSON values.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use tree_sitter::Node;

use super::routine_body::canonical_sql_body;
use crate::constants::ObjectType;
use crate::ddl::NodeExt;
use crate::models::{
    Definition, Domain, Function, Subscription, canonical_settings,
};
use crate::project::Project;
use crate::pull::{Assembly, without_password};
use crate::utils::quote_ident;

/// Identity of a database object on either side of the diff
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjectKey {
    pub desc: ObjectType,
    /// Empty for schemaless object types
    pub schema: String,
    /// Functions use the identity signature (`name(args)`), since the
    /// bare name is ambiguous across overloads
    pub name: String,
}

impl ObjectKey {
    pub fn new(desc: ObjectType, definition: &Definition) -> Self {
        let (schema, name) = object_identity(definition);
        Self { desc, schema, name }
    }
}

/// The schema and name that identify an object. A function is named by
/// its identity signature, since the bare name is ambiguous across
/// overloads. An extension name is unique in the database, and its
/// schema field is the installation target, so it is not part of the
/// identity. A cast or a transform has no schema: the project files it
/// under one, and pull picks one from its types, so the two need not
/// agree. A cast's name, `(source AS target)`, keeps its canonical
/// types, so `int4` matches the `integer` pg_dump writes. A cast or a
/// transform type has no typmod (see [`identity_type`]). An aggregate
/// is identified by its name and its input types, so one overload does
/// not stand for another. The aggregate and operator names remove
/// typmods, as PostgreSQL does not keep them in an argument type (see
/// [`identity_type`]).
fn object_identity(definition: &Definition) -> (String, String) {
    match definition {
        Definition::Function(f) => (f.schema.clone(), function_key_name(f)),
        Definition::Extension(e) => (String::new(), e.name.clone()),
        Definition::Aggregate(aggregate) => {
            let types = |args: &[crate::models::Argument]| {
                args.iter()
                    .map(|a| identity_type(&a.data_type))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let direct = types(&aggregate.arguments);
            let signature = match aggregate.order_by.as_deref() {
                Some(order_by) => {
                    format!("{direct} ORDER BY {}", types(order_by))
                }
                None if direct.is_empty() => String::from("*"),
                None => direct,
            };
            (
                aggregate.schema.clone(),
                format!("{}({signature})", aggregate.name),
            )
        }
        // overloads are separate objects, keyed by their input types
        Definition::Procedure(procedure) => (
            procedure.schema.clone(),
            function_tag_name(&procedure.as_function()),
        ),
        Definition::Operator(operator) => (
            operator.schema.clone(),
            format!(
                "{}({}, {})",
                operator.name,
                identity_type(operator.left_arg.as_deref().unwrap_or("NONE")),
                identity_type(operator.right_arg.as_deref().unwrap_or("NONE"))
            ),
        ),
        // one name can be used once for each index method, whose name
        // PostgreSQL folds to lowercase
        Definition::OperatorClass(class) => (
            class.schema.clone(),
            format!("{} USING {}", class.name, class.method.to_lowercase()),
        ),
        Definition::OperatorFamily(family) => (
            family.schema.clone(),
            format!("{} USING {}", family.name, family.method.to_lowercase()),
        ),
        Definition::Transform(transform) => (
            String::new(),
            format!(
                "FOR {} LANGUAGE {}",
                identity_type(&transform.data_type),
                transform.language
            ),
        ),
        Definition::Cast(cast) => (
            String::new(),
            format!(
                "({} AS {})",
                identity_type(cast.source_type.as_deref().unwrap_or_default()),
                identity_type(cast.target_type.as_deref().unwrap_or_default())
            ),
        ),
        // deploy splits a container into one for each object, which is
        // keyed by its kind and name (`alter::text_search::split`)
        Definition::TextSearch(container) => (
            container.schema.clone(),
            super::alter::text_search::key_name(container),
        ),
        _ => (
            definition.schema().unwrap_or_default().to_string(),
            definition.name(),
        ),
    }
}

/// The function identity signature with canonicalized parameter
/// types, so a repo `fn(int4)` keys identically to the server's
/// `fn(integer)` (mirrors [`crate::models::Function::identity`]). A
/// parameter type has no typmod (see [`identity_type`]).
fn function_key_name(function: &crate::models::Function) -> String {
    let args: Vec<String> = function
        .parameters
        .iter()
        .flatten()
        .filter(|p| p.mode != "OUT" && p.mode != "TABLE")
        .map(|p| {
            let mut parts: Vec<String> = Vec::new();
            if p.mode != "IN" {
                parts.push(p.mode.clone());
            }
            if let Some(name) = &p.name {
                parts.push(name.clone());
            }
            parts.push(identity_type(&p.data_type));
            parts.join(" ")
        })
        .collect();
    format!("{}({})", function.name, args.join(", "))
}

/// The function's parameter *types* only, in the format pg_dump's
/// archive TOC tag uses (no argument names, modes or typmods, see
/// [`identity_type`]). It is also the identity of a procedure. `deploy`'s
/// drop-ordering pass (`entry_key` in `mod.rs`) keys snapshot entries
/// by their literal tag, which does not carry argument names, so it
/// cannot be compared against [`function_key_name`]'s identity
/// signature directly; this gives that pass a key in the same shape. A
/// name that has its argument list and no `parameters` keeps its list,
/// as `build` does.
pub(crate) fn function_tag_name(function: &crate::models::Function) -> String {
    let parameters = function.parameters.as_deref().unwrap_or_default();
    if parameters.is_empty() && function.name.contains('(') {
        return function.name.clone();
    }
    let args: Vec<String> = parameters
        .iter()
        .filter(|p| p.mode != "OUT" && p.mode != "TABLE")
        .map(|p| identity_type(&p.data_type))
        .collect();
    format!("{}({})", function.name, args.join(", "))
}

impl std::fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.desc == ObjectType::TextSearch
            && let Some((kind, name)) =
                super::alter::text_search::split_key_name(&self.name)
        {
            write!(f, "TEXT SEARCH {kind} {}.{name}", self.schema)
        } else if self.schema.is_empty() {
            write!(f, "{} {}", self.desc.as_str(), self.name)
        } else {
            write!(f, "{} {}.{}", self.desc.as_str(), self.schema, self.name)
        }
    }
}

/// How a repo inventory item relates to the database
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Change {
    /// In the repo but not the database → CREATE
    Added,
    /// In both and identical → nothing to do
    Unchanged,
    /// In both but different → ALTER, or the gated drop+recreate
    /// fallback
    Changed,
    /// Exists on both sides, but the project writes it as a raw `sql`
    /// statement, which deploy does not compare
    Undiffable,
    /// Out of deploy's scope (roles, users, groups, tablespaces)
    Skipped,
}

/// The classification of every repo inventory item plus the
/// database-only objects
pub struct Diff {
    /// Inventory item id → change classification
    pub items: BTreeMap<usize, Change>,
    /// Inventory item id → the database-side definition, for items
    /// classified [`Change::Changed`] (the ALTER renderers need both
    /// sides)
    pub changed: BTreeMap<usize, Definition>,
    /// Objects in the database with no repo counterpart → DROP, keyed
    /// to their database-side definition (some drops, e.g. user
    /// mappings, need more than the key to render)
    pub removed: BTreeMap<ObjectKey, Definition>,
    /// Inventory item ids whose project definition names an owner:
    /// deploy gives each one that it creates this owner
    pub owned: BTreeSet<usize>,
    /// Inventory item ids that the database has with another owner
    /// than the project. The owner is not part of the definition
    /// comparison: `ALTER ... OWNER TO` changes it in place for every
    /// type
    pub owner_changed: BTreeSet<usize>,
}

/// How deploy compares the objects of one type
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Compare {
    /// Compare the definitions; a difference is a change, and an
    /// object that only the database has is removed
    Definition,
    /// Out of deploy's scope: roles, users, and groups require
    /// cluster-level access pg_dump does not capture, and tablespaces
    /// are likewise absent from a single-database dump — diffing them
    /// would re-create them on every run
    Skip,
}

/// How deploy compares each object type. Every type that deploy
/// manages is compared by definition: deploy reconciles its changes
/// (`alter::resolve_with`) and drops it in dependency order
/// (`entry_key` and `drop_sql` in `mod.rs`).
fn compare(desc: ObjectType) -> Compare {
    match desc {
        ObjectType::AccessMethod => Compare::Definition,
        ObjectType::Aggregate => Compare::Definition,
        ObjectType::Cast => Compare::Definition,
        ObjectType::Collation => Compare::Definition,
        ObjectType::Conversion => Compare::Definition,
        ObjectType::DefaultPrivileges => Compare::Definition,
        ObjectType::Domain => Compare::Definition,
        ObjectType::EventTrigger => Compare::Definition,
        ObjectType::Extension => Compare::Definition,
        ObjectType::ForeignDataWrapper => Compare::Definition,
        ObjectType::Function => Compare::Definition,
        ObjectType::Group => Compare::Skip,
        ObjectType::MaterializedView => Compare::Definition,
        ObjectType::Operator => Compare::Definition,
        ObjectType::OperatorClass => Compare::Definition,
        ObjectType::OperatorFamily => Compare::Definition,
        ObjectType::ProceduralLanguage => Compare::Definition,
        ObjectType::Procedure => Compare::Definition,
        ObjectType::Publication => Compare::Definition,
        ObjectType::Role => Compare::Skip,
        ObjectType::Schema => Compare::Definition,
        ObjectType::Sequence => Compare::Definition,
        ObjectType::Server => Compare::Definition,
        ObjectType::Statistics => Compare::Definition,
        ObjectType::Subscription => Compare::Definition,
        ObjectType::Table => Compare::Definition,
        ObjectType::Tablespace => Compare::Skip,
        ObjectType::TextSearch => Compare::Definition,
        ObjectType::Transform => Compare::Definition,
        ObjectType::Type => Compare::Definition,
        ObjectType::User => Compare::Skip,
        ObjectType::UserMapping => Compare::Definition,
        ObjectType::View => Compare::Definition,
    }
}

pub fn diff(project: &Project, assembly: &Assembly) -> Diff {
    let mut database = database_index(assembly);
    super::alter::operator::align(project, &mut database);
    super::alter::operator_class::align(project, &mut database);
    let mut items = BTreeMap::new();
    let mut changed = BTreeMap::new();
    let mut owned = BTreeSet::new();
    let mut owner_changed = BTreeSet::new();
    for item in &project.inventory {
        if compare(item.desc) != Compare::Skip
            && item.definition.owner().is_some()
        {
            owned.insert(item.id);
        }
        let change = match compare(item.desc) {
            Compare::Skip => {
                log::debug!(
                    "Skipping {} {}: not managed by deploy",
                    item.desc.as_str(),
                    item.definition.name()
                );
                Change::Skipped
            }
            // pull writes the structured fields, so a raw statement
            // never compares equal: deploy only finds the object that
            // it stands for, so that the object is not dropped
            Compare::Definition if item.definition.raw_sql() => {
                match take_raw(&mut database, item.desc, &item.definition) {
                    Some(_) => Change::Undiffable,
                    None => Change::Added,
                }
            }
            Compare::Definition => {
                let key = ObjectKey::new(item.desc, &item.definition);
                match database.remove(&key) {
                    None => Change::Added,
                    Some(db) => {
                        let db = match (&item.definition, db) {
                            (
                                Definition::Table(repo),
                                Definition::Table(db),
                            ) => Definition::Table(
                                db.without_unmanaged_security(repo),
                            ),
                            (_, db) => db,
                        };
                        // a dump made without owners gives none to
                        // compare
                        if db.owner().is_some_and(|owner| !owner.is_empty())
                            && item.definition.owner() != db.owner()
                        {
                            owner_changed.insert(item.id);
                        }
                        // pull removes a password from a connection, and
                        // a copied text search configuration compares by
                        // the mappings that the project gives
                        let compared = match (&item.definition, &db) {
                            (
                                Definition::Subscription(repo),
                                Definition::Subscription(db),
                            ) => Some(Definition::Subscription(
                                without_redacted_password(repo, db),
                            )),
                            (
                                Definition::TextSearch(repo),
                                Definition::TextSearch(db),
                            ) => Some(Definition::TextSearch(
                                super::alter::text_search::copied_view(
                                    repo, db,
                                ),
                            )),
                            _ => None,
                        };
                        if normalized(&item.definition)
                            == normalized(compared.as_ref().unwrap_or(&db))
                        {
                            Change::Unchanged
                        } else {
                            changed.insert(item.id, db);
                            Change::Changed
                        }
                    }
                }
            }
        };
        items.insert(item.id, change);
    }
    Diff {
        items,
        changed,
        removed: database,
        owned,
        owner_changed,
    }
}

/// Remove the database object that a raw `sql` item stands for, and
/// return it. The item's key is tried first. A raw function has no
/// parameter list, so its key can differ from the database key: then
/// the first object of the same type, schema and bare name (the name
/// with no argument list) matches. A raw cast must have its source and
/// target types, and it matches only by them.
fn take_raw(
    database: &mut BTreeMap<ObjectKey, Definition>,
    desc: ObjectType,
    definition: &Definition,
) -> Option<Definition> {
    let key = ObjectKey::new(desc, definition);
    if let Some(db) = database.remove(&key) {
        return Some(db);
    }
    // a cast key, `(source AS target)`, has no bare name
    if desc == ObjectType::Cast {
        return None;
    }
    let bare = |name: &str| {
        crate::utils::split_signature(name)
            .map_or(name, |(name, _)| name)
            .trim_end()
            .to_string()
    };
    let name = bare(&key.name);
    let found = database
        .keys()
        .find(|k| {
            k.desc == key.desc
                && k.schema == key.schema
                && bare(&k.name) == name
        })
        .cloned()?;
    database.remove(&found)
}

/// Every object the snapshot parsed into a model, with its type
fn snapshot_definitions(assembly: &Assembly) -> Vec<(ObjectType, Definition)> {
    fn all<'a, T: Clone + 'a>(
        desc: ObjectType,
        items: &'a [T],
        wrap: fn(T) -> Definition,
    ) -> impl Iterator<Item = (ObjectType, Definition)> + 'a {
        items.iter().map(move |d| (desc, wrap(d.clone())))
    }
    use ObjectType as O;
    let a = assembly;
    std::iter::empty()
        .chain(all(O::Schema, &a.schemas, Definition::Schema))
        .chain(all(O::Extension, &a.extensions, Definition::Extension))
        .chain(all(
            O::ProceduralLanguage,
            &a.languages,
            Definition::Language,
        ))
        .chain(all(O::Domain, &a.domains, Definition::Domain))
        .chain(all(O::Type, &a.types, Definition::Type))
        .chain(all(O::Sequence, &a.sequences, Definition::Sequence))
        .chain(all(O::Table, &a.tables, Definition::Table))
        .chain(all(O::View, &a.views, Definition::View))
        .chain(all(
            O::MaterializedView,
            &a.materialized_views,
            Definition::MaterializedView,
        ))
        .chain(all(O::Function, &a.functions, Definition::Function))
        .chain(all(
            O::ForeignDataWrapper,
            &a.foreign_data_wrappers,
            Definition::ForeignDataWrapper,
        ))
        .chain(all(O::Server, &a.servers, Definition::Server))
        .chain(all(
            O::UserMapping,
            &a.user_mappings,
            Definition::UserMapping,
        ))
        .chain(all(O::Aggregate, &a.aggregates, Definition::Aggregate))
        .chain(all(O::Cast, &a.casts, Definition::Cast))
        .chain(all(O::Transform, &a.transforms, Definition::Transform))
        .chain(all(O::Collation, &a.collations, Definition::Collation))
        .chain(all(O::Conversion, &a.conversions, Definition::Conversion))
        .chain(all(
            O::EventTrigger,
            &a.event_triggers,
            Definition::EventTrigger,
        ))
        .chain(all(
            O::Publication,
            &a.publications,
            Definition::Publication,
        ))
        .chain(all(
            O::Subscription,
            &a.subscriptions,
            Definition::Subscription,
        ))
        .chain(
            a.text_search
                .iter()
                .flat_map(super::alter::text_search::split)
                .map(|t| (O::TextSearch, Definition::TextSearch(t))),
        )
        .chain(all(O::Procedure, &a.procedures, Definition::Procedure))
        .chain(all(O::Operator, &a.operators, Definition::Operator))
        .chain(all(O::Statistics, &a.statistics, Definition::Statistics))
        .chain(all(
            O::OperatorFamily,
            &a.operator_families,
            Definition::OperatorFamily,
        ))
        .chain(all(
            O::OperatorClass,
            &a.operator_classes,
            Definition::OperatorClass,
        ))
        .chain(all(
            O::AccessMethod,
            &a.access_methods,
            Definition::AccessMethod,
        ))
        .chain(all(
            O::DefaultPrivileges,
            &a.default_privileges,
            Definition::DefaultPrivileges,
        ))
        .collect()
}

/// `ObjectKey → Definition` for every snapshot object whose type is
/// compared by definition
fn database_index(assembly: &Assembly) -> BTreeMap<ObjectKey, Definition> {
    snapshot_definitions(assembly)
        .into_iter()
        .filter(|(desc, _)| compare(*desc) == Compare::Definition)
        .map(|(desc, definition)| {
            (ObjectKey::new(desc, &definition), definition)
        })
        .collect()
}

/// A definition as a JSON value with the fields deploy does not
/// manage removed and type aliases canonicalized
fn normalized(definition: &Definition) -> Value {
    // a table compares in its canonical form, so the same constraint
    // written two ways, the same policies in another order, or a value
    // written at its default, is not a change
    let canonical;
    let definition = match definition {
        Definition::Table(table) => {
            canonical = Definition::Table(table.canonical());
            &canonical
        }
        Definition::MaterializedView(view) => {
            canonical = Definition::MaterializedView(view.canonical());
            &canonical
        }
        Definition::Function(function) => {
            canonical = Definition::Function(Function {
                configuration: function
                    .configuration
                    .as_ref()
                    .map(canonical_settings),
                parameters: function.parameters.as_ref().map(|parameters| {
                    parameters
                        .iter()
                        .map(|p| crate::models::FunctionParameter {
                            data_type: identity_type(&p.data_type),
                            ..p.clone()
                        })
                        .collect()
                }),
                returns: function.returns.as_deref().map(return_type),
                sql_body: function.sql_body.as_deref().map(canonical_sql_body),
                ..function.clone()
            });
            &canonical
        }
        Definition::Procedure(procedure) => {
            let procedure = procedure.canonical();
            canonical = Definition::Procedure(crate::models::Procedure {
                sql_body: procedure
                    .sql_body
                    .as_deref()
                    .map(canonical_sql_body),
                ..procedure
            });
            &canonical
        }
        Definition::Domain(domain) => {
            canonical = Definition::Domain(canonical_domain(domain));
            &canonical
        }
        Definition::Publication(publication) => {
            canonical = Definition::Publication(publication.canonical());
            &canonical
        }
        Definition::Subscription(subscription) => {
            canonical = Definition::Subscription(subscription.canonical());
            &canonical
        }
        Definition::Aggregate(aggregate) => {
            canonical = Definition::Aggregate(
                super::alter::aggregate::canonical(aggregate),
            );
            &canonical
        }
        Definition::Cast(cast) => {
            canonical = Definition::Cast(super::alter::cast::canonical(cast));
            &canonical
        }
        Definition::Operator(operator) => {
            canonical = Definition::Operator(
                super::alter::operator::canonical(operator),
            );
            &canonical
        }
        Definition::OperatorClass(class) => {
            canonical = Definition::OperatorClass(
                super::alter::operator_class::canonical_class(class),
            );
            &canonical
        }
        Definition::OperatorFamily(family) => {
            canonical = Definition::OperatorFamily(
                super::alter::operator_class::canonical_family(family),
            );
            &canonical
        }
        Definition::Transform(transform) => {
            canonical = Definition::Transform(
                super::alter::transform::canonical(transform),
            );
            &canonical
        }
        Definition::Statistics(statistics) => {
            canonical = Definition::Statistics(
                super::alter::statistics::canonical(statistics),
            );
            &canonical
        }
        Definition::EventTrigger(trigger) => {
            canonical = Definition::EventTrigger(
                super::alter::event_trigger::canonical(trigger),
            );
            &canonical
        }
        Definition::Collation(collation) => {
            canonical = Definition::Collation(
                super::alter::collation::canonical(collation),
            );
            &canonical
        }
        Definition::Conversion(conversion) => {
            canonical = Definition::Conversion(
                super::alter::conversion::canonical(conversion),
            );
            &canonical
        }
        Definition::AccessMethod(method) => {
            canonical = Definition::AccessMethod(
                super::alter::access_method::canonical(method),
            );
            &canonical
        }
        Definition::TextSearch(container) => {
            canonical = Definition::TextSearch(
                super::alter::text_search::canonical(container),
            );
            &canonical
        }
        // default privileges compare by the privileges they give, so
        // the same privileges declared two ways are not a change
        Definition::DefaultPrivileges(defaults) => {
            let acl: Vec<_> =
                super::alter::default_privileges::effective(defaults)
                    .into_iter()
                    .collect();
            return serde_json::to_value(acl).unwrap_or(Value::Null);
        }
        other => other,
    };
    let mut value = serde_json::to_value(definition).unwrap_or(Value::Null);
    normalize(&mut value);
    value
}

/// The domain with the type of each cast in its default and its CHECK
/// constraints in the form that PostgreSQL writes (see
/// [`canonical_casts`]). A NULL default is in the form that PostgreSQL
/// stores (see [`stored_null_default`]).
pub(crate) fn canonical_domain(domain: &Domain) -> Domain {
    let mut domain = domain.clone();
    if let (Some(data_type), Some(default)) =
        (&domain.data_type, &domain.default)
        && let Some(stored) =
            stored_null_default(data_type, default, &UserTypes::new())
    {
        domain.default = stored;
    }
    if let Some(default) = &mut domain.default {
        *default = canonical_casts(default);
    }
    for check in domain.check_constraints.iter_mut().flatten() {
        if let Some(expression) = &mut check.expression {
            *expression = canonical_casts(expression);
        }
    }
    domain
}

/// A type of the project that is not built in, as
/// [`stored_null_default`] finds it
pub(crate) enum UserType {
    /// A domain, with its data type
    Domain(String),
    /// An enum, a composite or a range type
    Other,
}

/// The [`UserType`]s of the project, by their qualified names in the
/// form of `names::name`
pub(crate) type UserTypes = BTreeMap<String, UserType>;

/// The user types of the project
fn user_types(project: &Project) -> UserTypes {
    let mut types = UserTypes::new();
    for item in &project.inventory {
        let (schema, name, user_type) = match &item.definition {
            Definition::Domain(domain) => match &domain.data_type {
                Some(data_type) => (
                    &domain.schema,
                    &domain.name,
                    UserType::Domain(data_type.clone()),
                ),
                None => continue,
            },
            // a type with no kind is a composite type
            Definition::Type(user_type)
                if matches!(
                    user_type.type_kind.as_deref(),
                    None | Some("enum" | "composite" | "range")
                ) =>
            {
                (&user_type.schema, &user_type.name, UserType::Other)
            }
            _ => continue,
        };
        types.insert(
            super::alter::names::name(&format!(
                "{}.{}",
                quote_ident(schema),
                quote_ident(name)
            )),
            user_type,
        );
    }
    types
}

/// Each NULL default of the project's tables and domains in the form
/// that PostgreSQL stores (see [`stored_null_default`]), with the
/// project's domains and other types. The form of a domain or an enum
/// is not known from the name of the type, thus deploy changes the
/// project before it compares it. A default on an inherited column
/// has the type of the column in a parent table of the project.
pub(crate) fn store_null_defaults(project: &mut Project) {
    let types = user_types(project);
    // the parents and the column types of each table
    let mut tables = BTreeMap::new();
    for item in &project.inventory {
        if let Definition::Table(table) = &item.definition {
            let columns: BTreeMap<String, String> = table
                .columns
                .iter()
                .flatten()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect();
            let parents: Vec<String> = table
                .parents
                .iter()
                .flatten()
                .map(|p| super::alter::names::name(p))
                .collect();
            tables.insert(table_key(table), (parents, columns));
        }
    }
    // the type of the column in the first parent of the table, or in
    // the parents of the parent, that has it
    let inherited = |table: &String, column: &str| {
        let mut queue: Vec<&String> =
            tables.get(table)?.0.iter().rev().collect();
        let mut seen = BTreeSet::new();
        while let Some(parent) = queue.pop() {
            let Some((grandparents, columns)) = tables.get(parent) else {
                continue;
            };
            if !seen.insert(parent) {
                continue;
            }
            if let Some(data_type) = columns.get(column) {
                return Some(data_type.clone());
            }
            queue.extend(grandparents.iter().rev());
        }
        None
    };
    for item in &mut project.inventory {
        match &mut item.definition {
            Definition::Table(table) => {
                for column in table.columns.iter_mut().flatten() {
                    if let Some(Value::String(text)) = &column.default
                        && let Some(stored) = stored_null_default(
                            &column.data_type,
                            text,
                            &types,
                        )
                    {
                        column.default = stored.map(Value::String);
                    }
                }
                let key = table_key(table);
                if let Some(defaults) = &mut table.column_defaults {
                    defaults.retain_mut(|column_default| {
                        let Value::String(text) = &column_default.default
                        else {
                            return true;
                        };
                        let stored = inherited(&key, &column_default.column)
                            .and_then(|data_type| {
                                stored_null_default(&data_type, text, &types)
                            });
                        match stored {
                            Some(Some(text)) => {
                                column_default.default = Value::String(text);
                                true
                            }
                            Some(None) => false,
                            None => true,
                        }
                    });
                    if defaults.is_empty() {
                        table.column_defaults = None;
                    }
                }
            }
            Definition::Domain(domain) => {
                if let (Some(data_type), Some(default)) =
                    (&domain.data_type, &domain.default)
                    && let Some(stored) =
                        stored_null_default(data_type, default, &types)
                {
                    domain.default = stored;
                }
            }
            _ => {}
        }
    }
}

/// The qualified name of the table in the form of `names::name`
fn table_key(table: &crate::models::Table) -> String {
    super::alter::names::name(&format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    ))
}

/// What PostgreSQL stores for the default on a column (or a domain)
/// of the type, when the default is a NULL with no cast, or with
/// casts only to the type of the NULL that PostgreSQL makes for the
/// column (see [`null_type`]), or to a domain that is the type of the
/// column. PostgreSQL stores no default (`Some(None)`) when that NULL
/// has the type of the column: on a column of a built-in type with no
/// modifier (`NULL::integer` on an integer column), of an interval
/// with a modifier, of an enum, a composite or a range type, or of an
/// array of a domain. Otherwise it stores that NULL
/// (`NULL::character varying` on a varchar(10) column,
/// `NULL::integer` on a domain over integer), with a cast to the
/// domain when the default has one (`(NULL::integer)::test.dint`).
/// `None` is a default that is not such a NULL, or a type that is not
/// built in and not in `types`.
pub(crate) fn stored_null_default(
    data_type: &str,
    default: &str,
    types: &UserTypes,
) -> Option<Option<String>> {
    let data_type = canonical_type(data_type);
    let null = null_type(&data_type, types, 0)?;
    let domain = matches!(
        types.get(&super::alter::names::name(&data_type)),
        Some(UserType::Domain(_))
    );
    let null_cast = format!("::{null}");
    let domain_cast = format!("::{data_type}");
    let mut cast_to_domain = false;
    let mut text = canonical_casts(default);
    loop {
        let trimmed = text.trim();
        let operand = match trimmed
            .strip_prefix('(')
            .and_then(|inner| inner.strip_suffix(')'))
        {
            Some(inner) => inner,
            None => match trimmed.strip_suffix(&null_cast) {
                Some(operand) => operand,
                None => match trimmed.strip_suffix(&domain_cast) {
                    Some(operand) if domain => {
                        cast_to_domain = true;
                        operand
                    }
                    _ if trimmed.eq_ignore_ascii_case("null") => break,
                    _ => return None,
                },
            },
        };
        text = operand.to_string();
    }
    Some(if domain && cast_to_domain {
        Some(format!("(NULL::{null})::{data_type}"))
    } else if null == data_type {
        None
    } else {
        Some(format!("NULL::{null}"))
    })
}

/// The type of the NULL that PostgreSQL makes for a NULL default on a
/// column of the type (a type in the form of [`canonical_type`]), as
/// it writes it: the type with no modifier, where `character(n)` is
/// `bpchar` and `bit(n)` is `"bit"`. An interval keeps its modifier,
/// other than in an array. The NULL of a domain is the NULL of its
/// data type. `None` is a type that is not built in and not in
/// `types`.
fn null_type(
    data_type: &str,
    types: &UserTypes,
    depth: usize,
) -> Option<String> {
    let (element, array) = match data_type.strip_suffix("[]") {
        Some(element) => (element, "[]"),
        None => (data_type, ""),
    };
    let base = match identity_type(element).as_str() {
        "character" => String::from("bpchar"),
        "bit" => String::from("\"bit\""),
        base => base.to_string(),
    };
    let built_in = BUILT_IN_TYPES.contains(&base.as_str())
        || matches!(
            base.as_str(),
            "integer"
                | "smallint"
                | "bigint"
                | "real"
                | "double precision"
                | "boolean"
                | "numeric"
                | "character varying"
                | "bit varying"
                | "timestamp without time zone"
                | "timestamp with time zone"
                | "time without time zone"
                | "time with time zone"
                | "\"char\""
                | "\"bit\""
        );
    if built_in {
        return Some(if array.is_empty() && base == "interval" {
            element.to_string()
        } else {
            format!("{base}{array}")
        });
    }
    match types.get(&super::alter::names::name(element))? {
        // a domain over a domain is limited, so that a loop of domains
        // in a project that is not valid ends
        UserType::Domain(base) if array.is_empty() && depth < 16 => {
            null_type(&canonical_type(base), types, depth + 1)
        }
        UserType::Domain(_) if array.is_empty() => None,
        _ => Some(data_type.to_string()),
    }
}

/// The database subscription without the password in its connection
/// when the project connection has none. Pull removes the password, so
/// a project that does not carry one does not remove or change it (as
/// `alter::keep_redacted_password` does for a user mapping).
pub(crate) fn without_redacted_password(
    repo: &Subscription,
    db: &Subscription,
) -> Subscription {
    let mut db = db.clone();
    if without_password(&repo.connection).is_none()
        && let Some(connection) = without_password(&db.connection)
    {
        db.connection = connection;
    }
    db
}

fn normalize(value: &mut Value) {
    match value {
        Value::Object(map) => {
            // the owner is compared on its own (`Diff::owner_changed`)
            map.remove("owner");
            for (key, child) in map.iter_mut() {
                if (key == "data_type" || key == "returns")
                    && let Some(data_type) = child.as_str()
                {
                    *child = Value::String(canonical_type(data_type));
                } else if key == "collation"
                    && let Some(collation) = child.as_str()
                {
                    *child = Value::String(canonical_collation(collation));
                } else {
                    normalize(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(normalize),
        _ => {}
    }
}

/// A type name in the form that PostgreSQL's `format_type` writes for
/// a column of that type, so a hand-written `int4` does not falsely
/// diff against the server's `integer` (PLAN.md risk #5). The forms
/// that change are the ones that PostgreSQL 18 changes:
///
/// - an alias (`int4`, `varchar`, `timestamptz`, `decimal`) is the
///   standard name;
/// - `float` is `double precision`, and `float(p)` is `real` for a
///   precision of 1 to 24 and `double precision` for 25 to 53;
/// - `char` and `bit` with no length have the length 1;
/// - `timestamp` and `time` are `without time zone`, with the
///   precision after the first word (`timestamp(3) with time zone`);
/// - `numeric(p)` is `numeric(p,0)`;
/// - an array bound (`int[3]`), `ARRAY` and the array type name of a
///   built-in type (`_int4`, `_text`) are `[]`, and a modifier of an
///   array type name is the modifier of its element type
///   (`_varchar(4)` is `character varying(4)[]`);
/// - a quoted name of a built-in type (`"int4"`, `"_text"`) is the name
///   with no quotes, as PostgreSQL finds the type in pg_catalog;
/// - a modifier has no spaces next to its parentheses and commas, and
///   a name that is not quoted is in lowercase.
///
/// A user-defined type keeps its name, and so does a quoted name that
/// is not the name of a built-in type (`"integer"` is not a type). A
/// built-in type has no `pg_catalog` schema, as pg_dump writes it with
/// none. `SETOF` before a return type stays.
pub(crate) fn canonical_type(data_type: &str) -> String {
    let parts = type_parts(data_type);
    match parts.split_first() {
        Some((TypePart::Word(first), rest))
            if first == "setof" && !rest.is_empty() =>
        {
            format!("setof {}", canonical_parts(rest))
        }
        _ => canonical_parts(&parts),
    }
}

/// One part of a type name
#[derive(Debug, PartialEq)]
enum TypePart {
    /// A name, with its schema and its quoted parts
    Word(String),
    /// A modifier with its parentheses
    Modifier(String),
    /// An array bound in brackets
    Bound,
}

/// The parts of a type name. PostgreSQL folds a name that is not quoted
/// to lowercase, so `TEXT` is `text`; a quoted name keeps its case.
fn type_parts(data_type: &str) -> Vec<TypePart> {
    let mut parts = Vec::new();
    let mut word = String::new();
    let mut chars = data_type.chars();
    let flush = |word: &mut String, parts: &mut Vec<TypePart>| {
        if !word.is_empty() {
            parts.push(TypePart::Word(std::mem::take(word)));
        }
    };
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                word.push(c);
                for c in chars.by_ref() {
                    word.push(c);
                    if c == '"' {
                        break;
                    }
                }
            }
            '(' => {
                flush(&mut word, &mut parts);
                parts.push(TypePart::Modifier(modifier(&mut chars)));
            }
            '[' => {
                flush(&mut word, &mut parts);
                chars.by_ref().find(|c| *c == ']');
                parts.push(TypePart::Bound);
            }
            c if c.is_whitespace() => flush(&mut word, &mut parts),
            c => word.push(c.to_ascii_lowercase()),
        }
    }
    flush(&mut word, &mut parts);
    parts
}

/// A modifier, from after its `(` to its `)`, in lowercase and with no
/// spaces next to its parentheses and commas, other than in quotes
fn modifier(chars: &mut std::str::Chars) -> String {
    let mut text = String::from("(");
    let mut depth = 1;
    let mut quote = None;
    let mut space = false;
    for c in chars.by_ref() {
        if let Some(open) = quote {
            text.push(c);
            if c == open {
                quote = None;
            }
            continue;
        }
        if c.is_whitespace() {
            space = true;
            continue;
        }
        if space && !matches!(c, ')' | ',') && !text.ends_with(['(', ',']) {
            text.push(' ');
        }
        space = false;
        match c {
            '"' | '\'' => quote = Some(c),
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        text.push(c.to_ascii_lowercase());
        if depth == 0 {
            break;
        }
    }
    text
}

/// The names of the types in pg_catalog on PostgreSQL 18, other than
/// array types and the row types of the system catalogs. A quoted name
/// in this list is the built-in type. The list does not change in a
/// major version of PostgreSQL.
const BUILT_IN_TYPES: &[&str] = &[
    "aclitem",
    "any",
    "anyarray",
    "anycompatible",
    "anycompatiblearray",
    "anycompatiblemultirange",
    "anycompatiblenonarray",
    "anycompatiblerange",
    "anyelement",
    "anyenum",
    "anymultirange",
    "anynonarray",
    "anyrange",
    "bit",
    "bool",
    "box",
    "bpchar",
    "bytea",
    "char",
    "cid",
    "cidr",
    "circle",
    "cstring",
    "date",
    "datemultirange",
    "daterange",
    "event_trigger",
    "fdw_handler",
    "float4",
    "float8",
    "gtsvector",
    "index_am_handler",
    "inet",
    "int2",
    "int2vector",
    "int4",
    "int4multirange",
    "int4range",
    "int8",
    "int8multirange",
    "int8range",
    "internal",
    "interval",
    "json",
    "jsonb",
    "jsonpath",
    "language_handler",
    "line",
    "lseg",
    "macaddr",
    "macaddr8",
    "money",
    "name",
    "numeric",
    "nummultirange",
    "numrange",
    "oid",
    "oidvector",
    "path",
    "pg_brin_bloom_summary",
    "pg_brin_minmax_multi_summary",
    "pg_ddl_command",
    "pg_dependencies",
    "pg_lsn",
    "pg_mcv_list",
    "pg_ndistinct",
    "pg_node_tree",
    "pg_snapshot",
    "point",
    "polygon",
    "record",
    "refcursor",
    "regclass",
    "regcollation",
    "regconfig",
    "regdictionary",
    "regnamespace",
    "regoper",
    "regoperator",
    "regproc",
    "regprocedure",
    "regrole",
    "regtype",
    "table_am_handler",
    "text",
    "tid",
    "time",
    "timestamp",
    "timestamptz",
    "timetz",
    "trigger",
    "tsm_handler",
    "tsmultirange",
    "tsquery",
    "tsrange",
    "tstzmultirange",
    "tstzrange",
    "tsvector",
    "txid_snapshot",
    "unknown",
    "uuid",
    "varbit",
    "varchar",
    "void",
    "xid",
    "xid8",
    "xml",
];

/// The types of [`BUILT_IN_TYPES`] that have no array type: the
/// pseudo-types other than `record` and `cstring`, and the types of
/// planner statistics and summaries
const WITHOUT_ARRAY: &[&str] = &[
    "any",
    "anyarray",
    "anycompatible",
    "anycompatiblearray",
    "anycompatiblemultirange",
    "anycompatiblenonarray",
    "anycompatiblerange",
    "anyelement",
    "anyenum",
    "anymultirange",
    "anynonarray",
    "anyrange",
    "event_trigger",
    "fdw_handler",
    "index_am_handler",
    "internal",
    "language_handler",
    "pg_brin_bloom_summary",
    "pg_brin_minmax_multi_summary",
    "pg_ddl_command",
    "pg_dependencies",
    "pg_mcv_list",
    "pg_ndistinct",
    "pg_node_tree",
    "table_am_handler",
    "trigger",
    "tsm_handler",
    "unknown",
    "void",
];

/// The type that the parts name, with `[]` for an array
fn canonical_parts(parts: &[TypePart]) -> String {
    let mut parts = parts;
    let mut array = false;
    while let [rest @ .., TypePart::Bound] = parts {
        parts = rest;
        array = true;
    }
    if let [rest @ .., TypePart::Word(word)] = parts
        && !rest.is_empty()
        && word == "array"
    {
        parts = rest;
        array = true;
    }
    let mut base = base_type(parts);
    if array && !base.ends_with("[]") {
        base.push_str("[]");
    }
    base
}

/// The type that the parts name, other than an array bound
fn base_type(parts: &[TypePart]) -> String {
    let mut words = Vec::new();
    let mut modifier = None;
    // the number of words before the modifier
    let mut position = 0;
    for part in parts {
        match part {
            TypePart::Word(word) => words.push(word.as_str()),
            TypePart::Modifier(text) if modifier.is_none() => {
                modifier = Some(text.as_str());
                position = words.len();
            }
            _ => return written(&words, modifier, position),
        }
    }
    let Some(first) = words.first_mut() else {
        return written(&words, modifier, position);
    };
    // a built-in type is in pg_catalog, and pg_dump writes it with no
    // schema. The keywords `char` and `bit` are `character` and
    // `bit(1)` only when they have no schema: `pg_catalog.char` is the
    // one-byte type `"char"`, and `pg_catalog.bit` has no length.
    let mut qualified = match first
        .strip_prefix("pg_catalog.")
        .or_else(|| first.strip_prefix("\"pg_catalog\"."))
    {
        Some(name) => {
            *first = name;
            true
        }
        None => false,
    };
    // PostgreSQL finds a quoted name in pg_catalog first, so `"int4"`
    // is the type int4, as `pg_catalog.int4` is
    if let [name] = words[..]
        && let Some(name) = name
            .strip_prefix('"')
            .and_then(|name| name.strip_suffix('"'))
        && (BUILT_IN_TYPES.contains(&name)
            || name.strip_prefix('_').is_some_and(has_array))
    {
        words[0] = name;
        qualified = true;
    }
    if let [name] = words[..]
        && let Some(element) = name.strip_prefix('_')
        && has_array(element)
    {
        return format!(
            "{}[]",
            canonical_type(&format!(
                "pg_catalog.{element}{}",
                modifier.unwrap_or_default()
            ))
        );
    }
    let name = words.join(" ");
    // the modifier is after the last word, or after the first word of
    // a date and time type
    let at_end = modifier.is_none() || position == words.len();
    let after_first = modifier.is_none() || position == 1;
    let with = |base: &str| format!("{base}{}", modifier.unwrap_or_default());
    let zone = |base: &str, zone: &str| {
        format!("{base}{} {zone} time zone", modifier.unwrap_or_default())
    };
    match name.as_str() {
        "any" if qualified => String::from("\"any\""),
        "char" if qualified && modifier.is_none() => String::from("\"char\""),
        "bit" if qualified && modifier.is_none() => String::from("\"bit\""),
        "timestamp" | "timestamp without time zone" if after_first => {
            zone("timestamp", "without")
        }
        "timestamptz" | "timestamp with time zone" if after_first => {
            zone("timestamp", "with")
        }
        "time" | "time without time zone" if after_first => {
            zone("time", "without")
        }
        "timetz" | "time with time zone" if after_first => {
            zone("time", "with")
        }
        _ if !at_end => written(&words, modifier, position),
        "int" | "integer" | "int4" => with("integer"),
        "smallint" | "int2" => with("smallint"),
        "bigint" | "int8" => with("bigint"),
        "real" | "float4" => with("real"),
        "double precision" | "float8" => with("double precision"),
        "boolean" | "bool" => with("boolean"),
        "float" => match modifier.map(precision) {
            None => String::from("double precision"),
            Some(Some(1..=24)) => String::from("real"),
            Some(Some(25..=53)) => String::from("double precision"),
            Some(_) => written(&words, modifier, position),
        },
        "decimal" | "dec" | "numeric" => match modifier.map(precision) {
            Some(Some(precision)) => format!("numeric({precision},0)"),
            _ => with("numeric"),
        },
        "char" | "character" | "nchar" | "national char"
        | "national character" => {
            format!("character{}", modifier.unwrap_or("(1)"))
        }
        "bpchar" if modifier.is_some() => with("character"),
        "varchar"
        | "char varying"
        | "character varying"
        | "nchar varying"
        | "national char varying"
        | "national character varying" => with("character varying"),
        "bit" => format!("bit{}", modifier.unwrap_or("(1)")),
        "varbit" | "bit varying" => with("bit varying"),
        _ => written(&words, modifier, position),
    }
}

/// Whether the built-in type has an array type, whose name is `_` and
/// the name of the type
fn has_array(name: &str) -> bool {
    BUILT_IN_TYPES.contains(&name) && !WITHOUT_ARRAY.contains(&name)
}

/// The number in a modifier of one number, `(10)`
fn precision(modifier: &str) -> Option<u32> {
    modifier.strip_prefix('(')?.strip_suffix(')')?.parse().ok()
}

/// The words with one space between them, and the modifier after the
/// word that it follows
fn written(words: &[&str], modifier: Option<&str>, position: usize) -> String {
    let mut text = String::new();
    for (index, word) in words.iter().enumerate() {
        if index == position
            && let Some(modifier) = modifier
        {
            text.push_str(modifier);
        }
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(word);
    }
    if position == words.len()
        && let Some(modifier) = modifier
    {
        text.push_str(modifier);
    }
    text
}

/// A collation name as PostgreSQL finds it. PostgreSQL always searches
/// pg_catalog, so `"C"` and `pg_catalog."C"` are one collation, and
/// pull writes the two forms in different places (`pg_catalog."C"` for
/// a column, `"C"` for an index column). A part that is not quoted is
/// in lowercase, so `C` is the collation `c`, which is not `"C"`.
pub(crate) fn canonical_collation(collation: &str) -> String {
    super::alter::names::name(collation)
}

/// An expression with each cast in the form that [`cast_syntax`]
/// gives, and with the type of each cast (`::type`) in the form that
/// [`canonical_type`] gives. These are the forms PostgreSQL writes:
/// `a::varchar(20)` is `(a)::character varying(20)`, and
/// `CAST(a AS TIMESTAMP WITH TIME ZONE)` is `(a)::timestamp with time
/// zone`. Text in a string literal (also an `E'...'` string and a
/// dollar-quoted string) or in a quoted name stays as it is. This
/// changes only the casts: deploy compares the remaining text as it
/// is.
pub(crate) fn canonical_casts(expression: &str) -> String {
    let expression = cast_syntax(expression);
    let mut result = String::with_capacity(expression.len());
    let mut rest = expression.as_str();
    // the character before `rest`, which tells if `E` or `$` starts a
    // string or is a part of a name
    let mut previous = None;
    while let Some(c) = rest.chars().next() {
        let after_name = previous.is_some_and(is_name_char);
        let length = match c {
            '\'' => quoted_length(rest, false),
            'e' | 'E' if !after_name && rest[1..].starts_with('\'') => {
                1 + quoted_length(&rest[1..], true)
            }
            '"' => rest[1..].find('"').map_or(rest.len(), |end| end + 2),
            '$' if !after_name => dollar_quoted_length(rest).unwrap_or(1),
            ':' if rest.starts_with("::") => {
                result.push_str("::");
                rest = &rest[2..];
                let length = cast_type_length(rest);
                result.push_str(&canonical_type(&rest[..length]));
                rest = &rest[length..];
                previous = result.chars().last();
                continue;
            }
            c => c.len_utf8(),
        };
        result.push_str(&rest[..length]);
        previous = rest[..length].chars().last();
        rest = &rest[length..];
    }
    result
}

/// An expression with each cast (`x::type` and `CAST(x AS type)`) in
/// the form that PostgreSQL writes, `(x)::type`. PostgreSQL keeps the
/// two forms as one cast, and when it writes the cast, it puts the
/// operand in parentheses. A string literal or a NULL is a constant
/// of the type of the cast, which PostgreSQL writes with no
/// parentheses (`'a'::text`, `NULL::integer`), and so is an empty
/// `ARRAY[]`. An operand that has parentheses stays as it is. An
/// expression that the grammar cannot read stays as it is.
fn cast_syntax(expression: &str) -> String {
    // the grammar reads an expression only in a statement
    let prefix = "SELECT ";
    let source = format!("{prefix}{expression}");
    let mut parser = tree_sitter::Parser::new();
    if parser
        .set_language(&tree_sitter_postgres::LANGUAGE.into())
        .is_err()
    {
        return expression.to_string();
    }
    let Some(tree) = parser.parse(&source, None) else {
        return expression.to_string();
    };
    let root = tree.root_node();
    if root.has_error() {
        return expression.to_string();
    }
    let mut result = String::with_capacity(source.len());
    write_casts(&root, &source, &mut result);
    result.split_off(prefix.len())
}

/// The text of the node with each cast in it in the form that
/// [`cast_syntax`] gives
fn write_casts(node: &Node, source: &str, result: &mut String) {
    let children: Vec<Node> = node.children(&mut node.walk()).collect();
    let kinds: Vec<&str> = children.iter().map(Node::kind).collect();
    let cast = match kinds.as_slice() {
        [_, "::", "Typename"] => Some((children[0], children[2])),
        ["kw_cast", "(", "a_expr", "kw_as", "Typename", ")"] => {
            Some((children[2], children[4]))
        }
        _ => None,
    };
    if let Some((operand, typename)) = cast {
        let parentheses = !constant_or_parenthesized(&operand);
        if parentheses {
            result.push('(');
        }
        write_casts(&operand, source, result);
        if parentheses {
            result.push(')');
        }
        result.push_str("::");
        result.push_str(typename.text(source));
        return;
    }
    let mut position = node.start_byte();
    for child in &children {
        result.push_str(&source[position..child.start_byte()]);
        write_casts(child, source, result);
        position = child.end_byte();
    }
    result.push_str(&source[position..node.end_byte()]);
}

/// Whether the operand of a cast is a string literal, a NULL, an empty
/// `ARRAY[]` or an expression in parentheses
fn constant_or_parenthesized(operand: &Node) -> bool {
    let mut node = *operand;
    loop {
        let children: Vec<Node> = node.children(&mut node.walk()).collect();
        let kinds: Vec<&str> = children.iter().map(Node::kind).collect();
        match (node.kind(), kinds.as_slice()) {
            ("AexprConst", ["Sconst" | "kw_null"]) => return true,
            ("c_expr", ["(", _, ")"]) => return true,
            ("c_expr", ["kw_array", "array_expr"]) => {
                return children[1].child_count() == 2;
            }
            (_, [_]) => node = children[0],
            _ => return false,
        }
    }
}

/// Whether the character can be in a name that is not quoted
fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// The length of the string literal at the start of the text, with its
/// quotes. A doubled quote is in the string; with `escapes`, a
/// backslash escapes the next character too.
fn quoted_length(text: &str, escapes: bool) -> usize {
    let mut chars = text.char_indices().skip(1);
    while let Some((index, c)) = chars.next() {
        match c {
            '\\' if escapes => {
                chars.next();
            }
            '\'' if text[index + 1..].starts_with('\'') => {
                chars.next();
            }
            '\'' => return index + 1,
            _ => {}
        }
    }
    text.len()
}

/// The length of the dollar-quoted string at the start of the text
/// (`$$...$$` or `$tag$...$tag$`), or none when the `$` does not start
/// one (as in the parameter `$1`)
fn dollar_quoted_length(text: &str) -> Option<usize> {
    let tag_end = text[1..].find('$')? + 2;
    let tag = &text[..tag_end];
    let name = &tag[1..tag_end - 1];
    if name.starts_with(|c: char| c.is_ascii_digit())
        || !name.chars().all(|c| c.is_alphanumeric() || c == '_')
    {
        return None;
    }
    Some(
        text[tag_end..]
            .find(tag)
            .map_or(text.len(), |end| tag_end + end + tag.len()),
    )
}

/// The length of the type name at the start of the text, after a `::`:
/// a name (quoted parts, or not, with a `.` between them), the words
/// that continue a type name of more than one word (`double
/// precision`, `character varying`, `timestamp with time zone`,
/// `interval day to second`), a modifier, and array bounds or `ARRAY`
fn cast_type_length(text: &str) -> usize {
    let mut length = name_length(text);
    if length == 0 {
        return 0;
    }
    let first = text[..length].to_ascii_lowercase();
    let first = first.strip_prefix("pg_catalog.").unwrap_or(&first);
    let mut previous = first.to_string();
    let mut words = 1;
    let mut modifier = false;
    loop {
        let rest = &text[length..];
        let spaces = rest.len() - rest.trim_start().len();
        let next = &rest[spaces..];
        if next.starts_with('(') && !modifier {
            let Some(end) = modifier_length(next) else {
                break;
            };
            length += spaces + end;
            modifier = true;
            continue;
        }
        if next.starts_with('[') {
            let Some(end) = next.find(']') else {
                break;
            };
            length += spaces + end + 1;
            previous = String::from("]");
            continue;
        }
        let word_length = name_length(next);
        if spaces == 0 || word_length == 0 {
            break;
        }
        let word = next[..word_length].to_ascii_lowercase();
        let continues = match (previous.as_str(), word.as_str()) {
            ("]", _) => false,
            (_, "array") => true,
            ("double", "precision") => true,
            ("national", "character" | "char") => true,
            ("character" | "char" | "nchar" | "bit", "varying") => true,
            ("timestamp" | "time", "with" | "without") if words == 1 => true,
            ("with" | "without", "time") => true,
            ("time", "zone") => words > 1,
            (
                "interval" | "year" | "month" | "day" | "hour" | "minute"
                | "to",
                "year" | "month" | "day" | "hour" | "minute" | "second" | "to",
            ) => first == "interval",
            _ => false,
        };
        if !continues {
            break;
        }
        length += spaces + word_length;
        previous = word;
        words += 1;
    }
    length
}

/// The length of a name at the start of the text: quoted parts, or
/// not, with a `.` between them
fn name_length(text: &str) -> usize {
    let mut length = 0;
    while let Some(c) = text[length..].chars().next() {
        if c == '"' {
            length += text[length + 1..]
                .find('"')
                .map_or(text.len() - length, |end| end + 2);
        } else if is_name_char(c) || c == '.' {
            length += c.len_utf8();
        } else {
            break;
        }
    }
    length
}

/// The length of the modifier at the start of the text, to its `)`
fn modifier_length(text: &str) -> Option<usize> {
    let mut depth = 0;
    let mut quote = None;
    for (index, c) in text.char_indices() {
        match (quote, c) {
            (Some(open), c) if c == open => quote = None,
            (Some(_), _) => {}
            (None, '"' | '\'') => quote = Some(c),
            (None, '(') => depth += 1,
            (None, ')') => {
                depth -= 1;
                if depth == 0 {
                    return Some(index + 1);
                }
            }
            _ => {}
        }
    }
    None
}

/// A return type as PostgreSQL keeps it: [`identity_type`], as a
/// return type has no typmod either. The type of each column of a
/// `TABLE(...)` return type is an [`identity_type`] too: PostgreSQL
/// keeps the columns as OUT parameters.
pub(crate) fn return_type(returns: &str) -> String {
    let canonical = canonical_type(returns);
    match canonical
        .strip_prefix("table(")
        .and_then(|columns| columns.strip_suffix(')'))
    {
        Some(columns) => {
            let columns: Vec<String> = split_columns(columns)
                .into_iter()
                .map(|column| {
                    let length = name_length(column);
                    format!(
                        "{} {}",
                        &column[..length],
                        identity_type(&column[length..])
                    )
                })
                .collect();
            format!("table({})", columns.join(", "))
        }
        None => identity_type(&canonical),
    }
}

/// The columns of a `TABLE(...)` return type, split at each comma that
/// is not in parentheses or quotes
fn split_columns(columns: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0usize;
    let mut quoted = false;
    let mut start = 0;
    for (index, c) in columns.char_indices() {
        match c {
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => depth = depth.saturating_sub(1),
            ',' if !quoted && depth == 0 => {
                result.push(columns[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    result.push(columns[start..].trim());
    result
}

/// A type as it identifies an argument: [`canonical_type`] without
/// its modifiers. PostgreSQL does not keep a typmod in an argument
/// type, so `varchar(10)` and `varchar` give the same aggregate or
/// operator. A modifier in a quoted name is kept. With no typmod,
/// `bpchar` is `character`, `"bit"` is `bit`, and an interval has no
/// fields (`interval day` is `interval`): PostgreSQL writes an
/// argument type in that form.
pub(crate) fn identity_type(data_type: &str) -> String {
    let mut quoted = false;
    let mut depth = 0usize;
    let mut result = String::new();
    for c in canonical_type(data_type).chars() {
        if c == '"' && depth == 0 {
            quoted = !quoted;
        }
        if !quoted && c == '(' {
            depth += 1;
        } else if !quoted && c == ')' && depth > 0 {
            depth -= 1;
        } else if depth == 0 {
            result.push(c);
        }
    }
    let result = result.trim_end();
    // `SETOF` before a return type stays, and the type after it is
    // made the same as an argument type
    let (setof, rest) = match result.strip_prefix("setof ") {
        Some(rest) => ("setof ", rest),
        None => ("", result),
    };
    let (base, array) = match rest.strip_suffix("[]") {
        Some(base) => (base, "[]"),
        None => (rest, ""),
    };
    match base {
        "bpchar" => format!("{setof}character{array}"),
        "\"bit\"" => format!("{setof}bit{array}"),
        _ if base.starts_with("interval ") => {
            format!("{setof}interval{array}")
        }
        _ => result.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models;

    fn table(name: &str, comment: Option<&str>) -> models::Table {
        let mut value = serde_json::json!({
            "name": name,
            "schema": "test",
            "owner": "postgres",
        });
        if let Some(comment) = comment {
            value["comment"] = comment.into();
        }
        serde_json::from_value(value).expect("table deserializes")
    }

    #[test]
    fn canonicalizes_type_aliases() {
        assert_eq!(canonical_type("int4"), "integer");
        assert_eq!(canonical_type("varchar"), "character varying");
        assert_eq!(
            canonical_type("timestamptz[]"),
            "timestamp with time \
            zone[]"
        );
        assert_eq!(canonical_type("uuid"), "uuid");
    }

    #[test]
    fn canonicalizes_type_case_outside_quotes() {
        assert_eq!(canonical_type("TEXT"), "text");
        assert_eq!(canonical_type("INT"), "integer");
        assert_eq!(canonical_type("VARCHAR(20)[]"), "character varying(20)[]");
        assert_eq!(
            canonical_type("TIMESTAMP WITH TIME ZONE"),
            "timestamp with time zone"
        );
        assert_eq!(canonical_type("Public.\"Mood\""), "public.\"Mood\"");
    }

    #[test]
    fn canonicalizes_types_qualified_with_pg_catalog() {
        assert_eq!(canonical_type("pg_catalog.int4"), "integer");
        assert_eq!(canonical_type("PG_CATALOG.TEXT[]"), "text[]");
        assert_eq!(
            canonical_type("\"pg_catalog\".varchar(10)"),
            "character varying(10)"
        );
        assert_eq!(canonical_type("pg_catalog.char"), "\"char\"");
        assert_eq!(canonical_type("pg_catalog.\"char\""), "\"char\"");
        assert_eq!(canonical_type("public.int4"), "public.int4");
        assert_eq!(identity_type("pg_catalog.INT4"), "integer");
    }

    /// Each hand-written form against the form that `format_type`
    /// gives on PostgreSQL 18 for a column of that type
    fn same_type(written: &str, stored: &str) {
        assert_eq!(canonical_type(written), stored, "{written}");
        assert_eq!(canonical_type(stored), stored, "{stored}");
    }

    #[test]
    fn canonicalizes_float_by_precision() {
        same_type("float", "double precision");
        same_type("FLOAT", "double precision");
        same_type("float(1)", "real");
        same_type("float(24)", "real");
        same_type("float (10)", "real");
        same_type("float(25)", "double precision");
        same_type("float(53)", "double precision");
        same_type("float(10)[]", "real[]");
        same_type("double  precision", "double precision");
        // not types: PostgreSQL refuses them, so they stay as written
        assert_eq!(canonical_type("float(0)"), "float(0)");
        assert_eq!(canonical_type("float(54)"), "float(54)");
        assert_eq!(canonical_type("double"), "double");
    }

    #[test]
    fn canonicalizes_character_and_bit_lengths() {
        same_type("char", "character(1)");
        same_type("CHAR", "character(1)");
        same_type("character", "character(1)");
        same_type("nchar", "character(1)");
        same_type("national character", "character(1)");
        same_type("national char(4)", "character(4)");
        same_type("char[]", "character(1)[]");
        same_type("bpchar(5)", "character(5)");
        same_type("bpchar", "bpchar");
        same_type("pg_catalog.bpchar", "bpchar");
        same_type("varchar", "character varying");
        same_type("char varying(5)", "character varying(5)");
        same_type("nchar varying", "character varying");
        same_type("national character varying(7)", "character varying(7)");
        same_type("character  varying(7)", "character varying(7)");
        same_type("bit", "bit(1)");
        same_type("bit(3)", "bit(3)");
        same_type("pg_catalog.bit(3)", "bit(3)");
        same_type("pg_catalog.bit", "\"bit\"");
        same_type("\"bit\"", "\"bit\"");
        same_type("bit varying", "bit varying");
        same_type("varbit", "bit varying");
        same_type("varbit(4)", "bit varying(4)");
        same_type("\"char\"", "\"char\"");
    }

    #[test]
    fn canonicalizes_date_and_time_types() {
        same_type("timestamp", "timestamp without time zone");
        same_type("timestamp(3)", "timestamp(3) without time zone");
        same_type(
            "TIMESTAMP(3) WITHOUT TIME ZONE",
            "timestamp(3) without time zone",
        );
        same_type("TIMESTAMP WITH TIME ZONE", "timestamp with time zone");
        same_type(
            "timestamp (3) with time zone",
            "timestamp(3) with time zone",
        );
        same_type("timestamp(3)with time zone", "timestamp(3) with time zone");
        same_type("timestamptz(3)", "timestamp(3) with time zone");
        same_type("timestamptz(3)[]", "timestamp(3) with time zone[]");
        same_type("pg_catalog.timestamp(3)", "timestamp(3) without time zone");
        same_type("time", "time without time zone");
        same_type("time(2)", "time(2) without time zone");
        same_type("time   with time zone", "time with time zone");
        same_type("timetz", "time with time zone");
        same_type("timetz(0)", "time(0) with time zone");
        same_type("pg_catalog.timetz(0)", "time(0) with time zone");
        same_type("interval", "interval");
        same_type("interval(3)", "interval(3)");
        same_type("interval day", "interval day");
        same_type("interval day to second (3)", "interval day to second(3)");
        same_type("INTERVAL HOUR TO MINUTE", "interval hour to minute");
    }

    #[test]
    fn canonicalizes_numeric_modifiers() {
        same_type("decimal", "numeric");
        same_type("dec", "numeric");
        same_type("decimal(10, 2)", "numeric(10,2)");
        same_type("numeric( 10 , 2 )", "numeric(10,2)");
        same_type("dec(5,1)", "numeric(5,1)");
        same_type("decimal(10)", "numeric(10,0)");
        same_type("numeric(10, -2)", "numeric(10,-2)");
        same_type("int2", "smallint");
        same_type("int8", "bigint");
        same_type("bool", "boolean");
    }

    #[test]
    fn canonicalizes_array_forms() {
        same_type("int4[]", "integer[]");
        same_type("int4 []", "integer[]");
        same_type("int[3]", "integer[]");
        same_type("int[3][4]", "integer[]");
        same_type("int[][]", "integer[]");
        same_type("integer ARRAY", "integer[]");
        same_type("int4 ARRAY[ 3 ]", "integer[]");
        same_type("_int4", "integer[]");
        same_type("pg_catalog._int4", "integer[]");
        same_type("_varchar", "character varying[]");
        same_type("_bpchar", "bpchar[]");
        same_type("varchar(20)[]", "character varying(20)[]");
    }

    /// Each form against the form that `format_type` gives on
    /// PostgreSQL 18 for a column of that type. A quoted name is a
    /// built-in type when pg_catalog has a type of that name.
    #[test]
    fn canonicalizes_quoted_built_in_names() {
        same_type("\"int4\"", "integer");
        same_type("\"varchar\"", "character varying");
        same_type("\"varchar\"(10)", "character varying(10)");
        same_type("\"_int4\"", "integer[]");
        same_type("\"bpchar\"", "bpchar");
        same_type("\"bpchar\"(3)", "character(3)");
        same_type("\"bit\"(3)", "bit(3)");
        same_type("\"numeric\"(10)", "numeric(10,0)");
        same_type("\"timestamptz\"(3)", "timestamp(3) with time zone");
        same_type("\"timestamp\"", "timestamp without time zone");
        same_type("\"time\"(2)", "time(2) without time zone");
        same_type("\"timetz\"", "time with time zone");
        same_type("\"float8\"", "double precision");
        same_type("\"bool\"[]", "boolean[]");
        same_type("\"text\"", "text");
        same_type("\"_char\"", "\"char\"[]");
        same_type("\"_varchar\"(5)", "character varying(5)[]");
        same_type("pg_catalog.\"int4\"", "integer");
        same_type("\"pg_catalog\".\"_int4\"", "integer[]");
        same_type("\"int2vector\"", "int2vector");
        same_type("\"any\"", "\"any\"");
        same_type("pg_catalog.any", "\"any\"");
        // not built-in type names: PostgreSQL 18 refuses them, or finds
        // a type of the project, so they stay as written
        assert_eq!(canonical_type("\"integer\""), "\"integer\"");
        assert_eq!(canonical_type("\"INT4\""), "\"INT4\"");
        assert_eq!(
            canonical_type("\"double precision\""),
            "\"double precision\""
        );
        assert_eq!(canonical_type("public.\"int4\""), "public.\"int4\"");
        assert_eq!(identity_type("\"int4\""), "integer");
        assert_eq!(identity_type("\"varchar\"(10)"), "character varying");
    }

    /// The array type name of a built-in type is `_` and the name of
    /// the element type, and a typmod applies to the element
    #[test]
    fn canonicalizes_built_in_array_names() {
        same_type("_text", "text[]");
        same_type("_uuid", "uuid[]");
        same_type("_TEXT", "text[]");
        same_type("_jsonb", "jsonb[]");
        same_type("_int4range", "int4range[]");
        same_type("_int4multirange", "int4multirange[]");
        same_type("_timestamptz", "timestamp with time zone[]");
        same_type("_timestamp(3)", "timestamp(3) without time zone[]");
        same_type("_varchar(4)", "character varying(4)[]");
        same_type("_numeric(10)", "numeric(10,0)[]");
        same_type("_bit", "\"bit\"[]");
        same_type("_oidvector", "oidvector[]");
        same_type("_record", "record[]");
        same_type("pg_catalog._text", "text[]");
        // a type with no array type, and a type of the project
        assert_eq!(canonical_type("_void"), "_void");
        assert_eq!(canonical_type("_trigger"), "_trigger");
        assert_eq!(canonical_type("_x"), "_x");
        assert_eq!(canonical_type("public._text"), "public._text");
        assert_eq!(canonical_type("test._uuid"), "test._uuid");
        assert_eq!(identity_type("_varchar(4)"), "character varying[]");
    }

    #[test]
    fn keeps_user_defined_and_quoted_types() {
        assert_eq!(canonical_type("public.int4"), "public.int4");
        assert_eq!(canonical_type("public._int4"), "public._int4");
        assert_eq!(canonical_type("_mood"), "_mood");
        assert_eq!(canonical_type("\"Mood\"[]"), "\"Mood\"[]");
        assert_eq!(
            canonical_type("public.\"My  Type\"(a, b)"),
            "public.\"My  Type\"(a,b)"
        );
    }

    #[test]
    fn canonicalizes_setof_return_types() {
        same_type("SETOF int4", "setof integer");
        same_type("setof varchar(3)[]", "setof character varying(3)[]");
    }

    /// A real change of type is still a change
    #[test]
    fn different_types_stay_different() {
        let different = |a: &str, b: &str| {
            assert_ne!(canonical_type(a), canonical_type(b), "{a} = {b}")
        };
        different("varchar(10)", "varchar(20)");
        different("float(24)", "float(25)");
        different("char", "char(2)");
        different("char", "bpchar");
        different("char", "\"char\"");
        different("bit", "bit varying");
        different("bit", "pg_catalog.bit");
        different("timestamp", "timestamptz");
        different("time(2)", "time(3)");
        different("interval", "interval day");
        different("numeric(10)", "numeric(10,2)");
        different("int4", "int4[]");
        different("SETOF int4", "int4");
    }

    #[test]
    fn identity_type_is_the_argument_type() {
        // PostgreSQL 18 writes an argument type with no typmod, so
        // `character` there is any length, as `bpchar` is
        assert_eq!(identity_type("char(3)"), "character");
        assert_eq!(identity_type("char"), "character");
        assert_eq!(identity_type("bpchar"), "character");
        assert_eq!(identity_type("_bpchar"), "character[]");
        assert_eq!(identity_type("\"bit\""), "bit");
        assert_eq!(identity_type("bit(3)"), "bit");
        assert_eq!(identity_type("float(10)"), "real");
        assert_eq!(
            identity_type("timestamptz(3)"),
            "timestamp with time zone"
        );
        assert_eq!(identity_type("int[3]"), "integer[]");
        // the fields of an interval are its typmod
        assert_eq!(identity_type("interval day to second(3)"), "interval");
        assert_eq!(identity_type("INTERVAL HOUR[]"), "interval[]");
    }

    #[test]
    fn owner_differences_are_ignored() {
        let mut a = table("users", None);
        let mut b = table("users", None);
        a.owner = String::from("postgres");
        b.owner = String::from("app");
        assert_eq!(
            normalized(&Definition::Table(a)),
            normalized(&Definition::Table(b))
        );
    }

    #[test]
    fn comment_differences_are_changes() {
        let a = table("users", Some("Users"));
        let b = table("users", Some("User records"));
        assert_ne!(
            normalized(&Definition::Table(a)),
            normalized(&Definition::Table(b))
        );
    }

    #[test]
    fn canonicalizes_cast_types_only() {
        assert_eq!(
            canonical_casts("(a)::varchar(20)"),
            "(a)::character varying(20)"
        );
        assert_eq!(
            canonical_casts("((a)::INT4 + (b)::pg_catalog.int8)"),
            "((a)::integer + (b)::bigint)"
        );
        assert_eq!(canonical_casts("(a)::bool[]"), "(a)::boolean[]");
        assert_eq!(
            canonical_casts("(a)::Public.\"My Type\""),
            "(a)::public.\"My Type\""
        );
        // a string literal with a doubled quote, and a quoted name
        assert_eq!(
            canonical_casts("('it''s::int4'::text || \"x::int4\")"),
            "('it''s::int4'::text || \"x::int4\")"
        );
        // the other text keeps its case
        assert_eq!(canonical_casts("LOWER((a)::TEXT)"), "LOWER((a)::text)");
    }

    /// Each hand-written cast against the form that `pg_get_expr`
    /// gives on PostgreSQL 18
    #[test]
    fn canonicalizes_cast_type_names_of_more_than_one_word() {
        let same = |written: &str, stored: &str| {
            assert_eq!(canonical_casts(written), stored, "{written}");
            assert_eq!(canonical_casts(stored), stored, "{stored}");
        };
        same(
            "((a)::TIMESTAMP WITH TIME ZONE IS NOT NULL)",
            "((a)::timestamp with time zone IS NOT NULL)",
        );
        same(
            "((a)::timestamptz(3) IS NOT NULL)",
            "((a)::timestamp(3) with time zone IS NOT NULL)",
        );
        same(
            "(a)::timestamp AT TIME ZONE 'UTC'",
            "(a)::timestamp without time zone AT TIME ZONE 'UTC'",
        );
        same("((a)::DOUBLE PRECISION + 1)", "((a)::double precision + 1)");
        same("(a)::float(10)", "(a)::real");
        same("(a)::float", "(a)::double precision");
        same("(a)::character varying(20)", "(a)::character varying(20)");
        same("(a)::national char varying(4)", "(a)::character varying(4)");
        same("(a)::char", "(a)::character(1)");
        same("(a)::bit", "(a)::bit(1)");
        same("(a)::\"bit\"", "(a)::\"bit\"");
        same("(a)::decimal(10, 2)", "(a)::numeric(10,2)");
        same(
            "(a)::interval day to second(3)",
            "(a)::interval day to second(3)",
        );
        same(
            "(a)::INTERVAL HOUR TO MINUTE",
            "(a)::interval hour to minute",
        );
        same("(a)::int[3]", "(a)::integer[]");
        same("(a)::integer ARRAY", "(a)::integer[]");
        same("(a)::_int4", "(a)::integer[]");
        same(
            "((a)::varchar(3)[] IS NULL)",
            "((a)::character varying(3)[] IS NULL)",
        );
    }

    /// Each hand-written cast syntax against the form that
    /// `pg_get_constraintdef` gives on PostgreSQL 18
    #[test]
    fn canonicalizes_cast_syntax() {
        let same = |written: &str, stored: &str| {
            assert_eq!(canonical_casts(written), stored, "{written}");
            assert_eq!(canonical_casts(stored), stored, "{stored}");
        };
        same("(i::int > 0)", "((i)::integer > 0)");
        same("(CAST(i AS integer) > 0)", "((i)::integer > 0)");
        same("(cast ( i as int ) > 0)", "((i)::integer > 0)");
        same("(i :: int > 0)", "((i)::integer > 0)");
        same("((i::int4 + 1) > 0)", "(((i)::integer + 1) > 0)");
        same("(- i::int < 0)", "(- (i)::integer < 0)");
        same("(new.i::int > 0)", "((new.i)::integer > 0)");
        same("(\"I\"::int > 0)", "((\"I\")::integer > 0)");
        same("(abs(i)::int > 0)", "((abs(i))::integer > 0)");
        same("(abs(i::int) > 0)", "(abs((i)::integer) > 0)");
        same(
            "(now()::date > '2020-01-01'::date)",
            "((now())::date > '2020-01-01'::date)",
        );
        same("(i::int::bigint > 0)", "(((i)::integer)::bigint > 0)");
        same(
            "(CAST(CAST(i AS int) AS bigint) > 0)",
            "(((i)::integer)::bigint > 0)",
        );
        same("(i::int)::bigint", "((i)::integer)::bigint");
        same("((x).y::int > 0)", "(((x).y)::integer > 0)");
        // an operand in parentheses stays as it is (PostgreSQL writes
        // `((j ->> 'k'::text))::integer`)
        same("(j->>'k')::int", "(j->>'k')::integer");
        same("(a <> ARRAY[i::int])", "(a <> ARRAY[(i)::integer])");
        same("(a::bigint[] <> '{}')", "((a)::bigint[] <> '{}')");
        same("(i::numeric(10,2) > 0)", "((i)::numeric(10,2) > 0)");
        // a constant that is not a string literal or a NULL
        same("1::bigint", "(1)::bigint");
        same("CAST(2 AS bigint)", "(2)::bigint");
        same("(true::text <> t)", "((true)::text <> t)");
        // a string literal, a NULL and an empty array stay as they are
        same("(t <> 'a'::text)", "(t <> 'a'::text)");
        same("(t <> CAST('a' AS text))", "(t <> 'a'::text)");
        same("(t <> E'a'::text)", "(t <> E'a'::text)");
        same("'x'::varchar", "'x'::character varying");
        same("CAST(NULL AS int)", "NULL::integer");
        same("ARRAY[]::int[]", "ARRAY[]::integer[]");
        same(
            "('a'::text::varchar <> t)",
            "(('a'::text)::character varying <> t)",
        );
        // text that the grammar cannot read stays as it is
        assert_eq!(canonical_casts("(i::INT4 >"), "(i::integer >");
    }

    #[test]
    fn cast_types_in_escape_and_dollar_strings_stay() {
        assert_eq!(
            canonical_casts("(E'it\\'s::int4' || (a)::INT4)"),
            "(E'it\\'s::int4' || (a)::integer)"
        );
        assert_eq!(
            canonical_casts("(e'\\\\'::text || 'x::int4')"),
            "(e'\\\\'::text || 'x::int4')"
        );
        assert_eq!(
            canonical_casts("($$x::int4$$ || $t$y::int4$t$ || (a)::INT4)"),
            "($$x::int4$$ || $t$y::int4$t$ || (a)::integer)"
        );
        // `$` in a name does not start a string
        assert_eq!(canonical_casts("(a$b)::INT4"), "(a$b)::integer");
    }

    #[test]
    fn materialized_view_written_forms_are_not_a_change() {
        let view = |fillfactor: Value, expression: &str| {
            Definition::MaterializedView(
                serde_json::from_value(serde_json::json!({
                    "name": "m", "schema": "test", "owner": "postgres",
                    "query": "SELECT 1 AS n",
                    "storage_parameters": {"fillfactor": fillfactor},
                    "indexes": [{
                        "name": "m_n",
                        "columns": [{"expression": expression}],
                        "storage_parameters": {"fillfactor": fillfactor},
                    }],
                }))
                .unwrap(),
            )
        };
        assert_eq!(
            normalized(&view(90.into(), "(n)::int8")),
            normalized(&view("90".into(), "(n)::bigint"))
        );
        assert_ne!(
            normalized(&view(70.into(), "(n)::bigint")),
            normalized(&view("90".into(), "(n)::bigint"))
        );
    }

    #[test]
    fn collation_without_pg_catalog_is_not_a_change() {
        let domain = |collation: &str| {
            Definition::Domain(
                serde_json::from_value(serde_json::json!({
                    "name": "d", "schema": "test", "owner": "postgres",
                    "data_type": "text", "collation": collation,
                }))
                .unwrap(),
            )
        };
        assert_eq!(
            normalized(&domain("\"C\"")),
            normalized(&domain("pg_catalog.\"C\""))
        );
        assert_ne!(
            normalized(&domain("\"C\"")),
            normalized(&domain("\"POSIX\""))
        );
    }

    #[test]
    fn object_key_display() {
        let key = ObjectKey {
            desc: ObjectType::Table,
            schema: String::from("test"),
            name: String::from("users"),
        };
        assert_eq!(key.to_string(), "TABLE test.users");
        let key = ObjectKey {
            desc: ObjectType::Schema,
            schema: String::new(),
            name: String::from("test"),
        };
        assert_eq!(key.to_string(), "SCHEMA test");
    }

    #[test]
    fn aggregate_key_keeps_input_types() {
        let aggregate = |data_type: &str| {
            Definition::Aggregate(
                serde_json::from_value(serde_json::json!({
                    "name": "agg",
                    "schema": "test",
                    "owner": "postgres",
                    "arguments": [{"data_type": data_type}],
                    "sfunc": "f",
                    "state_data_type": "integer",
                }))
                .unwrap(),
            )
        };
        let key =
            |d: &str| ObjectKey::new(ObjectType::Aggregate, &aggregate(d));
        // one overload does not stand for another
        assert_ne!(key("integer"), key("text"));
        // a type alias is the same input type
        assert_eq!(key("int4"), key("integer"));
        // a typmod is not part of the input type
        assert_eq!(key("varchar(10)"), key("character varying"));
    }

    #[test]
    fn identity_type_removes_typmods() {
        assert_eq!(identity_type("varchar(10)[]"), "character varying[]");
        assert_eq!(identity_type("NUMERIC(10, 2)"), "numeric");
        assert_eq!(
            identity_type("timestamp(3) with time zone"),
            "timestamp with time zone"
        );
        assert_eq!(identity_type("public.\"a(b)\""), "public.\"a(b)\"");
    }

    /// PostgreSQL 18 writes `SELECT 'x'::text` for `SELECT 'x'`, and
    /// `SELECT 'x'::text AS text` for the routine that deploy makes
    /// from that body
    #[test]
    fn routine_body_literal_names_are_not_a_change() {
        let body = |select: &str| format!("BEGIN ATOMIC\n {select};\nEND");
        let f = |select: &str| {
            function(serde_json::json!({
                "name": "f", "schema": "test", "owner": "postgres",
                "language": "sql", "returns": "text",
                "sql_body": body(select),
            }))
        };
        assert_eq!(
            normalized(&f("SELECT 'e\\f'::text AS text")),
            normalized(&f("SELECT 'e\\f'::text"))
        );
        assert_ne!(
            normalized(&f("SELECT 'g'::text AS text")),
            normalized(&f("SELECT 'e\\f'::text"))
        );
        let p = |select: &str| {
            Definition::Procedure(
                serde_json::from_value(serde_json::json!({
                    "name": "p", "schema": "test", "owner": "postgres",
                    "language": "sql", "sql_body": body(select),
                }))
                .unwrap(),
            )
        };
        assert_eq!(
            normalized(&p("SELECT 'x'::text AS text")),
            normalized(&p("SELECT 'x'::text"))
        );
        assert_ne!(
            normalized(&p("SELECT 'y'::text AS text")),
            normalized(&p("SELECT 'x'::text"))
        );
    }

    /// What PostgreSQL 18 stores for a NULL default (the cases are from
    /// a PostgreSQL 18 database)
    #[test]
    fn null_defaults_are_in_the_stored_form() {
        let mut types = UserTypes::new();
        for (name, user_type) in [
            ("test.dint", UserType::Domain("integer".into())),
            ("test.dvc", UserType::Domain("varchar(10)".into())),
            ("test.ddint", UserType::Domain("test.dint".into())),
            ("test.div", UserType::Domain("interval(2)".into())),
            ("test.dia", UserType::Domain("int[]".into())),
            ("test.den", UserType::Domain("test.mood".into())),
            ("test.mood", UserType::Other),
            ("test.pair", UserType::Other),
        ] {
            types.insert(name.into(), user_type);
        }
        for (data_type, default, stored) in [
            ("integer", "NULL", None),
            ("integer", "null", None),
            ("integer", "(NULL)", None),
            ("int", "NULL::int", None),
            ("integer", "NULL::INT4", None),
            ("integer", "NULL::pg_catalog.int4", None),
            ("integer", "CAST(NULL AS int)", None),
            ("integer", "(NULL)::integer", None),
            ("integer", "((NULL)::integer)", None),
            ("integer", "NULL::integer::integer", None),
            ("integer", "CAST(NULL AS integer)::integer", None),
            ("bigint", "NULL::int8", None),
            ("numeric", "NULL::numeric", None),
            ("varchar", "NULL::character varying", None),
            ("text", "NULL::text", None),
            ("timestamptz", "NULL::timestamp with time zone", None),
            ("integer[]", "NULL::int[]", None),
            ("double precision", "NULL::float8", None),
            ("bpchar", "NULL", None),
            ("\"char\"", "NULL", None),
            ("interval(2)", "NULL", None),
            ("interval(2)", "NULL::interval(2)", None),
            ("interval year", "NULL", None),
            ("varchar(10)", "NULL", Some("NULL::character varying")),
            (
                "varchar(10)",
                "NULL::varchar",
                Some("NULL::character varying"),
            ),
            (
                "varchar(10)",
                "CAST(NULL AS varchar)",
                Some("NULL::character varying"),
            ),
            (
                "varchar(10)",
                "NULL::character varying",
                Some("NULL::character varying"),
            ),
            ("numeric(5,2)", "NULL", Some("NULL::numeric")),
            ("char", "NULL", Some("NULL::bpchar")),
            ("character(3)", "NULL::bpchar", Some("NULL::bpchar")),
            ("bit", "NULL", Some("NULL::\"bit\"")),
            ("bit varying(4)", "NULL", Some("NULL::bit varying")),
            (
                "timestamp(3)",
                "NULL",
                Some("NULL::timestamp without time zone"),
            ),
            (
                "timestamptz(3)",
                "NULL",
                Some("NULL::timestamp with time zone"),
            ),
            ("timetz(2)", "NULL", Some("NULL::time with time zone")),
            ("varchar(10)[]", "NULL", Some("NULL::character varying[]")),
            ("numeric(5,2)[]", "NULL", Some("NULL::numeric[]")),
            ("interval(2)[]", "NULL", Some("NULL::interval[]")),
            ("char[]", "NULL", Some("NULL::bpchar[]")),
            ("bit(3)[]", "NULL", Some("NULL::\"bit\"[]")),
            ("test.dint", "NULL", Some("NULL::integer")),
            ("test.dint", "NULL::integer", Some("NULL::integer")),
            (
                "test.dint",
                "NULL::test.dint",
                Some("(NULL::integer)::test.dint"),
            ),
            (
                "test.dint",
                "CAST(NULL AS test.dint)",
                Some("(NULL::integer)::test.dint"),
            ),
            (
                "test.dint",
                "(NULL::integer)::test.dint",
                Some("(NULL::integer)::test.dint"),
            ),
            ("test.dvc", "NULL", Some("NULL::character varying")),
            (
                "test.dvc",
                "NULL::test.dvc",
                Some("(NULL::character varying)::test.dvc"),
            ),
            ("test.ddint", "NULL", Some("NULL::integer")),
            (
                "test.ddint",
                "NULL::test.ddint",
                Some("(NULL::integer)::test.ddint"),
            ),
            ("test.div", "NULL", Some("NULL::interval(2)")),
            ("test.dia", "NULL", Some("NULL::integer[]")),
            ("test.den", "NULL", Some("NULL::test.mood")),
            ("test.dint[]", "NULL", None),
            ("test.mood", "NULL", None),
            ("test.mood", "NULL::test.mood", None),
            ("test.mood[]", "NULL", None),
            ("test.pair", "NULL", None),
        ] {
            assert_eq!(
                stored_null_default(data_type, default, &types),
                Some(stored.map(String::from)),
                "{data_type} {default}"
            );
        }
        for (data_type, default) in [
            ("integer", "NULL::bigint"),
            ("integer", "NULL::int2"),
            ("integer", "NULL::text::integer"),
            ("integer", "-NULL::integer"),
            ("integer", "0"),
            ("integer", "'NULL'"),
            ("varchar", "NULL::text"),
            ("varchar(10)", "NULL::text"),
            ("varchar(10)", "NULL::varchar(10)"),
            ("numeric(5,2)", "NULL::numeric(5,2)"),
            ("char(3)", "NULL::char"),
            ("interval(2)", "NULL::interval"),
            ("test.dint", "NULL::bigint"),
            ("test.ddint", "NULL::test.dint"),
            // a type that is not in the project
            ("test.other", "NULL"),
            ("test.other[]", "NULL"),
        ] {
            assert_eq!(
                stored_null_default(data_type, default, &types),
                None,
                "{data_type} {default}"
            );
        }
        // with no types, only a built-in type is known
        assert_eq!(
            stored_null_default("test.mood", "NULL", &UserTypes::new()),
            None
        );
    }

    /// A NULL default of a table, of an inherited column and of a
    /// domain of the project is in the form that PostgreSQL stores
    #[test]
    fn project_null_defaults_are_stored() {
        let definition = |desc, value: Value| crate::models::Item {
            id: 0,
            desc,
            definition: match desc {
                ObjectType::Domain => {
                    Definition::Domain(serde_json::from_value(value).unwrap())
                }
                ObjectType::Type => {
                    Definition::Type(serde_json::from_value(value).unwrap())
                }
                _ => Definition::Table(serde_json::from_value(value).unwrap()),
            },
            dependencies: Default::default(),
        };
        let mut project = Project {
            name: String::from("test"),
            superuser: String::from("postgres"),
            default_schema: String::from("public"),
            path: std::path::PathBuf::new(),
            inventory: vec![
                definition(
                    ObjectType::Domain,
                    serde_json::json!({
                        "name": "dint", "schema": "test", "owner": "o",
                        "data_type": "integer",
                    }),
                ),
                definition(
                    ObjectType::Domain,
                    serde_json::json!({
                        "name": "ddint", "schema": "test", "owner": "o",
                        "data_type": "test.dint", "default": "NULL",
                    }),
                ),
                definition(
                    ObjectType::Type,
                    serde_json::json!({
                        "name": "mood", "schema": "test", "owner": "o",
                        "type": "enum", "enum": ["a"],
                    }),
                ),
                definition(
                    ObjectType::Table,
                    serde_json::json!({
                        "name": "parent", "schema": "test", "owner": "o",
                        "columns": [
                            {"name": "v", "data_type": "varchar(10)"},
                            {"name": "i", "data_type": "integer"},
                        ],
                    }),
                ),
                definition(
                    ObjectType::Table,
                    serde_json::json!({
                        "name": "child", "schema": "test", "owner": "o",
                        "parents": ["test.parent"],
                        "columns": [
                            {"name": "d", "data_type": "test.dint",
                             "default": "NULL"},
                            {"name": "m", "data_type": "test.mood",
                             "default": "NULL::test.mood"},
                        ],
                        "column_defaults": [
                            {"column": "v", "default": "NULL"},
                            {"column": "i", "default": "NULL"},
                        ],
                    }),
                ),
                definition(
                    ObjectType::Table,
                    serde_json::json!({
                        "name": "grandchild", "schema": "test", "owner": "o",
                        "parents": ["test.child"],
                        "column_defaults": [{"column": "i", "default": "NULL"}],
                    }),
                ),
            ],
        };
        store_null_defaults(&mut project);
        let value = |index: usize| {
            match &project.inventory[index].definition {
                Definition::Domain(domain) => serde_json::to_value(domain),
                Definition::Table(table) => serde_json::to_value(table),
                _ => unreachable!(),
            }
            .unwrap()
        };
        assert_eq!(value(1)["default"], "NULL::integer");
        let child = value(4);
        assert_eq!(child["columns"][0]["default"], "NULL::integer");
        assert_eq!(child["columns"][1].get("default"), None);
        assert_eq!(
            child["column_defaults"],
            serde_json::json!([
                {"column": "v", "default": "NULL::character varying"},
            ])
        );
        assert_eq!(value(5).get("column_defaults"), None);
    }

    #[test]
    fn null_defaults_compare_as_no_default() {
        let table = |default: Option<&str>| {
            let mut column = serde_json::json!({
                "name": "a", "data_type": "integer",
            });
            if let Some(default) = default {
                column["default"] = default.into();
            }
            Definition::Table(
                serde_json::from_value(serde_json::json!({
                    "name": "t", "schema": "test", "owner": "postgres",
                    "columns": [column],
                }))
                .unwrap(),
            )
        };
        assert_eq!(
            normalized(&table(Some("CAST(NULL AS int)"))),
            normalized(&table(None))
        );
        assert_ne!(
            normalized(&table(Some("NULL::bigint"))),
            normalized(&table(None))
        );
        let domain = |default: Option<&str>| {
            Definition::Domain(
                serde_json::from_value(serde_json::json!({
                    "name": "d", "schema": "test", "owner": "postgres",
                    "data_type": "integer", "default": default,
                }))
                .unwrap(),
            )
        };
        assert_eq!(
            normalized(&domain(Some("NULL::int4"))),
            normalized(&domain(None))
        );
        assert_ne!(
            normalized(&domain(Some("(NULL::text)::integer"))),
            normalized(&domain(None))
        );
    }

    #[test]
    fn domain_cast_types_are_not_a_change() {
        let domain = |default: &str, check: &str| {
            Definition::Domain(
                serde_json::from_value(serde_json::json!({
                    "name": "d", "schema": "test", "owner": "postgres",
                    "data_type": "integer", "default": default,
                    "check_constraints": [
                        {"name": "d_check", "expression": check},
                    ],
                }))
                .unwrap(),
            )
        };
        let pulled = domain("(1)::smallint", "((VALUE)::bigint > 0)");
        assert_eq!(
            normalized(&domain("(1)::INT2", "((VALUE)::INT8 > 0)")),
            normalized(&pulled)
        );
        assert_ne!(
            normalized(&domain("(1)::INT2", "((VALUE)::INT4 > 0)")),
            normalized(&pulled)
        );
        assert_ne!(
            normalized(&domain("(1)::INT4", "((VALUE)::INT8 > 0)")),
            normalized(&pulled)
        );
    }

    /// PostgreSQL 18 keeps no typmod in a RETURNS TABLE column type,
    /// and pg_dump writes `TABLE(a integer, b character varying)` for
    /// `TABLE(a int4, b varchar(3))`
    #[test]
    fn return_table_columns_are_argument_types() {
        let pulled = "TABLE(a integer, b character varying, \"C\" text[])";
        assert_eq!(
            return_type("TABLE(a int4, B VARCHAR(3), \"C\" _text)"),
            return_type(pulled)
        );
        assert_eq!(
            return_type("table( a int4 ,b varchar(3) , \"C\" text ARRAY )"),
            return_type(pulled)
        );
        assert_eq!(
            return_type("TABLE(d numeric(10, 2), e timestamptz(3))"),
            return_type("TABLE(d numeric, e timestamp with time zone)")
        );
        assert_eq!(
            return_type("TABLE(\"a,b\" int4)"),
            return_type("TABLE(\"a,b\" integer)")
        );
        // a real change is still a change
        assert_ne!(
            return_type("TABLE(a int4, b text)"),
            return_type("TABLE(a integer, b character varying)")
        );
        assert_ne!(
            return_type("TABLE(a integer)"),
            return_type("TABLE(b integer)")
        );
        assert_ne!(
            return_type("TABLE(\"A\" integer)"),
            return_type("TABLE(a integer)")
        );
    }

    #[test]
    fn return_type_keeps_setof_in_the_postgresql_form() {
        // PostgreSQL 18 writes each of these as `SETOF` and the type
        // with no typmod
        assert_eq!(return_type("SETOF bpchar"), "setof character");
        assert_eq!(return_type("SETOF bpchar[]"), "setof character[]");
        assert_eq!(return_type("SETOF \"bit\""), "setof bit");
        assert_eq!(return_type("SETOF interval day"), "setof interval");
        assert_eq!(
            return_type("SETOF bpchar"),
            return_type("SETOF character")
        );
    }

    #[test]
    fn raw_cast_takes_only_its_own_types() {
        let cast = |target: &str, sql: Option<&str>| {
            let mut value = serde_json::json!({
                "schema": "test", "owner": "postgres",
                "source_type": "test.point_pair", "target_type": target,
                "inout": true,
            });
            if let Some(sql) = sql {
                value["sql"] = sql.into();
            }
            Definition::Cast(serde_json::from_value(value).unwrap())
        };
        let db = cast("text", None);
        let mut database = BTreeMap::new();
        database.insert(ObjectKey::new(ObjectType::Cast, &db), db);
        // a different cast does not stand for the raw cast
        let other = cast(
            "character varying",
            Some("CREATE CAST (test.point_pair AS varchar) WITH INOUT"),
        );
        assert!(take_raw(&mut database, ObjectType::Cast, &other).is_none());
        let raw = cast(
            "text",
            Some("CREATE CAST (test.point_pair AS text) WITH INOUT"),
        );
        assert!(take_raw(&mut database, ObjectType::Cast, &raw).is_some());
        assert!(database.is_empty());
    }

    #[test]
    fn function_returns_alias_is_not_a_change() {
        let f = |returns: &str| -> Definition {
            Definition::Function(
                serde_json::from_value(serde_json::json!({
                    "name": "f",
                    "schema": "test",
                    "owner": "postgres",
                    "returns": returns,
                    "language": "sql",
                    "definition": "SELECT 1",
                }))
                .unwrap(),
            )
        };
        // a repo `returns: int4` must not diff against the server's
        // `integer` on every deploy
        assert_eq!(normalized(&f("int4")), normalized(&f("integer")));
    }

    /// A list setting compares element by element, so the same names
    /// in another order are a change. A list of one element is the
    /// one name that pull writes as a string.
    #[test]
    fn function_list_settings_compare_by_element() {
        let f = |kind: &str, value: serde_json::Value| -> Value {
            let mut json = serde_json::json!({
                "name": "f", "schema": "test", "owner": "postgres",
                "language": "sql", "definition": "SELECT 1",
                "configuration": {"search_path": value},
            });
            if kind == "function" {
                json["returns"] = serde_json::json!("integer");
                normalized(&function(json))
            } else {
                normalized(&procedure(json))
            }
        };
        for kind in ["function", "procedure"] {
            let pulled = f(kind, serde_json::json!(["pg_catalog", "pg_temp"]));
            assert_eq!(
                f(kind, serde_json::json!(["pg_catalog", "pg_temp"])),
                pulled
            );
            assert_ne!(
                f(kind, serde_json::json!(["pg_temp", "pg_catalog"])),
                pulled
            );
            assert_ne!(f(kind, serde_json::json!(["pg_catalog"])), pulled);
            assert_eq!(
                f(kind, serde_json::json!(["pg_catalog"])),
                f(kind, serde_json::json!("pg_catalog"))
            );
        }
    }

    #[test]
    fn function_key_canonicalizes_parameter_types() {
        let repo: crate::models::Function =
            serde_json::from_value(serde_json::json!({
                "name": "f",
                "schema": "test",
                "owner": "postgres",
                "returns": "integer",
                "language": "sql",
                "parameters": [{"mode": "IN", "data_type": "int4"}],
            }))
            .unwrap();
        let server: crate::models::Function =
            serde_json::from_value(serde_json::json!({
                "name": "f",
                "schema": "test",
                "owner": "postgres",
                "returns": "integer",
                "language": "sql",
                "parameters": [{"mode": "IN", "data_type": "integer"}],
            }))
            .unwrap();
        assert_eq!(
            ObjectKey::new(ObjectType::Function, &Definition::Function(repo)),
            ObjectKey::new(
                ObjectType::Function,
                &Definition::Function(server)
            )
        );
    }

    fn function(json: serde_json::Value) -> Definition {
        Definition::Function(
            serde_json::from_value(json).expect("function deserializes"),
        )
    }

    /// PostgreSQL keeps no typmod in a parameter or a return type, and
    /// writes `bpchar` there as `character`: pg_dump writes
    /// `f(a character varying) RETURNS numeric` for `f(a varchar(10))
    /// RETURNS numeric(10,2)`
    #[test]
    fn function_types_compare_without_typmods() {
        let f = |parameter: &str, returns: &str| {
            function(serde_json::json!({
                "name": "f", "schema": "test", "owner": "postgres",
                "language": "sql", "definition": "SELECT 1",
                "parameters": [
                    {"mode": "IN", "name": "a", "data_type": parameter},
                    {"mode": "OUT", "name": "b", "data_type": parameter},
                ],
                "returns": returns,
            }))
        };
        let key = |d: &Definition| ObjectKey::new(ObjectType::Function, d);
        let pulled = f("character varying", "numeric");
        for written in
            [f("varchar(10)", "numeric(10,2)"), f("VARCHAR", "NUMERIC")]
        {
            assert_eq!(key(&written), key(&pulled));
            assert_eq!(normalized(&written), normalized(&pulled));
        }
        let pulled = f("character", "SETOF character");
        let written = f("bpchar", "SETOF bpchar(3)");
        assert_eq!(key(&written), key(&pulled));
        assert_eq!(normalized(&written), normalized(&pulled));
        // a real change of type is still a change
        let other = f("text", "numeric");
        assert_ne!(key(&other), key(&f("character varying", "numeric")));
        assert_ne!(
            normalized(&f("character varying", "integer")),
            normalized(&f("character varying", "numeric"))
        );
        assert_ne!(
            normalized(&f("text", "TABLE(a integer)")),
            normalized(&f("text", "TABLE(a text)"))
        );
    }

    #[test]
    fn raw_function_takes_its_overload_by_bare_name() {
        // a raw function has no parameter list, so its key is `f()`
        // and the database key is `f(n integer)`
        let db = function(serde_json::json!({
            "name": "f",
            "schema": "test",
            "owner": "postgres",
            "parameters": [
                {"mode": "IN", "name": "n", "data_type": "integer"}
            ],
            "returns": "integer",
            "language": "sql",
            "definition": "SELECT n",
        }));
        let raw = function(serde_json::json!({
            "name": "f",
            "schema": "test",
            "owner": "postgres",
            "sql": "CREATE FUNCTION test.f(n integer) RETURNS integer \
                    LANGUAGE sql AS $$SELECT n$$",
        }));
        assert!(raw.raw_sql());
        let mut database = BTreeMap::new();
        database.insert(ObjectKey::new(ObjectType::Function, &db), db);
        assert!(take_raw(&mut database, ObjectType::Function, &raw).is_some());
        // the database object is taken, so it is not dropped
        assert!(database.is_empty());
    }

    #[test]
    fn raw_table_takes_its_key() {
        let db = Definition::Table(table("users", None));
        let raw = Definition::Table(
            serde_json::from_value(serde_json::json!({
                "name": "users",
                "schema": "test",
                "owner": "postgres",
                "sql": "CREATE TABLE test.users (id integer)",
            }))
            .expect("table deserializes"),
        );
        let other = Definition::Table(table("users", None));
        let mut database = BTreeMap::new();
        database.insert(ObjectKey::new(ObjectType::Table, &db), db);
        assert!(take_raw(&mut database, ObjectType::Table, &raw).is_some());
        // a second lookup finds nothing: the item was a match only once
        assert!(take_raw(&mut database, ObjectType::Table, &other).is_none());
    }

    fn procedure(json: serde_json::Value) -> Definition {
        Definition::Procedure(
            serde_json::from_value(json).expect("procedure deserializes"),
        )
    }

    #[test]
    fn procedure_short_forms_are_not_a_change() {
        // as pull writes it
        let pulled = procedure(serde_json::json!({
            "name": "p",
            "schema": "test",
            "owner": "postgres",
            "parameters": [
                {"mode": "IN", "name": "label",
                 "data_type": "character varying"},
                {"mode": "INOUT", "name": "n", "data_type": "integer",
                 "default": "0"},
                {"mode": "IN", "name": "flag", "data_type": "boolean",
                 "default": "true"},
            ],
            "language": "plpgsql",
            "configuration": {
                "search_path": "test",
                "statement_timeout": "1000",
            },
            "definition": "BEGIN\n  n := 1;\nEND;",
        }));
        // as a person writes it
        let written = procedure(serde_json::json!({
            "name": "p",
            "schema": "test",
            "owner": "app",
            "parameters": [
                {"mode": "IN", "name": "label", "data_type": "VARCHAR(20)"},
                {"mode": "INOUT", "name": "n", "data_type": "int4",
                 "default": 0},
                {"mode": "IN", "name": "flag", "data_type": "BOOL",
                 "default": true},
            ],
            "language": "PLPGSQL",
            "security": "INVOKER",
            "configuration": {
                "statement_timeout": 1000,
                "Search_Path": "test",
            },
            "definition": "BEGIN\n  n := 1;\nEND;",
        }));
        assert_eq!(normalized(&written), normalized(&pulled));
        assert_eq!(
            ObjectKey::new(ObjectType::Procedure, &written),
            ObjectKey::new(ObjectType::Procedure, &pulled)
        );
        // pull writes no parameters for an empty list
        let bare = |parameters: Option<serde_json::Value>| {
            let mut value = serde_json::json!({
                "name": "q", "schema": "test", "owner": "postgres",
                "language": "sql", "sql_body": "BEGIN ATOMIC\n SELECT 1;\nEND",
            });
            if let Some(parameters) = parameters {
                value["parameters"] = parameters;
            }
            procedure(value)
        };
        assert_eq!(
            normalized(&bare(Some(serde_json::json!([])))),
            normalized(&bare(None))
        );
    }

    #[test]
    fn procedure_changes_are_changes() {
        let p = |security: &str, body: &str| {
            procedure(serde_json::json!({
                "name": "p", "schema": "test", "owner": "postgres",
                "language": "sql", "security": security,
                "definition": body,
            }))
        };
        assert_ne!(
            normalized(&p("DEFINER", "SELECT 1;")),
            normalized(&p("INVOKER", "SELECT 1;"))
        );
        assert_ne!(
            normalized(&p("DEFINER", "SELECT 1;")),
            normalized(&p("DEFINER", "SELECT 2;"))
        );
    }

    #[test]
    fn procedure_key_is_the_archive_tag() {
        // pg_dump tags a procedure with its input types: an INOUT
        // parameter is one, an OUT parameter is not
        let p = procedure(serde_json::json!({
            "name": "archive_before",
            "schema": "test",
            "owner": "postgres",
            "parameters": [
                {"mode": "IN", "name": "days", "data_type": "integer"},
                {"mode": "INOUT", "name": "archived", "data_type": "integer"},
                {"mode": "OUT", "name": "total", "data_type": "bigint"},
            ],
            "language": "sql",
            "definition": "SELECT 1, 2",
        }));
        assert_eq!(
            ObjectKey::new(ObjectType::Procedure, &p).name,
            "archive_before(integer, integer)"
        );
        // a name that has its argument list and no parameters keeps
        // its list, and a bare name gets an empty list
        let named = |name: &str| {
            procedure(serde_json::json!({
                "name": name, "schema": "test", "owner": "postgres",
                "language": "sql", "definition": "SELECT 1",
            }))
        };
        assert_eq!(
            ObjectKey::new(ObjectType::Procedure, &named("q(integer)")).name,
            "q(integer)"
        );
        assert_eq!(
            ObjectKey::new(ObjectType::Procedure, &named("q")).name,
            "q()"
        );
    }

    #[test]
    fn objects_with_no_data_compare_in_canonical_form() {
        let statistics = |owner: &str, kinds: serde_json::Value| {
            Definition::Statistics(
                serde_json::from_value(serde_json::json!({
                    "name": "s", "schema": "test", "owner": owner,
                    "table": "test.t", "kinds": kinds,
                    "elements": ["a", "b"],
                }))
                .expect("statistics deserialize"),
            )
        };
        assert_eq!(
            normalized(&statistics(
                "app",
                serde_json::json!(["mcv", "dependencies", "ndistinct"])
            )),
            normalized(&statistics(
                "postgres",
                serde_json::json!(["ndistinct", "dependencies", "mcv"])
            ))
        );
        let trigger = |tags: serde_json::Value| {
            Definition::EventTrigger(
                serde_json::from_value(serde_json::json!({
                    "name": "e", "event": "ddl_command_start",
                    "filter": {"tags": tags}, "function": "test.f()",
                }))
                .expect("event trigger deserializes"),
            )
        };
        assert_eq!(
            normalized(&trigger(serde_json::json!([
                "drop table",
                "ALTER TABLE"
            ]))),
            normalized(&trigger(serde_json::json!([
                "ALTER TABLE",
                "DROP TABLE"
            ])))
        );
        for desc in [
            ObjectType::AccessMethod,
            ObjectType::Collation,
            ObjectType::Conversion,
            ObjectType::EventTrigger,
            ObjectType::Statistics,
        ] {
            assert_eq!(compare(desc), Compare::Definition);
        }
    }

    #[test]
    fn skipped_types_are_not_compared() {
        for desc in [
            ObjectType::Group,
            ObjectType::Role,
            ObjectType::Tablespace,
            ObjectType::User,
        ] {
            assert_eq!(compare(desc), Compare::Skip);
        }
    }
}
