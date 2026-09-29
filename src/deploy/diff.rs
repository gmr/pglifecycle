//! Model-level comparison between a loaded project and a database
//! snapshot. Both sides hold the same `models::` structs (the project
//! via [`crate::project::load`], the database via
//! [`crate::pull::Assembly`]), so definitions compare as normalized
//! JSON values.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::constants::ObjectType;
use crate::models::Definition;
use crate::project::Project;
use crate::pull::Assembly;

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
/// types, so `int4` matches the `integer` pg_dump writes. An aggregate
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
        // one name can be used once for each index method
        Definition::OperatorClass(class) => (
            class.schema.clone(),
            format!("{} USING {}", class.name, class.method),
        ),
        Definition::OperatorFamily(family) => (
            family.schema.clone(),
            format!("{} USING {}", family.name, family.method),
        ),
        Definition::Transform(transform) => (
            String::new(),
            format!(
                "FOR {} LANGUAGE {}",
                canonical_type(&transform.data_type),
                transform.language
            ),
        ),
        Definition::Cast(cast) => (
            String::new(),
            format!(
                "({} AS {})",
                canonical_type(
                    cast.source_type.as_deref().unwrap_or_default()
                ),
                canonical_type(
                    cast.target_type.as_deref().unwrap_or_default()
                )
            ),
        ),
        _ => (
            definition.schema().unwrap_or_default().to_string(),
            definition.name(),
        ),
    }
}

/// The function identity signature with canonicalized parameter
/// types, so a repo `fn(int4)` keys identically to the server's
/// `fn(integer)` (mirrors [`crate::models::Function::identity`])
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
            parts.push(canonical_type(&p.data_type));
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
        if self.schema.is_empty() {
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
    /// Exists on both sides but the type is not yet model-diffable
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
}

/// How deploy compares the objects of one type
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Compare {
    /// Compare the definitions; a difference is a change, and an
    /// object that only the database has is removed
    Definition,
    /// Check only that the object exists: a changed definition is left
    /// as the database has it, and a database-only object is kept
    Existence,
    /// Out of deploy's scope: roles, users, and groups require
    /// cluster-level access pg_dump does not capture, and tablespaces
    /// are likewise absent from a single-database dump — diffing them
    /// would re-create them on every run
    Skip,
}

/// How deploy compares each object type. A type moves from
/// `Existence` to `Definition` when deploy can reconcile its changes
/// (`alter::resolve_with`) and drop it in dependency order
/// (`entry_key` and `drop_sql` in `mod.rs`).
fn compare(desc: ObjectType) -> Compare {
    match desc {
        ObjectType::AccessMethod => Compare::Existence,
        ObjectType::Aggregate => Compare::Existence,
        ObjectType::Cast => Compare::Existence,
        ObjectType::Collation => Compare::Existence,
        ObjectType::Conversion => Compare::Existence,
        ObjectType::DefaultPrivileges => Compare::Definition,
        ObjectType::Domain => Compare::Definition,
        ObjectType::EventTrigger => Compare::Existence,
        ObjectType::Extension => Compare::Definition,
        ObjectType::ForeignDataWrapper => Compare::Definition,
        ObjectType::Function => Compare::Definition,
        ObjectType::Group => Compare::Skip,
        ObjectType::MaterializedView => Compare::Definition,
        ObjectType::Operator => Compare::Existence,
        ObjectType::OperatorClass => Compare::Existence,
        ObjectType::OperatorFamily => Compare::Existence,
        ObjectType::ProceduralLanguage => Compare::Definition,
        ObjectType::Procedure => Compare::Definition,
        ObjectType::Publication => Compare::Existence,
        ObjectType::Role => Compare::Skip,
        ObjectType::Schema => Compare::Definition,
        ObjectType::Sequence => Compare::Definition,
        ObjectType::Server => Compare::Definition,
        ObjectType::Statistics => Compare::Existence,
        ObjectType::Subscription => Compare::Existence,
        ObjectType::Table => Compare::Definition,
        ObjectType::Tablespace => Compare::Skip,
        ObjectType::TextSearch => Compare::Existence,
        ObjectType::Transform => Compare::Existence,
        ObjectType::Type => Compare::Definition,
        ObjectType::User => Compare::Skip,
        ObjectType::UserMapping => Compare::Definition,
        ObjectType::View => Compare::Definition,
    }
}

pub fn diff(project: &Project, assembly: &Assembly) -> Diff {
    let mut database = database_index(assembly);
    let existing = existence_index(assembly);
    let mut items = BTreeMap::new();
    let mut changed = BTreeMap::new();
    for item in &project.inventory {
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
            // never compares equal: it is only checked for existence
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
                        if normalized(&item.definition) == normalized(&db) {
                            Change::Unchanged
                        } else {
                            changed.insert(item.id, db);
                            Change::Changed
                        }
                    }
                }
            }
            Compare::Existence => {
                let key =
                    definition_existence_key(item.desc, &item.definition);
                let found = if item.definition.raw_sql() {
                    raw_exists(&existing, item.desc, &key)
                } else {
                    existing.contains(&key)
                };
                if found {
                    Change::Undiffable
                } else {
                    Change::Added
                }
            }
        };
        items.insert(item.id, change);
    }
    Diff {
        items,
        changed,
        removed: database,
    }
}

/// Remove the database object that a raw `sql` item stands for, and
/// return it. The item's key is tried first. A raw function has no
/// parameter list, so its key can differ from the database key: then
/// the first object of the same type, schema and bare name matches, as
/// in [`existence_key`].
fn take_raw(
    database: &mut BTreeMap<ObjectKey, Definition>,
    desc: ObjectType,
    definition: &Definition,
) -> Option<Definition> {
    let key = ObjectKey::new(desc, definition);
    if let Some(db) = database.remove(&key) {
        return Some(db);
    }
    let bare = |name: &str| {
        name.split('(')
            .next()
            .unwrap_or(name)
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

/// Whether the database has the object that a raw `sql` item of an
/// existence-checked type stands for. A raw statement can have no
/// structured input types, so any object of its type, schema and bare
/// name matches. A cast key, `(source AS target)`, has no bare name,
/// and the cast schema requires both types, so a cast matches only by
/// its full key.
fn raw_exists(
    existing: &BTreeSet<(String, String, String)>,
    desc: ObjectType,
    key: &(String, String, String),
) -> bool {
    if existing.contains(key) {
        return true;
    }
    if desc == ObjectType::Cast {
        return false;
    }
    let bare = existence_key(&key.0, &key.1, &key.2);
    existing
        .iter()
        .any(|k| existence_key(&k.0, &k.1, &k.2) == bare)
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
        .chain(all(O::TextSearch, &a.text_search, Definition::TextSearch))
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

/// Existence-only index over the snapshot objects whose type is
/// compared by existence, and the entries that were not parsed into
/// models (`Assembly::remaining`)
fn existence_index(assembly: &Assembly) -> BTreeSet<(String, String, String)> {
    let remaining = assembly.remaining.iter().filter_map(|r| {
        let tag = r.tag.as_deref()?;
        Some(existence_key(
            &r.desc,
            r.namespace.as_deref().unwrap_or_default(),
            tag,
        ))
    });
    let modeled = snapshot_definitions(assembly)
        .into_iter()
        .filter(|(desc, _)| compare(*desc) == Compare::Existence)
        .map(|(desc, definition)| definition_existence_key(desc, &definition));
    remaining.chain(modeled).collect()
}

/// [`existence_key`] for a model: its [`ObjectKey`] as a tuple
fn definition_existence_key(
    desc: ObjectType,
    definition: &Definition,
) -> (String, String, String) {
    let key = ObjectKey::new(desc, definition);
    (desc.as_str().to_string(), key.schema, key.name)
}

/// Match key for existence-only comparison; argument lists are
/// stripped because pg_dump's signature formatting and the build's
/// may differ (overloads of unmodeled types therefore conflate)
fn existence_key(
    desc: &str,
    namespace: &str,
    tag: &str,
) -> (String, String, String) {
    let name = tag.split('(').next().unwrap_or(tag).trim_end();
    (desc.to_string(), namespace.to_string(), name.to_string())
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
        Definition::Procedure(procedure) => {
            canonical = Definition::Procedure(procedure.canonical());
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

fn normalize(value: &mut Value) {
    match value {
        Value::Object(map) => {
            // ownership is out of deploy's scope (a flat SQL script
            // cannot apply pg_restore-style ownership anyway)
            map.remove("owner");
            for (key, child) in map.iter_mut() {
                if (key == "data_type" || key == "returns")
                    && let Some(data_type) = child.as_str()
                {
                    *child = Value::String(canonical_type(data_type));
                } else {
                    normalize(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(normalize),
        _ => {}
    }
}

/// Canonicalize common type-name aliases the way PostgreSQL does on
/// ingest, so a hand-edited `int4` does not falsely diff against the
/// server's `integer` (PLAN.md risk #5). A length/precision modifier
/// (`varchar(255)`, `numeric(10,2)`) and an array suffix are split
/// off the base name so the alias can be matched and reattached.
pub(crate) fn canonical_type(data_type: &str) -> String {
    // PostgreSQL folds a name that is not quoted to lowercase, so `TEXT`
    // is `text`; a quoted name keeps its case
    let mut quoted = false;
    let data_type: String = data_type
        .chars()
        .map(|c| {
            if c == '"' {
                quoted = !quoted;
            }
            if quoted { c } else { c.to_ascii_lowercase() }
        })
        .collect();
    let (body, array) = match data_type.trim_end().strip_suffix("[]") {
        Some(body) => (body.trim_end(), "[]"),
        None => (data_type.trim_end(), ""),
    };
    let (name, modifier) = match body.find('(') {
        Some(index) => (body[..index].trim_end(), &body[index..]),
        None => (body, ""),
    };
    let canonical = match name {
        "bool" => "boolean",
        "char" => "character",
        "decimal" => "numeric",
        "float4" => "real",
        "float8" => "double precision",
        "int2" => "smallint",
        "int4" | "int" => "integer",
        "int8" => "bigint",
        "timestamptz" => "timestamp with time zone",
        "timetz" => "time with time zone",
        "varchar" => "character varying",
        other => other,
    };
    format!("{canonical}{modifier}{array}")
}

/// A type as it identifies an argument: [`canonical_type`] without
/// its modifiers. PostgreSQL does not keep a typmod in an argument
/// type, so `varchar(10)` and `varchar` give the same aggregate or
/// operator. A modifier in a quoted name is kept.
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
    result.trim_end().to_string()
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
    fn aggregate_existence_key_keeps_input_types() {
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
        let key = |d: &str| {
            definition_existence_key(ObjectType::Aggregate, &aggregate(d))
        };
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

    #[test]
    fn existence_key_strips_signature() {
        // unmodeled-type overloads conflate to one existence key: an
        // aggregate present in both sides under any overload reads as
        // "exists" regardless of argument types (documented limit —
        // these types are existence-checked, not diffed)
        assert_eq!(
            existence_key("AGGREGATE", "test", "sum(integer)"),
            existence_key("AGGREGATE", "test", "sum(numeric)")
        );
        assert_ne!(
            existence_key("AGGREGATE", "test", "sum(integer)"),
            existence_key("AGGREGATE", "test", "max(integer)")
        );
    }

    #[test]
    fn raw_cast_matches_only_its_own_types() {
        let key = |desc: &str, schema: &str, name: &str| {
            (desc.to_string(), schema.to_string(), name.to_string())
        };
        let existing = BTreeSet::from([
            key("CAST", "", "(test.point_pair AS text)"),
            key("PROCEDURE", "test", "p(integer)"),
        ]);
        // a different cast does not stand for the raw cast
        assert!(!raw_exists(
            &existing,
            ObjectType::Cast,
            &key("CAST", "", "(test.point_pair AS character varying)")
        ));
        assert!(raw_exists(
            &existing,
            ObjectType::Cast,
            &key("CAST", "", "(test.point_pair AS text)")
        ));
        // a raw procedure with no parameters matches by its bare name
        assert!(raw_exists(
            &existing,
            ObjectType::Procedure,
            &key("PROCEDURE", "test", "p()")
        ));
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
