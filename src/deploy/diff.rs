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
        let name = match definition {
            Definition::Function(f) => function_key_name(f),
            _ => definition.name(),
        };
        // extension names are database-unique; their schema field is
        // the installation target, not part of their identity
        let schema = match definition {
            Definition::Extension(_) => String::new(),
            _ => definition.schema().unwrap_or_default().to_string(),
        };
        Self { desc, schema, name }
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
    /// Objects in the database with no repo counterpart → DROP
    pub removed: Vec<ObjectKey>,
}

/// Object types deploy does not manage: roles, users, and groups
/// require cluster-level access pg_dump does not capture, and
/// tablespaces are likewise absent from a single-database dump —
/// diffing them would re-create them on every run
const SKIPPED: &[ObjectType] = &[
    ObjectType::Group,
    ObjectType::Role,
    ObjectType::Tablespace,
    ObjectType::User,
];

pub fn diff(project: &Project, assembly: &Assembly) -> Diff {
    let mut database = database_index(assembly);
    let existing = existence_index(assembly);
    let mut items = BTreeMap::new();
    for item in &project.inventory {
        let change = if SKIPPED.contains(&item.desc) {
            log::debug!(
                "Skipping {} {}: not managed by deploy",
                item.desc.as_str(),
                item.definition.name()
            );
            Change::Skipped
        } else if modeled(item.desc) {
            let key = ObjectKey::new(item.desc, &item.definition);
            match database.remove(&key) {
                None => Change::Added,
                Some(db) => {
                    if normalized(&item.definition) == normalized(&db) {
                        Change::Unchanged
                    } else {
                        Change::Changed
                    }
                }
            }
        } else if existing.contains(&existence_key(
            item.desc.as_str(),
            item.definition.schema().unwrap_or_default(),
            &item.definition.name(),
        )) {
            Change::Undiffable
        } else {
            Change::Added
        };
        items.insert(item.id, change);
    }
    let removed = database.into_keys().collect();
    Diff { items, removed }
}

/// Object types the [`Assembly`] parses into models; everything else
/// lands in `Assembly::remaining` and can only be existence-checked
fn modeled(desc: ObjectType) -> bool {
    matches!(
        desc,
        ObjectType::Domain
            | ObjectType::Extension
            | ObjectType::Function
            | ObjectType::MaterializedView
            | ObjectType::ProceduralLanguage
            | ObjectType::Schema
            | ObjectType::Sequence
            | ObjectType::Table
            | ObjectType::Type
            | ObjectType::View
    )
}

/// `ObjectKey → Definition` for every object the snapshot modeled
fn database_index(assembly: &Assembly) -> BTreeMap<ObjectKey, Definition> {
    let mut index = BTreeMap::new();
    let mut insert = |desc: ObjectType, definition: Definition| {
        index.insert(ObjectKey::new(desc, &definition), definition);
    };
    for d in &assembly.schemas {
        insert(ObjectType::Schema, Definition::Schema(d.clone()));
    }
    for d in &assembly.extensions {
        insert(ObjectType::Extension, Definition::Extension(d.clone()));
    }
    for d in &assembly.languages {
        insert(
            ObjectType::ProceduralLanguage,
            Definition::Language(d.clone()),
        );
    }
    for d in &assembly.domains {
        insert(ObjectType::Domain, Definition::Domain(d.clone()));
    }
    for d in &assembly.types {
        insert(ObjectType::Type, Definition::Type(d.clone()));
    }
    for d in &assembly.sequences {
        insert(ObjectType::Sequence, Definition::Sequence(d.clone()));
    }
    for d in &assembly.tables {
        insert(ObjectType::Table, Definition::Table(d.clone()));
    }
    for d in &assembly.views {
        insert(ObjectType::View, Definition::View(d.clone()));
    }
    for d in &assembly.materialized_views {
        insert(
            ObjectType::MaterializedView,
            Definition::MaterializedView(d.clone()),
        );
    }
    for d in &assembly.functions {
        insert(ObjectType::Function, Definition::Function(d.clone()));
    }
    index
}

/// Existence-only index over the snapshot entries that were not
/// parsed into models (`Assembly::remaining`)
fn existence_index(assembly: &Assembly) -> BTreeSet<(String, String, String)> {
    assembly
        .remaining
        .iter()
        .filter_map(|r| {
            let tag = r.tag.as_deref()?;
            Some(existence_key(
                &r.desc,
                r.namespace.as_deref().unwrap_or_default(),
                tag,
            ))
        })
        .collect()
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
                if key == "data_type"
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
fn canonical_type(data_type: &str) -> String {
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
}
