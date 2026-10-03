//! The type names that a project gives with no schema. The deploy
//! script and the restore run with an empty `search_path`, so such a
//! name is found only when it is a built-in type. The build and deploy
//! do not resolve these names; the load only finds the names that can
//! fail.

use std::collections::HashSet;
use std::path::PathBuf;

use crate::constants::ObjectType;
use crate::deploy::{identity_type, is_built_in, serial_integer_type};
use crate::models::{Definition, Item};
use crate::project::split_sql_name;

/// A message for each type name with no schema that is not a built-in
/// type and not a type or a domain of the project in the schema of
/// the object that uses it. Each message names the file, the field
/// and the type. `paths` is parallel to `inventory`.
pub(super) fn unresolved_type_names(
    inventory: &[Item],
    paths: &[Option<PathBuf>],
) -> Vec<String> {
    let types: HashSet<(&str, String)> = inventory
        .iter()
        .filter(|item| {
            matches!(item.desc, ObjectType::Type | ObjectType::Domain)
        })
        .filter_map(|item| {
            Some((item.definition.schema()?, item.definition.name()))
        })
        .collect();
    let mut messages = Vec::new();
    for (item, path) in inventory.iter().zip(paths) {
        let schema = item.definition.schema().unwrap_or_default();
        for (field, data_type) in type_fields(&item.definition) {
            let Some(name) = unqualified(&data_type) else {
                continue;
            };
            if types.contains(&(schema, name.clone())) {
                continue;
            }
            messages.push(format!(
                "{}: {} {}.{}: {field} has the type {data_type} with no \
                 schema. It is not a built-in type, and not a type or a \
                 domain of the project in schema {schema}. The restore and \
                 the deploy script run with an empty search_path, thus \
                 PostgreSQL does not find it. Qualify it with its schema, \
                 for example public.{name}",
                path.as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| String::from("<unknown>")),
                item.desc.as_str(),
                schema,
                item.definition.name(),
            ));
        }
    }
    messages
}

/// The name of the type with no modifier and no array suffix, when it
/// has no schema and is not a built-in type. A name that refers to a
/// column (`t.c%TYPE`) or `NONE` (no operator argument) is no type,
/// and a serial type is an integer type.
fn unqualified(data_type: &str) -> Option<String> {
    let text = data_type.trim();
    let lower = text.to_ascii_lowercase();
    if text.is_empty()
        || text.contains('%')
        || lower == "none"
        || serial_integer_type(text).is_some()
        || lower.starts_with("pg_catalog.")
        || lower.starts_with("\"pg_catalog\".")
    {
        return None;
    }
    let base = identity_type(text);
    let base = base.strip_prefix("setof ").unwrap_or(&base);
    let base = base.trim_end_matches("[]");
    let (namespace, name) = split_sql_name(base);
    (namespace.is_empty() && !is_built_in(base)).then_some(name)
}

/// Each field of the definition that holds a type name, with a label
/// for it, and the type name
fn type_fields(definition: &Definition) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    let mut push = |field: String, data_type: Option<&String>| {
        if let Some(data_type) = data_type {
            fields.push((field, data_type.clone()));
        }
    };
    match definition {
        Definition::Table(t) => {
            for column in t.columns.iter().flatten() {
                push(
                    format!("column {} data_type", column.name),
                    Some(&column.data_type),
                );
            }
            push(String::from("from_type"), t.from_type.as_ref());
        }
        Definition::Type(t) => {
            for column in t.columns.iter().flatten() {
                push(
                    format!("column {} data_type", column.name),
                    Some(&column.data_type),
                );
            }
            push(String::from("like_type"), t.like_type.as_ref());
            push(String::from("element"), t.element.as_ref());
            push(String::from("subtype"), t.subtype.as_ref());
        }
        Definition::Domain(d) => {
            push(String::from("data_type"), d.data_type.as_ref());
        }
        Definition::Sequence(s) => {
            push(String::from("data_type"), s.data_type.as_ref());
        }
        Definition::Function(f) => {
            for (index, p) in f.parameters.iter().flatten().enumerate() {
                push(parameter(index, &p.name), Some(&p.data_type));
            }
            for data_type in returns(f.returns.as_deref()) {
                push(String::from("returns"), Some(&data_type));
            }
            for data_type in f.transform_types.iter().flatten() {
                push(String::from("transform_types"), Some(data_type));
            }
        }
        Definition::Procedure(p) => {
            for (index, p) in p.parameters.iter().flatten().enumerate() {
                push(parameter(index, &p.name), Some(&p.data_type));
            }
            for data_type in p.transform_types.iter().flatten() {
                push(String::from("transform_types"), Some(data_type));
            }
        }
        Definition::Aggregate(a) => {
            let arguments = a
                .arguments
                .iter()
                .map(|argument| ("arguments", argument))
                .chain(
                    a.order_by
                        .iter()
                        .flatten()
                        .map(|argument| ("order_by", argument)),
                );
            for (key, argument) in arguments {
                push(format!("{key} data_type"), Some(&argument.data_type));
            }
            push(String::from("state_data_type"), Some(&a.state_data_type));
            push(
                String::from("mstate_data_type"),
                a.mstate_data_type.as_ref(),
            );
        }
        Definition::Cast(c) => {
            push(String::from("source_type"), c.source_type.as_ref());
            push(String::from("target_type"), c.target_type.as_ref());
        }
        Definition::Transform(t) => {
            push(String::from("type"), Some(&t.data_type));
        }
        Definition::Operator(o) => {
            push(String::from("left_arg"), o.left_arg.as_ref());
            push(String::from("right_arg"), o.right_arg.as_ref());
        }
        Definition::OperatorClass(c) => {
            push(String::from("data_type"), Some(&c.data_type));
            push(String::from("storage"), c.storage.as_ref());
            for data_type in c
                .operators
                .iter()
                .flatten()
                .flat_map(|o| o.arguments.iter().flatten())
            {
                push(String::from("operators arguments"), Some(data_type));
            }
            for data_type in c
                .functions
                .iter()
                .flatten()
                .flat_map(|f| f.types.iter().flatten())
            {
                push(String::from("functions types"), Some(data_type));
            }
        }
        Definition::OperatorFamily(f) => {
            for data_type in f
                .operators
                .iter()
                .flatten()
                .flat_map(|o| o.arguments.iter().flatten())
            {
                push(String::from("operators arguments"), Some(data_type));
            }
            for data_type in f
                .functions
                .iter()
                .flatten()
                .flat_map(|f| f.types.iter().flatten())
            {
                push(String::from("functions types"), Some(data_type));
            }
        }
        _ => {}
    }
    fields
}

/// The label of a parameter: its name, or its position from 1
fn parameter(index: usize, name: &Option<String>) -> String {
    match name {
        Some(name) => format!("parameter {name} data_type"),
        None => format!("parameter {} data_type", index + 1),
    }
}

/// The types of a return type: the type, or the type of each column
/// of `TABLE(name type, ...)`
fn returns(returns: Option<&str>) -> Vec<String> {
    let Some(returns) = returns else {
        return Vec::new();
    };
    let columns = returns
        .trim()
        .strip_prefix("TABLE")
        .or_else(|| returns.trim().strip_prefix("table"))
        .map(str::trim_start)
        .and_then(|rest| rest.strip_prefix('('))
        .and_then(|rest| rest.strip_suffix(')'));
    let Some(columns) = columns else {
        return vec![returns.to_string()];
    };
    split_columns(columns)
        .into_iter()
        .filter_map(|column| {
            let column = column.trim();
            // the name is one word, or a quoted name
            let rest = match column.strip_prefix('"') {
                Some(quoted) => {
                    let mut chars = quoted.char_indices();
                    let mut end = None;
                    while let Some((i, c)) = chars.next() {
                        if c == '"' {
                            if quoted[i + 1..].starts_with('"') {
                                chars.next();
                            } else {
                                end = Some(i + 1);
                                break;
                            }
                        }
                    }
                    &quoted[end?..]
                }
                None => column.split_once(char::is_whitespace)?.1,
            };
            Some(rest.trim().to_string())
        })
        .collect()
}

/// The parts of a list split at each comma that is not in parentheses
/// or quotes
fn split_columns(list: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let (mut start, mut depth, mut quoted) = (0, 0usize, false);
    for (index, c) in list.char_indices() {
        match c {
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => depth = depth.saturating_sub(1),
            ',' if !quoted && depth == 0 => {
                parts.push(&list[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&list[start..]);
    parts
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::*;

    #[test]
    fn built_in_and_qualified_names_are_found() {
        for data_type in [
            "integer",
            "INT4",
            "int",
            "varchar(20)",
            "character varying(20)[]",
            "char(3)",
            "\"char\"",
            "bit(1)",
            "TIMESTAMP WITH TIME ZONE",
            "timestamp(3) without time zone",
            "interval day to second",
            "double precision",
            "numeric(10,2)",
            "setof text",
            "trigger",
            "event_trigger",
            "void",
            "record",
            "_int4",
            "int[3]",
            "public.citext",
            "public.citext[]",
            "\"My Schema\".\"My Type\"",
            "pg_catalog.pg_class",
            "test.t.c%TYPE",
            "serial",
            "BIGSERIAL",
            "serial2",
            "NONE",
        ] {
            assert_eq!(unqualified(data_type), None, "{data_type}");
        }
        for (data_type, name) in [
            ("citext", "citext"),
            ("citext[]", "citext"),
            ("CITEXT", "citext"),
            ("\"Mood\"", "Mood"),
            ("setof mood", "mood"),
            ("halfvec(1536)", "halfvec"),
        ] {
            assert_eq!(
                unqualified(data_type).as_deref(),
                Some(name),
                "{data_type}"
            );
        }
    }

    #[test]
    fn table_return_columns_are_types() {
        assert_eq!(
            returns(Some("TABLE(a integer, \"B c\" citext, d numeric(1,0))")),
            ["integer", "citext", "numeric(1,0)"]
        );
        assert_eq!(returns(Some("setof text")), ["setof text"]);
    }

    fn item(id: usize, desc: ObjectType, definition: Definition) -> Item {
        Item {
            id,
            desc,
            definition,
            dependencies: BTreeSet::new(),
        }
    }

    /// A name of a project type or domain in the schema of the object is
    /// found; a type of another schema, or of no object, is not
    #[test]
    fn names_each_unresolved_type_with_its_file_and_field() {
        let inventory = vec![
            item(
                0,
                ObjectType::Type,
                Definition::Type(
                    serde_json::from_value(json!({
                        "name": "mood", "schema": "test", "owner": "o",
                        "type": "enum", "enum": ["happy"],
                    }))
                    .unwrap(),
                ),
            ),
            item(
                1,
                ObjectType::Table,
                Definition::Table(
                    serde_json::from_value(json!({
                        "name": "t", "schema": "test", "owner": "o",
                        "columns": [
                            {"name": "a", "data_type": "mood"},
                            {"name": "b", "data_type": "text"},
                            {"name": "c", "data_type": "citext"},
                        ],
                    }))
                    .unwrap(),
                ),
            ),
            item(
                2,
                ObjectType::Function,
                Definition::Function(
                    serde_json::from_value(json!({
                        "name": "f", "schema": "other", "owner": "o",
                        "parameters": [
                            {"mode": "IN", "name": "m", "data_type": "mood"},
                        ],
                        "returns": "TABLE(x halfvec)",
                        "language": "sql", "definition": "SELECT 1",
                    }))
                    .unwrap(),
                ),
            ),
        ];
        let paths = vec![
            Some(PathBuf::from("types/test.yaml")),
            Some(PathBuf::from("tables/test/t.yaml")),
            Some(PathBuf::from("functions/other/f.yaml")),
        ];
        let messages = unresolved_type_names(&inventory, &paths);
        assert_eq!(messages.len(), 3, "{messages:#?}");
        for (message, (path, field, data_type)) in messages.iter().zip([
            ("tables/test/t.yaml", "column c data_type", "citext"),
            ("functions/other/f.yaml", "parameter m data_type", "mood"),
            ("functions/other/f.yaml", "returns", "halfvec"),
        ]) {
            assert!(message.starts_with(path), "{message}");
            assert!(message.contains(field), "{message}");
            assert!(
                message.contains(&format!("the type {data_type} ")),
                "{message}"
            );
            assert!(
                message.contains(&format!("public.{data_type}")),
                "{message}"
            );
        }
    }
}
