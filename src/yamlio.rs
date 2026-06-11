//! YAML load/save (ports yaml.py + the ruamel emission style)
//!
//! Loading goes through serde_norway into `serde_json::Value` (built with
//! `preserve_order`, so mappings keep file order). Emission is a small
//! custom emitter because no maintained serde YAML emitter offers scalar
//! style control: multi-line strings must emit as literal block scalars
//! to match the ruamel.yaml output the project format was built on
//! (mapping indent 2, sequence indent 4, dash offset 2).

use std::fmt::Write;
use std::path::Path;

use serde::Deserialize;
use serde_json::Value;

pub fn is_yaml(path: &Path) -> bool {
    path.is_file()
        && matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("yml") | Some("yaml")
        )
}

pub fn load(path: &Path) -> Result<Value, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    load_str(&content).map_err(|e| {
        format!("failed to parse YAML from {}: {e}", path.display())
    })
}

pub fn load_str(content: &str) -> Result<Value, serde_norway::Error> {
    Value::deserialize(serde_norway::Deserializer::from_str(content))
}

/// Render a value as a YAML document with a `---` header
pub fn dump(value: &Value) -> String {
    let mut out = String::from("---\n");
    match value {
        Value::Object(_) | Value::Array(_) => emit(&mut out, value, 0),
        scalar => {
            out.push_str(&scalar_repr(scalar, 0));
            out.push('\n');
        }
    }
    out
}

fn emit(out: &mut String, value: &Value, indent: usize) {
    match value {
        Value::Object(map) => {
            for (key, val) in map {
                pad(out, indent);
                out.push_str(&plain_or_quoted(key));
                out.push(':');
                emit_nested(out, val, indent);
            }
        }
        Value::Array(items) => {
            for item in items {
                pad(out, indent);
                out.push('-');
                emit_nested(out, item, indent + 2);
            }
        }
        scalar => {
            pad(out, indent);
            out.push_str(&scalar_repr(scalar, indent));
            out.push('\n');
        }
    }
}

/// Emit a mapping value or sequence item after its `key:` / `-` prefix
fn emit_nested(out: &mut String, value: &Value, indent: usize) {
    match value {
        Value::Object(map) if !map.is_empty() => {
            out.push('\n');
            emit(out, value, indent + 2);
        }
        Value::Array(items) if !items.is_empty() => {
            out.push('\n');
            emit(out, value, indent + 2);
        }
        Value::Object(_) => out.push_str(" {}\n"),
        Value::Array(_) => out.push_str(" []\n"),
        scalar => {
            out.push(' ');
            out.push_str(&scalar_repr(scalar, indent));
            out.push('\n');
        }
    }
}

fn pad(out: &mut String, indent: usize) {
    for _ in 0..indent {
        out.push(' ');
    }
}

fn scalar_repr(value: &Value, indent: usize) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) if s.contains('\n') => block_scalar(s, indent),
        Value::String(s) => plain_or_quoted(s),
        _ => unreachable!("containers handled by emit"),
    }
}

/// Literal block scalar (`|` / `|-` / `|+`) with content indented two
/// spaces past the current level, matching ruamel.yaml
fn block_scalar(value: &str, indent: usize) -> String {
    let header = if value.ends_with('\n') {
        if value.ends_with("\n\n") { "|+" } else { "|" }
    } else {
        "|-"
    };
    let mut out = String::from(header);
    for line in value.trim_end_matches('\n').split('\n') {
        out.push('\n');
        if !line.is_empty() {
            let _ = write!(out, "{:width$}{line}", "", width = indent + 2);
        }
    }
    for _ in value.trim_end_matches('\n').len()..value.len().saturating_sub(1)
    {
        out.push('\n');
    }
    out
}

fn plain_or_quoted(value: &str) -> String {
    if is_plain_safe(value) {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

/// Conservative plain-scalar test: anything ambiguous gets single-quoted
fn is_plain_safe(value: &str) -> bool {
    if value.is_empty()
        || value != value.trim()
        || value.starts_with([
            '!', '&', '*', '-', '?', '#', '|', '>', '@', '`', '"', '\'', '%',
            '[', ']', '{', '}', ',', ' ',
        ])
    {
        return false;
    }
    if value.contains(": ")
        || value.ends_with(':')
        || value.contains(" #")
        || value.contains('\t')
    {
        return false;
    }
    // strings that would parse as another scalar type need quoting
    let lowered = value.to_ascii_lowercase();
    if matches!(
        lowered.as_str(),
        "null" | "~" | "true" | "false" | "yes" | "no" | "on" | "off"
    ) {
        return false;
    }
    !looks_numeric(value)
}

fn looks_numeric(value: &str) -> bool {
    value.parse::<i64>().is_ok()
        || value.parse::<f64>().is_ok()
        || (value.starts_with("0x") && value.len() > 2)
        || (value.starts_with("0o") && value.len() > 2)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn round_trips_nested_structures() {
        let value = json!({
            "name": "users",
            "columns": [
                {"name": "id", "nullable": false},
                {"name": "email", "tags": ["a", "b"]},
            ],
            "count": 3,
            "ratio": 1.5,
        });
        let parsed = load_str(&dump(&value)).unwrap();
        assert_eq!(parsed, value);
    }

    #[test]
    fn multiline_strings_emit_literal_blocks() {
        let value = json!({"query": "SELECT id\n  FROM users\n"});
        let text = dump(&value);
        assert!(text.contains("query: |\n  SELECT id\n    FROM users\n"));
        assert_eq!(load_str(&text).unwrap(), value);
    }

    #[test]
    fn preserves_multiple_trailing_newlines() {
        for body in ["text\n\n", "text\n\n\n"] {
            let value = json!({ "body": body });
            let emitted = dump(&value);
            assert!(emitted.contains("|+"));
            assert_eq!(load_str(&emitted).unwrap(), value);
        }
    }

    #[test]
    fn quotes_ambiguous_scalars() {
        let value = json!({
            "a": "true", "b": "123", "c": "===", "d": "with: colon",
            "e": "", "f": "trailing \n", "g": "it's",
        });
        assert_eq!(load_str(&dump(&value)).unwrap(), value);
    }
}
