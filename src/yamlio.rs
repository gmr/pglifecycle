//! YAML load/save (ports yaml.py)
//!
//! Both directions go through serde_norway (the maintained successor to
//! the deprecated serde-yaml): loading deserializes into a
//! `serde_json::Value` (built with `preserve_order`, so mappings keep
//! file order) and emission serializes that `Value` back. serde_norway
//! renders multi-line strings as literal block scalars, so formatted
//! view queries and function bodies stay readable.

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
    let body = serde_norway::to_string(value)
        .expect("serializing a serde_json::Value to YAML cannot fail");
    format!("---\n{body}")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn starts_with_document_marker() {
        assert!(dump(&json!({"name": "x"})).starts_with("---\n"));
    }

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
        // a literal block scalar keeps the SQL readable rather than a
        // single double-quoted line with `\n` escapes
        assert!(text.contains("query: |"), "expected block scalar:\n{text}");
        assert!(text.contains("\n  SELECT id\n"), "{text}");
        assert_eq!(load_str(&text).unwrap(), value);
    }

    #[test]
    fn round_trips_multiple_trailing_newlines() {
        for body in ["text\n\n", "text\n\n\n"] {
            let value = json!({ "body": body });
            assert_eq!(load_str(&dump(&value)).unwrap(), value);
        }
    }

    #[test]
    fn round_trips_ambiguous_scalars() {
        let value = json!({
            "a": "true", "b": "123", "c": "===", "d": "with: colon",
            "e": "", "f": "trailing \n", "g": "it's",
        });
        assert_eq!(load_str(&dump(&value)).unwrap(), value);
    }
}
