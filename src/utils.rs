//! Misc utilities (ports utils.py)

use serde_json::Value;

/// Quote a PostgreSQL identifier (object name, etc)
pub fn quote_ident(value: &str) -> String {
    if !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        value.to_string()
    } else {
        format!("\"{}\"", value.replace('"', "\"\""))
    }
}

/// Quote a USER MAPPING subject, leaving `PUBLIC` as an unquoted
/// keyword. PostgreSQL's CREATE/ALTER/DROP USER MAPPING grammar treats
/// `PUBLIC` as a keyword, so quoting it (`"PUBLIC"`) produces invalid
/// SQL; every other user name is a normal identifier.
pub fn user_mapping_subject(name: &str) -> String {
    if name.eq_ignore_ascii_case("PUBLIC") {
        String::from("PUBLIC")
    } else {
        quote_ident(name)
    }
}

/// Return a Postgres value as a string, quoted if required. Mirrors
/// utils.postgres_value including Python's `str()` rendering of bools
/// (`True`/`False`), which the Phase 3 round-trip work will revisit.
pub fn postgres_value(value: &Value) -> String {
    render_value(value, false)
}

fn render_value(value: &Value, nested: bool) -> String {
    match value {
        Value::String(s) if s.contains('\'') => format!("$${s}$$"),
        Value::String(s) => format!("'{s}'"),
        Value::Array(items) => {
            let inner: Vec<String> =
                items.iter().map(|v| render_value(v, true)).collect();
            if nested {
                format!("[{}]", inner.join(", "))
            } else {
                format!("ARRAY[{}]", inner.join(", "))
            }
        }
        Value::Bool(true) => String::from("True"),
        Value::Bool(false) => String::from("False"),
        other => other.to_string(),
    }
}

/// Render a value the way Python's `str()` does inside string
/// interpolation (no quoting) — used for storage parameters and
/// trigger arguments
pub fn raw_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Bool(true) => String::from("True"),
        Value::Bool(false) => String::from("False"),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_ident("users"), "users");
        assert_eq!(quote_ident("uuid-ossp"), "\"uuid-ossp\"");
        assert_eq!(quote_ident("==="), "\"===\"");
        assert_eq!(quote_ident("Has\"Quote"), "\"Has\"\"Quote\"");
    }

    #[test]
    fn user_mapping_subjects_keep_public_unquoted() {
        assert_eq!(user_mapping_subject("PUBLIC"), "PUBLIC");
        assert_eq!(user_mapping_subject("public"), "PUBLIC");
        assert_eq!(user_mapping_subject("app_user"), "app_user");
        assert_eq!(user_mapping_subject("Mixed"), "\"Mixed\"");
    }

    #[test]
    fn renders_postgres_values() {
        assert_eq!(postgres_value(&json!("simple")), "'simple'");
        assert_eq!(postgres_value(&json!("it's")), "$$it's$$");
        assert_eq!(postgres_value(&json!(5)), "5");
        assert_eq!(postgres_value(&json!(true)), "True");
        assert_eq!(postgres_value(&json!(["a", ["b"]])), "ARRAY['a', ['b']]");
    }
}
