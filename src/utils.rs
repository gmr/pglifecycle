//! Misc utilities (ports utils.py)

use serde_json::Value;

/// PostgreSQL keywords that pg_dump's `fmtId` quotes even when they
/// otherwise look like a safe unquoted identifier. This covers the
/// `RESERVED_KEYWORD` and `TYPE_FUNC_NAME_KEYWORD` sets. The
/// `COL_NAME_KEYWORD` set (e.g. `int`, `timestamp`, `values`) is
/// deliberately excluded: those double as type names and
/// `quote_ident` is also applied to type names in some render paths
/// (e.g. CAST target types), where quoting them would diverge from
/// pg_dump. Must stay sorted for `binary_search`.
const RESERVED_KEYWORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "binary",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "group",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "lateral",
    "leading",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "natural",
    "not",
    "notnull",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "outer",
    "overlaps",
    "placing",
    "primary",
    "references",
    "returning",
    "right",
    "select",
    "session_user",
    "similar",
    "some",
    "symmetric",
    "table",
    "tablesample",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
];

/// Quote a PostgreSQL identifier (object name, etc)
pub fn quote_ident(value: &str) -> String {
    let is_safe_shape = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !value.as_bytes()[0].is_ascii_digit();
    let is_reserved =
        is_safe_shape && RESERVED_KEYWORDS.binary_search(&value).is_ok();
    if is_safe_shape && !is_reserved {
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
        Value::String(s) if s.contains('\'') => dollar_quote(s),
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

/// Wrap `body` in a dollar-quoted string literal, choosing a tag that
/// does not occur in `body` (pg_dump style: `$$` when possible,
/// otherwise `$c1$`, `$c2$`, ... until the tag is collision-free).
pub(crate) fn dollar_quote(body: &str) -> String {
    if !body.contains("$$") {
        return format!("$${body}$$");
    }
    let mut n = 1;
    loop {
        let tag = format!("$c{n}$");
        if !body.contains(&tag) {
            return format!("{tag}{body}{tag}");
        }
        n += 1;
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
        assert_eq!(quote_ident("orders"), "orders");
        assert_eq!(quote_ident("my_table"), "my_table");
        assert_eq!(quote_ident("order"), "\"order\"");
        assert_eq!(quote_ident("user"), "\"user\"");
        assert_eq!(quote_ident("2fa"), "\"2fa\"");
        // `all` is a RESERVED keyword pg_dump also quotes
        assert_eq!(quote_ident("all"), "\"all\"");
    }

    #[test]
    fn reserved_keywords_stay_sorted() {
        assert!(
            RESERVED_KEYWORDS.windows(2).all(|w| w[0] < w[1]),
            "RESERVED_KEYWORDS must be sorted and unique for binary_search"
        );
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

    #[test]
    fn dollar_quote_uses_bare_delimiter_when_safe() {
        assert_eq!(dollar_quote("it's fine"), "$$it's fine$$");
    }

    #[test]
    fn dollar_quote_avoids_embedded_dollar_pair() {
        let body = "has a $$ inside";
        let quoted = dollar_quote(body);
        assert_eq!(quoted, format!("$c1${body}$c1$"));
        assert!(!body.contains("$c1$"));
    }

    #[test]
    fn dollar_quote_escalates_past_a_taken_tag() {
        let body = "has $$ and $c1$ both";
        let quoted = dollar_quote(body);
        assert_eq!(quoted, format!("$c2${body}$c2$"));
    }
}
