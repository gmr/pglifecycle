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

/// An SQL expression without the parentheses that enclose all of it,
/// as in `((a + b))`. The pair in `(a) + (b)` does not enclose all of
/// it, so that expression does not change. A parenthesis in a quoted
/// string or name does not count. Quoted strings include E-strings
/// with backslash escapes and dollar-quoted strings.
pub fn strip_outer_parens(expression: &str) -> &str {
    let mut expression = expression.trim();
    loop {
        let inner = strip_one_pair(expression);
        if inner == expression {
            return expression;
        }
        expression = inner;
    }
}

fn strip_one_pair(expression: &str) -> &str {
    let trimmed = expression.trim();
    let Some(inner) = trimmed
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
    else {
        return trimmed;
    };
    let bytes = inner.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    while i < bytes.len() {
        let previous = i.checked_sub(1).map(|p| bytes[p]);
        let after_ident = previous.is_some_and(is_ident_byte);
        match bytes[i] {
            b'\'' => {
                // `E'...'` (not `name'...'`) lets a backslash escape
                let escapes = matches!(previous, Some(b'e' | b'E'))
                    && !i
                        .checked_sub(2)
                        .is_some_and(|p| is_ident_byte(bytes[p]));
                i = skip_quoted(bytes, i + 1, b"'", escapes);
            }
            b'"' => i = skip_quoted(bytes, i + 1, b"\"", false),
            b'$' if !after_ident => match dollar_tag(&bytes[i..]) {
                Some(tag) => {
                    i = skip_quoted(bytes, i + tag.len(), tag, false);
                }
                None => i += 1,
            },
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth -= 1;
                // the first parenthesis closes before the end
                if depth < 0 {
                    return trimmed;
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    if depth == 0 { inner.trim() } else { trimmed }
}

/// A byte that can continue an unquoted identifier
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

/// The `$tag$` delimiter at the start of `bytes`, if one is there.
/// `$1` is a parameter, not a delimiter.
fn dollar_tag(bytes: &[u8]) -> Option<&[u8]> {
    let mut end = 1;
    if bytes
        .get(1)
        .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_' || *b >= 0x80)
    {
        while bytes.get(end).is_some_and(|b| {
            b.is_ascii_alphanumeric() || *b == b'_' || *b >= 0x80
        }) {
            end += 1;
        }
    }
    (bytes.get(end) == Some(&b'$')).then(|| &bytes[..=end])
}

/// The index after the `end` delimiter that closes a quoted span
/// that starts at `start`. If `escapes`, a backslash escapes the
/// byte after it.
fn skip_quoted(
    bytes: &[u8],
    start: usize,
    end: &[u8],
    escapes: bool,
) -> usize {
    let mut i = start;
    while i < bytes.len() {
        if escapes && bytes[i] == b'\\' {
            i += 2;
        } else if bytes[i..].starts_with(end) {
            return i + end.len();
        } else {
            i += 1;
        }
    }
    bytes.len()
}

/// Split a routine name or tag at the `(` that opens its argument
/// list, as `split_once('(')` splits it: `f(x)(integer)` is `f(x)`
/// and `integer)`. The argument list is the parenthesized text at the
/// end, so a name can contain `(`, and a `(` in a quoted type is not
/// the split point. A name with no argument list at its end gives
/// `None`.
pub fn split_signature(value: &str) -> Option<(&str, &str)> {
    let bytes = value.trim_end().as_bytes();
    if bytes.last() != Some(&b')') {
        return None;
    }
    let (mut depth, mut quoted) = (0usize, false);
    for (i, b) in bytes.iter().enumerate().rev() {
        match b {
            b'"' => quoted = !quoted,
            b')' if !quoted => depth += 1,
            b'(' if !quoted => {
                depth -= 1;
                if depth == 0 {
                    return Some((&value[..i], &value[i + 1..]));
                }
            }
            _ => {}
        }
    }
    None
}

/// A routine name, quoted, with the argument list that may follow it
/// kept as written: `Quoted Fn(integer)` is `"Quoted Fn"(integer)`
pub fn quote_routine_name(name: &str) -> String {
    match split_signature(name) {
        Some((base, arguments)) => {
            format!("{}({arguments}", quote_ident(base))
        }
        None => quote_ident(name),
    }
}

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

/// Return the value of the SET clause of a routine, a role or a user.
/// A list is one string constant for each element, `'a', 'b'`, as
/// pg_dump and pg_dumpall write it: SET takes no array, and one string
/// is one element.
pub fn setting_value(value: &Value) -> String {
    match value {
        Value::Array(items) => items
            .iter()
            .map(postgres_value)
            .collect::<Vec<_>>()
            .join(", "),
        other => postgres_value(other),
    }
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
///
/// Matches pg_dump's `appendStringLiteralDQ`: a delimiter is safe when
/// its form *without* the trailing `$` is absent from `body`. Checking
/// only the full delimiter would miss a body ending in the delimiter
/// prefix (e.g. `foo$`), which merges with the closing `$` and yields
/// invalid SQL.
pub(crate) fn dollar_quote(body: &str) -> String {
    if !body.contains('$') {
        return format!("$${body}$$");
    }
    let mut n = 1;
    loop {
        let prefix = format!("$c{n}");
        if !body.contains(&prefix) {
            return format!("{prefix}${body}{prefix}$");
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

/// The name PostgreSQL gives an object that has no name of its own,
/// `<name1>_<name2>_<label>` or `<name1>_<label>` (ports `makeObjectName`
/// in `src/backend/commands/indexcmds.c`). The name is cut to 63 bytes:
/// it takes a byte from the longer of the two names until the name
/// fits, then cuts each name back to a character boundary, as
/// `pg_mbcliplen` does. PostgreSQL adds a number to the label when the
/// name is in use in the schema; that case is not known here.
pub(crate) fn make_object_name(
    name1: &str,
    name2: Option<&str>,
    label: &str,
) -> String {
    // NAMEDATALEN - 1
    const MAX: usize = 63;
    let overhead = label.len() + 1 + usize::from(name2.is_some());
    let available = MAX - overhead;
    let mut name1_len = name1.len();
    let mut name2_len = name2.map_or(0, str::len);
    while name1_len + name2_len > available {
        if name1_len > name2_len {
            name1_len -= 1;
        } else {
            name2_len -= 1;
        }
    }
    let mut name = clip(name1, name1_len).to_string();
    if let Some(name2) = name2 {
        name.push('_');
        name.push_str(clip(name2, name2_len));
    }
    name.push('_');
    name.push_str(label);
    name
}

/// The longest start of `name` that is not more than `len` bytes and
/// ends on a character boundary, as `pg_mbcliplen` gives
fn clip(name: &str, mut len: usize) -> &str {
    while !name.is_char_boundary(len) {
        len -= 1;
    }
    &name[..len]
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Each expected name is the one PostgreSQL 18 gave the object
    #[test]
    fn makes_object_names_as_postgres() {
        let a40 = "a".repeat(40);
        let c40 = "c".repeat(40);
        let t63 = "t".repeat(63);
        assert_eq!(
            make_object_name("addresses", Some("user_id"), "fkey"),
            "addresses_user_id_fkey"
        );
        assert_eq!(make_object_name("users", None, "pkey"), "users_pkey");
        assert_eq!(
            make_object_name(&a40, Some(&"b".repeat(40)), "fkey"),
            format!("{}_{}_fkey", "a".repeat(29), "b".repeat(28))
        );
        assert_eq!(
            make_object_name(&a40, Some(&c40), "not_null"),
            format!("{}_{}_not_null", "a".repeat(27), "c".repeat(26))
        );
        assert_eq!(
            make_object_name(&a40, Some(&c40), "check"),
            format!("{}_{}_check", "a".repeat(28), "c".repeat(28))
        );
        assert_eq!(
            make_object_name(&a40, Some(&c40), "key"),
            format!("{}_{}_key", "a".repeat(29), "c".repeat(29))
        );
        assert_eq!(
            make_object_name(&a40, Some(&c40), "seq"),
            format!("{}_{}_seq", "a".repeat(29), "c".repeat(29))
        );
        assert_eq!(
            make_object_name(&a40, Some("ee_ff_dd"), "key"),
            format!("{a40}_ee_ff_dd_key")
        );
        assert_eq!(
            make_object_name(&a40, None, "pkey"),
            format!("{a40}_pkey")
        );
        assert_eq!(
            make_object_name(&t63, None, "pkey"),
            format!("{}_pkey", "t".repeat(58))
        );
        assert_eq!(
            make_object_name(&t63, Some("id"), "not_null"),
            format!("{}_id_not_null", "t".repeat(51))
        );
        assert_eq!(
            make_object_name(&t63, Some("x"), "not_null"),
            format!("{}_x_not_null", "t".repeat(52))
        );
        assert_eq!(
            make_object_name(&"d".repeat(61), None, "not_null"),
            format!("{}_not_null", "d".repeat(54))
        );
    }

    /// PostgreSQL balances the byte lengths first, then cuts each name
    /// back to a character boundary. Each expected name is the one
    /// PostgreSQL 18 gave the object, in a UTF8 database.
    #[test]
    fn makes_multibyte_object_names_as_postgres() {
        let e20 = "\u{e9}".repeat(20);
        let e31 = "\u{e9}".repeat(31);
        assert_eq!(
            make_object_name(&e20, Some(&"x".repeat(35)), "not_null"),
            format!("{}_{}_not_null", "\u{e9}".repeat(13), "x".repeat(26))
        );
        assert_eq!(
            make_object_name(&e20, None, "pkey"),
            format!("{e20}_pkey")
        );
        assert_eq!(
            make_object_name(&e31, None, "pkey"),
            format!("{}_pkey", "\u{e9}".repeat(29))
        );
        assert_eq!(
            make_object_name(&e31, Some("id"), "not_null"),
            format!("{}_id_not_null", "\u{e9}".repeat(25))
        );
        assert_eq!(
            make_object_name(&e31, Some(&"\u{15d}".repeat(20)), "not_null"),
            format!(
                "{}_{}_not_null",
                "\u{e9}".repeat(13),
                "\u{15d}".repeat(13)
            )
        );
        assert_eq!(
            make_object_name(
                &"\u{e9}".repeat(20),
                Some(&"x".repeat(35)),
                "fkey"
            ),
            format!("{}_{}_fkey", "\u{e9}".repeat(14), "x".repeat(28))
        );
    }

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

    /// A list renders one string constant for each element, as
    /// pg_dump writes it, and not as an array, which SET rejects
    #[test]
    fn renders_setting_values() {
        assert_eq!(
            setting_value(&json!(["pg_catalog", "pg_temp"])),
            "'pg_catalog', 'pg_temp'"
        );
        assert_eq!(
            setting_value(&json!(["my schema", "$user", "it's", ""])),
            "'my schema', '$user', $$it's$$, ''"
        );
        assert_eq!(setting_value(&json!("pg_catalog")), "'pg_catalog'");
        assert_eq!(setting_value(&json!(1000)), "1000");
    }

    #[test]
    fn quotes_a_routine_name_and_keeps_its_arguments() {
        assert_eq!(
            quote_routine_name("Quoted Fn(integer, text)"),
            "\"Quoted Fn\"(integer, text)"
        );
        assert_eq!(quote_routine_name("f()"), "f()");
        assert_eq!(quote_routine_name("Bare"), "\"Bare\"");
        assert_eq!(quote_routine_name("f(x)(integer)"), "\"f(x)\"(integer)");
    }

    #[test]
    fn splits_a_routine_at_its_argument_list() {
        assert_eq!(split_signature("f(integer)"), Some(("f", "integer)")));
        assert_eq!(
            split_signature("f(x)(integer)"),
            Some(("f(x)", "integer)"))
        );
        assert_eq!(split_signature("g\"(y(text)"), Some(("g\"(y", "text)")));
        assert_eq!(
            split_signature("\"g\"\"(y\"(text)"),
            Some(("\"g\"\"(y\"", "text)"))
        );
        assert_eq!(
            split_signature("f(numeric(10,2), \"a)b\")"),
            Some(("f", "numeric(10,2), \"a)b\")"))
        );
        assert_eq!(split_signature("f(x)()"), Some(("f(x)", ")")));
        assert_eq!(split_signature("f"), None);
        assert_eq!(split_signature("f)"), None);
    }

    #[test]
    fn strips_only_enclosing_parentheses() {
        assert_eq!(strip_outer_parens("((a + b))"), "a + b");
        assert_eq!(
            strip_outer_parens("((label)::character varying(20))"),
            "(label)::character varying(20)"
        );
        assert_eq!(strip_outer_parens("(a) + (b)"), "(a) + (b)");
        assert_eq!(strip_outer_parens("lower(name)"), "lower(name)");
        assert_eq!(strip_outer_parens("(a || ')(')"), "a || ')('");
        assert_eq!(strip_outer_parens("(\"odd)\" + 1)"), "\"odd)\" + 1");
        assert_eq!(strip_outer_parens("(a || $$)$$)"), "a || $$)$$");
        assert_eq!(strip_outer_parens("(a || $x$)$$($x$)"), "a || $x$)$$($x$");
        assert_eq!(strip_outer_parens("(E'it\\')'::text)"), "E'it\\')'::text");
        assert_eq!(strip_outer_parens("(a || 'x\\')"), "a || 'x\\'");
        assert_eq!(strip_outer_parens("($1) + ($2)"), "($1) + ($2)");
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

    #[test]
    fn dollar_quote_handles_trailing_dollar_prefix() {
        // A body ending in `$` would merge with a bare `$$` closing
        // delimiter, so a tagged delimiter must be chosen instead.
        let body = "ends with a $";
        let quoted = dollar_quote(body);
        assert_eq!(quoted, format!("$c1${body}$c1$"));
    }
}
