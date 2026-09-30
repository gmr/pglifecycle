//! Canonical forms of the names that aggregates, operators, casts,
//! transforms, operator classes and operator families refer to. pull
//! writes the forms of pg_dump: a type as `format_type` writes it, and
//! a function or an operator in `pg_catalog` without its schema. A
//! person can write the same name in another form (in uppercase, with
//! `pg_catalog.`, with a type alias), so both sides compare in the
//! forms that these functions give.

use crate::deploy::diff::{canonical_type, identity_type};
use crate::utils::quote_ident;

/// The parts of a qualified name, as PostgreSQL reads them: a part
/// that is not quoted is in lower case, and a quoted part keeps its
/// case and loses its quotes
fn parts(value: &str) -> Vec<String> {
    let mut parts = vec![String::new()];
    let mut quoted = false;
    let mut chars = value.trim().chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if quoted && chars.peek() == Some(&'"') => {
                chars.next();
                parts.last_mut().unwrap().push('"');
            }
            '"' => quoted = !quoted,
            '.' if !quoted => parts.push(String::new()),
            c if quoted => parts.last_mut().unwrap().push(c),
            c if c.is_whitespace() => {}
            c => parts.last_mut().unwrap().push(c.to_ascii_lowercase()),
        }
    }
    parts
}

/// A qualified name of a function or an operator family: each part in
/// the case that PostgreSQL keeps, quoted only where it must be, and
/// without the `pg_catalog` schema, which is always searched
pub(crate) fn name(value: &str) -> String {
    let mut parts = parts(value);
    if parts.len() > 1 && parts[0] == "pg_catalog" {
        parts.remove(0);
    }
    parts
        .iter()
        .map(|part| quote_ident(part))
        .collect::<Vec<_>>()
        .join(".")
}

/// A type name: [`canonical_type`], without the `pg_catalog` schema
pub(crate) fn type_name(value: &str) -> String {
    let canonical = canonical_type(value.trim());
    match canonical.strip_prefix("pg_catalog.") {
        Some(rest) => canonical_type(rest),
        None => canonical,
    }
}

/// An argument type: [`type_name`] without a typmod, which PostgreSQL
/// does not keep in an argument type
pub(crate) fn argument_type(value: &str) -> String {
    identity_type(&type_name(value))
}

/// The index of the first `(` that is not in quotes
fn open_paren(value: &str) -> Option<usize> {
    let mut quoted = false;
    for (index, c) in value.char_indices() {
        match c {
            '"' => quoted = !quoted,
            '(' if !quoted => return Some(index),
            _ => {}
        }
    }
    None
}

/// The items of a list, split at each comma that is not in quotes or
/// in parentheses
pub(crate) fn split_list(value: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut depth = 0usize;
    for c in value.chars() {
        match c {
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => depth = depth.saturating_sub(1),
            ',' if !quoted && depth == 0 => {
                items.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    items.push(current);
    items
        .into_iter()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect()
}

/// The argument types of a function signature, `name(type, ...)`
pub(crate) fn signature_types(value: &str) -> Vec<String> {
    let Some(open) = open_paren(value) else {
        return Vec::new();
    };
    let arguments = value[open + 1..].trim_end();
    let arguments = arguments.strip_suffix(')').unwrap_or(arguments);
    split_list(arguments)
        .iter()
        .map(|argument| argument_type(argument))
        .collect()
}

/// A function signature, `name(type, ...)`, with a canonical [`name`]
/// and canonical argument types. A function name with no argument list
/// (a `regproc`, for example an aggregate's state function) is only a
/// canonical name.
pub(crate) fn signature(value: &str) -> String {
    match open_paren(value) {
        Some(open) => format!(
            "{}({})",
            name(&value[..open]),
            signature_types(value).join(", ")
        ),
        None => name(value),
    }
}

/// An operator reference: `OPERATOR(schema.op)`, `schema.op` or `op`
/// as `schema.op` with a canonical schema, or `op` for an operator
/// with no schema or in `pg_catalog`
pub(crate) fn operator(value: &str) -> String {
    let value = value.trim();
    let inner = value
        .get(..9)
        .filter(|keyword| keyword.eq_ignore_ascii_case("OPERATOR("))
        .and_then(|_| value[9..].strip_suffix(')'))
        .unwrap_or(value)
        .trim();
    // an operator name has no period, so the last one ends the schema
    let (schema, symbol) = match inner.rfind('.') {
        Some(index) => (name(&inner[..index]), &inner[index + 1..]),
        None => (String::new(), inner),
    };
    let symbol: String =
        symbol.chars().filter(|c| !c.is_whitespace()).collect();
    if schema.is_empty() || schema == "pg_catalog" {
        symbol
    } else {
        format!("{schema}.{symbol}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_canonical() {
        assert_eq!(name("PG_CATALOG.INT4LT"), "int4lt");
        assert_eq!(name("test.Add_Ints"), "test.add_ints");
        assert_eq!(name("\"test\".\"add_ints\""), "test.add_ints");
        assert_eq!(name("\"Quoted Schema\".f"), "\"Quoted Schema\".f");
    }

    #[test]
    fn signatures_are_canonical() {
        assert_eq!(
            signature("pg_catalog.btint4cmp(INT4, int)"),
            "btint4cmp(integer, integer)"
        );
        assert_eq!(
            signature("btint4cmp(integer,integer)"),
            "btint4cmp(integer, integer)"
        );
        assert_eq!(
            signature("TEST.POINT_PAIR_X( TEST.POINT_PAIR )"),
            "test.point_pair_x(test.point_pair)"
        );
        assert_eq!(
            signature("f(varchar(10), numeric(10, 2))"),
            "f(character varying, numeric)"
        );
        assert_eq!(signature("int4pl"), "int4pl");
        assert_eq!(
            signature_types("gist_box_same(box, box, internal)"),
            ["box", "box", "internal"]
        );
    }

    #[test]
    fn operators_are_canonical() {
        assert_eq!(operator("OPERATOR(pg_catalog.>)"), ">");
        assert_eq!(operator("operator(test.=~=)"), "test.=~=");
        assert_eq!(operator("TEST.=~="), "test.=~=");
        assert_eq!(operator("<->"), "<->");
        assert_eq!(operator("pg_catalog.<"), "<");
    }

    #[test]
    fn types_lose_pg_catalog() {
        assert_eq!(type_name("pg_catalog.int4"), "integer");
        assert_eq!(type_name("INT8"), "bigint");
        assert_eq!(argument_type("VARCHAR(10)"), "character varying");
    }
}
