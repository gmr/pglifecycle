//! A SQL-standard routine body (`BEGIN ATOMIC ... END`) in the form
//! that deploy compares.
//!
//! PostgreSQL keeps the body as a parsed query, and writes it again
//! from that query. A string literal or a NULL that has no type gets
//! its type when PostgreSQL reads it, and PostgreSQL writes it with a
//! cast: `SELECT 'x'` is `SELECT 'x'::text`. The output column of `'x'`
//! has the name `?column?`, and PostgreSQL writes no `AS` for that
//! name. But the output column of `'x'::text` has the name `text` (the
//! name of the type), so the routine that deploy makes from the body
//! that pull wrote is `SELECT 'x'::text AS text`. The two bodies do
//! the same work: the name of an output column of a statement in a
//! routine body has no effect on the result of the routine.

use tree_sitter::Node;

use crate::ddl::NodeExt;
use crate::deploy::identity_type;

/// The body (`definition`) of a routine in `language` in the form that
/// deploy compares. Pull formats a SQL or PL/pgSQL body, and the
/// formatter adds or removes space at the start and end of the body. A
/// body that a person writes often has no such space. That space has no
/// effect in these languages, so it is removed. Only the space that the
/// PostgreSQL scanner ignores (`[ \t\n\r\f\v]`) is removed; for
/// example, U+00A0 stays. The space in the body stays. A body in
/// another language stays as it is: for example, the space at the
/// start of a PL/Python body is its indent.
///
/// Pull also formats a SQL body in the `pg_dump` style, which changes
/// its layout: `SELECT a, a;` is on two lines. Thus a SQL body is also
/// in that style, and a body in another layout is not a change. The
/// formatter gives the same text when it formats its own text again.
/// A body that the formatter cannot read stays as it is.
pub(crate) fn canonical_definition(
    body: &str,
    language: Option<&str>,
) -> String {
    if language.is_some_and(|language| language.eq_ignore_ascii_case("sql"))
        && let Some(formatted) = crate::pull::format_pg_dump(body)
    {
        return formatted.trim_matches(scanner_space).to_string();
    }
    let formatted = language.is_some_and(|language| {
        language.eq_ignore_ascii_case("sql")
            || language.eq_ignore_ascii_case("plpgsql")
    });
    if formatted {
        body.trim_matches(scanner_space)
    } else {
        body
    }
    .to_string()
}

/// The space that the PostgreSQL scanner ignores. `trim_ascii` keeps
/// `\v`, so the set is given here.
fn scanner_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0c' | '\x0b')
}

/// The body without the space that the PostgreSQL scanner ignores at
/// its start and end, and without each `AS` name of an output column
/// that is the name that PostgreSQL gives to the column when it has no
/// `AS`, for a column that is a constant with a cast (`'x'::text AS
/// text`), also in parentheses or with a COLLATE clause, and for a
/// subquery that gives one of these columns. A body that the grammar
/// cannot read keeps its `AS` names.
pub(crate) fn canonical_sql_body(body: &str) -> String {
    let body = body.trim_matches(scanner_space);
    // the grammar reads a body only in its statement
    let prefix = "CREATE FUNCTION f() RETURNS void LANGUAGE sql ";
    let source = format!("{prefix}{body};");
    let mut parser = tree_sitter::Parser::new();
    if parser
        .set_language(&tree_sitter_postgres::LANGUAGE.into())
        .is_err()
    {
        return body.to_string();
    }
    let Some(tree) = parser.parse(&source, None) else {
        return body.to_string();
    };
    let root = tree.root_node();
    let Some(routine) = root.find("opt_routine_body") else {
        return body.to_string();
    };
    if root.has_error() {
        return body.to_string();
    }
    // the text from the end of each expression to the end of its name
    let mut targets = Vec::new();
    targets_in(&routine, &mut targets);
    let mut removed: Vec<(usize, usize)> = targets
        .into_iter()
        .filter_map(|target| {
            let expression = target.child_of_kind("a_expr")?;
            let label = target.child_of_kind("ColLabel")?;
            target.child_of_kind("kw_as")?;
            let name = column_name(&expression, &source)?;
            (name == identifier(label.text(&source)))
                .then(|| (expression.end_byte(), target.end_byte()))
        })
        .collect();
    removed.sort_unstable();
    let mut result = String::with_capacity(body.len());
    let mut position = prefix.len();
    for (start, end) in removed {
        result.push_str(&source[position..start]);
        position = end;
    }
    result.push_str(&source[position..prefix.len() + body.len()]);
    result
}

/// Each output column in the tree, also in a subquery
fn targets_in<'tree>(node: &Node<'tree>, targets: &mut Vec<Node<'tree>>) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == "target_el" {
            targets.push(child);
        }
        targets_in(&child, targets);
    }
}

/// The name that PostgreSQL gives to an output column of the
/// expression with no `AS`, as `FigureColname` finds it: none for a
/// constant, which then has the name `?column?`. The outer option is
/// none for an expression of another kind, whose name is not found
/// here.
fn column_name(node: &Node, source: &str) -> Option<String> {
    name_of(node, source)?
}

fn name_of(node: &Node, source: &str) -> Option<Option<String>> {
    let mut cursor = node.walk();
    let children: Vec<Node> = node.children(&mut cursor).collect();
    let kinds: Vec<&str> = children.iter().map(Node::kind).collect();
    match (node.kind(), kinds.as_slice()) {
        ("AexprConst", _) => Some(None),
        // a subquery of one value: the name of the column of the
        // subquery
        ("c_expr", ["select_with_parens"]) => {
            subquery_name(&children[0], source)
        }
        (_, [_]) => name_of(&children[0], source),
        // a cast: the name of its argument, or else the name of the type
        (_, [_, "::", "Typename"]) => match name_of(&children[0], source)? {
            None => Some(Some(type_name(children[2].text(source)))),
            name => Some(name),
        },
        (_, [_, "kw_collate", _]) => name_of(&children[0], source),
        ("c_expr", ["(", "a_expr", ")"]) => name_of(&children[1], source),
        _ => None,
    }
}

/// The name of the first output column of a subquery that is one
/// SELECT, not a set operation
fn subquery_name(node: &Node, source: &str) -> Option<Option<String>> {
    let select = node
        .child_of_kind("select_no_parens")?
        .child_of_kind("simple_select")?;
    select.child_of_kind("kw_select")?;
    let target = select.find("target_el")?;
    match target.child_of_kind("ColLabel") {
        Some(label) => Some(Some(identifier(label.text(source)))),
        None => name_of(&target.child_of_kind("a_expr")?, source),
    }
}

/// The name that PostgreSQL gives to a cast column: the last name of
/// the type as the grammar reads it, which is the name in pg_type for
/// a built-in type (`character varying` is `varchar`)
fn type_name(data_type: &str) -> String {
    let data_type = identity_type(data_type);
    let data_type = data_type.trim_end_matches("[]");
    let name = match data_type {
        "bigint" => "int8",
        "bit varying" => "varbit",
        "boolean" => "bool",
        "character" => "bpchar",
        "character varying" => "varchar",
        "double precision" => "float8",
        "integer" => "int4",
        "real" => "float4",
        "smallint" => "int2",
        "time with time zone" => "timetz",
        "time without time zone" => "time",
        "timestamp with time zone" => "timestamptz",
        "timestamp without time zone" => "timestamp",
        other => other,
    };
    // the last name, after a `.` that is not in quotes
    let mut quoted = false;
    let mut start = 0;
    for (index, c) in name.char_indices() {
        match c {
            '"' => quoted = !quoted,
            '.' if !quoted => start = index + 1,
            _ => {}
        }
    }
    identifier(&name[start..])
}

/// A name as PostgreSQL keeps it: a quoted name with no quotes, and a
/// name with no quotes with its ASCII letters in lowercase
pub(crate) fn identifier(name: &str) -> String {
    match name.strip_prefix('"').and_then(|n| n.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => name.to_ascii_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn atomic(statements: &str) -> String {
        format!("BEGIN ATOMIC\n {statements}\nEND")
    }

    /// PostgreSQL folds only the ASCII letters of a name with no quotes
    /// to lowercase
    #[test]
    fn an_identifier_folds_only_ascii_letters() {
        assert_eq!(identifier("Größe"), "größe");
        assert_eq!(identifier("GRÖßE"), "grÖße");
        assert_eq!(identifier("\"GRÖßE\""), "GRÖßE");
    }

    /// The body that pull writes against the body of the routine that
    /// deploy makes from it, as PostgreSQL 18 writes each one
    fn same(pulled: &str, made: &str) {
        let pulled = atomic(pulled);
        let made = atomic(made);
        assert_eq!(canonical_sql_body(&made), pulled, "{made}");
        assert_eq!(canonical_sql_body(&pulled), pulled, "{pulled}");
    }

    #[test]
    fn literal_column_names_are_not_a_change() {
        same("SELECT 'e\\f'::text;", "SELECT 'e\\f'::text AS text;");
        same(
            "SELECT 'x'::text,\n     2,\n     NULL::text,\n     \
             ('y'::text COLLATE \"C\");",
            "SELECT 'x'::text AS text,\n     2,\n     NULL::text AS text,\n     \
             ('y'::text COLLATE \"C\") AS text;",
        );
        same(
            "SELECT 'x'::text\n UNION\n  SELECT DISTINCT 'y'::text\n   \
             WHERE ('z'::text = 'z'::text)\n  ORDER BY 1;",
            "SELECT 'x'::text AS text\n UNION\n  SELECT DISTINCT 'y'::text \
             AS text\n   WHERE ('z'::text = 'z'::text)\n  ORDER BY 1;",
        );
        same(
            "SELECT 'q'::text;\n SELECT ( SELECT 'x'::text);",
            "SELECT 'q'::text AS text;\n SELECT ( SELECT 'x'::text AS text) \
             AS text;",
        );
        same(
            "SELECT 'x'::character varying;",
            "SELECT 'x'::character varying AS \"varchar\";",
        );
        same("SELECT '-1'::integer;", "SELECT '-1'::integer AS int4;");
        same(
            "SELECT '-1.5'::numeric;",
            "SELECT '-1.5'::numeric AS \"numeric\";",
        );
        same(
            "SELECT '3000000000'::bigint;",
            "SELECT '3000000000'::bigint AS int8;",
        );
        same("SELECT '1'::\"bit\";", "SELECT '1'::\"bit\" AS \"bit\";");
        same(
            "SELECT '2020-01-01 00:00:00+00'::timestamp with time zone;",
            "SELECT '2020-01-01 00:00:00+00'::timestamp with time zone AS \
             timestamptz;",
        );
        same(
            "SELECT 'x'::character(3);",
            "SELECT 'x'::character(3) AS bpchar;",
        );
        same("SELECT '{}'::text[];", "SELECT '{}'::text[] AS text;");
        same(
            "SELECT 'a'::public.mood;",
            "SELECT 'a'::public.mood AS mood;",
        );
    }

    #[test]
    fn other_column_names_stay() {
        let stays = |statements: &str| {
            let body = atomic(statements);
            assert_eq!(canonical_sql_body(&body), body, "{body}");
        };
        // the name is not the name that PostgreSQL gives the column
        stays("SELECT 'x'::text AS label;");
        stays("SELECT 'x'::text AS \"Text\";");
        stays("SELECT 'x'::text AS int4;");
        stays("SELECT (t.b)::text AS text\n    FROM t;");
        stays("SELECT now() AS now;");
        stays("SELECT ARRAY['x'::text] AS \"array\";");
        stays("SELECT ( SELECT 'x'::text AS label) AS text;");
        // a column name in a string stays
        stays("SELECT 'x::text AS text'::text AS label;");
        // not a body that PostgreSQL can read
        stays("SELECT 'x'::text AS text");
        assert_eq!(canonical_sql_body("RETURN 'x'::text"), "RETURN 'x'::text");
    }

    /// A SQL body in another layout than the one that pull writes is
    /// not a change; a real change is
    #[test]
    fn sql_body_layout_is_not_a_change() {
        let sql = |body: &str| canonical_definition(body, Some("sql"));
        assert_eq!(sql("SELECT a, a;"), sql("\n SELECT a,\n    a;\n"));
        assert_eq!(sql("SELECT a, a;"), "SELECT a,\n    a;");
        assert_eq!(
            sql("SELECT a FROM (SELECT 1 AS a, 2 AS b) s;"),
            sql(" SELECT a\n   FROM ( SELECT 1 AS a,\n            2 AS b) s;")
        );
        assert_ne!(sql("SELECT a, a;"), sql("SELECT a, b;"));
        assert_ne!(sql("SELECT 'a';"), sql("SELECT 'A';"));
        // a PL/pgSQL body is not formatted
        let plpgsql = |body: &str| canonical_definition(body, Some("plpgsql"));
        assert_ne!(
            plpgsql("BEGIN RETURN 1; END"),
            plpgsql("BEGIN\nRETURN 1;\nEND")
        );
    }

    /// A real change of the body is still a change
    #[test]
    fn changed_literals_stay_different() {
        assert_ne!(
            canonical_sql_body(&atomic("SELECT 'x'::text AS text;")),
            canonical_sql_body(&atomic("SELECT 'y'::text;"))
        );
        assert_ne!(
            canonical_sql_body(&atomic("SELECT 'x'::text AS text;")),
            canonical_sql_body(&atomic("SELECT 'x'::character varying;"))
        );
    }
}
