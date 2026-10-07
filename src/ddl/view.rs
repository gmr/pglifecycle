//! Views and materialized views

use tree_sitter::Node;

use crate::ddl::object::reloptions;
use crate::ddl::{NodeExt, Statement, column_elems, qualified_name, unquote};
use crate::models::{MaterializedView, View, ViewColumn};

/// CREATE [OR REPLACE] VIEW → View
pub(crate) fn create_view(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .find("qualified_name")
        .ok_or_else(|| String::from("CREATE VIEW without a name"))?;
    let name = qualified_name(&name, src)?;
    let columns = view_columns(node, src);
    let query = node
        .child_of_kind("SelectStmt")
        .map(|n| n.text(src).to_string());
    Ok(Statement::CreateView(View {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        sql: None,
        recursive: node.has("kw_recursive").then_some(true),
        columns,
        check_option: node.find("opt_check_option").map(|n| {
            if n.has("kw_local") {
                "LOCAL"
            } else {
                "CASCADED"
            }
            .to_string()
        }),
        security_barrier: node
            .child_of_kind("opt_reloptions")
            .and_then(|n| reloptions(&n, src))
            .and_then(|m| m.get("security_barrier").cloned())
            .and_then(|v| match v {
                serde_json::Value::String(s) => pg_bool(&s),
                _ => None,
            }),
        query,
        triggers: None,
        rules: None,
        comment: None,
        security_labels: None,
    }))
}

/// CREATE MATERIALIZED VIEW → MaterializedView
pub(crate) fn create_materialized_view(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .find("create_mv_target")
        .and_then(|n| n.find("qualified_name"))
        .ok_or_else(|| {
            String::from("CREATE MATERIALIZED VIEW without a name")
        })?;
    let name = qualified_name(&name, src)?;
    let query = node
        .child_of_kind("SelectStmt")
        .map(|n| n.text(src).to_string());
    let columns = view_columns(node, src);
    Ok(Statement::CreateMaterializedView(MaterializedView {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        sql: None,
        columns,
        table_access_method: node
            .find("table_access_method_clause")
            .and_then(|n| n.find("name"))
            .map(|n| unquote(n.text(src))),
        storage_parameters: node
            .find("opt_reloptions")
            .and_then(|n| reloptions(&n, src)),
        tablespace: node
            .find("OptTableSpace")
            .and_then(|n| n.find("name"))
            .map(|n| unquote(n.text(src))),
        query,
        indexes: None,
        comment: None,
        security_labels: None,
    }))
}

/// The optional parenthesized column-name list before AS
fn view_columns(node: &Node, src: &str) -> Option<Vec<ViewColumn>> {
    let list = node.child_of_kind("opt_column_list").or_else(|| {
        node.find("create_mv_target")
            .and_then(|n| n.child_of_kind("opt_column_list"))
    })?;
    let columns: Vec<ViewColumn> = column_elems(&list, src)
        .into_iter()
        .map(ViewColumn::Name)
        .collect();
    (!columns.is_empty()).then_some(columns)
}

/// The names of the output columns of a query, as PostgreSQL gives
/// them: the name after `AS` (or a label with no `AS`), or the last
/// name of a column reference. `pg_get_viewdef` writes `AS` for each
/// other expression. None when a name is not known this way (`*`, an
/// expression with no label, a query in parentheses, VALUES): then the
/// caller cannot compare the columns
pub(crate) fn query_column_names(query: &str) -> Option<Vec<String>> {
    use crate::deploy::routine_body::identifier;
    let sql = format!("{};", query.trim().trim_end_matches(';'));
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_postgres::LANGUAGE.into())
        .ok()?;
    let tree = parser.parse(&sql, None)?;
    let root = tree.root_node();
    if root.has_error() {
        return None;
    }
    let statement = root.find("SelectStmt")?;
    let query = statement.child_of_kind("select_no_parens")?;
    // a WITH query has its outer query in a select_clause
    let mut select = query.child_of_kind("simple_select").or_else(|| {
        query
            .child_of_kind("select_clause")?
            .child_of_kind("simple_select")
    })?;
    // a set operation has the names of its first query
    while let Some(first) = select.child_of_kind("select_clause") {
        select = first.child_of_kind("simple_select")?;
    }
    select.child_of_kind("kw_select")?;
    let list = select
        .child_of_kind("opt_target_list")
        .and_then(|list| list.child_of_kind("target_list"))
        .or_else(|| select.child_of_kind("target_list"))?;
    let mut targets = Vec::new();
    target_elements(list, &mut targets);
    targets
        .iter()
        .map(|target| {
            if let Some(label) = target
                .child_of_kind("ColLabel")
                .or_else(|| target.child_of_kind("BareColLabel"))
            {
                return Some(identifier(label.text(&sql)));
            }
            // a column reference in its expression, with no operator
            let mut node = target.child_of_kind("a_expr")?;
            while matches!(node.kind(), "a_expr" | "c_expr")
                && node.child_count() == 1
            {
                node = node.child(0)?;
            }
            if node.kind() != "columnref" {
                return None;
            }
            let name = match node.child_of_kind("indirection") {
                Some(indirection) => {
                    // the last element must be a name, not a subscript
                    // or `*`
                    let last = indirection
                        .child(indirection.child_count().checked_sub(1)?)?;
                    last.child_of_kind("attr_name")?.text(&sql)
                }
                None => node.child_of_kind("ColId")?.text(&sql),
            };
            Some(identifier(name))
        })
        .collect()
}

/// The `target_el` nodes of a `target_list`, which the grammar nests
fn target_elements<'tree>(
    list: tree_sitter::Node<'tree>,
    targets: &mut Vec<tree_sitter::Node<'tree>>,
) {
    let mut cursor = list.walk();
    for child in list.children(&mut cursor) {
        match child.kind() {
            "target_list" => target_elements(child, targets),
            "target_el" => targets.push(child),
            _ => {}
        }
    }
}

/// Parse a PostgreSQL boolean reloption value. PostgreSQL accepts
/// `true/t/on/yes/y/1` and `false/f/off/no/n/0` (case-insensitively)
/// for boolean reloptions; a bare option key round-trips through
/// `reloptions` as `"true"`.
fn pg_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "on" | "yes" | "y" | "1" => Some(true),
        "false" | "f" | "off" | "no" | "n" | "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::Parser;

    fn parse_one(sql: &str) -> Statement {
        let mut parser = Parser::new().unwrap();
        let mut statements = parser.parse(sql).unwrap();
        assert_eq!(statements.len(), 1, "expected one statement");
        statements.remove(0)
    }

    #[test]
    fn parses_create_view() {
        let Statement::CreateView(view) = parse_one(
            "CREATE VIEW test.us_users AS\n SELECT id,\n    email\n   \
             FROM test.users\n  WHERE (country = 'US'::text);",
        ) else {
            panic!("expected CreateView")
        };
        assert_eq!(view.schema, "test");
        assert_eq!(view.name, "us_users");
        let query = view.query.unwrap();
        assert!(query.starts_with("SELECT id,"));
        assert!(query.ends_with("WHERE (country = 'US'::text)"));
    }

    #[test]
    fn parses_view_columns() {
        let Statement::CreateView(view) =
            parse_one("CREATE VIEW v (a, b) AS SELECT 1, 2;")
        else {
            panic!("expected CreateView")
        };
        assert_eq!(
            view.columns,
            Some(vec![
                ViewColumn::Name("a".into()),
                ViewColumn::Name("b".into())
            ])
        );
    }

    #[test]
    fn query_column_names_of_a_with_query() {
        assert_eq!(
            query_column_names(
                "WITH t AS (SELECT 1 AS inner_col) SELECT inner_col AS a, \
                 t.inner_col FROM t"
            ),
            Some(vec!["a".into(), "inner_col".into()])
        );
        assert_eq!(
            query_column_names(
                "WITH t AS (SELECT 1 AS x) SELECT x AS a FROM t \
                 UNION SELECT 2"
            ),
            Some(vec!["a".into()])
        );
    }

    #[test]
    fn parses_materialized_view() {
        let Statement::CreateMaterializedView(view) = parse_one(
            "CREATE MATERIALIZED VIEW test.mv AS\n SELECT id FROM \
             test.users\n  WITH NO DATA;",
        ) else {
            panic!("expected CreateMaterializedView")
        };
        assert_eq!(view.schema, "test");
        assert_eq!(view.name, "mv");
        assert_eq!(view.query, Some("SELECT id FROM test.users".into()));
    }

    #[test]
    fn parses_view_security_barrier() {
        let Statement::CreateView(view) = parse_one(
            "CREATE VIEW test.v WITH (security_barrier=true) AS \
             SELECT 1;",
        ) else {
            panic!("expected CreateView")
        };
        assert_eq!(view.security_barrier, Some(true));
    }

    #[test]
    fn parses_view_bare_security_barrier() {
        let Statement::CreateView(view) = parse_one(
            "CREATE VIEW test.v WITH (security_barrier) AS SELECT 1;",
        ) else {
            panic!("expected CreateView")
        };
        assert_eq!(view.security_barrier, Some(true));
    }

    #[test]
    fn parses_view_security_barrier_boolean_forms() {
        for (sql, expected) in [
            ("WITH (security_barrier=on)", Some(true)),
            ("WITH (security_barrier=off)", Some(false)),
            ("WITH (security_barrier='no')", Some(false)),
            ("WITH (security_barrier='t')", Some(true)),
            ("WITH (security_barrier='f')", Some(false)),
            ("WITH (security_barrier='y')", Some(true)),
            ("WITH (security_barrier='n')", Some(false)),
        ] {
            let stmt =
                parse_one(&format!("CREATE VIEW test.v {sql} AS SELECT 1;"));
            let Statement::CreateView(view) = stmt else {
                panic!("expected CreateView")
            };
            assert_eq!(view.security_barrier, expected, "for {sql}");
        }
    }

    #[test]
    fn parses_materialized_view_storage_parameters() {
        let Statement::CreateMaterializedView(view) = parse_one(
            "CREATE MATERIALIZED VIEW test.mv WITH (fillfactor=90) AS \
             SELECT id FROM test.users;",
        ) else {
            panic!("expected CreateMaterializedView")
        };
        assert_eq!(
            view.storage_parameters.unwrap().get("fillfactor"),
            Some(&serde_json::Value::String("90".into()))
        );
    }
}
