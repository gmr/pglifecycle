//! Views and materialized views

use tree_sitter::Node;

use crate::ddl::{NodeExt, Statement, qualified_name, unquote};
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
        security_barrier: None,
        query,
        comment: None,
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
        storage_parameters: None,
        tablespace: node
            .find("OptTableSpace")
            .and_then(|n| n.find("name"))
            .map(|n| unquote(n.text(src))),
        query,
        comment: None,
    }))
}

/// The optional parenthesized column-name list before AS
fn view_columns(node: &Node, src: &str) -> Option<Vec<ViewColumn>> {
    let list = node.child_of_kind("opt_column_list").or_else(|| {
        node.find("create_mv_target")
            .and_then(|n| n.child_of_kind("opt_column_list"))
    })?;
    let columns: Vec<ViewColumn> = list
        .find_all("columnElem")
        .iter()
        .map(|c| ViewColumn::Name(unquote(c.text(src))))
        .collect();
    (!columns.is_empty()).then_some(columns)
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
}
