//! CreateStmt / IndexStmt / AlterTableStmt → table models

use tree_sitter::Node;

use crate::ddl::{
    NodeExt, Statement, TableConstraint, qualified_name, unquote,
};
use crate::models::{
    CheckConstraint, Column, ColumnGenerated, ConstraintColumns, ForeignKey,
    ForeignKeyReference, Index, IndexColumn, Table,
};

/// CREATE TABLE → Table (columns + inline constraints)
pub(crate) fn create_table(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let name = node
        .find("qualified_name")
        .ok_or_else(|| String::from("CREATE TABLE without a name"))?;
    let name = qualified_name(&name, src)?;
    let mut table = Table {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        sql: None,
        unlogged: node.has("kw_unlogged").then_some(true),
        from_type: None,
        parents: None,
        like_table: None,
        columns: None,
        indexes: None,
        primary_key: None,
        check_constraints: None,
        unique_constraints: None,
        foreign_keys: None,
        triggers: None,
        partition: None,
        partitions: None,
        access_method: None,
        storage_parameters: None,
        tablespace: None,
        index_tablespace: None,
        server: None,
        options: None,
        comment: None,
    };
    let mut columns = Vec::new();
    for element in node.find_all("TableElement") {
        if let Some(column_def) = element.child_of_kind("columnDef") {
            columns.push(column(&column_def, src));
        } else if let Some(constraint) =
            element.child_of_kind("TableConstraint")
        {
            let (name, parsed) = table_constraint(&constraint, src)?;
            apply_constraint(&mut table, name, parsed);
        }
    }
    if !columns.is_empty() {
        table.columns = Some(columns);
    }
    Ok(Statement::CreateTable(Box::new(table)))
}

fn column(node: &Node, src: &str) -> Column {
    let name = node
        .child_of_kind("ColId")
        .map(|n| unquote(n.text(src)))
        .unwrap_or_default();
    let data_type = node
        .child_of_kind("Typename")
        .map(|n| n.text(src).to_string())
        .unwrap_or_default();
    let mut column = Column {
        name,
        data_type,
        nullable: None,
        default: None,
        collation: None,
        check_constraint: None,
        generated: None,
        comment: None,
    };
    for constraint in node.find_all("ColConstraintElem") {
        if constraint.has("kw_default") {
            if let Some(expr) = constraint.child_of_kind("b_expr") {
                column.default =
                    Some(serde_json::Value::String(expr.text(src).into()));
            }
        } else if constraint.has("kw_not") && constraint.has("kw_null") {
            column.nullable = Some(false);
        } else if constraint.has("kw_check") {
            if let Some(expr) = constraint.find("a_expr") {
                column.check_constraint = Some(expr.text(src).to_string());
            }
        } else if constraint.has("kw_identity") {
            column.generated = Some(ColumnGenerated {
                expression: None,
                sequence: None,
                sequence_behavior: Some(
                    if constraint.has("kw_always") {
                        "ALWAYS"
                    } else {
                        "BY DEFAULT"
                    }
                    .to_string(),
                ),
            });
        } else if constraint.has("kw_generated")
            && let Some(expr) = constraint.find("a_expr")
        {
            column.generated = Some(ColumnGenerated {
                expression: Some(expr.text(src).to_string()),
                sequence: None,
                sequence_behavior: None,
            });
        }
    }
    // COLLATE lives in the column qualifier list alongside constraints
    for qual in node.find_all("ColConstraint") {
        if qual.has("kw_collate")
            && let Some(name) = qual.child_of_kind("any_name")
        {
            column.collation = Some(name.text(src).to_string());
        }
    }
    column
}

/// CREATE INDEX → (table, Index)
pub(crate) fn create_index(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    let table = node
        .find("relation_expr")
        .and_then(|n| n.find("qualified_name"))
        .ok_or_else(|| String::from("CREATE INDEX without a relation"))?;
    let table = qualified_name(&table, src)?;
    let name = node
        .child_of_kind("opt_single_name")
        .map(|n| unquote(n.text(src)))
        .unwrap_or_default();
    let columns: Vec<IndexColumn> = node
        .find_all("index_elem")
        .iter()
        .map(|elem| index_column(elem, src))
        .collect();
    let index = Index {
        name,
        sql: None,
        unique: node.has("opt_unique").then_some(true),
        recurse: None,
        parent: None,
        method: node
            .child_of_kind("access_method_clause")
            .and_then(|n| n.child_of_kind("name"))
            .map(|n| unquote(n.text(src))),
        columns: (!columns.is_empty()).then_some(columns),
        include: node.find("opt_c_include").map(|n| {
            n.find_all("columnElem")
                .iter()
                .map(|c| unquote(c.text(src)))
                .collect()
        }),
        where_clause: node
            .child_of_kind("where_clause")
            .and_then(|n| n.find("a_expr"))
            .map(|n| n.text(src).to_string()),
        storage_parameters: None,
        tablespace: node
            .child_of_kind("OptTableSpace")
            .and_then(|n| n.find("name"))
            .map(|n| unquote(n.text(src))),
        comment: None,
    };
    Ok(Statement::CreateIndex { table, index })
}

fn index_column(node: &Node, src: &str) -> IndexColumn {
    let name = node.child_of_kind("ColId").map(|n| unquote(n.text(src)));
    let expression = if name.is_none() {
        node.child_of_kind("func_expr_windowless")
            .or_else(|| node.child_of_kind("a_expr"))
            .map(|n| n.text(src).to_string())
    } else {
        None
    };
    IndexColumn {
        name,
        expression,
        collation: node
            .find("opt_collate")
            .and_then(|n| n.find("any_name"))
            .map(|n| n.text(src).to_string()),
        opclass: node
            .find("opt_qualified_name")
            .map(|n| n.text(src).to_string()),
        direction: direction(node),
        null_placement: node.find("opt_nulls_order").map(|n| {
            if n.has("kw_first") { "FIRST" } else { "LAST" }.to_string()
        }),
    }
}

fn direction(node: &Node) -> Option<String> {
    node.find("opt_asc_desc")
        .map(|n| if n.has("kw_desc") { "DESC" } else { "ASC" }.to_string())
}

/// ALTER TABLE ... ADD CONSTRAINT, one statement per command
/// (other forms → Unsupported)
pub(crate) fn alter_table(
    node: &Node,
    src: &str,
) -> Result<Vec<Statement>, String> {
    let table = node
        .find("relation_expr")
        .and_then(|n| n.find("qualified_name"))
        .ok_or_else(|| String::from("ALTER TABLE without a relation"))?;
    let table = qualified_name(&table, src)?;
    let mut statements = Vec::new();
    for cmd in node.find_all("alter_table_cmd") {
        if !cmd.has("kw_add") {
            continue;
        }
        let Some(constraint) = cmd.find("TableConstraint") else {
            continue;
        };
        let (name, parsed) = table_constraint(&constraint, src)?;
        statements.push(Statement::AddConstraint {
            table: table.clone(),
            name,
            constraint: parsed,
        });
    }
    if statements.is_empty() {
        statements.push(Statement::Unsupported(format!(
            "ALTER TABLE {table}: {}",
            crate::ddl::truncate(node.text(src), 80)
        )));
    }
    Ok(statements)
}

/// Parse a TableConstraint node into (name, constraint)
fn table_constraint(
    node: &Node,
    src: &str,
) -> Result<(Option<String>, TableConstraint), String> {
    let name = node.child_of_kind("name").map(|n| unquote(n.text(src)));
    let elem = node
        .child_of_kind("ConstraintElem")
        .ok_or_else(|| String::from("constraint without ConstraintElem"))?;
    let constraint = if elem.has("kw_foreign") {
        TableConstraint::ForeignKey(foreign_key(
            &elem,
            src,
            name.clone().unwrap_or_default(),
        )?)
    } else if elem.has("kw_primary") {
        TableConstraint::PrimaryKey(constraint_columns(&elem, src))
    } else if elem.has("kw_unique") {
        TableConstraint::Unique(constraint_columns(&elem, src))
    } else if elem.has("kw_check") {
        let expression = elem
            .find("a_expr")
            .map(|n| n.text(src).to_string())
            .ok_or_else(|| String::from("CHECK without an expression"))?;
        TableConstraint::Check(expression)
    } else {
        return Err(format!(
            "unsupported constraint: {}",
            crate::ddl::truncate(elem.text(src), 80)
        ));
    };
    Ok((name, constraint))
}

fn constraint_columns(elem: &Node, src: &str) -> ConstraintColumns {
    let columns = column_list(elem, src);
    let include: Vec<String> = elem
        .find("opt_c_include")
        .map(|n| {
            n.find_all("columnElem")
                .iter()
                .map(|c| unquote(c.text(src)))
                .collect()
        })
        .unwrap_or_default();
    if include.is_empty() {
        ConstraintColumns::Columns(columns)
    } else {
        ConstraintColumns::Detailed {
            columns,
            include: Some(include),
        }
    }
}

fn column_list(node: &Node, src: &str) -> Vec<String> {
    node.child_of_kind("columnList")
        .map(|list| {
            list.find_all("columnElem")
                .iter()
                .map(|c| unquote(c.text(src)))
                .collect()
        })
        .unwrap_or_default()
}

fn foreign_key(
    elem: &Node,
    src: &str,
    name: String,
) -> Result<ForeignKey, String> {
    let columns = column_list(elem, src);
    let references = elem
        .find("qualified_name")
        .ok_or_else(|| String::from("FOREIGN KEY without a reference"))?;
    let references = qualified_name(&references, src)?;
    let ref_columns: Vec<String> = elem
        .child_of_kind("opt_column_and_period_list")
        .map(|n| {
            n.find_all("columnElem")
                .iter()
                .map(|c| unquote(c.text(src)))
                .collect()
        })
        .unwrap_or_default();
    let mut on_delete = None;
    let mut on_update = None;
    if let Some(actions) = elem.child_of_kind("key_actions") {
        if let Some(delete) = actions.child_of_kind("key_delete") {
            on_delete = delete
                .child_of_kind("key_action")
                .map(|n| n.text(src).to_uppercase());
        }
        if let Some(update) = actions.child_of_kind("key_update") {
            on_update = update
                .child_of_kind("key_action")
                .map(|n| n.text(src).to_uppercase());
        }
    }
    Ok(ForeignKey {
        name,
        columns,
        references: ForeignKeyReference {
            name: references.to_string(),
            columns: ref_columns,
        },
        match_type: elem.find("key_match").map(|n| {
            if n.has("kw_full") {
                "FULL"
            } else if n.has("kw_partial") {
                "PARTIAL"
            } else {
                "SIMPLE"
            }
            .to_string()
        }),
        on_delete,
        on_update,
        deferrable: None,
        initially_deferred: None,
    })
}

/// Merge a parsed constraint into a table model
pub(crate) fn apply_constraint(
    table: &mut Table,
    name: Option<String>,
    constraint: TableConstraint,
) {
    match constraint {
        TableConstraint::PrimaryKey(columns) => {
            table.primary_key = Some(columns);
        }
        TableConstraint::Unique(columns) => {
            table
                .unique_constraints
                .get_or_insert_default()
                .push(columns);
        }
        TableConstraint::Check(expression) => {
            table.check_constraints.get_or_insert_default().push(
                CheckConstraint {
                    name: name.unwrap_or_default(),
                    expression,
                },
            );
        }
        TableConstraint::ForeignKey(fk) => {
            table.foreign_keys.get_or_insert_default().push(fk);
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::ddl::Parser;

    fn parse_one(sql: &str) -> Statement {
        let mut parser = Parser::new().unwrap();
        let mut statements = parser.parse(sql).unwrap();
        assert_eq!(statements.len(), 1, "expected one statement");
        statements.remove(0)
    }

    #[test]
    fn parses_create_table() {
        let statement = parse_one(
            "CREATE TABLE test.users (\n\
             id uuid DEFAULT public.uuid_generate_v4() NOT NULL,\n\
             state test.user_state DEFAULT 'unverified'::test.user_state \
             NOT NULL,\n\
             email test.email_address NOT NULL,\n\
             icon oid\n\
             );",
        );
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable, got {statement:?}")
        };
        assert_eq!(table.schema, "test");
        assert_eq!(table.name, "users");
        let columns = table.columns.unwrap();
        assert_eq!(columns.len(), 4);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].data_type, "uuid");
        assert_eq!(
            columns[0].default,
            Some(json!("public.uuid_generate_v4()"))
        );
        assert_eq!(columns[0].nullable, Some(false));
        assert_eq!(
            columns[1].default,
            Some(json!("'unverified'::test.user_state"))
        );
        assert_eq!(columns[3].name, "icon");
        assert_eq!(columns[3].nullable, None);
        assert_eq!(columns[3].default, None);
    }

    #[test]
    fn parses_identity_and_inline_constraints() {
        let statement = parse_one(
            "CREATE TABLE test.t (\n\
             id bigint GENERATED ALWAYS AS IDENTITY,\n\
             total numeric CHECK (total > 0),\n\
             CONSTRAINT t_pkey PRIMARY KEY (id),\n\
             UNIQUE (total)\n\
             );",
        );
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable")
        };
        let columns = table.columns.unwrap();
        assert_eq!(
            columns[0].generated,
            Some(ColumnGenerated {
                expression: None,
                sequence: None,
                sequence_behavior: Some("ALWAYS".into()),
            })
        );
        assert_eq!(columns[1].check_constraint, Some("total > 0".into()));
        assert_eq!(
            table.primary_key,
            Some(ConstraintColumns::Columns(vec!["id".into()]))
        );
        assert_eq!(
            table.unique_constraints,
            Some(vec![ConstraintColumns::Columns(vec!["total".into()])])
        );
    }

    #[test]
    fn parses_create_index() {
        let statement = parse_one(
            "CREATE UNIQUE INDEX users_unique_email ON test.users \
             USING btree (email);",
        );
        let Statement::CreateIndex { table, index } = statement else {
            panic!("expected CreateIndex")
        };
        assert_eq!(table.to_string(), "test.users");
        assert_eq!(index.name, "users_unique_email");
        assert_eq!(index.unique, Some(true));
        assert_eq!(index.method, Some("btree".into()));
        let columns = index.columns.unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, Some("email".into()));
    }

    #[test]
    fn parses_partial_index() {
        let statement = parse_one("CREATE INDEX i ON t (c) WHERE d IS NULL;");
        let Statement::CreateIndex { index, .. } = statement else {
            panic!("expected CreateIndex")
        };
        assert_eq!(index.where_clause, Some("d IS NULL".into()));
        assert_eq!(index.unique, None);
    }

    #[test]
    fn parses_index_options() {
        let statement = parse_one(
            "CREATE INDEX i ON t (created_at DESC NULLS LAST) \
             WHERE deleted_at IS NULL;",
        );
        let Statement::CreateIndex { index, .. } = statement else {
            panic!("expected CreateIndex")
        };
        let columns = index.columns.unwrap();
        assert_eq!(columns[0].direction, Some("DESC".into()));
        assert_eq!(columns[0].null_placement, Some("LAST".into()));
        assert_eq!(index.where_clause, Some("deleted_at IS NULL".into()));
    }

    #[test]
    fn parses_alter_table_primary_key() {
        let statement = parse_one(
            "ALTER TABLE ONLY test.users\n    \
             ADD CONSTRAINT users_pkey PRIMARY KEY (id);",
        );
        let Statement::AddConstraint {
            table,
            name,
            constraint,
        } = statement
        else {
            panic!("expected AddConstraint, got {statement:?}")
        };
        assert_eq!(table.to_string(), "test.users");
        assert_eq!(name, Some("users_pkey".into()));
        assert_eq!(
            constraint,
            TableConstraint::PrimaryKey(ConstraintColumns::Columns(vec![
                "id".into()
            ]))
        );
    }

    #[test]
    fn parses_alter_table_foreign_key() {
        let statement = parse_one(
            "ALTER TABLE ONLY test.addresses\n    \
             ADD CONSTRAINT addresses_user_id_fkey FOREIGN KEY (user_id) \
             REFERENCES test.users(id) ON UPDATE CASCADE \
             ON DELETE CASCADE;",
        );
        let Statement::AddConstraint {
            name, constraint, ..
        } = statement
        else {
            panic!("expected AddConstraint")
        };
        assert_eq!(name, Some("addresses_user_id_fkey".into()));
        let TableConstraint::ForeignKey(fk) = constraint else {
            panic!("expected ForeignKey")
        };
        assert_eq!(fk.columns, vec!["user_id"]);
        assert_eq!(fk.references.name, "test.users");
        assert_eq!(fk.references.columns, vec!["id"]);
        assert_eq!(fk.on_delete, Some("CASCADE".into()));
        assert_eq!(fk.on_update, Some("CASCADE".into()));
    }

    #[test]
    fn parses_alter_table_multiple_constraints() {
        let mut parser = Parser::new().unwrap();
        let statements = parser
            .parse(
                "ALTER TABLE t ADD CONSTRAINT positive CHECK (value > 0), \
                 ADD CONSTRAINT t_value_key UNIQUE (value);",
            )
            .unwrap();
        assert_eq!(statements.len(), 2);
        let Statement::AddConstraint { name, .. } = &statements[0] else {
            panic!("expected AddConstraint, got {:?}", statements[0])
        };
        assert_eq!(name.as_deref(), Some("positive"));
        let Statement::AddConstraint { name, .. } = &statements[1] else {
            panic!("expected AddConstraint, got {:?}", statements[1])
        };
        assert_eq!(name.as_deref(), Some("t_value_key"));
    }

    #[test]
    fn parses_alter_table_check_constraint() {
        let statement = parse_one(
            "ALTER TABLE t ADD CONSTRAINT positive CHECK (value > 0);",
        );
        let Statement::AddConstraint {
            name, constraint, ..
        } = statement
        else {
            panic!("expected AddConstraint")
        };
        assert_eq!(name, Some("positive".into()));
        assert_eq!(constraint, TableConstraint::Check("value > 0".into()));
    }

    #[test]
    fn quoted_identifiers_unquote() {
        let statement =
            parse_one("CREATE TABLE \"Sch\"\"ema\".\"Tab le\" (id int);");
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable")
        };
        assert_eq!(table.schema, "Sch\"ema");
        assert_eq!(table.name, "Tab le");
    }

    #[test]
    fn other_statements_are_unsupported() {
        let statement = parse_one("VACUUM ANALYZE test.users;");
        assert!(matches!(statement, Statement::Unsupported(_)));
    }
}
