//! CreateStmt / IndexStmt / AlterTableStmt → table models

use serde_json::Value;
use tree_sitter::Node;

use crate::ddl::object::{reloptions, string_value};
use crate::ddl::{
    NodeExt, QualifiedName, Statement, TableConstraint, any_name,
    column_elems, qualified_name, unquote,
};
use crate::models::{
    CheckConstraint, Column, ColumnGenerated, ColumnNotNull,
    ConstraintColumns, ForeignKey, ForeignKeyReference, GeneratedKind, Index,
    IndexColumn, LikeTable, NotNullConstraint, Sequence, SequenceOptions,
    Table, TablePartition, TablePartitionBehavior, TablePartitionColumn,
};
use crate::utils::quote_ident;

/// CREATE TABLE → Table (columns + inline constraints), or a
/// `PARTITION OF` child, returned as [`Statement::CreateTablePartition`]
/// so the pull assembly can fold it into its parent's `partitions`
pub(crate) fn create_table(
    node: &Node,
    src: &str,
) -> Result<Statement, String> {
    // two variants carry a second qualified_name: `PARTITION OF parent`
    // (handled below) and inline `REFERENCES` (nested inside a column
    // constraint, so it never surfaces as a *direct* child); find_all
    // here only needs the table's own name plus, for PARTITION OF, the
    // parent's
    let names = node.find_all("qualified_name");
    let name = match names.first() {
        Some(n) => qualified_name(n, src)?,
        None => return Err(String::from("CREATE TABLE without a name")),
    };
    // `PARTITION OF parent FOR VALUES ...` — a partition child, not a
    // standalone table. The kw_partition + kw_of combination is unique
    // to this form (PARTITION BY uses kw_partition + kw_by instead)
    if node.has("kw_partition") && node.has("kw_of") {
        let parent = match names.get(1) {
            Some(n) => qualified_name(n, src)?,
            None => {
                return Err(String::from(
                    "PARTITION OF without a parent table",
                ));
            }
        };
        let bound =
            node.child_of_kind("PartitionBoundSpec").ok_or_else(|| {
                String::from("PARTITION OF without a FOR VALUES clause")
            })?;
        let bound = partition_bound(&bound, src);
        return Ok(Statement::CreateTablePartition {
            parent,
            partition: TablePartition {
                name: name.name,
                schema: name.schema.unwrap_or_default(),
                default: bound.default,
                for_values_in: bound.for_values_in,
                for_values_from: bound.for_values_from,
                for_values_to: bound.for_values_to,
                for_values_with: bound.for_values_with,
                comment: None,
            },
        });
    }
    let mut table = Table {
        name: name.name,
        schema: name.schema.unwrap_or_default(),
        owner: String::new(),
        sql: None,
        unlogged: node.has("kw_unlogged").then_some(true),
        // `CREATE TABLE name OF typename` — direct child only, since
        // `any_name` also appears nested inside Typename for other
        // CreateStmt forms
        from_type: node
            .has("kw_of")
            .then(|| {
                node.child_of_kind("any_name").map(|n| {
                    let of_type = any_name(&n, src);
                    match of_type.schema {
                        Some(schema) => format!(
                            "{}.{}",
                            quote_ident(&schema),
                            quote_ident(&of_type.name)
                        ),
                        None => quote_ident(&of_type.name),
                    }
                })
            })
            .flatten(),
        parents: None,
        like_table: None,
        columns: None,
        column_defaults: None,
        indexes: None,
        primary_key: None,
        check_constraints: None,
        not_null_constraints: None,
        unique_constraints: None,
        foreign_keys: None,
        triggers: None,
        row_level_security: None,
        policies: None,
        partition: table_partition_behavior(node, src),
        partitions: None,
        access_method: node
            .child_of_kind("table_access_method_clause")
            .and_then(|n| n.child_of_kind("name"))
            .map(|n| unquote(n.text(src))),
        storage_parameters: node
            .child_of_kind("OptWith")
            .and_then(|n| reloptions(&n, src)),
        tablespace: node
            .child_of_kind("OptTableSpace")
            .and_then(|n| n.find("name"))
            .map(|n| unquote(n.text(src))),
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
            // USING INDEX TABLESPACE, on PRIMARY KEY/UNIQUE/EXCLUDE —
            // the model keeps a single table-level index_tablespace, so
            // the first constraint that carries one wins
            if table.index_tablespace.is_none() {
                table.index_tablespace = constraint
                    .find("OptConsTableSpace")
                    .and_then(|n| n.find("name"))
                    .map(|n| unquote(n.text(src)));
            }
            let (name, parsed) = table_constraint(&constraint, src)?;
            apply_constraint(&mut table, name, parsed);
        } else if let Some(like_clause) =
            element.child_of_kind("TableLikeClause")
        {
            table.like_table = Some(like_table(&like_clause, src));
        }
    }
    // `CREATE TABLE x OF type (col WITH OPTIONS ..., ...)` — a typed
    // table's element list is a different production (no columnDef,
    // since the columns themselves come from the composite type; only
    // per-column constraints via `columnOptions`, plus ordinary table
    // constraints, are legal here)
    for element in node.find_all("TypedTableElement") {
        if let Some(column_options) = element.child_of_kind("columnOptions") {
            columns.push(column(&column_options, src));
        } else if let Some(constraint) =
            element.child_of_kind("TableConstraint")
        {
            // USING INDEX TABLESPACE, on PRIMARY KEY/UNIQUE/EXCLUDE —
            // same single table-level index_tablespace as the
            // TableElement loop above; first constraint carrying one wins
            if table.index_tablespace.is_none() {
                table.index_tablespace = constraint
                    .find("OptConsTableSpace")
                    .and_then(|n| n.find("name"))
                    .map(|n| unquote(n.text(src)));
            }
            let (name, parsed) = table_constraint(&constraint, src)?;
            apply_constraint(&mut table, name, parsed);
        }
    }
    if !columns.is_empty() {
        table.columns = Some(columns);
    }
    table.parents = inherits(node, src);
    Ok(Statement::CreateTable(Box::new(table)))
}

/// `INHERITS (parent, ...)` — the parents are qualified_names scoped to
/// the OptInherit node, not the statement's own table name. Shared with
/// CREATE FOREIGN TABLE, which takes the same clause.
pub(crate) fn inherits(node: &Node, src: &str) -> Option<Vec<String>> {
    let inherit = node.child_of_kind("OptInherit")?;
    let parents: Vec<String> = inherit
        .find_all("qualified_name")
        .iter()
        .filter_map(|n| qualified_name(n, src).ok())
        .map(|q| match q.schema {
            Some(schema) => {
                format!("{}.{}", quote_ident(&schema), quote_ident(&q.name))
            }
            None => quote_ident(&q.name),
        })
        .collect();
    (!parents.is_empty()).then_some(parents)
}

pub(crate) fn column(node: &Node, src: &str) -> Column {
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
        not_null_constraint: None,
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
            // the column parser has no table, so a sequence name here
            // is kept as written rather than compared with the one
            // PostgreSQL would generate. pg_dump never writes this
            // inline form; it writes the ALTER TABLE handled below.
            column.generated =
                Some(identity(&constraint, src, None, &column.name));
        } else if constraint.has("kw_generated")
            && let Some(expr) = constraint.find("a_expr")
        {
            // PostgreSQL 18 made VIRTUAL the default, so pg_dump
            // writes the keyword only for a stored column; an absent
            // one means virtual rather than unknown
            column.generated = Some(ColumnGenerated {
                expression: Some(expr.text(src).to_string()),
                kind: Some(if constraint.has("kw_stored") {
                    GeneratedKind::Stored
                } else {
                    GeneratedKind::Virtual
                }),
                sequence: None,
                sequence_behavior: None,
                sequence_options: None,
            });
        }
    }
    // COLLATE lives in the column qualifier list alongside constraints,
    // as does a NOT NULL constraint's name — the name is a sibling of
    // the ColConstraintElem walked above, not a child of it
    for qual in node.find_all("ColConstraint") {
        if qual.has("kw_collate")
            && let Some(name) = qual.child_of_kind("any_name")
        {
            column.collation = Some(name.text(src).to_string());
            continue;
        }
        let Some(elem) = qual.child_of_kind("ColConstraintElem") else {
            continue;
        };
        if !(elem.has("kw_not") && elem.has("kw_null")) {
            continue;
        }
        // pg_dump prints the name only when it is not the generated
        // `<table>_<column>_not_null`, so any name here is worth
        // keeping; a bare NOT NULL needs nothing beyond `nullable`
        let name = qual.child_of_kind("name").map(|n| unquote(n.text(src)));
        let no_inherit = elem.child_of_kind("opt_no_inherit").map(|_| true);
        if name.is_some() || no_inherit.is_some() {
            column.not_null_constraint =
                Some(ColumnNotNull { name, no_inherit });
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
        include: node.find("opt_c_include").map(|n| column_elems(&n, src)),
        nulls_not_distinct: node
            .child_of_kind("opt_unique_null_treatment")
            .map(|n| n.has("kw_not"))
            .filter(|not_distinct| *not_distinct),
        where_clause: node
            .child_of_kind("where_clause")
            .and_then(|n| n.find("a_expr"))
            .map(|n| n.text(src).to_string()),
        storage_parameters: node
            .child_of_kind("opt_reloptions")
            .and_then(|n| reloptions(&n, src)),
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
        // ADD GENERATED ... AS IDENTITY carries kw_add too, so it has to
        // be recognised before the ADD CONSTRAINT arm, which skips any
        // ADD without a TableConstraint. That skip is how every identity
        // column used to vanish from a pulled project.
        if cmd.has("kw_add")
            && cmd.has("kw_generated")
            && cmd.has("kw_identity")
        {
            let column = cmd
                .child_of_kind("ColId")
                .map(|n| unquote(n.text(src)))
                .unwrap_or_default();
            let generated = identity(&cmd, src, Some(&table), &column);
            statements.push(Statement::AddIdentity {
                table: table.clone(),
                column,
                generated,
            });
        } else if cmd.child_of_kind("kw_row").is_some()
            && cmd.child_of_kind("kw_security").is_some()
        {
            let negated = cmd.child_of_kind("kw_no").is_some()
                || cmd.child_of_kind("kw_disable").is_some();
            let forced = cmd.child_of_kind("kw_force").is_some();
            statements.push(Statement::RowSecurity {
                table: table.clone(),
                enabled: (!forced).then_some(!negated),
                forced: forced.then_some(!negated),
            });
        } else if cmd.has("kw_add") {
            let Some(constraint) = cmd.find("TableConstraint") else {
                continue;
            };
            let (name, parsed) = table_constraint(&constraint, src)?;
            statements.push(Statement::AddConstraint {
                table: table.clone(),
                name,
                constraint: parsed,
            });
        } else if let Some(default) = cmd.child_of_kind("alter_column_default")
            && let Some(expr) = default.child_of_kind("a_expr")
        {
            let column = cmd
                .child_of_kind("ColId")
                .map(|n| unquote(n.text(src)))
                .unwrap_or_default();
            statements.push(Statement::SetColumnDefault {
                table: table.clone(),
                column,
                default: Value::String(expr.text(src).to_string()),
            });
        }
    }
    // ATTACH PARTITION lives in a `partition_cmd` child (sibling of the
    // `alter_table_cmd`s), carrying the child relation and its bounds
    for cmd in node.find_all("partition_cmd") {
        if !cmd.has("kw_attach") {
            continue;
        }
        let child = cmd
            .child_of_kind("qualified_name")
            .ok_or_else(|| String::from("ATTACH PARTITION without a child"))?;
        let child = qualified_name(&child, src)?;
        let bound =
            cmd.child_of_kind("PartitionBoundSpec").ok_or_else(|| {
                String::from("ATTACH PARTITION without a FOR VALUES clause")
            })?;
        let bound = partition_bound(&bound, src);
        statements.push(Statement::AttachPartition {
            parent: table.clone(),
            partition: TablePartition {
                name: child.name,
                schema: child.schema.unwrap_or_default(),
                default: bound.default,
                for_values_in: bound.for_values_in,
                for_values_from: bound.for_values_from,
                for_values_to: bound.for_values_to,
                for_values_with: bound.for_values_with,
                comment: None,
            },
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

/// An identity column's generation and sequence options, from either
/// the inline `GENERATED ... AS IDENTITY (...)` column constraint or the
/// `ALTER TABLE ... ADD GENERATED ... AS IDENTITY (...)` pg_dump writes.
/// `table` is known only for the ALTER form, and it lets a sequence name
/// PostgreSQL generated be dropped.
fn identity(
    node: &Node,
    src: &str,
    table: Option<&QualifiedName>,
    column: &str,
) -> ColumnGenerated {
    let behavior = if node
        .child_of_kind("generated_when")
        .is_some_and(|g| g.has("kw_always"))
    {
        "ALWAYS"
    } else {
        "BY DEFAULT"
    };
    let options = node
        .child_of_kind("OptParenthesizedSeqOptList")
        .map(|list| sequence_options(&list, src, table, column))
        .filter(|options| *options != SequenceOptions::default());
    ColumnGenerated {
        expression: None,
        kind: None,
        sequence: None,
        sequence_behavior: Some(behavior.to_string()),
        sequence_options: options,
    }
}

/// The non-default options of an identity column's sequence.
///
/// pg_dump writes every option, defaults included — `START WITH 1`,
/// `INCREMENT BY 1`, `NO MINVALUE`, `NO MAXVALUE`, `CACHE 1` — so
/// keeping them all would put five values nobody chose into every
/// identity column, and a hand-written identity that states none would
/// never compare equal to the one pulled from the database. Only a
/// value that differs from PostgreSQL's default is kept.
fn sequence_options(
    list: &Node,
    src: &str,
    table: Option<&QualifiedName>,
    column: &str,
) -> SequenceOptions {
    let mut parsed = Sequence {
        name: String::new(),
        schema: String::new(),
        owner: String::new(),
        sql: None,
        data_type: None,
        increment_by: None,
        min_value: None,
        max_value: None,
        start_with: None,
        cache: None,
        cycle: None,
        owned_by: None,
        comment: None,
    };
    crate::ddl::object::apply_seq_options(&mut parsed, list, src);
    let name = list
        .find_all("SeqOptElem")
        .into_iter()
        .find(|e| e.has("kw_name"))
        .and_then(|e| e.child_of_kind("any_name"))
        .filter(|n| {
            let name = crate::ddl::any_name(n, src);
            !is_generated_sequence_name(&name, table, column)
        })
        .map(|n| n.text(src).to_string());
    // an ascending sequence starts at its minimum and a descending one
    // at its maximum, which default to 1 and -1
    let ascending = parsed.increment_by.is_none_or(|by| by > 0);
    let default_start = if ascending {
        parsed.min_value.unwrap_or(1)
    } else {
        parsed.max_value.unwrap_or(-1)
    };
    SequenceOptions {
        name,
        start_with: parsed.start_with.filter(|start| *start != default_start),
        increment_by: parsed.increment_by.filter(|by| *by != 1),
        min_value: parsed.min_value,
        max_value: parsed.max_value,
        cache: parsed.cache.filter(|cache| *cache != 1),
        cycle: parsed.cycle.filter(|cycle| *cycle),
    }
}

/// Whether `name` is the `<table>_<column>_seq` PostgreSQL gives an
/// identity column's sequence, in the table's own schema. Both names
/// are unquoted, so a quoted `"Orders_id_seq"` matches table `Orders`.
fn is_generated_sequence_name(
    name: &QualifiedName,
    table: Option<&QualifiedName>,
    column: &str,
) -> bool {
    let Some(table) = table else {
        return false;
    };
    name.name == format!("{}_{column}_seq", table.name)
        && name
            .schema
            .as_ref()
            .is_none_or(|schema| Some(schema) == table.schema.as_ref())
}

/// Parse a TableConstraint node into (name, constraint)
pub(crate) fn table_constraint(
    node: &Node,
    src: &str,
) -> Result<(Option<String>, TableConstraint), String> {
    let name = node.child_of_kind("name").map(|n| unquote(n.text(src)));
    let elem = node
        .child_of_kind("ConstraintElem")
        .ok_or_else(|| String::from("constraint without ConstraintElem"))?;
    // FOREIGN KEY / PRIMARY KEY / UNIQUE / CHECK / NOT NULL are
    // mutually exclusive direct children of ConstraintElem (one per
    // grammar alternative), so a single child-kind scan replaces five
    // separate recursive `has()` walks of the same subtree
    let mut cursor = elem.walk();
    let kind = elem.children(&mut cursor).find_map(|c| match c.kind() {
        "kw_foreign" | "kw_primary" | "kw_unique" | "kw_check" | "kw_not" => {
            Some(c.kind())
        }
        _ => None,
    });
    let constraint = match kind {
        Some("kw_foreign") => TableConstraint::ForeignKey(foreign_key(
            &elem,
            src,
            name.clone().unwrap_or_default(),
        )?),
        Some("kw_primary") => {
            TableConstraint::PrimaryKey(constraint_columns(&elem, src, &name))
        }
        Some("kw_unique") => {
            TableConstraint::Unique(constraint_columns(&elem, src, &name))
        }
        Some("kw_check") => {
            let expression = elem
                .child_of_kind("a_expr")
                .map(|n| n.text(src).to_string())
                .ok_or_else(|| String::from("CHECK without an expression"))?;
            TableConstraint::Check(CheckConstraint {
                name: String::new(),
                expression,
                enforced: enforced(&elem),
                not_valid: not_valid(&elem),
            })
        }
        Some("kw_not") => {
            let column = elem
                .child_of_kind("ColId")
                .map(|n| unquote(n.text(src)))
                .ok_or_else(|| String::from("NOT NULL without a column"))?;
            TableConstraint::NotNull(NotNullConstraint {
                name: name.clone(),
                column,
                no_inherit: elem.has("kw_inherit").then_some(true),
                not_valid: not_valid(&elem),
            })
        }
        _ => {
            return Err(format!(
                "unsupported constraint: {}",
                crate::ddl::truncate(elem.text(src), 80)
            ));
        }
    };
    Ok((name, constraint))
}

/// `ENFORCED` / `NOT ENFORCED` from a constraint's attribute spec.
/// `None` means the clause is absent, which is enforced, the default.
/// PostgreSQL accepts the clause on CHECK and FOREIGN KEY only, so no
/// other constraint reads it.
fn enforced(elem: &Node) -> Option<bool> {
    elem.child_of_kind("ConstraintAttributeSpec")?
        .find_all("ConstraintAttributeElem")
        .iter()
        .find(|e| e.has("kw_enforced"))
        .map(|e| !e.has("kw_not"))
}

/// `NOT VALID` from a constraint's attribute spec. `None` means valid,
/// the default; there is no `VALID` keyword to make it `Some(false)`.
fn not_valid(elem: &Node) -> Option<bool> {
    elem.child_of_kind("ConstraintAttributeSpec")?
        .find_all("ConstraintAttributeElem")
        .iter()
        .any(|e| e.has("kw_valid") && e.has("kw_not"))
        .then_some(true)
}

fn constraint_columns(
    elem: &Node,
    src: &str,
    name: &Option<String>,
) -> ConstraintColumns {
    let columns = column_list(elem, src);
    let include: Vec<String> = elem
        .find("opt_c_include")
        .map(|n| column_elems(&n, src))
        .unwrap_or_default();
    // the grammar gives NULLS NOT DISTINCT its own node holding the
    // keywords, so the clause is present either way and only `kw_not`
    // distinguishes it from the default NULLS DISTINCT
    let nulls_not_distinct = elem
        .child_of_kind("opt_unique_null_treatment")
        .map(|n| n.has("kw_not"))
        .filter(|not_distinct| *not_distinct);
    // WITHOUT OVERLAPS carries no column of its own: it always applies
    // to the last column of the list
    let without_overlaps = elem.has("opt_without_overlaps").then_some(true);
    if name.is_none()
        && include.is_empty()
        && nulls_not_distinct.is_none()
        && without_overlaps.is_none()
    {
        ConstraintColumns::Columns(columns)
    } else {
        ConstraintColumns::Detailed {
            name: name.clone(),
            columns,
            include: (!include.is_empty()).then_some(include),
            nulls_not_distinct,
            without_overlaps,
        }
    }
}

fn column_list(node: &Node, src: &str) -> Vec<String> {
    node.child_of_kind("columnList")
        .map(|list| column_elems(&list, src))
        .unwrap_or_default()
}

fn foreign_key(
    elem: &Node,
    src: &str,
    name: String,
) -> Result<ForeignKey, String> {
    let columns = column_list(elem, src);
    // a temporal foreign key names its range column after PERIOD on
    // both sides. The grammar keeps each in its own optionalPeriodName
    // node, so reading the referenced side with column_elems alone
    // swept the period column into the ordinary column list and left
    // the two sides with different arities, which does not restore
    let period = elem
        .child_of_kind("optionalPeriodName")
        .and_then(|n| n.child_of_kind("columnElem"))
        .map(|n| unquote(n.text(src)));
    let references = elem
        .find("qualified_name")
        .ok_or_else(|| String::from("FOREIGN KEY without a reference"))?;
    let references = qualified_name(&references, src)?;
    let ref_period = elem
        .child_of_kind("opt_column_and_period_list")
        .and_then(|n| n.child_of_kind("optionalPeriodName"))
        .and_then(|n| n.child_of_kind("columnElem"))
        .map(|n| unquote(n.text(src)));
    let ref_columns: Vec<String> = elem
        .child_of_kind("opt_column_and_period_list")
        .and_then(|n| n.child_of_kind("columnList"))
        .map(|n| column_elems(&n, src))
        .unwrap_or_default();
    let spec = elem.child_of_kind("ConstraintAttributeSpec");
    let deferrable = spec.and_then(|s| {
        s.find_all("ConstraintAttributeElem")
            .iter()
            .find(|e| e.has("kw_deferrable"))
            .map(|e| !e.has("kw_not"))
    });
    let initially_deferred = spec.and_then(|s| {
        s.find_all("ConstraintAttributeElem")
            .iter()
            .find(|e| e.has("kw_initially"))
            .map(|e| e.has("kw_deferred"))
    });
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
            period: ref_period,
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
        deferrable,
        initially_deferred,
        period,
        enforced: enforced(elem),
        not_valid: not_valid(elem),
    })
}

/// Drop a primary key or unique constraint's name when it is the one
/// PostgreSQL generates, `<table>_pkey` or `<table>_<columns>_key`.
///
/// pg_dump always writes the name in `ALTER TABLE ... ADD CONSTRAINT`,
/// unlike a NOT NULL constraint where it writes one only when it is
/// not the default. Keeping every generated name would put a value in
/// the project for something nobody chose, and it would churn the
/// file whenever a column is renamed. The name is then written only
/// when someone picked it.
fn drop_generated_name(
    columns: &mut ConstraintColumns,
    table: &str,
    suffix: &str,
) {
    let ConstraintColumns::Detailed {
        name,
        columns: cols,
        include,
        nulls_not_distinct,
        without_overlaps,
    } = columns
    else {
        return;
    };
    let generated = if suffix == "pkey" {
        format!("{table}_pkey")
    } else {
        format!("{table}_{}_key", cols.join("_"))
    };
    if name.as_deref() == Some(generated.as_str()) {
        *name = None;
    }
    // with nothing left that the plain list cannot say, collapse back
    // to it so the file keeps its simpler shape
    if name.is_none()
        && include.is_none()
        && nulls_not_distinct.is_none()
        && without_overlaps.is_none()
    {
        *columns = ConstraintColumns::Columns(std::mem::take(cols));
    }
}

/// Merge a parsed constraint into a table model
pub(crate) fn apply_constraint(
    table: &mut Table,
    name: Option<String>,
    constraint: TableConstraint,
) {
    match constraint {
        TableConstraint::PrimaryKey(mut columns) => {
            drop_generated_name(&mut columns, &table.name, "pkey");
            table.primary_key = Some(columns);
        }
        TableConstraint::Unique(mut columns) => {
            drop_generated_name(&mut columns, &table.name, "key");
            table
                .unique_constraints
                .get_or_insert_default()
                .push(columns);
        }
        TableConstraint::Check(check) => {
            table.check_constraints.get_or_insert_default().push(
                CheckConstraint {
                    name: name.unwrap_or_default(),
                    ..check
                },
            );
        }
        TableConstraint::ForeignKey(fk) => {
            table.foreign_keys.get_or_insert_default().push(fk);
        }
        TableConstraint::NotNull(mut not_null) => {
            // pg_dump names a NOT VALID one it adds with ALTER TABLE
            // even when the name is the generated one, which the model
            // records as none, as it does inline in CREATE TABLE
            let generated =
                format!("{}_{}_not_null", table.name, not_null.column);
            if not_null.name.as_deref() == Some(generated.as_str()) {
                not_null.name = None;
            }
            table
                .not_null_constraints
                .get_or_insert_default()
                .push(not_null);
        }
    }
}

/// `PARTITION BY <type> (<part_params>)`, if present
fn table_partition_behavior(
    node: &Node,
    src: &str,
) -> Option<TablePartitionBehavior> {
    let spec = node.find("PartitionSpec")?;
    let partition_type = spec
        .child_of_kind("ColId")
        .map(|n| n.text(src).to_uppercase())
        .unwrap_or_default();
    let columns: Vec<TablePartitionColumn> = spec
        .find_all("part_elem")
        .iter()
        .map(|elem| partition_column(elem, src))
        .collect();
    (!columns.is_empty()).then_some(TablePartitionBehavior {
        partition_type,
        columns,
    })
}

fn partition_column(elem: &Node, src: &str) -> TablePartitionColumn {
    let name = elem.child_of_kind("ColId").map(|n| unquote(n.text(src)));
    let expression = if name.is_none() {
        elem.child_of_kind("func_expr_windowless")
            .or_else(|| elem.child_of_kind("a_expr"))
            .map(|n| n.text(src).to_string())
    } else {
        None
    };
    let collation = elem
        .find("opt_collate")
        .and_then(|n| n.find("any_name"))
        .map(|n| n.text(src).to_string());
    let opclass = elem
        .find("opt_qualified_name")
        .map(|n| n.text(src).to_string());
    match (&name, &collation, &opclass) {
        (Some(name), None, None) => TablePartitionColumn::Name(name.clone()),
        _ => TablePartitionColumn::Detailed {
            name,
            expression,
            collation,
            opclass,
        },
    }
}

/// The bound of a `PARTITION OF ...` child, one field of which is set
/// depending on the `PartitionBoundSpec` alternative matched
#[derive(Default)]
struct PartitionBound {
    default: Option<bool>,
    for_values_in: Option<Vec<Value>>,
    for_values_from: Option<Value>,
    for_values_to: Option<Value>,
    for_values_with: Option<String>,
}

fn partition_bound(spec: &Node, src: &str) -> PartitionBound {
    if spec.has("kw_default") {
        return PartitionBound {
            default: Some(true),
            ..Default::default()
        };
    }
    if spec.has("kw_with") {
        return PartitionBound {
            for_values_with: spec
                .child_of_kind("hash_partbound")
                .map(|n| n.text(src).to_string()),
            ..Default::default()
        };
    }
    if spec.has("kw_from") {
        let lists = spec.find_all("expr_list");
        return PartitionBound {
            for_values_from: lists.first().map(|n| partition_value(n, src)),
            for_values_to: lists.get(1).map(|n| partition_value(n, src)),
            ..Default::default()
        };
    }
    if spec.has("kw_in") {
        let values = spec
            .child_of_kind("expr_list")
            .map(|n| partition_values(&n, src))
            .unwrap_or_default();
        return PartitionBound {
            for_values_in: (!values.is_empty()).then_some(values),
            ..Default::default()
        };
    }
    PartitionBound::default()
}

fn partition_value(list: &Node, src: &str) -> Value {
    let exprs = list.find_all("a_expr");
    match exprs.as_slice() {
        [one] => single_expr_value(one, src),
        _ => Value::Array(
            exprs.iter().map(|e| single_expr_value(e, src)).collect(),
        ),
    }
}

fn partition_values(list: &Node, src: &str) -> Vec<Value> {
    list.find_all("a_expr")
        .iter()
        .map(|e| single_expr_value(e, src))
        .collect()
}

fn single_expr_value(node: &Node, src: &str) -> Value {
    if let Some(s) = node.find("Sconst") {
        Value::String(string_value(&s, src))
    } else if let Ok(n) = node.text(src).parse::<i64>() {
        Value::Number(n.into())
    } else {
        Value::String(node.text(src).to_string())
    }
}

/// `LIKE source_table [{INCLUDING|EXCLUDING} option ...]`
fn like_table(clause: &Node, src: &str) -> LikeTable {
    let name = clause
        .child_of_kind("qualified_name")
        .and_then(|n| qualified_name(&n, src).ok())
        .map(|q| match q.schema {
            Some(schema) => {
                format!("{}.{}", quote_ident(&schema), quote_ident(&q.name))
            }
            None => quote_ident(&q.name),
        })
        .unwrap_or_default();
    let mut like = LikeTable {
        name,
        include_comments: None,
        include_constraints: None,
        include_defaults: None,
        include_generated: None,
        include_identity: None,
        include_indexes: None,
        include_statistics: None,
        include_storage: None,
        include_all: None,
    };
    if let Some(list) = clause.child_of_kind("TableLikeOptionList") {
        for (including, option) in like_options(&list) {
            match option {
                "COMMENTS" => like.include_comments = Some(including),
                "CONSTRAINTS" => like.include_constraints = Some(including),
                "DEFAULTS" => like.include_defaults = Some(including),
                "GENERATED" => like.include_generated = Some(including),
                "IDENTITY" => like.include_identity = Some(including),
                "INDEXES" => like.include_indexes = Some(including),
                "STATISTICS" => like.include_statistics = Some(including),
                "STORAGE" => like.include_storage = Some(including),
                "ALL" => like.include_all = Some(including),
                _ => {}
            }
        }
    }
    like
}

/// Walk the left-recursive `TableLikeOptionList` chain into
/// `(including, OPTION_NAME)` pairs
fn like_options(node: &Node) -> Vec<(bool, &'static str)> {
    let mut results = Vec::new();
    if let Some(inner) = node.child_of_kind("TableLikeOptionList") {
        results.extend(like_options(&inner));
    }
    if let Some(option) = node.child_of_kind("TableLikeOption") {
        let including = node.child_of_kind("kw_including").is_some();
        results.push((including, like_option_name(&option)));
    }
    results
}

fn like_option_name(node: &Node) -> &'static str {
    if node.has("kw_comments") {
        "COMMENTS"
    } else if node.has("kw_constraints") {
        "CONSTRAINTS"
    } else if node.has("kw_defaults") {
        "DEFAULTS"
    } else if node.has("kw_generated") {
        "GENERATED"
    } else if node.has("kw_identity") {
        "IDENTITY"
    } else if node.has("kw_indexes") {
        "INDEXES"
    } else if node.has("kw_statistics") {
        "STATISTICS"
    } else if node.has("kw_storage") {
        "STORAGE"
    } else if node.has("kw_all") {
        "ALL"
    } else {
        ""
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
    fn inherits_parents_are_captured() {
        // an inheritance child has its columns from the parent (so the
        // column list may be empty); the INHERITS clause must populate
        // `parents` or the table satisfies no oneOf branch
        let statement = parse_one(
            "CREATE TABLE test.child (\n\
             CONSTRAINT c CHECK ((x % 2) = 0)\n\
             ) INHERITS (test.parent, other.base);",
        );
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable")
        };
        assert_eq!(
            table.parents,
            Some(vec!["test.parent".into(), "other.base".into()])
        );
    }

    /// PostgreSQL 18 made NOT NULL a named constraint. pg_dump writes
    /// the name only when it is not the generated one, so a name here
    /// is always worth keeping; `nullable: false` alone cannot hold it
    #[test]
    fn parses_named_column_not_null() {
        let Statement::CreateTable(table) = parse_one(
            "CREATE TABLE test.t (a integer CONSTRAINT a_nn NOT NULL,              b integer CONSTRAINT b_ni NOT NULL NO INHERIT, c integer              NOT NULL, d integer);",
        ) else {
            panic!("expected CreateTable")
        };
        let columns = table.columns.unwrap();
        assert_eq!(
            columns[0].not_null_constraint,
            Some(ColumnNotNull {
                name: Some("a_nn".into()),
                no_inherit: None,
            })
        );
        assert_eq!(
            columns[1].not_null_constraint,
            Some(ColumnNotNull {
                name: Some("b_ni".into()),
                no_inherit: Some(true),
            })
        );
        // a bare NOT NULL needs nothing beyond `nullable`
        assert_eq!(columns[2].nullable, Some(false));
        assert_eq!(columns[2].not_null_constraint, None);
        assert_eq!(columns[3].nullable, None);
        assert_eq!(columns[3].not_null_constraint, None);
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
                kind: None,
                sequence: None,
                sequence_behavior: Some("ALWAYS".into()),
                sequence_options: None,
            })
        );
        assert_eq!(columns[1].check_constraint, Some("total > 0".into()));
        // an identity column records no kind: the field describes an
        // expression's materialization, and there is no expression
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

    /// pg_dump writes a NOT VALID constraint as its own ALTER TABLE, for
    /// each of the three kinds that accept the clause
    #[test]
    fn parses_not_valid_constraints() {
        let constraint = |sql: &str| {
            let Statement::AddConstraint { constraint, .. } = parse_one(sql)
            else {
                panic!("expected AddConstraint for {sql}")
            };
            constraint
        };
        let TableConstraint::Check(check) = constraint(
            "ALTER TABLE public.t ADD CONSTRAINT t_pos CHECK ((a > 0)) \
             NOT VALID;",
        ) else {
            panic!("expected a CHECK")
        };
        assert_eq!(check.not_valid, Some(true));
        let TableConstraint::ForeignKey(fk) = constraint(
            "ALTER TABLE ONLY public.t ADD CONSTRAINT t_fk FOREIGN KEY (p) \
             REFERENCES public.r(id) NOT VALID;",
        ) else {
            panic!("expected a FOREIGN KEY")
        };
        assert_eq!(fk.not_valid, Some(true));
        let TableConstraint::NotNull(not_null) = constraint(
            "ALTER TABLE public.t ADD CONSTRAINT t_nn NOT NULL b NOT VALID;",
        ) else {
            panic!("expected a NOT NULL")
        };
        assert_eq!(not_null.not_valid, Some(true));
        // a valid constraint carries no field at all
        let TableConstraint::Check(valid) = constraint(
            "ALTER TABLE public.t ADD CONSTRAINT t_pos CHECK ((a > 0));",
        ) else {
            panic!("expected a CHECK")
        };
        assert_eq!(valid.not_valid, None);
    }

    /// The statement pg_dump writes for an identity column, which spells
    /// out every option. Parse it into the column's generation, keeping
    /// only what differs from PostgreSQL's defaults.
    fn identity_of(sql: &str) -> ColumnGenerated {
        let Statement::AddIdentity { generated, .. } = parse_one(sql) else {
            panic!("expected AddIdentity for {sql}")
        };
        generated
    }

    #[test]
    fn identity_drops_every_default_pg_dump_spells_out() {
        let generated = identity_of(
            "ALTER TABLE public.t ALTER COLUMN id ADD GENERATED ALWAYS AS \
             IDENTITY (SEQUENCE NAME public.t_id_seq START WITH 1 \
             INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);",
        );
        assert_eq!(generated.sequence_behavior, Some("ALWAYS".into()));
        // the generated name and all five defaults are dropped, so this
        // equals a hand-written identity that states none of them
        assert_eq!(generated.sequence_options, None);
        assert_eq!(generated.sequence, None);
    }

    #[test]
    fn identity_keeps_non_default_options() {
        let generated = identity_of(
            "ALTER TABLE public.t ALTER COLUMN id ADD GENERATED BY DEFAULT \
             AS IDENTITY (SEQUENCE NAME public.t_id_seq START WITH 100 \
             INCREMENT BY 5 NO MINVALUE MAXVALUE 900 CACHE 20 CYCLE);",
        );
        assert_eq!(generated.sequence_behavior, Some("BY DEFAULT".into()));
        assert_eq!(
            generated.sequence_options,
            Some(SequenceOptions {
                name: None,
                start_with: Some(100),
                increment_by: Some(5),
                min_value: None,
                max_value: Some(900),
                cache: Some(20),
                cycle: Some(true),
            })
        );
    }

    #[test]
    fn identity_keeps_a_chosen_sequence_name() {
        let generated = identity_of(
            "ALTER TABLE public.t ALTER COLUMN id ADD GENERATED ALWAYS AS \
             IDENTITY (SEQUENCE NAME public.custom_ids START WITH 1 \
             INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);",
        );
        assert_eq!(
            generated.sequence_options.and_then(|o| o.name),
            Some("public.custom_ids".into())
        );
    }

    /// A quoted table gives a quoted sequence name, which is still the
    /// generated one once both names are unquoted
    #[test]
    fn identity_drops_a_quoted_generated_name() {
        let generated = identity_of(
            "ALTER TABLE public.\"Orders\" ALTER COLUMN id ADD GENERATED \
             ALWAYS AS IDENTITY (SEQUENCE NAME public.\"Orders_id_seq\" \
             START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);",
        );
        assert_eq!(generated.sequence_options, None);
    }

    /// A descending sequence starts at its maximum, -1 by default, so
    /// `START WITH -1` is the default there and `START WITH 1` is not
    #[test]
    fn identity_default_start_follows_the_direction() {
        let generated = identity_of(
            "ALTER TABLE public.t ALTER COLUMN id ADD GENERATED ALWAYS AS \
             IDENTITY (SEQUENCE NAME public.t_id_seq START WITH -1 \
             INCREMENT BY -1 NO MINVALUE NO MAXVALUE CACHE 1);",
        );
        assert_eq!(
            generated.sequence_options,
            Some(SequenceOptions {
                increment_by: Some(-1),
                ..Default::default()
            })
        );
    }

    /// A generated constraint name is nobody's choice, so it stays out
    /// of the project file; a chosen one is kept, since rebuilding
    /// under a different name loses it and `deploy` matches by name.
    #[test]
    fn apply_constraint_drops_only_generated_names() {
        let detailed =
            |name: &str, columns: &[&str]| ConstraintColumns::Detailed {
                name: Some(name.into()),
                columns: columns.iter().map(|c| (*c).to_string()).collect(),
                include: None,
                nulls_not_distinct: None,
                without_overlaps: None,
            };
        // the parser's own product, so the shape stays in step with
        // the model rather than being spelled out again here
        let Statement::CreateTable(mut table) =
            parse_one("CREATE TABLE test.users (id integer, email text);")
        else {
            panic!("expected CreateTable")
        };
        apply_constraint(
            &mut table,
            Some("users_pkey".into()),
            TableConstraint::PrimaryKey(detailed("users_pkey", &["id"])),
        );
        assert_eq!(
            table.primary_key,
            Some(ConstraintColumns::Columns(vec!["id".into()]))
        );

        apply_constraint(
            &mut table,
            Some("users_email_key".into()),
            TableConstraint::Unique(detailed("users_email_key", &["email"])),
        );
        apply_constraint(
            &mut table,
            Some("users_one_email".into()),
            TableConstraint::Unique(detailed("users_one_email", &["email"])),
        );
        assert_eq!(
            table.unique_constraints,
            Some(vec![
                ConstraintColumns::Columns(vec!["email".into()]),
                detailed("users_one_email", &["email"]),
            ])
        );

        // pg_dump names a NOT VALID NOT NULL it adds with ALTER TABLE
        let not_null = |name: &str| NotNullConstraint {
            name: Some(name.into()),
            column: "email".into(),
            no_inherit: None,
            not_valid: Some(true),
        };
        for name in ["users_email_not_null", "email_required"] {
            apply_constraint(
                &mut table,
                Some(name.into()),
                TableConstraint::NotNull(not_null(name)),
            );
        }
        assert_eq!(
            table.not_null_constraints,
            Some(vec![
                NotNullConstraint {
                    name: None,
                    ..not_null("users_email_not_null")
                },
                not_null("email_required"),
            ])
        );
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
        // the parse keeps the name; apply_constraint drops it when it
        // is the one PostgreSQL generates, which needs the table
        assert_eq!(
            constraint,
            TableConstraint::PrimaryKey(ConstraintColumns::Detailed {
                name: Some("users_pkey".into()),
                columns: vec!["id".into()],
                include: None,
                nulls_not_distinct: None,
                without_overlaps: None,
            })
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
        assert_eq!(
            constraint,
            TableConstraint::Check(CheckConstraint {
                name: String::new(),
                expression: "value > 0".into(),
                enforced: None,
                not_valid: None,
            })
        );
    }

    /// PostgreSQL 18 dumps a child's NOT NULL on an inherited column
    /// as a table constraint; before it was recognized, the whole
    /// CREATE TABLE failed to parse and the table was dropped
    #[test]
    fn parses_inherited_not_null_table_constraint() {
        let Statement::CreateTable(table) = parse_one(
            "CREATE TABLE probe.c (\n    NOT NULL ts,\n    CONSTRAINT \
             ts_nn NOT NULL sid\n)\nINHERITS (probe.p);",
        ) else {
            panic!("expected CreateTable")
        };
        assert_eq!(table.parents, Some(vec!["probe.p".into()]));
        assert_eq!(table.columns, None);
        assert_eq!(
            table.not_null_constraints,
            Some(vec![
                NotNullConstraint {
                    name: None,
                    column: "ts".into(),
                    no_inherit: None,
                    not_valid: None,
                },
                NotNullConstraint {
                    name: Some("ts_nn".into()),
                    column: "sid".into(),
                    no_inherit: None,
                    not_valid: None,
                },
            ])
        );
    }

    #[test]
    fn parses_not_null_table_constraint_no_inherit() {
        let Statement::CreateTable(table) = parse_one(
            "CREATE TABLE probe.c (CONSTRAINT ts_ni NOT NULL ts NO \
             INHERIT) INHERITS (probe.p);",
        ) else {
            panic!("expected CreateTable")
        };
        assert_eq!(
            table.not_null_constraints,
            Some(vec![NotNullConstraint {
                name: Some("ts_ni".into()),
                column: "ts".into(),
                no_inherit: Some(true),
                not_valid: None,
            }])
        );
    }

    #[test]
    fn parses_alter_table_set_column_default() {
        let statement = parse_one(
            "ALTER TABLE ONLY test.t ALTER COLUMN id SET DEFAULT \
             nextval('test.t_id_seq'::regclass);",
        );
        let Statement::SetColumnDefault {
            table,
            column,
            default,
        } = statement
        else {
            panic!("expected SetColumnDefault, got {statement:?}")
        };
        assert_eq!(table.to_string(), "test.t");
        assert_eq!(column, "id");
        assert_eq!(default, json!("nextval('test.t_id_seq'::regclass)"));
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

    #[test]
    fn parses_partition_by() {
        let statement = parse_one(
            "CREATE TABLE test.events (id bigint, ts timestamp) \
             PARTITION BY RANGE (ts);",
        );
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable")
        };
        let partition = table.partition.unwrap();
        assert_eq!(partition.partition_type, "RANGE");
        assert_eq!(
            partition.columns,
            vec![TablePartitionColumn::Name("ts".into())]
        );
    }

    #[test]
    fn parses_partition_of() {
        let statement = parse_one(
            "CREATE TABLE test.events_2024 PARTITION OF test.events \
             FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');",
        );
        let Statement::CreateTablePartition { parent, partition } = statement
        else {
            panic!("expected CreateTablePartition")
        };
        assert_eq!(parent.to_string(), "test.events");
        assert_eq!(partition.schema, "test");
        assert_eq!(partition.name, "events_2024");
        assert_eq!(partition.for_values_from, Some(json!("2024-01-01")));
        assert_eq!(partition.for_values_to, Some(json!("2025-01-01")));
    }

    #[test]
    fn parses_attach_partition() {
        // pg_dump's real form: child is a plain CREATE TABLE, attached
        // later via ALTER TABLE ONLY parent ATTACH PARTITION ...
        let statement = parse_one(
            "ALTER TABLE ONLY test.events ATTACH PARTITION \
             test.events_2024 FOR VALUES FROM ('2024-01-01') \
             TO ('2025-01-01');",
        );
        let Statement::AttachPartition { parent, partition } = statement
        else {
            panic!("expected AttachPartition")
        };
        assert_eq!(parent.to_string(), "test.events");
        assert_eq!(partition.schema, "test");
        assert_eq!(partition.name, "events_2024");
        assert_eq!(partition.for_values_from, Some(json!("2024-01-01")));
        assert_eq!(partition.for_values_to, Some(json!("2025-01-01")));
    }

    #[test]
    fn parses_attach_partition_list_and_default() {
        let Statement::AttachPartition { partition, .. } = parse_one(
            "ALTER TABLE ONLY test.t ATTACH PARTITION test.p \
             FOR VALUES IN ('a', 'b');",
        ) else {
            panic!("expected AttachPartition")
        };
        assert_eq!(
            partition.for_values_in,
            Some(vec![json!("a"), json!("b")])
        );

        let Statement::AttachPartition { partition, .. } = parse_one(
            "ALTER TABLE ONLY test.t ATTACH PARTITION test.pd DEFAULT;",
        ) else {
            panic!("expected AttachPartition")
        };
        assert_eq!(partition.default, Some(true));
    }

    #[test]
    fn parses_partition_of_multicolumn_bounds() {
        let statement = parse_one(
            "CREATE TABLE test.events_2024 PARTITION OF test.events \
             FOR VALUES FROM (2020, 1) TO (2021, 1);",
        );
        let Statement::CreateTablePartition { partition, .. } = statement
        else {
            panic!("expected CreateTablePartition")
        };
        assert_eq!(partition.for_values_from, Some(json!([2020, 1])));
        assert_eq!(partition.for_values_to, Some(json!([2021, 1])));
    }

    #[test]
    fn parses_partition_of_default() {
        let statement = parse_one(
            "CREATE TABLE test.events_default PARTITION OF test.events \
             DEFAULT;",
        );
        let Statement::CreateTablePartition { partition, .. } = statement
        else {
            panic!("expected CreateTablePartition")
        };
        assert_eq!(partition.default, Some(true));
    }

    #[test]
    fn parses_create_table_of_type() {
        let statement =
            parse_one("CREATE TABLE test.person OF test.person_type;");
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable")
        };
        assert_eq!(table.from_type, Some("test.person_type".into()));
    }

    #[test]
    fn parses_typed_table_inline_column_constraints() {
        let statement = parse_one(
            "CREATE TABLE test.person OF test.person_type (\n\
             name WITH OPTIONS NOT NULL,\n\
             age WITH OPTIONS DEFAULT 0\n\
             );",
        );
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable")
        };
        assert_eq!(table.from_type, Some("test.person_type".into()));
        let columns = table.columns.unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "name");
        assert_eq!(columns[0].data_type, "");
        assert_eq!(columns[0].nullable, Some(false));
        assert_eq!(columns[1].name, "age");
        assert_eq!(columns[1].default, Some(json!("0")));
    }

    #[test]
    fn parses_typed_table_check_constraint() {
        let statement = parse_one(
            "CREATE TABLE test.person OF test.person_type (\n\
             CONSTRAINT person_age_check CHECK (age >= 0)\n\
             );",
        );
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable")
        };
        let constraints = table.check_constraints.unwrap();
        assert_eq!(constraints.len(), 1);
        assert_eq!(constraints[0].name, "person_age_check");
        assert_eq!(constraints[0].expression, "age >= 0");
    }

    #[test]
    fn parses_create_table_like() {
        let statement = parse_one(
            "CREATE TABLE test.copy (\n\
             LIKE test.original INCLUDING DEFAULTS INCLUDING INDEXES\n\
             );",
        );
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable")
        };
        let like_table = table.like_table.unwrap();
        assert_eq!(like_table.name, "test.original");
        assert_eq!(like_table.include_defaults, Some(true));
        assert_eq!(like_table.include_indexes, Some(true));
    }

    #[test]
    fn parses_table_storage_options() {
        let statement = parse_one(
            "CREATE TABLE test.t (id int) USING heap \
             WITH (fillfactor=70) TABLESPACE fastdisk;",
        );
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable")
        };
        assert_eq!(table.access_method, Some("heap".into()));
        assert_eq!(
            table.storage_parameters.unwrap().get("fillfactor"),
            Some(&json!("70"))
        );
        assert_eq!(table.tablespace, Some("fastdisk".into()));
    }

    #[test]
    fn parses_primary_key_index_tablespace() {
        let statement = parse_one(
            "CREATE TABLE test.t (\n\
             id int,\n\
             CONSTRAINT t_pkey PRIMARY KEY (id) USING INDEX TABLESPACE fast\n\
             );",
        );
        let Statement::CreateTable(table) = statement else {
            panic!("expected CreateTable")
        };
        assert_eq!(table.index_tablespace, Some("fast".into()));
    }

    #[test]
    fn parses_index_storage_parameters() {
        let statement =
            parse_one("CREATE INDEX i ON t (c) WITH (fillfactor=80);");
        let Statement::CreateIndex { index, .. } = statement else {
            panic!("expected CreateIndex")
        };
        assert_eq!(
            index.storage_parameters.unwrap().get("fillfactor"),
            Some(&json!("80"))
        );
    }

    #[test]
    fn parses_foreign_key_deferrable() {
        let statement = parse_one(
            "ALTER TABLE ONLY test.addresses\n    \
             ADD CONSTRAINT addresses_user_id_fkey FOREIGN KEY (user_id) \
             REFERENCES test.users(id) DEFERRABLE INITIALLY DEFERRED;",
        );
        let Statement::AddConstraint { constraint, .. } = statement else {
            panic!("expected AddConstraint")
        };
        let TableConstraint::ForeignKey(fk) = constraint else {
            panic!("expected ForeignKey")
        };
        assert_eq!(fk.deferrable, Some(true));
        assert_eq!(fk.initially_deferred, Some(true));
    }
}
