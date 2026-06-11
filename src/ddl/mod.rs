//! DDL parsing: tree-sitter-postgres CST → models (replaces
//! tokenizer.py)
//!
//! The grammar is generated from PostgreSQL's gram.y, so node kinds
//! mirror grammar productions (`CreateStmt`, `columnDef`,
//! `ConstraintElem`, ...) and carry no named fields — extraction walks
//! the tree by kind via the [`NodeExt`] helpers.

mod table;

use tree_sitter::Node;

use crate::models;

/// A single parsed DDL statement, mapped onto the project models
#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    CreateTable(Box<models::Table>),
    /// CREATE INDEX — `table` is the qualified relation name
    CreateIndex {
        table: QualifiedName,
        index: models::Index,
    },
    /// ALTER TABLE ... ADD CONSTRAINT
    AddConstraint {
        table: QualifiedName,
        name: Option<String>,
        constraint: TableConstraint,
    },
    /// Parsed successfully but not (yet) a supported statement type
    Unsupported(String),
}

/// A table constraint from inline DDL or ALTER TABLE ... ADD
#[derive(Clone, Debug, PartialEq)]
pub enum TableConstraint {
    PrimaryKey(models::ConstraintColumns),
    Unique(models::ConstraintColumns),
    Check(String),
    ForeignKey(models::ForeignKey),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QualifiedName {
    pub schema: Option<String>,
    pub name: String,
}

impl std::fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.schema {
            Some(schema) => write!(f, "{schema}.{}", self.name),
            None => f.write_str(&self.name),
        }
    }
}

pub struct Parser {
    inner: tree_sitter::Parser,
}

impl Parser {
    pub fn new() -> Result<Self, String> {
        let mut inner = tree_sitter::Parser::new();
        inner
            .set_language(&tree_sitter_postgres::LANGUAGE.into())
            .map_err(|e| format!("failed to load postgres grammar: {e}"))?;
        Ok(Self { inner })
    }

    /// Parse a blob of one or more SQL statements
    pub fn parse(&mut self, sql: &str) -> Result<Vec<Statement>, String> {
        let tree = self
            .inner
            .parse(sql, None)
            .ok_or_else(|| String::from("tree-sitter returned no tree"))?;
        let root = tree.root_node();
        if root.has_error() {
            return Err(format!(
                "syntax error parsing: {}",
                truncate(sql, 120)
            ));
        }
        let mut statements = Vec::new();
        for stmt in root.find_all("stmt") {
            let Some(node) = stmt.named_child(0) else {
                continue;
            };
            statements.extend(dispatch(&node, sql)?);
        }
        Ok(statements)
    }
}

fn dispatch(node: &Node, src: &str) -> Result<Vec<Statement>, String> {
    match node.kind() {
        "CreateStmt" => Ok(vec![table::create_table(node, src)?]),
        "IndexStmt" => Ok(vec![table::create_index(node, src)?]),
        "AlterTableStmt" => table::alter_table(node, src),
        other => Ok(vec![Statement::Unsupported(other.to_string())]),
    }
}

fn truncate(value: &str, len: usize) -> &str {
    match value.char_indices().nth(len) {
        Some((offset, _)) => &value[..offset],
        None => value,
    }
}

/// Walk-by-kind helpers for the fieldless CST
pub(crate) trait NodeExt<'tree> {
    /// The first descendant of `kind`, depth-first
    fn find(&self, kind: &str) -> Option<Node<'tree>>;
    /// All descendants of `kind`, not descending into matches (so
    /// left-recursive list productions flatten naturally)
    fn find_all(&self, kind: &str) -> Vec<Node<'tree>>;
    /// The first direct child of `kind`
    fn child_of_kind(&self, kind: &str) -> Option<Node<'tree>>;
    /// Whether any descendant of `kind` exists
    fn has(&self, kind: &str) -> bool {
        self.find(kind).is_some()
    }
    /// The source text for this node
    fn text<'src>(&self, src: &'src str) -> &'src str;
}

impl<'tree> NodeExt<'tree> for Node<'tree> {
    fn find(&self, kind: &str) -> Option<Node<'tree>> {
        let mut cursor = self.walk();
        for child in self.children(&mut cursor) {
            if child.kind() == kind {
                return Some(child);
            }
        }
        let mut cursor = self.walk();
        for child in self.children(&mut cursor) {
            if let Some(found) = child.find(kind) {
                return Some(found);
            }
        }
        None
    }

    fn find_all(&self, kind: &str) -> Vec<Node<'tree>> {
        let mut results = Vec::new();
        collect(self, kind, &mut results);
        results
    }

    fn child_of_kind(&self, kind: &str) -> Option<Node<'tree>> {
        let mut cursor = self.walk();
        self.children(&mut cursor).find(|c| c.kind() == kind)
    }

    fn text<'src>(&self, src: &'src str) -> &'src str {
        &src[self.start_byte()..self.end_byte()]
    }
}

fn collect<'tree>(
    node: &Node<'tree>,
    kind: &str,
    results: &mut Vec<Node<'tree>>,
) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.kind() == kind {
            results.push(child);
        } else {
            collect(&child, kind, results);
        }
    }
}

/// Extract (schema, name) from a `qualified_name` node
pub(crate) fn qualified_name(
    node: &Node,
    src: &str,
) -> Result<QualifiedName, String> {
    let head = node
        .child_of_kind("ColId")
        .map(|n| unquote(n.text(src)))
        .ok_or_else(|| {
            format!(
                "qualified_name missing ColId: {}",
                truncate(node.text(src), 80)
            )
        })?;
    Ok(match node.find("attr_name") {
        Some(attr) => QualifiedName {
            schema: Some(head),
            name: unquote(attr.text(src)),
        },
        None => QualifiedName {
            schema: None,
            name: head,
        },
    })
}

/// Strip identifier quoting: `"Name"` → `Name`, `""` escapes collapse
pub(crate) fn unquote(value: &str) -> String {
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1].replace("\"\"", "\"")
    } else {
        value.to_string()
    }
}
