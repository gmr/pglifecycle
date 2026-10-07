//! Security labels: a label that is different or that only the
//! project has gets `SECURITY LABEL FOR provider ON ... IS label`, and
//! a label that only the database has gets `IS NULL`, as a comment
//! does. The removal is not destructive: it loses no data.
//!
//! A project object with no `security_labels` does not manage its
//! labels: deploy leaves the labels of the database as they are (see
//! [`without_unmanaged`]). An explicit map, also an empty one, is
//! compared, so a label that it does not have is removed. The same is
//! true for each column and partition of a table, and for each column
//! in the `column_security_labels` of a view or a materialized view.
//!
//! A change to the labels only changes nothing else, so the object
//! keeps its in-place form, even a type that otherwise has no in-place
//! form (a language). The labels of a table's columns and partitions,
//! and of a view's columns, change with the table or the view.

use std::collections::BTreeSet;

use super::{Alter, function_target, qualified};
use crate::build::security_label_sql;
use crate::models::{ColumnSecurityLabels, Definition, SecurityLabels};
use crate::utils::quote_ident;

/// The statements that make the labels `db` of `kind name` the labels
/// `repo`, in the order of the providers
pub(crate) fn changes(
    kind: &str,
    name: &str,
    repo: Option<&SecurityLabels>,
    db: Option<&SecurityLabels>,
) -> Vec<String> {
    let label = |labels: Option<&SecurityLabels>, provider: &String| {
        labels.and_then(|labels| labels.get(provider)).cloned()
    };
    let providers: BTreeSet<&String> =
        repo.into_iter().chain(db).flat_map(|l| l.keys()).collect();
    providers
        .into_iter()
        .filter_map(|provider| {
            let wanted = label(repo, provider);
            (wanted != label(db, provider)).then(|| match wanted {
                Some(wanted) => security_label_sql(
                    kind,
                    name,
                    &SecurityLabels::from([(provider.clone(), wanted)]),
                ),
                None => format!(
                    "SECURITY LABEL FOR {} ON {kind} {name} IS NULL;\n",
                    quote_ident(provider)
                ),
            })
        })
        .collect()
}

/// The labels of the columns of a view or a materialized view
fn column_labels(definition: &Definition) -> Option<&ColumnSecurityLabels> {
    match definition {
        Definition::View(d) => d.column_security_labels.as_ref(),
        Definition::MaterializedView(d) => d.column_security_labels.as_ref(),
        _ => None,
    }
}

/// The labels of the columns of a view or a materialized view, to change
fn column_labels_mut(
    definition: &mut Definition,
) -> Option<&mut Option<ColumnSecurityLabels>> {
    match definition {
        Definition::View(d) => Some(&mut d.column_security_labels),
        Definition::MaterializedView(d) => Some(&mut d.column_security_labels),
        _ => None,
    }
}

/// The statements that make the labels of `db`, and of its columns
/// and its partitions, the labels of `repo`
pub(super) fn alters(repo: &Definition, db: &Definition) -> Vec<Alter> {
    let mut sql = Vec::new();
    if let Some((kind, name)) = target(repo) {
        sql.extend(changes(
            kind,
            &name,
            repo.security_labels(),
            db.security_labels(),
        ));
    }
    if let (Some(columns), Some((_, view))) =
        (column_labels(repo), target(repo))
    {
        // a column that is not in the map is not managed
        for (column, labels) in columns {
            sql.extend(changes(
                "COLUMN",
                &format!("{view}.{}", quote_ident(column)),
                Some(labels),
                column_labels(db).and_then(|c| c.get(column)),
            ));
        }
    }
    if let (Definition::Table(repo), Definition::Table(db)) = (repo, db) {
        let table = qualified(&repo.schema, &repo.name);
        // a column that only the database has is dropped with its labels
        for column in repo.columns.iter().flatten() {
            let existing = db
                .columns
                .iter()
                .flatten()
                .find(|c| c.name == column.name)
                .and_then(|c| c.security_labels.as_ref());
            sql.extend(changes(
                "COLUMN",
                &format!("{table}.{}", quote_ident(&column.name)),
                column.security_labels.as_ref(),
                existing,
            ));
        }
        for partition in repo.partitions.iter().flatten() {
            let existing = db
                .partitions
                .iter()
                .flatten()
                .find(|p| {
                    p.schema == partition.schema && p.name == partition.name
                })
                .and_then(|p| p.security_labels.as_ref());
            sql.extend(changes(
                "TABLE",
                &qualified(&partition.schema, &partition.name),
                partition.security_labels.as_ref(),
                existing,
            ));
        }
    }
    sql.into_iter().map(Alter::new).collect()
}

/// `db` with no labels where `repo` has no `security_labels`: on the
/// object, on each column and partition of a table, and on each column
/// of a view (see [`keep_children`]). The project does not manage those
/// labels, so they are not a change.
pub(crate) fn without_unmanaged(
    repo: &Definition,
    db: Definition,
) -> Definition {
    if repo.security_labels().is_none() {
        // the object labels; a table or a view keeps those of its
        // children
        let mut stripped = without(&db);
        if let (Definition::Table(stripped), Definition::Table(db)) =
            (&mut stripped, &db)
        {
            stripped.columns.clone_from(&db.columns);
            stripped.partitions.clone_from(&db.partitions);
        }
        if let Some(columns) = column_labels_mut(&mut stripped) {
            *columns = column_labels(&db).cloned();
        }
        return keep_children(repo, stripped);
    }
    keep_children(repo, db)
}

/// `db` with no labels on each column and partition that has no
/// `security_labels` in `repo`, and on each view column that is not in
/// the `column_security_labels` of `repo`
fn keep_children(repo: &Definition, mut db: Definition) -> Definition {
    if let Some(columns) = column_labels_mut(&mut db) {
        match column_labels(repo) {
            Some(managed) => {
                if let Some(columns) = columns {
                    columns.retain(|column, _| managed.contains_key(column));
                }
            }
            None => *columns = None,
        }
    }
    if let (Definition::Table(repo), Definition::Table(db)) = (repo, &mut db) {
        for column in db.columns.iter_mut().flatten() {
            let managed = repo
                .columns
                .iter()
                .flatten()
                .find(|c| c.name == column.name)
                .is_some_and(|c| c.security_labels.is_some());
            if !managed {
                column.security_labels = None;
            }
        }
        for partition in db.partitions.iter_mut().flatten() {
            let managed = repo
                .partitions
                .iter()
                .flatten()
                .find(|p| {
                    p.schema == partition.schema && p.name == partition.name
                })
                .is_some_and(|p| p.security_labels.is_some());
            if !managed {
                partition.security_labels = None;
            }
        }
    }
    db
}

/// `definition` with no security labels, also on its columns and its
/// partitions
pub(super) fn without(definition: &Definition) -> Definition {
    let mut definition = definition.clone();
    match &mut definition {
        Definition::Aggregate(d) => d.security_labels = None,
        Definition::Domain(d) => d.security_labels = None,
        Definition::Function(d) => d.security_labels = None,
        Definition::Group(d) => d.security_labels = None,
        Definition::Language(d) => d.security_labels = None,
        Definition::MaterializedView(d) => {
            d.security_labels = None;
            d.column_security_labels = None;
        }
        Definition::Procedure(d) => d.security_labels = None,
        Definition::Publication(d) => d.security_labels = None,
        Definition::Role(d) => d.security_labels = None,
        Definition::Schema(d) => d.security_labels = None,
        Definition::Sequence(d) => d.security_labels = None,
        Definition::Subscription(d) => d.security_labels = None,
        Definition::Table(d) => {
            d.security_labels = None;
            for column in d.columns.iter_mut().flatten() {
                column.security_labels = None;
            }
            for partition in d.partitions.iter_mut().flatten() {
                partition.security_labels = None;
            }
        }
        Definition::Tablespace(d) => d.security_labels = None,
        Definition::Type(d) => d.security_labels = None,
        Definition::User(d) => d.security_labels = None,
        Definition::View(d) => {
            d.security_labels = None;
            d.column_security_labels = None;
        }
        _ => {}
    }
    definition
}

/// The object type and the name that `SECURITY LABEL ON` gives the
/// object, as its `COMMENT ON` names it
fn target(definition: &Definition) -> Option<(&'static str, String)> {
    Some(match definition {
        Definition::Aggregate(d) => ("AGGREGATE", super::aggregate::target(d)),
        Definition::Domain(d) => ("DOMAIN", qualified(&d.schema, &d.name)),
        Definition::Function(d) => ("FUNCTION", function_target(d)),
        Definition::Language(d) => ("LANGUAGE", quote_ident(&d.name)),
        Definition::MaterializedView(d) => {
            ("MATERIALIZED VIEW", qualified(&d.schema, &d.name))
        }
        Definition::Procedure(d) => {
            ("PROCEDURE", super::procedure::signature(d))
        }
        Definition::Publication(d) => ("PUBLICATION", quote_ident(&d.name)),
        Definition::Schema(d) => ("SCHEMA", quote_ident(&d.name)),
        Definition::Sequence(d) => ("SEQUENCE", qualified(&d.schema, &d.name)),
        Definition::Subscription(d) => ("SUBSCRIPTION", quote_ident(&d.name)),
        Definition::Table(d) => (
            if d.server.is_some() {
                "FOREIGN TABLE"
            } else {
                "TABLE"
            },
            qualified(&d.schema, &d.name),
        ),
        Definition::Type(d) => ("TYPE", qualified(&d.schema, &d.name)),
        Definition::View(d) => ("VIEW", qualified(&d.schema, &d.name)),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::{Resolution, resolve};
    use super::without_unmanaged;
    use crate::models::Definition;

    fn table(value: serde_json::Value) -> Definition {
        Definition::Table(serde_json::from_value(value).unwrap())
    }

    fn sql(resolution: Resolution) -> Vec<String> {
        match resolution {
            Resolution::Statements(alters) => {
                alters.into_iter().map(|a| a.sql).collect()
            }
            Resolution::OrReplace { then, .. } => {
                then.into_iter().map(|a| a.sql).collect()
            }
            _ => panic!("expected statements in place"),
        }
    }

    /// A label that changes or that only the project has is set, and a
    /// label that only the database has gets IS NULL, on the table and
    /// on its columns. A column that the deploy adds gets its labels
    /// after the ADD COLUMN.
    #[test]
    fn table_and_column_labels_change_in_place() {
        let repo = table(json!({
            "name": "t", "schema": "s", "owner": "postgres",
            "columns": [
                {"name": "a", "data_type": "text",
                 "security_labels": {"dummy": "secret"}},
                {"name": "b", "data_type": "text",
                 "security_labels": {"dummy": "new"}},
            ],
            "security_labels": {"dummy": "classified", "Other": "x"},
        }));
        let db = table(json!({
            "name": "t", "schema": "s", "owner": "postgres",
            "columns": [{"name": "a", "data_type": "text"}],
            "security_labels": {"dummy": "unclassified", "gone": "y"},
        }));
        assert_eq!(
            sql(resolve(&repo, &db)),
            [
                "ALTER TABLE s.t ADD COLUMN b text;\n",
                "SECURITY LABEL FOR \"Other\" ON TABLE s.t IS $$x$$;\n",
                "SECURITY LABEL FOR dummy ON TABLE s.t IS $$classified$$;\n",
                "SECURITY LABEL FOR gone ON TABLE s.t IS NULL;\n",
                "SECURITY LABEL FOR dummy ON COLUMN s.t.a IS $$secret$$;\n",
                "SECURITY LABEL FOR dummy ON COLUMN s.t.b IS $$new$$;\n",
            ]
        );
        assert!(sql(resolve(&repo, &repo)).is_empty());
    }

    /// A language has no in-place form, but a change to its labels only
    /// does not make it again
    #[test]
    fn label_change_alone_does_not_rebuild() {
        let language = |label: &str| {
            Definition::Language(
                serde_json::from_value(json!({
                    "name": "plx", "trusted": true,
                    "handler": "plx_call_handler",
                    "security_labels": {"dummy": label},
                }))
                .unwrap(),
            )
        };
        assert_eq!(
            sql(resolve(&language("new"), &language("old"))),
            ["SECURITY LABEL FOR dummy ON LANGUAGE plx IS $$new$$;\n"]
        );
    }

    /// CREATE OR REPLACE keeps the labels, so a changed label of a
    /// function that is replaced is set after it
    #[test]
    fn replaced_function_sets_its_labels_after() {
        let function = |body: &str, label: Option<&str>| {
            let mut value = json!({
                "name": "f", "schema": "s", "owner": "postgres",
                "parameters": [
                    {"mode": "IN", "name": "a", "data_type": "integer"},
                ],
                "returns": "integer", "language": "sql",
                "definition": body,
            });
            if let Some(label) = label {
                value["security_labels"] = json!({"dummy": label});
            }
            Definition::Function(serde_json::from_value(value).unwrap())
        };
        let resolution = resolve(
            &function("SELECT 2", Some("x")),
            &function("SELECT 1", None),
        );
        assert!(matches!(resolution, Resolution::OrReplace { .. }));
        assert_eq!(
            sql(resolution),
            ["SECURITY LABEL FOR dummy ON FUNCTION s.f IS $$x$$;\n"]
        );
    }

    /// A publication or a subscription compares in its canonical form,
    /// and a change to its labels only is a change
    #[test]
    fn publication_and_subscription_label_change_is_a_change() {
        let publication = |label: &str| {
            Definition::Publication(
                serde_json::from_value(json!({
                    "name": "p", "all_tables": true,
                    "security_labels": {"dummy": label},
                }))
                .unwrap(),
            )
        };
        let subscription = |label: &str| {
            Definition::Subscription(
                serde_json::from_value(json!({
                    "name": "s", "connection": "dbname=x",
                    "publications": ["p"],
                    "security_labels": {"dummy": label},
                }))
                .unwrap(),
            )
        };
        assert!(!crate::deploy::diff::same(
            &publication("new"),
            &publication("old")
        ));
        assert_eq!(
            sql(resolve(&publication("new"), &publication("old"))),
            ["SECURITY LABEL FOR dummy ON PUBLICATION p IS $$new$$;\n"]
        );
        assert!(!crate::deploy::diff::same(
            &subscription("new"),
            &subscription("old")
        ));
        assert_eq!(
            sql(resolve(&subscription("new"), &subscription("old"))),
            ["SECURITY LABEL FOR dummy ON SUBSCRIPTION s IS $$new$$;\n"]
        );
    }

    fn view(labels: Option<serde_json::Value>) -> Definition {
        let mut value = json!({
            "name": "v", "schema": "s", "owner": "postgres",
            "query": "SELECT 1",
        });
        if let Some(labels) = labels {
            value["security_labels"] = labels;
        }
        Definition::View(serde_json::from_value(value).unwrap())
    }

    /// With no `security_labels`, the project does not manage the
    /// labels: the labels of the database are not a change
    #[test]
    fn missing_field_leaves_labels_alone() {
        let db = view(Some(json!({"dummy": "x"})));
        let compared = without_unmanaged(&view(None), db.clone());
        assert!(crate::deploy::diff::same(&view(None), &compared));
        assert!(sql(resolve(&view(None), &compared)).is_empty());
        // a table that manages no labels has none to compare, also
        // on its columns
        let repo = table(json!({
            "name": "t", "schema": "s", "owner": "postgres",
            "columns": [{"name": "a", "data_type": "text"}],
        }));
        let db = table(json!({
            "name": "t", "schema": "s", "owner": "postgres",
            "columns": [{"name": "a", "data_type": "text",
                         "security_labels": {"dummy": "y"}}],
            "security_labels": {"dummy": "x"},
        }));
        assert_eq!(without_unmanaged(&repo, db), repo);
    }

    /// The labels of a view column and of a materialized view column
    /// change in place, as those of a table column do. A column that is
    /// not in `column_security_labels` keeps the labels of the database,
    /// and an empty map of a column removes its labels.
    #[test]
    fn view_column_labels_change_in_place() {
        for kind in ["View", "MaterializedView"] {
            let view = |labels: serde_json::Value| {
                let value = json!({
                    "name": "v", "schema": "s", "owner": "postgres",
                    "query": "SELECT 1 AS a, 2 AS b, 3 AS c",
                    "column_security_labels": labels,
                });
                match kind {
                    "View" => Definition::View(
                        serde_json::from_value(value).unwrap(),
                    ),
                    _ => Definition::MaterializedView(
                        serde_json::from_value(value).unwrap(),
                    ),
                }
            };
            let repo = view(json!({"a": {"dummy": "new"}, "b": {}}));
            let db = view(json!({
                "a": {"dummy": "old"}, "b": {"dummy": "x"},
                "c": {"dummy": "kept"},
            }));
            let compared = without_unmanaged(&repo, db);
            assert_eq!(
                sql(resolve(&repo, &compared)),
                [
                    "SECURITY LABEL FOR dummy ON COLUMN s.v.a IS $$new$$;\n",
                    "SECURITY LABEL FOR dummy ON COLUMN s.v.b IS NULL;\n",
                ],
                "{kind}"
            );
            // with no field, the labels of the columns are not managed
            let mut unmanaged = view(json!({}));
            match &mut unmanaged {
                Definition::View(v) => v.column_security_labels = None,
                Definition::MaterializedView(v) => {
                    v.column_security_labels = None
                }
                _ => unreachable!(),
            }
            let db = view(json!({"a": {"dummy": "x"}}));
            assert_eq!(without_unmanaged(&unmanaged, db), unmanaged);
        }
    }

    /// An explicit map is compared: a label that it does not have gets
    /// IS NULL, also when the map is empty
    #[test]
    fn explicit_map_removes_the_labels_it_omits() {
        let db = view(Some(json!({"dummy": "x", "other": "y"})));
        let repo = view(Some(json!({"other": "y"})));
        let compared = without_unmanaged(&repo, db.clone());
        assert_eq!(
            sql(resolve(&repo, &compared)),
            ["SECURITY LABEL FOR dummy ON VIEW s.v IS NULL;\n"]
        );
        let repo = view(Some(json!({})));
        let compared = without_unmanaged(&repo, db);
        assert_eq!(
            sql(resolve(&repo, &compared)),
            [
                "SECURITY LABEL FOR dummy ON VIEW s.v IS NULL;\n",
                "SECURITY LABEL FOR other ON VIEW s.v IS NULL;\n",
            ]
        );
    }
}
