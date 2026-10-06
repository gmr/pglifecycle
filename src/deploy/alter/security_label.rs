//! Security labels: a label that is different or that only the
//! project has gets `SECURITY LABEL FOR provider ON ... IS label`, and
//! a label that only the database has gets `IS NULL`, as a comment
//! does. The removal is not destructive: it loses no data.
//!
//! A project object with no `security_labels` does not manage its
//! labels: deploy leaves the labels of the database as they are (see
//! [`without_unmanaged`]). An explicit map, also an empty one, is
//! compared, so a label that it does not have is removed. The same is
//! true for each column and partition of a table.
//!
//! A change to the labels only changes nothing else, so the object
//! keeps its in-place form, even a type that otherwise has no in-place
//! form (a language). The labels of a table's columns and partitions
//! change with the table.

use std::collections::BTreeSet;

use super::{Alter, function_target, qualified};
use crate::build::security_label_sql;
use crate::models::{Definition, SecurityLabels};
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
/// object, and on each column and partition of a table. The project
/// does not manage those labels, so they are not a change.
pub(crate) fn without_unmanaged(
    repo: &Definition,
    db: Definition,
) -> Definition {
    if repo.security_labels().is_none() {
        // the object labels; a table keeps those of its children
        let mut stripped = without(&db);
        if let (Definition::Table(stripped), Definition::Table(db)) =
            (&mut stripped, &db)
        {
            stripped.columns.clone_from(&db.columns);
            stripped.partitions.clone_from(&db.partitions);
        }
        return keep_children(repo, stripped);
    }
    keep_children(repo, db)
}

/// `db` with no labels on each column and partition that has no
/// `security_labels` in `repo`
fn keep_children(repo: &Definition, mut db: Definition) -> Definition {
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
        Definition::MaterializedView(d) => d.security_labels = None,
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
        Definition::View(d) => d.security_labels = None,
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
