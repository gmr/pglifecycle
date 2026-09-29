//! Publication reconciliation. The members, the parameters and the
//! comment change in place. A change to or from FOR ALL TABLES has no
//! ALTER form, so it drops and makes the publication again.

use serde_json::Map;

use super::{Alter, Resolution, push_comment, sql_comment};
use crate::build::{render_publication_parameters, render_publication_table};
use crate::models::{Publication, PublicationTable};
use crate::utils::quote_ident;

/// Reconcile a publication. One `ALTER PUBLICATION ... SET` gives all
/// the tables and schemas, as SET replaces the members that the
/// publication has. A publication with no members takes no SET, so the
/// members that the database has are dropped.
///
/// PostgreSQL refuses a row filter or a column list on a partitioned
/// table while `publish_via_partition_root` is false, so the
/// parameters change first when the project sets it to true, and last
/// when it does not.
pub(super) fn publication(repo: &Publication, db: &Publication) -> Resolution {
    let wanted = repo.canonical();
    let existing = db.canonical();
    if wanted.all_tables != existing.all_tables {
        return Resolution::Replace;
    }
    let name = quote_ident(&repo.name);
    let mut members = Vec::new();
    if wanted.tables != existing.tables || wanted.schemas != existing.schemas {
        let objects =
            |publication: &Publication,
             table: fn(&PublicationTable) -> String| {
                publication
                    .tables
                    .iter()
                    .flatten()
                    .map(table)
                    .chain(publication.schemas.iter().flatten().map(
                        |schema| {
                            format!("TABLES IN SCHEMA {}", quote_ident(schema))
                        },
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
        let set = objects(&wanted, render_publication_table);
        members.push(Alter::new(if set.is_empty() {
            format!(
                "ALTER PUBLICATION {name} DROP {};\n",
                objects(&existing, |table| format!(
                    "TABLE ONLY {}",
                    table.name()
                ))
            )
        } else {
            format!("ALTER PUBLICATION {name} SET {set};\n")
        }));
    }
    let empty = Map::new();
    let repo_parameters = wanted.parameters.as_ref().unwrap_or(&empty);
    let db_parameters = existing.parameters.as_ref().unwrap_or(&empty);
    let mut changed = Map::new();
    let keys = repo_parameters.keys().chain(
        db_parameters
            .keys()
            .filter(|key| !repo_parameters.contains_key(*key)),
    );
    for key in keys {
        if repo_parameters.get(key) == db_parameters.get(key) {
            continue;
        }
        if let Some(value) = repo_parameters
            .get(key)
            .cloned()
            .or_else(|| Publication::parameter_default(key))
        {
            changed.insert(key.clone(), value);
        }
    }
    let mut parameters = Vec::new();
    if !changed.is_empty() {
        let mut sql = String::new();
        if changed.contains_key("publish_via_partition_root") {
            let warning = format!(
                "PUBLICATION {}: publish_via_partition_root changes, so a \
                 subscriber of a partitioned table can copy rows two times \
                 or lose rows. Do not change the partitions until each \
                 subscriber runs ALTER SUBSCRIPTION ... REFRESH PUBLICATION \
                 WITH (copy_data = false)",
                repo.name
            );
            log::warn!("{warning}");
            sql.push_str(&sql_comment(&format!("WARNING: {warning}")));
        }
        sql.push_str(&format!(
            "ALTER PUBLICATION {name} SET ({});\n",
            render_publication_parameters(&changed)
        ));
        parameters.push(Alter::new(sql));
    }
    let via_root = repo_parameters.get("publish_via_partition_root")
        == Some(&serde_json::Value::Bool(true));
    let mut alters = if via_root {
        parameters.into_iter().chain(members).collect()
    } else {
        members.into_iter().chain(parameters).collect()
    };
    push_comment(
        &mut alters,
        "PUBLICATION",
        &name,
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publication(json: serde_json::Value) -> Publication {
        serde_json::from_value(json).expect("publication deserializes")
    }

    fn statements(resolution: Resolution) -> Vec<String> {
        match resolution {
            Resolution::Statements(alters) => {
                alters.into_iter().map(|a| a.sql).collect()
            }
            _ => panic!("expected in-place statements"),
        }
    }

    /// The publication as pull reads it from pg_dump
    fn pulled() -> Publication {
        publication(serde_json::json!({
            "name": "pub_some",
            "tables": [
                {"name": "test.replicated", "columns": ["id", "amount"],
                 "where": "(amount > 0)"},
                "test.other",
            ],
            "parameters": {
                "publish": ["insert", "update"],
                "publish_via_partition_root": true,
            },
            "comment": "Positive amounts only",
        }))
    }

    #[test]
    fn a_short_form_is_the_same_publication() {
        let written = publication(serde_json::json!({
            "name": "pub_some",
            "tables": [
                "TEST.Other",
                {"name": "\"test\".\"replicated\"",
                 "columns": ["amount", "id"], "where": "amount > 0"},
            ],
            "parameters": {
                "publish": ["update", "INSERT"],
                "publish_via_partition_root": true,
                "publish_generated_columns": "NONE",
            },
            "comment": "Positive amounts only",
        }));
        assert_eq!(written.canonical(), pulled().canonical());
    }

    #[test]
    fn parameters_at_their_defaults_are_absent() {
        let written = publication(serde_json::json!({
            "name": "all",
            "all_tables": true,
            "parameters": {
                "publish": ["insert", "update", "delete", "truncate"],
                "publish_via_partition_root": false,
            },
        }));
        let pulled = publication(serde_json::json!({
            "name": "all",
            "all_tables": true,
        }));
        assert_eq!(written.canonical(), pulled.canonical());
        let empty = publication(serde_json::json!({
            "name": "none", "all_tables": false, "tables": [],
        }));
        assert_eq!(
            empty.canonical(),
            publication(serde_json::json!({"name": "none"}))
        );
    }

    #[test]
    fn members_change_in_one_set() {
        let mut repo = pulled();
        repo.tables = Some(vec![PublicationTable::Name("test.other".into())]);
        repo.schemas = Some(vec!["Audit".into()]);
        assert_eq!(
            statements(super::publication(&repo, &pulled())),
            vec![
                "ALTER PUBLICATION pub_some SET TABLE ONLY test.other, \
                 TABLES IN SCHEMA \"Audit\";\n"
            ]
        );
    }

    #[test]
    fn a_publication_with_no_members_drops_them() {
        let repo = publication(serde_json::json!({"name": "pub_some"}));
        let mut db = pulled();
        db.schemas = Some(vec!["audit".into()]);
        db.parameters = None;
        db.comment = None;
        assert_eq!(
            statements(super::publication(&repo, &db)),
            vec![
                "ALTER PUBLICATION pub_some DROP TABLE ONLY test.other, \
                 TABLE ONLY test.replicated, TABLES IN SCHEMA audit;\n"
            ]
        );
    }

    #[test]
    fn a_changed_row_filter_sets_the_table() {
        let mut repo = pulled();
        if let Some(PublicationTable::Filtered(table)) =
            repo.tables.as_mut().and_then(|t| t.first_mut())
        {
            table.row_filter = Some(String::from("amount > 10"));
        }
        assert_eq!(
            statements(super::publication(&repo, &pulled())),
            vec![
                "ALTER PUBLICATION pub_some SET TABLE ONLY test.other, \
                 TABLE ONLY test.replicated (amount, id) \
                 WHERE (amount > 10);\n"
            ]
        );
    }

    #[test]
    fn a_removed_parameter_is_set_to_its_default() {
        let mut repo = pulled();
        repo.parameters = None;
        let sql = statements(super::publication(&repo, &pulled()));
        assert_eq!(sql.len(), 1);
        assert!(sql[0].starts_with("-- WARNING: PUBLICATION pub_some: "));
        assert!(sql[0].ends_with(
            "ALTER PUBLICATION pub_some SET (publish = 'insert, update, \
             delete, truncate', publish_via_partition_root = false);\n"
        ));
    }

    #[test]
    fn via_partition_root_orders_the_statements() {
        // to true: the parameters first, so that a partitioned table
        // can take a row filter
        let mut db = pulled();
        db.parameters = None;
        db.tables = None;
        let sql = statements(super::publication(&pulled(), &db));
        assert!(sql[0].contains("SET (publish = 'insert, update'"));
        assert!(sql[1].contains("SET TABLE ONLY"));
        // to false: the members first
        let sql = statements(super::publication(&db, &pulled()));
        assert!(sql[0].contains("DROP TABLE ONLY"));
        assert!(sql[1].contains("publish_via_partition_root = false"));
    }

    #[test]
    fn for_all_tables_is_replaced() {
        let all = publication(serde_json::json!({
            "name": "pub_some", "all_tables": true,
        }));
        assert!(matches!(
            super::publication(&all, &pulled()),
            Resolution::Replace
        ));
        assert!(matches!(
            super::publication(&pulled(), &all),
            Resolution::Replace
        ));
    }

    #[test]
    fn a_comment_changes_in_place() {
        let mut repo = pulled();
        repo.comment = None;
        assert_eq!(
            statements(super::publication(&repo, &pulled())),
            vec!["COMMENT ON PUBLICATION pub_some IS NULL;\n"]
        );
    }
}
