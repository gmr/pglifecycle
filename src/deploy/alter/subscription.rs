//! Subscription reconciliation. Deploy runs in one transaction
//! (`psql --single-transaction`), so it uses only the forms of ALTER
//! SUBSCRIPTION that can run in a transaction block, and none of them
//! connects to the publisher. The connection, the publications, most
//! options and the comment change in place. A change that has no such
//! form is reported and not made: the script carries it as a comment.
//!
//! `enabled` is not compared: pg_dump does not write it, so deploy
//! cannot see if a subscription is enabled, and it emits no ENABLE or
//! DISABLE.

use serde_json::{Map, Value};

use super::{Alter, Resolution, push_comment, sql_comment};
use crate::deploy::diff::without_redacted_password;
use crate::models::Subscription;
use crate::pull::without_password;
use crate::utils::{postgres_value, quote_ident};

/// Why deploy does not change an option, for the options that it does
/// not change
fn not_changed(key: &str) -> Option<&'static str> {
    match key {
        "failover" => Some(
            "it cannot change in a transaction block, and it needs the \
             subscription disabled",
        ),
        "slot_name" => Some(
            "the new slot must be on the publisher, the old slot stays \
             there, and NONE needs the subscription disabled",
        ),
        "two_phase" => Some(
            "it needs the subscription disabled, and a change to false \
             cannot run in a transaction block",
        ),
        _ => None,
    }
}

/// Reconcile a subscription. Changed publications are set with
/// `refresh = false`, as a refresh cannot run in a transaction block,
/// so the report tells the operator to refresh them after the deploy.
pub(super) fn subscription(
    repo: &Subscription,
    db: &Subscription,
) -> Resolution {
    let wanted = repo.canonical();
    let existing = without_redacted_password(repo, db).canonical();
    let name = quote_ident(&repo.name);
    let mut alters = Vec::new();
    if wanted.connection != existing.connection {
        if without_password(&repo.connection).is_none()
            && without_password(&db.connection).is_some()
        {
            alters.push(not_made(
                &repo.name,
                "the connection changes, and the project connection has \
                 no password: deploy does not remove the password that \
                 the database has. Run ALTER SUBSCRIPTION ... CONNECTION \
                 with the password",
            ));
        } else {
            alters.push(Alter::new(format!(
                "ALTER SUBSCRIPTION {name} CONNECTION {};\n",
                postgres_value(&Value::String(repo.connection.clone()))
            )));
        }
    }
    if wanted.publications != existing.publications {
        let warning = format!(
            "SUBSCRIPTION {}: the publications change. After the deploy, \
             run ALTER SUBSCRIPTION {name} REFRESH PUBLICATION outside a \
             transaction block, with the subscription enabled",
            repo.name
        );
        log::warn!("{warning}");
        let publications: Vec<String> =
            repo.publications.iter().map(|p| quote_ident(p)).collect();
        alters.push(Alter::new(format!(
            "{}ALTER SUBSCRIPTION {name} SET PUBLICATION {} \
             WITH (refresh = false);\n",
            sql_comment(&warning),
            publications.join(", ")
        )));
    }
    let empty = Map::new();
    let repo_parameters = wanted.parameters.as_ref().unwrap_or(&empty);
    let db_parameters = existing.parameters.as_ref().unwrap_or(&empty);
    let mut options = Vec::new();
    let keys = repo_parameters.keys().chain(
        db_parameters
            .keys()
            .filter(|key| !repo_parameters.contains_key(*key)),
    );
    for key in keys {
        if repo_parameters.get(key) == db_parameters.get(key) {
            continue;
        }
        let value = match (key.as_str(), repo_parameters.get(key)) {
            ("slot_name", None) => Some(Value::String(repo.name.clone())),
            (_, Some(value)) => Some(value.clone()),
            (_, None) => Subscription::parameter_default(key),
        };
        let option = match value {
            Some(value) => format!("{key} = {}", option_value(key, &value)),
            None => {
                alters.push(not_made(
                    &repo.name,
                    &format!(
                        "the option {key} is only in the database, and \
                         deploy does not know its default"
                    ),
                ));
                continue;
            }
        };
        if let Some(reason) = not_changed(key) {
            alters.push(not_made(
                &repo.name,
                &format!(
                    "{key} changes, and deploy does not change it: {reason}. \
                     Run ALTER SUBSCRIPTION {name} SET ({option}) outside a \
                     transaction block"
                ),
            ));
        } else {
            options.push(option);
        }
    }
    if !options.is_empty() {
        alters.push(Alter::new(format!(
            "ALTER SUBSCRIPTION {name} SET ({});\n",
            options.join(", ")
        )));
    }
    push_comment(
        &mut alters,
        "SUBSCRIPTION",
        &name,
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

/// An option value as SQL. NONE is a keyword, and PostgreSQL reads a
/// boolean option from `true` or `false`.
fn option_value(key: &str, value: &Value) -> String {
    match value {
        Value::String(slot) if key == "slot_name" && slot == "NONE" => {
            String::from("NONE")
        }
        Value::Bool(value) => value.to_string(),
        value => postgres_value(value),
    }
}

/// A change that deploy reports and does not make. The script carries
/// it as a comment, so the plan is not empty while it is pending.
fn not_made(subscription: &str, reason: &str) -> Alter {
    let warning = format!("SUBSCRIPTION {subscription}: {reason}");
    log::warn!("{warning}; it was not applied");
    Alter::new(sql_comment(&format!("not applied: {warning}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscription(json: serde_json::Value) -> Subscription {
        serde_json::from_value(json).expect("subscription deserializes")
    }

    fn statements(resolution: Resolution) -> Vec<String> {
        match resolution {
            Resolution::Statements(alters) => {
                alters.into_iter().map(|a| a.sql).collect()
            }
            _ => panic!("expected in-place statements"),
        }
    }

    /// The subscription as pull reads it from pg_dump
    fn pulled() -> Subscription {
        subscription(serde_json::json!({
            "name": "sub",
            "connection": "dbname=elsewhere",
            "publications": ["pub", "Other Pub"],
            "parameters": {
                "connect": false,
                "slot_name": "sub",
                "binary": true,
                "streaming": "off",
                "origin": "none",
            },
            "comment": "A subscription",
        }))
    }

    #[test]
    fn a_short_form_is_the_same_subscription() {
        let written = subscription(serde_json::json!({
            "name": "sub",
            "connection": "dbname=elsewhere",
            "publications": ["Other Pub", "pub"],
            "parameters": {
                "enabled": false,
                "create_slot": false,
                "copy_data": false,
                "binary": true,
                "streaming": false,
                "origin": "NONE",
                "synchronous_commit": "off",
                "two_phase": false,
                "failover": false,
                "password_required": true,
            },
            "comment": "A subscription",
        }));
        assert_eq!(written.canonical(), pulled().canonical());
    }

    #[test]
    fn a_redacted_password_is_not_a_change() {
        let mut db = pulled();
        db.connection = String::from("dbname=elsewhere password=secret");
        assert_eq!(
            without_redacted_password(&pulled(), &db).canonical(),
            pulled().canonical()
        );
        // a project that carries a password changes it
        let mut repo = pulled();
        repo.connection = String::from("dbname=elsewhere password=new");
        assert_eq!(
            statements(super::subscription(&repo, &db)),
            vec![
                "ALTER SUBSCRIPTION sub CONNECTION \
                 'dbname=elsewhere password=new';\n"
            ]
        );
    }

    #[test]
    fn a_new_connection_keeps_a_password_that_the_project_has_not() {
        let mut repo = pulled();
        repo.connection = String::from("dbname=other");
        let mut db = pulled();
        db.connection = String::from("dbname=elsewhere password=secret");
        let sql = statements(super::subscription(&repo, &db));
        assert_eq!(sql.len(), 1);
        assert!(sql[0].starts_with("-- not applied: SUBSCRIPTION sub: "));
        assert!(sql[0].lines().all(|line| line.starts_with("--")));
    }

    #[test]
    fn changes_in_a_transaction_are_made_in_place() {
        let repo = subscription(serde_json::json!({
            "name": "sub",
            "connection": "dbname=other",
            "publications": ["pub"],
            "parameters": {
                "connect": false,
                "streaming": true,
                "disable_on_error": true,
                "synchronous_commit": "remote_apply",
            },
        }));
        let sql = statements(super::subscription(&repo, &pulled()));
        assert_eq!(sql.len(), 4);
        assert_eq!(
            sql[0],
            "ALTER SUBSCRIPTION sub CONNECTION 'dbname=other';\n"
        );
        assert!(sql[1].starts_with("-- SUBSCRIPTION sub: "));
        assert!(sql[1].contains("REFRESH PUBLICATION"));
        assert!(sql[1].ends_with(
            "\nALTER SUBSCRIPTION sub SET PUBLICATION pub \
             WITH (refresh = false);\n"
        ));
        assert_eq!(
            sql[2],
            "ALTER SUBSCRIPTION sub SET (streaming = 'on', \
             disable_on_error = true, synchronous_commit = 'remote_apply', \
             binary = false, origin = 'any');\n"
        );
        assert_eq!(sql[3], "COMMENT ON SUBSCRIPTION sub IS NULL;\n");
    }

    #[test]
    fn changes_outside_a_transaction_are_not_made() {
        for (key, value) in [
            ("two_phase", Value::Bool(true)),
            ("failover", Value::Bool(true)),
            ("slot_name", Value::from("none")),
            ("slot_name", Value::from("another")),
        ] {
            let mut repo = pulled();
            if let Some(parameters) = repo.parameters.as_mut() {
                parameters.insert(key.to_string(), value);
            }
            let sql = statements(super::subscription(&repo, &pulled()));
            assert_eq!(sql.len(), 1, "{key}");
            assert!(sql[0].starts_with("-- not applied: "), "{key}");
            assert!(sql[0].contains(key), "{key}");
        }
    }

    #[test]
    fn a_default_slot_name_is_the_subscription_name() {
        let mut repo = pulled();
        if let Some(parameters) = repo.parameters.as_mut() {
            parameters.remove("slot_name");
        }
        assert_eq!(repo.canonical(), pulled().canonical());
        assert_eq!(repo.slot_name(), Some("sub"));
        let mut slotless = pulled();
        if let Some(parameters) = slotless.parameters.as_mut() {
            parameters.insert("slot_name".into(), Value::from("NONE"));
        }
        assert_eq!(slotless.slot_name(), None);
        // a database without a slot name has the project's default
        let sql = statements(super::subscription(&repo, &slotless));
        assert!(sql[0].contains("SET (slot_name = 'sub')"));
    }
}
