//! Event triggers. `ALTER EVENT TRIGGER` changes the state in place:
//! `ENABLE`, `ENABLE REPLICA`, `ENABLE ALWAYS` or `DISABLE`. The comment
//! also changes in place. PostgreSQL has no ALTER for the event, the
//! tags or the function, so a change to one of them drops and makes the
//! trigger again.

use super::conversion::canonical_function;
use super::{Alter, Resolution, push_comment};
use crate::models::{EventTrigger, EventTriggerFilter};
use crate::utils::quote_ident;

pub(super) fn event_trigger(
    repo: &EventTrigger,
    db: &EventTrigger,
) -> Resolution {
    let wanted = canonical(repo);
    let existing = canonical(db);
    if wanted.event != existing.event
        || wanted.filter != existing.filter
        || wanted.function != existing.function
    {
        return Resolution::Replace;
    }
    let name = quote_ident(&repo.name);
    let mut alters = Vec::new();
    if wanted.enabled != existing.enabled {
        let state = match wanted.enabled.as_deref() {
            None => "ENABLE",
            Some("REPLICA") => "ENABLE REPLICA",
            Some("ALWAYS") => "ENABLE ALWAYS",
            Some("DISABLED") => "DISABLE",
            // the schema does not permit another state, and the build
            // refuses one
            Some(_) => return Resolution::Replace,
        };
        alters.push(Alter::new(format!(
            "ALTER EVENT TRIGGER {name} {state};\n"
        )));
    }
    push_comment(
        &mut alters,
        "EVENT TRIGGER",
        &name,
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

/// The event trigger in the form that deploy compares. PostgreSQL keeps
/// the tags in uppercase, and they are a set, so they are in a fixed
/// order with no duplicates. The event is in lowercase, the function is
/// named as `conversion::canonical_function` gives it, and the state is
/// in uppercase, with ORIGIN, the default, absent.
pub(crate) fn canonical(trigger: &EventTrigger) -> EventTrigger {
    let mut tags: Vec<String> = trigger
        .filter
        .iter()
        .flat_map(|filter| filter.tags.iter())
        .map(|tag| tag.trim().to_uppercase())
        .collect();
    tags.sort();
    tags.dedup();
    EventTrigger {
        event: trigger.event.as_deref().map(str::to_lowercase),
        filter: (!tags.is_empty()).then_some(EventTriggerFilter { tags }),
        function: trigger.function.as_deref().map(canonical_function),
        enabled: trigger
            .enabled
            .as_deref()
            .map(str::to_uppercase)
            .filter(|state| state != "ORIGIN"),
        ..trigger.clone()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse(value: serde_json::Value) -> EventTrigger {
        serde_json::from_value(value).expect("event trigger deserializes")
    }

    fn pulled(enabled: Option<&str>) -> EventTrigger {
        let mut value = json!({
            "name": "ddl_start",
            "event": "ddl_command_start",
            "filter": {"tags": ["CREATE TABLE", "DROP TABLE"]},
            "function": "test.note_ddl()",
        });
        if let Some(enabled) = enabled {
            value["enabled"] = enabled.into();
        }
        parse(value)
    }

    fn sql(resolution: Resolution) -> Vec<String> {
        match resolution {
            Resolution::Statements(alters) => {
                alters.into_iter().map(|a| a.sql).collect()
            }
            _ => panic!("expected in-place statements"),
        }
    }

    #[test]
    fn short_forms_are_the_same_trigger() {
        let written = parse(json!({
            "name": "ddl_start",
            "event": "DDL_COMMAND_START",
            "filter": {"tags": ["drop table", "Create Table", "DROP TABLE"]},
            "function": "TEST.Note_DDL()",
            "enabled": "origin",
        }));
        assert_eq!(canonical(&written), canonical(&pulled(None)));
        assert!(sql(event_trigger(&written, &pulled(None))).is_empty());
    }

    #[test]
    fn states_change_in_place() {
        for (repo, db, statement) in [
            (None, Some("DISABLED"), "ENABLE"),
            (Some("ORIGIN"), Some("ALWAYS"), "ENABLE"),
            (Some("REPLICA"), None, "ENABLE REPLICA"),
            (Some("ALWAYS"), None, "ENABLE ALWAYS"),
            (Some("DISABLED"), Some("REPLICA"), "DISABLE"),
        ] {
            assert_eq!(
                sql(event_trigger(&pulled(repo), &pulled(db))),
                [format!("ALTER EVENT TRIGGER ddl_start {statement};\n")]
            );
        }
    }

    #[test]
    fn a_comment_changes_in_place() {
        let with = |comment: Option<&str>| EventTrigger {
            name: String::from("Stray Trigger"),
            comment: comment.map(String::from),
            ..pulled(None)
        };
        assert_eq!(
            sql(event_trigger(&with(None), &with(Some("old")))),
            ["COMMENT ON EVENT TRIGGER \"Stray Trigger\" IS NULL;\n"]
        );
    }

    #[test]
    fn other_changes_replace() {
        let db = pulled(None);
        for repo in [
            EventTrigger {
                event: Some(String::from("ddl_command_end")),
                ..db.clone()
            },
            EventTrigger {
                filter: None,
                ..db.clone()
            },
            EventTrigger {
                function: Some(String::from("test.other()")),
                ..db.clone()
            },
        ] {
            assert!(matches!(event_trigger(&repo, &db), Resolution::Replace));
        }
    }
}
