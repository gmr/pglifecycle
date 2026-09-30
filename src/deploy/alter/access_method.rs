//! Access methods. PostgreSQL has no ALTER ACCESS METHOD, so a changed
//! type or handler drops and makes the access method again. The comment
//! changes in place. An access method has no owner.

use super::conversion::canonical_function;
use super::{Resolution, push_comment};
use crate::models::AccessMethod;
use crate::utils::quote_ident;

pub(super) fn access_method(
    repo: &AccessMethod,
    db: &AccessMethod,
) -> Resolution {
    let without_comment = |method: &AccessMethod| AccessMethod {
        comment: None,
        ..canonical(method)
    };
    if without_comment(repo) != without_comment(db) {
        return Resolution::Replace;
    }
    let mut alters = Vec::new();
    push_comment(
        &mut alters,
        "ACCESS METHOD",
        &quote_ident(&repo.name),
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

/// The access method in the form that deploy compares: the type in
/// uppercase, and the handler named as
/// `conversion::canonical_function` gives it
pub(crate) fn canonical(method: &AccessMethod) -> AccessMethod {
    AccessMethod {
        method_type: method.method_type.to_uppercase(),
        handler: canonical_function(&method.handler),
        ..method.clone()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse(value: serde_json::Value) -> AccessMethod {
        serde_json::from_value(value).expect("access method deserializes")
    }

    #[test]
    fn short_forms_are_the_same_access_method() {
        let pulled = parse(json!({
            "name": "btree_copy", "type": "INDEX", "handler": "bthandler",
        }));
        let written = parse(json!({
            "name": "btree_copy", "type": "index",
            "handler": "pg_catalog.BTHANDLER",
        }));
        assert_eq!(canonical(&written), canonical(&pulled));
        assert!(matches!(
            access_method(&written, &pulled),
            Resolution::Statements(alters) if alters.is_empty()
        ));
    }

    #[test]
    fn a_comment_changes_in_place() {
        let with = |comment: &str| {
            parse(json!({
                "name": "Heap Copy", "type": "TABLE",
                "handler": "heap_tableam_handler", "comment": comment,
            }))
        };
        let Resolution::Statements(alters) =
            access_method(&with("new"), &with("old"))
        else {
            panic!("expected in-place statements");
        };
        assert_eq!(
            alters.iter().map(|a| a.sql.as_str()).collect::<Vec<_>>(),
            ["COMMENT ON ACCESS METHOD \"Heap Copy\" IS $$new$$;\n"]
        );
    }

    #[test]
    fn other_changes_replace() {
        let db = parse(json!({
            "name": "am", "type": "INDEX", "handler": "bthandler",
        }));
        for repo in [
            AccessMethod {
                handler: String::from("hashhandler"),
                ..db.clone()
            },
            AccessMethod {
                method_type: String::from("TABLE"),
                ..db.clone()
            },
        ] {
            assert!(matches!(access_method(&repo, &db), Resolution::Replace));
        }
    }
}
