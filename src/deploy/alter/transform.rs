//! Transforms: a changed function is set with `CREATE OR REPLACE
//! TRANSFORM`, which replaces both functions and keeps the comment. A
//! changed comment alone is set with COMMENT ON TRANSFORM. The type and
//! the language are the identity of a transform, so no change needs a
//! drop. A transform has no schema and no owner of its own.

use super::names::{signature, type_name};
use super::{Resolution, comment_delta, push_comment};
use crate::deploy::diff::canonical_type;
use crate::models::Transform;
use crate::utils::quote_ident;

pub(super) fn transform(repo: &Transform, db: &Transform) -> Resolution {
    let without_comment = |transform: &Transform| Transform {
        comment: None,
        ..canonical(transform)
    };
    let target = target(repo);
    if without_comment(repo) == without_comment(db) {
        let mut alters = Vec::new();
        push_comment(
            &mut alters,
            "TRANSFORM",
            &target,
            &repo.comment,
            &db.comment,
        );
        return Resolution::Statements(alters);
    }
    Resolution::OrReplace {
        comment: comment_delta(
            "TRANSFORM",
            &target,
            &repo.comment,
            &db.comment,
        ),
        then: Vec::new(),
    }
}

/// The transform as PostgreSQL keeps it: a canonical type and
/// canonical functions. The schema only says where the project files
/// the transform.
pub(crate) fn canonical(transform: &Transform) -> Transform {
    Transform {
        schema: String::new(),
        data_type: type_name(&transform.data_type),
        from_sql: transform.from_sql.as_deref().map(signature),
        to_sql: transform.to_sql.as_deref().map(signature),
        ..transform.clone()
    }
}

/// `FOR type LANGUAGE language`, which COMMENT ON and DROP TRANSFORM
/// read. The build quotes the language, so this does too.
fn target(transform: &Transform) -> String {
    format!(
        "FOR {} LANGUAGE {}",
        transform.data_type,
        quote_ident(&transform.language)
    )
}

/// The DROP statement of a transform that only the database has
pub(crate) fn drop(transform: &Transform) -> String {
    format!("DROP TRANSFORM IF EXISTS {};\n", target(transform))
}

/// The identity of a transform archive entry, from its tag `TRANSFORM
/// FOR type LANGUAGE language`, in the form of `diff::object_identity`
pub(crate) fn entry_name(entry: &libpgdump::Entry) -> Option<String> {
    let tag = entry.tag.as_deref()?.strip_prefix("TRANSFORM FOR ")?;
    let (data_type, language) = tag.rsplit_once(" LANGUAGE ")?;
    Some(format!(
        "FOR {} LANGUAGE {language}",
        canonical_type(data_type)
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::constants::ObjectType;
    use crate::deploy::diff::ObjectKey;
    use crate::models::Definition;

    fn parse(value: serde_json::Value) -> Transform {
        serde_json::from_value(value).expect("transform deserializes")
    }

    fn base_int(from_sql: Option<&str>, comment: Option<&str>) -> Transform {
        let mut value = json!({
            "schema": "test", "type": "test.base_int", "language": "sql",
            "to_sql": "test.base_int_to_sql(internal)",
        });
        if let Some(from_sql) = from_sql {
            value["from_sql"] = from_sql.into();
        }
        if let Some(comment) = comment {
            value["comment"] = comment.into();
        }
        parse(value)
    }

    #[test]
    fn short_forms_are_canonical() {
        let written = parse(json!({
            "schema": "public", "type": "TEST.BASE_INT", "language": "sql",
            "to_sql": "TEST.BASE_INT_TO_SQL(INTERNAL)",
        }));
        assert_eq!(canonical(&written), canonical(&base_int(None, None)));
    }

    #[test]
    fn a_changed_function_uses_or_replace() {
        let repo = base_int(Some("test.base_int_from_sql(internal)"), None);
        let db = base_int(None, Some("old"));
        let Resolution::OrReplace { comment, .. } = transform(&repo, &db)
        else {
            panic!("expected OR REPLACE");
        };
        assert_eq!(
            comment.as_deref(),
            Some(
                "COMMENT ON TRANSFORM FOR test.base_int LANGUAGE sql IS NULL;\n"
            )
        );
        let Resolution::Statements(alters) =
            transform(&base_int(None, None), &db)
        else {
            panic!("expected statements");
        };
        assert_eq!(alters.len(), 1);
    }

    #[test]
    fn the_entry_name_is_the_transform_key() {
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let id = dump
            .add_entry(
                libpgdump::ObjectType::Transform,
                None,
                Some("TRANSFORM FOR test.base_int LANGUAGE plpgsql"),
                None,
                None,
                None,
                None,
                &[],
            )
            .expect("add entry");
        let entry = dump
            .entries()
            .iter()
            .find(|entry| entry.dump_id == id)
            .expect("the entry");
        let transform = parse(json!({
            "schema": "test", "type": "test.base_int",
            "language": "plpgsql",
            "to_sql": "test.base_int_to_sql(internal)",
        }));
        assert_eq!(
            entry_name(entry),
            Some(
                ObjectKey::new(
                    ObjectType::Transform,
                    &Definition::Transform(transform.clone())
                )
                .name
            )
        );
        assert_eq!(
            drop(&transform),
            "DROP TRANSFORM IF EXISTS FOR test.base_int LANGUAGE plpgsql;\n"
        );
    }
}
