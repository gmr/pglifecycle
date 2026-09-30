//! Casts: PostgreSQL has no ALTER CAST. A changed comment is set with
//! COMMENT ON CAST; any other change drops the cast and makes it
//! again. A cast has no schema and no owner of its own.

use super::names::{signature, type_name};
use super::{Resolution, push_comment};
use crate::deploy::diff::canonical_type;
use crate::models::Cast;

pub(super) fn cast(repo: &Cast, db: &Cast) -> Resolution {
    let without_comment = |cast: &Cast| Cast {
        comment: None,
        ..canonical(cast)
    };
    if without_comment(repo) != without_comment(db) {
        return Resolution::Replace;
    }
    let mut alters = Vec::new();
    push_comment(
        &mut alters,
        "CAST",
        &target(repo),
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

/// The cast as PostgreSQL keeps it: canonical types and function, and
/// no flag at its default. The schema only says where the project
/// files the cast, and the owner is not compared. A cast with a
/// function is not an I/O conversion cast.
pub(crate) fn canonical(cast: &Cast) -> Cast {
    let set = |value: Option<bool>| value.filter(|v| *v);
    let function = cast.function.as_deref().map(signature);
    Cast {
        schema: String::new(),
        owner: String::new(),
        source_type: cast.source_type.as_deref().map(type_name),
        target_type: cast.target_type.as_deref().map(type_name),
        inout: set(cast.inout).filter(|_| function.is_none()),
        function,
        assignment: set(cast.assignment),
        implicit: set(cast.implicit),
        ..cast.clone()
    }
}

/// `(source AS target)`, which COMMENT ON and DROP CAST read
fn target(cast: &Cast) -> String {
    format!(
        "({} AS {})",
        cast.source_type.as_deref().unwrap_or_default(),
        cast.target_type.as_deref().unwrap_or_default()
    )
}

/// The DROP statement of a cast that only the database has
pub(crate) fn drop(cast: &Cast) -> String {
    format!("DROP CAST IF EXISTS {};\n", target(cast))
}

/// The identity of a cast archive entry, from its tag `CAST (source AS
/// target)`, in the form of `diff::object_identity`
pub(crate) fn entry_name(entry: &libpgdump::Entry) -> Option<String> {
    let tag = entry.tag.as_deref()?.strip_prefix("CAST ")?;
    let inner = tag.strip_prefix('(')?.strip_suffix(')')?;
    let mut quoted = false;
    let split = inner.char_indices().find_map(|(index, c)| {
        if c == '"' {
            quoted = !quoted;
        }
        (!quoted && inner[index..].starts_with(" AS ")).then_some(index)
    })?;
    Some(format!(
        "({} AS {})",
        canonical_type(&inner[..split]),
        canonical_type(&inner[split + 4..])
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::constants::ObjectType;
    use crate::deploy::diff::ObjectKey;
    use crate::models::Definition;

    fn parse(value: serde_json::Value) -> Cast {
        serde_json::from_value(value).expect("cast deserializes")
    }

    #[test]
    fn short_forms_are_canonical() {
        let pulled = parse(json!({
            "schema": "test", "owner": "",
            "source_type": "test.point_pair", "target_type": "integer",
            "function": "test.point_pair_x(test.point_pair)",
            "assignment": true,
        }));
        let written = parse(json!({
            "schema": "public", "owner": "postgres",
            "source_type": "TEST.POINT_PAIR", "target_type": "INT4",
            "function": "TEST.POINT_PAIR_X( TEST.POINT_PAIR )",
            "inout": false, "assignment": true, "implicit": false,
        }));
        assert_eq!(canonical(&written), canonical(&pulled));
    }

    #[test]
    fn only_a_comment_changes_in_place() {
        let cast = |comment: Option<&str>, implicit: bool| {
            let mut value = json!({
                "schema": "test", "owner": "",
                "source_type": "test.point_pair", "target_type": "text",
                "inout": true, "implicit": implicit,
            });
            if let Some(comment) = comment {
                value["comment"] = comment.into();
            }
            parse(value)
        };
        let Resolution::Statements(alters) =
            super::cast(&cast(None, false), &cast(Some("drift"), false))
        else {
            panic!("expected statements");
        };
        assert_eq!(
            alters[0].sql,
            "COMMENT ON CAST (test.point_pair AS text) IS NULL;\n"
        );
        assert!(matches!(
            super::cast(&cast(None, false), &cast(None, true)),
            Resolution::Replace
        ));
    }

    #[test]
    fn the_entry_name_is_the_cast_key() {
        let mut dump =
            libpgdump::new("test", "UTF8", "18.0").expect("new dump");
        let id = dump
            .add_entry(
                libpgdump::ObjectType::Cast,
                None,
                Some("CAST (\"gate.dotted\".\"pair AS t\" AS text)"),
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
        let cast = parse(json!({
            "schema": "gate.dotted", "owner": "",
            "source_type": "\"gate.dotted\".\"pair AS t\"",
            "target_type": "TEXT", "inout": true,
        }));
        assert_eq!(
            entry_name(entry),
            Some(
                ObjectKey::new(
                    ObjectType::Cast,
                    &Definition::Cast(cast.clone())
                )
                .name
            )
        );
        assert_eq!(
            drop(&cast),
            "DROP CAST IF EXISTS (\"gate.dotted\".\"pair AS t\" AS TEXT);\n"
        );
    }
}
