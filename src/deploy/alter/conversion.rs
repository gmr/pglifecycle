//! Conversions. PostgreSQL has no ALTER for the encodings, the function
//! or the default flag, so a change to one of them drops and makes the
//! conversion again. The comment changes in place, and the owner too
//! (see `deploy::owner_sql`).

use super::{Resolution, push_comment, qualified};
use crate::models::{Conversion, canonical_relation};

pub(super) fn conversion(repo: &Conversion, db: &Conversion) -> Resolution {
    // the owner changes on its own (`Diff::owner_changed`)
    let settings = |conversion: &Conversion| Conversion {
        owner: String::new(),
        comment: None,
        ..canonical(conversion)
    };
    if settings(repo) != settings(db) {
        return Resolution::Replace;
    }
    let mut alters = Vec::new();
    push_comment(
        &mut alters,
        "CONVERSION",
        &qualified(&repo.schema, &repo.name),
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

/// The conversion in the form that deploy compares. `default: false`
/// is the same as no `default`. An encoding is the name that PostgreSQL
/// resolves it to (see [`canonical_encoding`]), and the function is
/// named as [`canonical_function`] gives it.
pub(crate) fn canonical(conversion: &Conversion) -> Conversion {
    Conversion {
        default: conversion.default.filter(|default| *default),
        encoding_from: conversion
            .encoding_from
            .as_deref()
            .map(canonical_encoding),
        encoding_to: conversion.encoding_to.as_deref().map(canonical_encoding),
        function: conversion.function.as_deref().map(canonical_function),
        ..conversion.clone()
    }
}

/// A function name as pg_dump writes it: a name that is not quoted
/// folds to lowercase, each part is quoted only when it must be, a
/// function in `pg_catalog` has no schema, and the empty argument list
/// of an event trigger function is removed. Thus `PG_CATALOG.BTHANDLER`
/// and `bthandler` are the same function.
pub(super) fn canonical_function(name: &str) -> String {
    let name = name.trim();
    let name = name
        .strip_suffix(')')
        .and_then(|name| name.trim_end().strip_suffix('('))
        .unwrap_or(name);
    let name = canonical_relation(name);
    match name.strip_prefix("pg_catalog.") {
        Some(name) => name.to_string(),
        None => name,
    }
}

/// An encoding name as PostgreSQL compares it (`clean_encoding_name`
/// and `pg_encname_tbl` in src/common/encnames.c): only its letters and
/// digits, in lowercase, with an alias changed to the encoding it is an
/// alias for. Thus `utf-8`, `Unicode` and `UTF8` are the same encoding.
fn canonical_encoding(name: &str) -> String {
    let clean: String = name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    // the aliases whose clean form is not the clean form of the
    // encoding name, as PostgreSQL 18 resolves them
    let encoding = match clean.as_str() {
        "abc" | "tcvn" | "tcvn5712" | "vscii" | "windows1258" => "win1258",
        "alt" | "windows866" => "win866",
        "iso88591" => "latin1",
        "iso88592" => "latin2",
        "iso88593" => "latin3",
        "iso88594" => "latin4",
        "iso88599" => "latin5",
        "iso885910" => "latin6",
        "iso885913" => "latin7",
        "iso885914" => "latin8",
        "iso885915" => "latin9",
        "iso885916" => "latin10",
        "koi8" => "koi8r",
        "mskanji" | "shiftjis" | "win932" | "windows932" => "sjis",
        "unicode" => "utf8",
        "win" | "windows1251" => "win1251",
        "win936" | "windows936" => "gbk",
        "win949" | "windows949" => "uhc",
        "win950" | "windows950" => "big5",
        "windows1250" => "win1250",
        "windows1252" => "win1252",
        "windows1253" => "win1253",
        "windows1254" => "win1254",
        "windows1255" => "win1255",
        "windows1256" => "win1256",
        "windows1257" => "win1257",
        "windows874" => "win874",
        other => other,
    };
    encoding.to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::deploy::alter::Alter;

    fn parse(value: serde_json::Value) -> Conversion {
        serde_json::from_value(value).expect("conversion deserializes")
    }

    fn pulled(comment: Option<&str>) -> Conversion {
        let mut value = json!({
            "name": "latin1_to_utf8",
            "schema": "test",
            "owner": "postgres",
            "default": true,
            "encoding_from": "LATIN1",
            "encoding_to": "UTF8",
            "function": "iso8859_1_to_utf8",
        });
        if let Some(comment) = comment {
            value["comment"] = comment.into();
        }
        parse(value)
    }

    fn sql(resolution: Resolution) -> Vec<String> {
        match resolution {
            Resolution::Statements(alters) => {
                alters.into_iter().map(|a: Alter| a.sql).collect()
            }
            _ => panic!("expected in-place statements"),
        }
    }

    #[test]
    fn short_forms_are_the_same_conversion() {
        let written = parse(json!({
            "name": "latin1_to_utf8",
            "schema": "test",
            "owner": "postgres",
            "default": true,
            "encoding_from": "iso-8859-1",
            "encoding_to": "Unicode",
            "function": "PG_CATALOG.ISO8859_1_TO_UTF8",
        }));
        assert_eq!(canonical(&written), canonical(&pulled(None)));
        assert!(sql(conversion(&written, &pulled(None))).is_empty());
        // the owner changes on its own, so it is not a rebuild
        let owned = Conversion {
            owner: String::from("app"),
            ..written.clone()
        };
        assert!(sql(conversion(&owned, &pulled(None))).is_empty());
        // false is the default
        let plain = |default: Option<bool>| {
            canonical(&Conversion {
                default,
                ..pulled(None)
            })
        };
        assert_eq!(plain(Some(false)), plain(None));
        assert_ne!(plain(Some(true)), plain(None));
    }

    #[test]
    fn encodings_resolve_as_postgres_resolves_them() {
        for (alias, encoding) in [
            ("utf-8", "utf8"),
            ("UTF8", "utf8"),
            ("Latin-1", "latin1"),
            ("ISO_8859_15", "latin9"),
            ("EUC_JP", "eucjp"),
            ("Shift_JIS", "sjis"),
            ("windows-1252", "win1252"),
            ("WIN", "win1251"),
        ] {
            assert_eq!(canonical_encoding(alias), encoding, "{alias}");
        }
    }

    #[test]
    fn function_names_resolve_as_pg_dump_writes_them() {
        assert_eq!(canonical_function("PG_CATALOG.BTHANDLER"), "bthandler");
        assert_eq!(canonical_function("Test.Note_DDL( )"), "test.note_ddl");
        assert_eq!(
            canonical_function("\"My Schema\".\"F\"()"),
            "\"My Schema\".\"F\""
        );
    }

    #[test]
    fn a_comment_changes_in_place() {
        assert_eq!(
            sql(conversion(&pulled(None), &pulled(Some("old")))),
            ["COMMENT ON CONVERSION test.latin1_to_utf8 IS NULL;\n"]
        );
    }

    #[test]
    fn other_changes_replace() {
        let db = pulled(None);
        for repo in [
            Conversion {
                default: None,
                ..db.clone()
            },
            Conversion {
                encoding_from: Some(String::from("LATIN2")),
                ..db.clone()
            },
            Conversion {
                function: Some(String::from("test.convert")),
                ..db.clone()
            },
        ] {
            assert!(matches!(conversion(&repo, &db), Resolution::Replace));
        }
    }
}
