//! Collations. `ALTER COLLATION` cannot change the provider, the locale
//! settings, the rules or `deterministic`, so a change to one of them
//! drops and makes the collation again. The comment changes in place,
//! and the owner too (see `deploy::owner_sql`).

use super::{Resolution, push_comment, qualified};
use crate::models::Collation;

pub(super) fn collation(repo: &Collation, db: &Collation) -> Resolution {
    // the owner changes on its own (`Diff::owner_changed`)
    let settings = |collation: &Collation| Collation {
        owner: String::new(),
        comment: None,
        ..canonical(collation)
    };
    if settings(repo) != settings(db) {
        return Resolution::Replace;
    }
    let mut alters = Vec::new();
    push_comment(
        &mut alters,
        "COLLATION",
        &qualified(&repo.schema, &repo.name),
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

/// The collation in the form that deploy compares. A value at its
/// default is absent: the provider `libc` and `deterministic: true`. The
/// same `lc_collate` and `lc_ctype` are one `locale`, as pg_dump writes
/// them. The version is not compared: PostgreSQL gets it from the
/// provider, and pg_dump does not write it. A simple ICU locale is in
/// the standard form that PostgreSQL gives it (see [`icu_locale`]).
///
/// A collation made `FROM` another is only checked for existence (see
/// `Definition::raw_sql`), as pg_dump writes the settings it copied.
pub(crate) fn canonical(collation: &Collation) -> Collation {
    let provider = collation
        .provider
        .as_deref()
        .map(str::to_lowercase)
        .filter(|provider| provider != "libc");
    let (mut locale, mut lc_collate, mut lc_ctype) = (
        collation.locale.clone(),
        collation.lc_collate.clone(),
        collation.lc_ctype.clone(),
    );
    if locale.is_none() && lc_collate.is_some() && lc_collate == lc_ctype {
        locale = lc_collate.take();
        lc_ctype = None;
    }
    if provider.as_deref() == Some("icu") {
        locale = locale.map(|locale| icu_locale(&locale));
    }
    Collation {
        provider,
        locale,
        lc_collate,
        lc_ctype,
        deterministic: collation.deterministic.filter(|d| !d),
        version: None,
        ..collation.clone()
    }
}

/// An ICU locale of a language, an optional script and an optional
/// region (`en_US`, `zh_Hant_TW`, `FR`) in the standard form that
/// PostgreSQL gives it: the parts joined with `-`, the language in
/// lowercase, the script in title case and the region in uppercase. A
/// locale with other parts is compared as written.
fn icu_locale(locale: &str) -> String {
    let parts: Vec<&str> = locale.split(['-', '_']).collect();
    let alphabetic = |part: &str, lengths: &[usize]| {
        lengths.contains(&part.len())
            && part.chars().all(|c| c.is_ascii_alphabetic())
    };
    let region = |part: &str| {
        alphabetic(part, &[2])
            || (part.len() == 3 && part.chars().all(|c| c.is_ascii_digit()))
    };
    let (language, rest) = match parts.split_first() {
        Some((language, rest)) if alphabetic(language, &[2, 3]) => {
            (language.to_ascii_lowercase(), rest)
        }
        _ => return locale.to_string(),
    };
    let (script, rest) = match rest.split_first() {
        Some((script, rest)) if alphabetic(script, &[4]) => {
            let mut script = script.to_ascii_lowercase();
            script[..1].make_ascii_uppercase();
            (Some(script), rest)
        }
        _ => (None, rest),
    };
    let region = match rest {
        [] => None,
        [part] if region(part) => Some(part.to_ascii_uppercase()),
        _ => return locale.to_string(),
    };
    std::iter::once(language)
        .chain(script)
        .chain(region)
        .collect::<Vec<_>>()
        .join("-")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn parse(value: serde_json::Value) -> Collation {
        serde_json::from_value(value).expect("collation deserializes")
    }

    #[test]
    fn short_forms_are_the_same_collation() {
        // as pull writes it
        let pulled = parse(json!({
            "name": "plain_c", "schema": "test", "owner": "postgres",
            "locale": "C", "provider": "libc",
        }));
        // as a person writes it
        let written = parse(json!({
            "name": "plain_c", "schema": "test", "owner": "postgres",
            "lc_collate": "C", "lc_ctype": "C", "deterministic": true,
            "version": "1",
        }));
        assert_eq!(canonical(&written), canonical(&pulled));
        assert!(matches!(
            collation(&written, &pulled),
            Resolution::Statements(alters) if alters.is_empty()
        ));
        // the owner changes on its own, so it is not a rebuild
        let owned = Collation {
            owner: String::from("app"),
            ..written.clone()
        };
        assert!(matches!(
            collation(&owned, &pulled),
            Resolution::Statements(alters) if alters.is_empty()
        ));
        // other locale categories are not one locale
        let mixed = parse(json!({
            "name": "plain_c", "schema": "test", "owner": "postgres",
            "lc_collate": "C", "lc_ctype": "POSIX",
        }));
        assert_ne!(canonical(&mixed), canonical(&pulled));
    }

    #[test]
    fn a_copied_collation_is_only_checked_for_existence() {
        let copied = parse(json!({
            "name": "c", "schema": "test", "owner": "postgres",
            "copy_from": "test.plain_c",
        }));
        assert!(crate::models::Definition::Collation(copied).raw_sql());
    }

    #[test]
    fn icu_locales_have_the_standard_form() {
        for (written, standard) in [
            ("EN_us", "en-US"),
            ("de-DE", "de-DE"),
            ("zh_hant_tw", "zh-Hant-TW"),
            ("es_419", "es-419"),
            ("FR", "fr"),
            ("und", "und"),
            ("und-u-ks-level2", "und-u-ks-level2"),
        ] {
            assert_eq!(icu_locale(written), standard, "{written}");
        }
        let icu = |locale: &str| {
            canonical(&parse(json!({
                "name": "c", "schema": "test", "owner": "postgres",
                "provider": "icu", "locale": locale,
            })))
        };
        assert_eq!(icu("en_US"), icu("en-US"));
        // a libc locale is compared as written
        let libc = |locale: &str| {
            canonical(&parse(json!({
                "name": "c", "schema": "test", "owner": "postgres",
                "locale": locale,
            })))
        };
        assert_ne!(libc("en_US"), libc("en-US"));
    }

    #[test]
    fn a_comment_changes_in_place() {
        let with = |comment: Option<&str>| {
            let mut value = json!({
                "name": "Case", "schema": "My Schema", "owner": "postgres",
                "locale": "und-u-ks-level2", "provider": "icu",
                "deterministic": false,
            });
            if let Some(comment) = comment {
                value["comment"] = comment.into();
            }
            parse(value)
        };
        let Resolution::Statements(alters) =
            collation(&with(Some("new")), &with(None))
        else {
            panic!("expected in-place statements");
        };
        assert_eq!(
            alters.iter().map(|a| a.sql.as_str()).collect::<Vec<_>>(),
            ["COMMENT ON COLLATION \"My Schema\".\"Case\" IS $$new$$;\n"]
        );
    }

    #[test]
    fn other_changes_replace() {
        let db = parse(json!({
            "name": "c", "schema": "test", "owner": "postgres",
            "locale": "und", "provider": "icu", "rules": "&b < a",
        }));
        for repo in [
            Collation {
                rules: Some(String::from("&c < a")),
                ..db.clone()
            },
            Collation {
                deterministic: Some(false),
                ..db.clone()
            },
            Collation {
                provider: None,
                ..db.clone()
            },
        ] {
            assert!(matches!(collation(&repo, &db), Resolution::Replace));
        }
    }
}
