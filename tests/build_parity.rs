//! Phase 2 parity gate: build test-project/ and compare the archive
//! entry-by-entry against the Python implementation's output
//! (tests/fixtures/python-build-entries.json, exported from a
//! pglifecycle 1.0 build of the same project).
//!
//! The comparison is exact except for documented deviations where the
//! Python output was broken; those entries are excluded from the exact
//! match and asserted against their corrected forms below.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use pglifecycle::{build, project};

type Key = (String, String, String);

fn entry_key(desc: &str, namespace: &str, tag: &str) -> Key {
    (desc.to_string(), namespace.to_string(), tag.to_string())
}

/// Python entries whose output was broken SQL; each maps to the
/// assertion applied to the corrected Rust entry instead
const DEVIATIONS: &[(&str, &str, &str)] = &[
    // libpgdump writes the prelude entries with empty tags where
    // pgdumplib repeated the desc (pg_dump itself uses the desc)
    ("ENCODING", "", "ENCODING"),
    ("STDSTRINGS", "", "STDSTRINGS"),
    ("SEARCHPATH", "", "SEARCHPATH"),
    // Python emitted CREATE ROLE for create: false roles (and
    // rendered BYPASSRLS from create_db); the Rust build skips them
    ("ROLE", "", "postgres"),
    // Python emitted OPTIONS without the required parentheses, which
    // does not parse
    ("SERVER", "", "localhost"),
    // Python interpolated a Python list repr into WHEN TAG IN
    ("EVENT TRIGGER", "", "disable_alter_domain"),
    // Python emitted CREATE INDEX schema.name, which does not parse
    ("INDEX", "test", "empty_table_created_at"),
    ("INDEX", "test", "users_unique_email"),
    // Python's loader dropped primary keys and rendered ON_DELETE /
    // ON_UPDATE
    ("TABLE", "test", "addresses"),
    ("TABLE", "test", "empty_table"),
    ("TABLE", "test", "users"),
    // Python tagged text search objects with their comment text (or
    // nothing) and interpolated list reprs into the option clauses
    (
        "TEXT SEARCH CONFIGURATION",
        "test",
        "Copy of default things",
    ),
    ("TEXT SEARCH CONFIGURATION", "test", "Copy of german config"),
    ("TEXT SEARCH DICTIONARY", "test", ""),
    (
        "TEXT SEARCH PARSER",
        "test",
        "Simple copy of the default parser values",
    ),
    (
        "TEXT SEARCH TEMPLATE",
        "test",
        "Copied from the snowball template",
    ),
];

/// (desc, namespace, tag, required defn fragment) for the corrected
/// Rust entries replacing the deviations above
const CORRECTED: &[(&str, &str, &str, &str)] = &[
    ("ENCODING", "", "", "SET client_encoding = 'UTF-8';\n"),
    (
        "STDSTRINGS",
        "",
        "",
        "SET standard_conforming_strings = 'on';\n",
    ),
    ("SEARCHPATH", "", "", "SELECT pg_catalog.set_config"),
    (
        "SERVER",
        "",
        "localhost",
        "OPTIONS (host 'localhost', port 5432, user 'fdw_user', dbname 'postgres')",
    ),
    (
        "EVENT TRIGGER",
        "",
        "disable_alter_domain",
        "WHEN TAG IN ('ALTER DOMAIN')",
    ),
    (
        "INDEX",
        "test",
        "empty_table_created_at",
        "CREATE INDEX empty_table_created_at ON test.empty_table",
    ),
    (
        "INDEX",
        "test",
        "users_unique_email",
        "CREATE UNIQUE INDEX users_unique_email ON test.users",
    ),
    (
        "TABLE",
        "test",
        "addresses",
        "PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES test.users (id) ON DELETE CASCADE ON UPDATE CASCADE",
    ),
    (
        "TABLE",
        "test",
        "empty_table",
        "value TEXT, PRIMARY KEY (id) )",
    ),
    ("TABLE", "test", "users", "icon oid, PRIMARY KEY (id) )"),
    // expression defaults render raw (Python quoted them)
    ("TABLE", "test", "users", "DEFAULT uuid_generate_v4()"),
    ("TABLE", "test", "users", "DEFAULT CURRENT_TIMESTAMP"),
    ("TABLE", "test", "users", "DEFAULT 'en-US'"),
    (
        "TEXT SEARCH CONFIGURATION",
        "test",
        "custom_english",
        "(PARSER = custom_default)",
    ),
    (
        "TEXT SEARCH CONFIGURATION",
        "test",
        "custom_german",
        "(SOURCE = german)",
    ),
    (
        "TEXT SEARCH DICTIONARY",
        "test",
        "custom_simple",
        "(TEMPLATE = custom_snowball, language = 'english', stopwords = 'english')",
    ),
    (
        "TEXT SEARCH PARSER",
        "test",
        "custom_default",
        "START = prsd_start",
    ),
    (
        "TEXT SEARCH TEMPLATE",
        "test",
        "custom_snowball",
        "(INIT = dsnowball_init, LEXIZE = dsnowball_lexize)",
    ),
];

/// Comment entries Python lost entirely to the text search name bug
const RECOVERED_COMMENTS: &[(&str, &str)] = &[
    ("custom_english", "Copy of default things"),
    ("custom_german", "Copy of german config"),
    ("custom_default", "Simple copy of the default parser values"),
    ("custom_snowball", "Copied from the snowball template"),
];

fn build_archive() -> libpgdump::Dump {
    let project = project::load(Path::new("test-project")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("build.dump");
    build::build(&project, &path).unwrap();
    libpgdump::load(&path).unwrap()
}

#[test]
fn matches_python_build_output() {
    let dump = build_archive();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/python-build-entries.json"
    ))
    .unwrap();

    let deviations: BTreeSet<Key> = DEVIATIONS
        .iter()
        .map(|(d, n, t)| entry_key(d, n, t))
        .collect();

    // full entry tuples (desc, ns, tag, owner, defn, drop, tablespace),
    // excluding deviant entries by key — compared as sorted multisets
    // so duplicate (desc, ns, tag) keys (e.g. two COMMENT ''.'test'
    // entries) stay distinct
    let rust_tuples: Vec<(Key, String, String, String, Option<String>)> = dump
        .entries()
        .iter()
        .map(|e| {
            (
                entry_key(
                    e.desc.as_str(),
                    e.namespace.as_deref().unwrap_or_default(),
                    e.tag.as_deref().unwrap_or_default(),
                ),
                e.owner.clone().unwrap_or_default(),
                e.defn.clone().unwrap_or_default(),
                e.drop_stmt.clone().unwrap_or_default(),
                e.tablespace.clone(),
            )
        })
        .collect();

    let mut expected: Vec<_> = fixture
        .as_array()
        .unwrap()
        .iter()
        .map(|py| {
            (
                entry_key(
                    py["desc"].as_str().unwrap(),
                    py["namespace"].as_str().unwrap(),
                    py["tag"].as_str().unwrap(),
                ),
                py["owner"].as_str().unwrap().to_string(),
                py["defn"].as_str().unwrap().to_string(),
                py["drop_stmt"].as_str().unwrap().to_string(),
                py["tablespace"].as_str().map(String::from),
            )
        })
        .filter(|(key, ..)| !deviations.contains(key))
        .collect();
    expected.sort();
    let mut actual: Vec<_> = rust_tuples
        .iter()
        .filter(|(key, ..)| {
            // exclude the corrected/recovered entries from the exact
            // comparison; asserted separately below. ACL entries are
            // new in the Rust build (Python never emitted them)
            key.0 != "ACL"
                && !CORRECTED
                    .iter()
                    .any(|(d, n, t, _)| &entry_key(d, n, t) == key)
                && !RECOVERED_COMMENTS
                    .iter()
                    .any(|(tag, _)| key == &entry_key("COMMENT", "test", tag))
        })
        .cloned()
        .collect();
    actual.sort();
    assert_eq!(actual, expected, "non-deviant entries differ");

    // the deviant entries must appear in their corrected forms
    for (desc, namespace, tag, fragment) in CORRECTED {
        let key = entry_key(desc, namespace, tag);
        let defn = rust_tuples
            .iter()
            .find(|(k, ..)| k == &key)
            .map(|(_, _, defn, ..)| defn.as_str())
            .unwrap_or_else(|| {
                panic!("missing corrected entry {key:?} in Rust archive")
            });
        assert!(
            defn.contains(fragment),
            "{key:?} defn missing {fragment:?}: {defn}"
        );
    }

    // comments Python lost to the text search tag bug
    for (tag, comment) in RECOVERED_COMMENTS {
        let key = entry_key("COMMENT", "test", tag);
        let defn = rust_tuples
            .iter()
            .find(|(k, ..)| k == &key)
            .map(|(_, _, defn, ..)| defn.as_str())
            .unwrap_or_else(|| panic!("missing recovered comment {key:?}"));
        assert!(defn.contains(comment), "{key:?} missing comment text");
    }

    // the developers group's grant emits an ACL entry, which the
    // Python build never did
    let acl = rust_tuples
        .iter()
        .find(|(key, ..)| key.0 == "ACL")
        .expect("missing ACL entry");
    assert_eq!(acl.0, entry_key("ACL", "public", "TABLE empty_table"));
    assert_eq!(
        acl.2,
        "GRANT SELECT, INSERT, DELETE, UPDATE ON TABLE \
         public.empty_table TO developers;\n"
    );

    // 60 Python entries + 4 recovered text search comments + 1 ACL
    // - 1 create: false role
    assert_eq!(dump.entries().len(), 64);
}

#[test]
fn records_inventory_dependency_edges() {
    let dump = build_archive();
    let by_id: BTreeMap<i32, String> = dump
        .entries()
        .iter()
        .map(|e| {
            (
                e.dump_id,
                format!(
                    "{}:{}",
                    e.desc.as_str(),
                    e.tag.as_deref().unwrap_or_default()
                ),
            )
        })
        .collect();
    let mut edges: Vec<String> = dump
        .entries()
        .iter()
        .filter(|e| {
            !e.dependencies.is_empty()
                && !matches!(
                    e.desc,
                    libpgdump::ObjectType::Comment
                        | libpgdump::ObjectType::Index
                )
        })
        .map(|e| {
            let mut parents: Vec<&str> =
                e.dependencies.iter().map(|d| by_id[d].as_str()).collect();
            parents.sort();
            format!(
                "{}:{} -> {}",
                e.desc.as_str(),
                e.tag.as_deref().unwrap_or_default(),
                parents.join(", ")
            )
        })
        .collect();
    edges.sort();
    // the same 7 inventory edges the loader resolves (Python recorded
    // no dependency edges at all; libpgdump's weighted toposort uses
    // these to order the archive)
    assert_eq!(
        edges,
        vec![
            "AGGREGATE:test_agg -> \
             FUNCTION:test_aggregate(integer, integer)",
            "DOMAIN:bcp47_locale -> EXTENSION:citext",
            "MATERIALIZED VIEW:user_addresses -> \
             TABLE:addresses, TABLE:users",
            "SERVER:localhost -> EXTENSION:postgres_fdw",
            "TABLE:addresses -> TYPE:address_type",
            "TABLE:users -> DOMAIN:bcp47_locale, DOMAIN:email_address, \
             TYPE:user_state",
            "VIEW:user_addresses -> TABLE:addresses, TABLE:users",
        ]
    );
}
