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

use pglifecycle::constants::ObjectType;
use pglifecycle::models::{Definition, Item};
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
    // deviation 17: pg_restore derives these three types' owner
    // statements from the stored drop statement, so IF EXISTS in one
    // produced `ALTER FUNCTION IF EXISTS ...`, which does not parse
    ("AGGREGATE", "test", "test_agg"),
    ("OPERATOR", "test", "==="),
    ("FUNCTION", "test", "disable_alter_domain()"),
    ("FUNCTION", "test", "test_aggregate(integer, integer)"),
    (
        "FUNCTION",
        "test",
        "utf8_to_latin1(integer, integer, cstring, internal, integer)",
    ),
    // deviation 23: Python rendered the locale and the encodings bare
    ("COLLATION", "test", "french"),
    ("CONVERSION", "test", "myconv"),
    // Python emitted OPTIONS without the required parentheses, which
    // does not parse
    ("SERVER", "", "localhost"),
    // Python interpolated a Python list repr into WHEN TAG IN
    ("EVENT TRIGGER", "", "disable_alter_domain"),
    // deviation 32: Python rendered the connection and the WITH
    // parameters bare, which does not parse
    ("SUBSCRIPTION", "", "localhost_test"),
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
    // Python rendered COMMENT ON CAST with an erroneous schema prefix
    // (a cast has no namespace) and COMMENT ON OPERATOR without the
    // argument signature required to disambiguate it
    ("COMMENT", "test", "(int4 AS timestamp)"),
    ("COMMENT", "test", "==="),
    // Python emitted CREATE/DROP FUNCTION with a bare, unqualified
    // name; pg_restore runs with search_path reset to '' and rejects it
    // ("no schema has been selected to create in"), so the Rust build
    // schema-qualifies the function reference
    ("FUNCTION", "test", "disable_alter_domain()"),
    ("FUNCTION", "test", "test_aggregate(integer, integer)"),
    (
        "FUNCTION",
        "test",
        "utf8_to_latin1(integer, integer, cstring, internal, integer)",
    ),
];

/// Deviation 17: entries whose *drop* statement was corrected, with
/// the exact statement the Rust build must now emit. pg_restore
/// derives these three types' owner statements from the stored drop
/// statement, so an `IF EXISTS` in one produced
/// `ALTER FUNCTION IF EXISTS ...`, which does not parse.
const CORRECTED_DROPS: &[(&str, &str, &str, &str)] = &[
    (
        "AGGREGATE",
        "test",
        "test_agg",
        "DROP AGGREGATE test.test_agg (IN integer);\n",
    ),
    (
        "OPERATOR",
        "test",
        "===",
        "DROP OPERATOR test.=== (box, box);\n",
    ),
    (
        "FUNCTION",
        "test",
        "disable_alter_domain()",
        "DROP FUNCTION test.disable_alter_domain();\n",
    ),
    (
        "FUNCTION",
        "test",
        "test_aggregate(integer, integer)",
        "DROP FUNCTION test.test_aggregate(integer, integer);\n",
    ),
    (
        "FUNCTION",
        "test",
        "utf8_to_latin1(integer, integer, cstring, internal, integer)",
        "DROP FUNCTION test.utf8_to_latin1(IN source_encoding_id INTEGER, \
         IN destination_encoding_id INTEGER, IN source CSTRING, IN \
         destination INTERNAL, IN source_length INTEGER);\n",
    ),
];

/// Deviation 38: pg_restore cannot set the owner of a role, a user or
/// a group, so these entries have no owner. Python gave them the
/// superuser. They are compared exactly, with no owner.
const OWNERLESS: &[(&str, &str, &str)] =
    &[("GROUP", "", "developers"), ("USER", "", "fwd_user")];

/// (desc, namespace, tag, required defn fragment) for the corrected
/// Rust entries replacing the deviations above
const CORRECTED: &[(&str, &str, &str, &str)] = &[
    // deviation 39: the text is UTF-8, whatever the project says
    ("ENCODING", "", "", "SET client_encoding = 'UTF8';\n"),
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
        "SUBSCRIPTION",
        "",
        "localhost_test",
        "CONNECTION 'host=localhost port=5432 dbname=logical_replication \
         user=postgres' PUBLICATION all_tables WITH (copy_data = True, \
         create_slot = True, enabled = True, synchronous_commit = 'on', \
         connect = True)",
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
    // deviation 14 moved the foreign key out of CREATE TABLE, so only
    // the recovered primary key is inline here; the foreign key is
    // asserted as its own entry in NEW_FK_CONSTRAINTS
    ("TABLE", "test", "addresses", "PRIMARY KEY (id)"),
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
        "(COPY = german)",
    ),
    ("COLLATION", "test", "french", "(LOCALE = 'fr_FR.utf8')"),
    (
        "CONVERSION",
        "test",
        "myconv",
        "FOR 'UTF8' TO 'LATIN1' FROM utf8_to_latin1",
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
    (
        "COMMENT",
        "test",
        "(int4 AS timestamp)",
        "COMMENT ON CAST (int4 AS timestamp) IS",
    ),
    (
        "COMMENT",
        "test",
        "===",
        "COMMENT ON OPERATOR test.=== (box, box) IS",
    ),
    (
        "FUNCTION",
        "test",
        "disable_alter_domain()",
        "CREATE FUNCTION test.disable_alter_domain()",
    ),
    (
        "FUNCTION",
        "test",
        "test_aggregate(integer, integer)",
        "CREATE FUNCTION test.test_aggregate(integer, integer)",
    ),
    (
        "FUNCTION",
        "test",
        "utf8_to_latin1(integer, integer, cstring, internal, integer)",
        "CREATE FUNCTION test.utf8_to_latin1(IN source_encoding_id INTEGER,",
    ),
];

/// (tag, comment) for COMMENT ON COLUMN entries: build now emits these
/// (data loss fix — Python's build.py never rendered column comments)
const NEW_COLUMN_COMMENTS: &[(&str, &str)] = &[
    ("addresses.created_at", "When the record was created"),
    ("addresses.id", "The user ID"),
    (
        "addresses.last_modified_at",
        "When the record was last modified",
    ),
    ("addresses.user_id", "Foreign Key to test.users"),
    ("empty_table.created_at", "When the record was created"),
    ("empty_table.id", "The auto-incrementing row ID value"),
    (
        "empty_table.last_modified_at",
        "When the record was last modified",
    ),
    ("empty_table.value", "Some random value"),
    ("users.created_at", "When the record was created"),
    ("users.id", "The user ID"),
    (
        "users.last_modified_at",
        "When the record was last modified",
    ),
    ("users.state", "The current state of the user"),
];

/// Comment entries Python lost entirely to the text search name bug
const RECOVERED_COMMENTS: &[(&str, &str)] = &[
    ("custom_english", "Copy of default things"),
    ("custom_german", "Copy of german config"),
    ("custom_default", "Simple copy of the default parser values"),
    ("custom_snowball", "Copied from the snowball template"),
];

/// The objects of the deviations that no test-project object shows
fn outside_items() -> Vec<Item> {
    let item = |id, desc, definition| Item {
        id,
        desc,
        definition,
        dependencies: BTreeSet::new(),
    };
    vec![
        // deviation 33
        item(
            0,
            ObjectType::TextSearch,
            Definition::TextSearch(
                serde_json::from_value(serde_json::json!({
                    "schema": "test",
                    "configurations": [{
                        "name": "urls",
                        "parser": "pg_catalog.default",
                        "mappings": {"URL": ["simple"]},
                    }],
                }))
                .unwrap(),
            ),
        ),
        // deviation 34
        item(
            1,
            ObjectType::Operator,
            Definition::Operator(
                serde_json::from_value(serde_json::json!({
                    "name": "~~~",
                    "schema": "test",
                    "owner": "postgres",
                    "function": "int4um",
                    "left_arg": "NONE",
                    "right_arg": "integer",
                }))
                .unwrap(),
            ),
        ),
        // deviation 35
        item(
            2,
            ObjectType::Operator,
            Definition::Operator(
                serde_json::from_value(serde_json::json!({
                    "name": "<~>",
                    "schema": "Gate Ops",
                    "owner": "postgres",
                    "function": "int4eq",
                    "left_arg": "integer",
                    "right_arg": "integer",
                    "comment": "Quoted schema",
                }))
                .unwrap(),
            ),
        ),
        // deviation 36
        item(
            3,
            ObjectType::Function,
            Definition::Function(
                serde_json::from_value(serde_json::json!({
                    "name": "definer",
                    "schema": "test",
                    "owner": "postgres",
                    "returns": "text",
                    "language": "sql",
                    "security": "DEFINER",
                    "configuration": {
                        "search_path": ["pg_catalog", "$user", "pg_temp"],
                    },
                    "definition": "SELECT current_user::text;",
                }))
                .unwrap(),
            ),
        ),
        item(
            4,
            ObjectType::Procedure,
            Definition::Procedure(
                serde_json::from_value(serde_json::json!({
                    "name": "definer_proc",
                    "schema": "test",
                    "owner": "postgres",
                    "language": "sql",
                    "configuration": {"search_path": ["pg_catalog", "pg_temp"]},
                    "definition": "SELECT 1;",
                }))
                .unwrap(),
            ),
        ),
        // deviation 37
        item(
            5,
            ObjectType::Role,
            Definition::Role(
                serde_json::from_value(serde_json::json!({
                    "name": "app",
                    "settings": [
                        {"search_path": ["$user", "my schema", "public"]},
                        {"application_name": "it's"},
                        {"enable_seqscan": false},
                        {"statement_timeout": 1000},
                    ],
                }))
                .unwrap(),
            ),
        ),
        item(
            6,
            ObjectType::User,
            Definition::User(
                serde_json::from_value(serde_json::json!({
                    "name": "App User",
                    "settings": [{"temp_tablespaces": ["a,b"]}],
                }))
                .unwrap(),
            ),
        ),
        // deviation 40: a default and a check that call a function
        // that reads the table
        item(
            7,
            ObjectType::Table,
            Definition::Table(
                serde_json::from_value(serde_json::json!({
                    "name": "tickets",
                    "schema": "test",
                    "owner": "postgres",
                    "columns": [
                        {"name": "id", "data_type": "integer"},
                        {"name": "n", "data_type": "integer",
                         "default": "test.next_ticket()"},
                    ],
                    "check_constraints": [
                        {"name": "tickets_n_check",
                         "expression": "(n <= test.next_ticket())"},
                        {"name": "tickets_id_check",
                         "expression": "(id > 0)"},
                    ],
                }))
                .unwrap(),
            ),
        ),
        Item {
            dependencies: [7].into(),
            ..item(
                8,
                ObjectType::Function,
                Definition::Function(
                    serde_json::from_value(serde_json::json!({
                        "name": "next_ticket",
                        "schema": "test",
                        "owner": "postgres",
                        "returns": "integer",
                        "language": "sql",
                        "sql_body": "RETURN (SELECT (COALESCE(max(tickets.n), \
                                     0) + 1) FROM test.tickets)",
                    }))
                    .unwrap(),
                ),
            )
        },
    ]
}

/// (desc, tag, defns) of the setting entries of the roles and users
/// that [`outside_items`] give, in the order of the build
const OUTSIDE_SETTINGS: &[(&str, &str, &[&str])] = &[
    // deviation 37: each element of a list setting is its own string
    // constant. The Python wrote each element bare, which does not
    // parse for `$user` or a name with a space
    (
        "ROLE",
        "app",
        &[
            "ALTER ROLE app SET search_path TO '$user', 'my schema', \
             'public';\n",
            "ALTER ROLE app SET application_name TO $$it's$$;\n",
            "ALTER ROLE app SET enable_seqscan TO False;\n",
            "ALTER ROLE app SET statement_timeout TO 1000;\n",
        ],
    ),
    (
        "USER",
        "App User",
        &["ALTER USER \"App User\" SET temp_tablespaces TO 'a,b';\n"],
    ),
];

/// (desc, namespace, tag, defn, drop) of the corrected entries that
/// [`outside_items`] give
const OUTSIDE_CORRECTED: &[(&str, &str, &str, &str, &str)] = &[
    // deviation 33: a token type as PostgreSQL reads one. The Python
    // rendered no mappings, and a quoted "URL" names no token type
    (
        "TEXT SEARCH CONFIGURATION",
        "test",
        "urls",
        "CREATE TEXT SEARCH CONFIGURATION test.urls (PARSER = \
         pg_catalog.default); ALTER TEXT SEARCH CONFIGURATION test.urls \
         ADD MAPPING FOR url WITH simple;\n",
        "DROP TEXT SEARCH CONFIGURATION IF EXISTS test.urls;\n",
    ),
    // deviation 34: NONE is no argument. The Python rendered `LEFTARG
    // = NONE`, which names no type
    (
        "OPERATOR",
        "test",
        "~~~",
        "CREATE OPERATOR test.~~~ (PROCEDURE = int4um, RIGHTARG = \
         integer);\n",
        "DROP OPERATOR test.~~~ (NONE, integer);\n",
    ),
    // deviation 35: the schema of an operator is quoted. The Python
    // wrote it bare, which does not parse for a name that needs quotes
    (
        "OPERATOR",
        "Gate Ops",
        "<~>",
        "CREATE OPERATOR \"Gate Ops\".<~> (PROCEDURE = int4eq, LEFTARG = \
         integer, RIGHTARG = integer);\n",
        "DROP OPERATOR \"Gate Ops\".<~> (integer, integer);\n",
    ),
    (
        "COMMENT",
        "Gate Ops",
        "<~>",
        "COMMENT ON OPERATOR \"Gate Ops\".<~> (integer, integer) IS \
         $$Quoted schema$$;\n;\n",
        "",
    ),
    // deviation 36: each element of a list setting is its own string
    // constant. The Python rendered a list as ARRAY[...], which SET
    // does not parse, and a string with commas as one name
    (
        "FUNCTION",
        "test",
        "definer",
        "CREATE FUNCTION test.definer() RETURNS text LANGUAGE sql SECURITY \
         DEFINER SET search_path = 'pg_catalog', '$user', 'pg_temp' AS \
         $$\nSELECT current_user::text;\n$$;\n",
        "DROP FUNCTION test.definer();\n",
    ),
    (
        "PROCEDURE",
        "test",
        "definer_proc",
        "CREATE PROCEDURE test.definer_proc() LANGUAGE sql SET search_path \
         = 'pg_catalog', 'pg_temp' AS $$\nSELECT 1;\n$$;\n",
        "DROP PROCEDURE test.definer_proc();\n",
    ),
    // deviation 40: the default and the check that call a function
    // that reads the table are their own entries, as pg_dump writes
    // them. The Python wrote them in CREATE TABLE, which failed,
    // because the function comes after the table. The check that calls
    // no function stays in CREATE TABLE
    (
        "TABLE",
        "test",
        "tickets",
        "CREATE TABLE test.tickets ( id integer, n integer, CONSTRAINT \
         tickets_id_check CHECK ((id > 0)) );\n",
        "DROP TABLE IF EXISTS test.tickets;\n",
    ),
    (
        "DEFAULT",
        "test",
        "tickets n",
        "ALTER TABLE ONLY test.tickets ALTER COLUMN n SET DEFAULT \
         test.next_ticket();\n",
        "ALTER TABLE ONLY test.tickets ALTER COLUMN n DROP DEFAULT;\n",
    ),
    (
        "CHECK CONSTRAINT",
        "test",
        "tickets tickets_n_check",
        "ALTER TABLE test.tickets ADD CONSTRAINT tickets_n_check CHECK ((n \
         <= test.next_ticket()));\n",
        "ALTER TABLE test.tickets DROP CONSTRAINT IF EXISTS \
         tickets_n_check;\n",
    ),
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
            let key = entry_key(
                py["desc"].as_str().unwrap(),
                py["namespace"].as_str().unwrap(),
                py["tag"].as_str().unwrap(),
            );
            // deviation 38: no owner for a role, a user or a group
            let owner =
                if OWNERLESS.iter().any(|(d, n, t)| entry_key(d, n, t) == key)
                {
                    String::new()
                } else {
                    py["owner"].as_str().unwrap().to_string()
                };
            (
                key,
                owner,
                py["defn"].as_str().unwrap().to_string(),
                // deviation 18: Python stored a bare ";" for an entry
                // with nothing to drop, which made pg_restore attempt
                // an owner statement it cannot build for a COMMENT
                match py["drop_stmt"].as_str().unwrap() {
                    ";\n" => String::new(),
                    other => other.to_string(),
                },
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
                // deviation 14: Python rendered foreign keys inline in
                // CREATE TABLE, so it emitted no FK CONSTRAINT entries
                && key.0 != "FK CONSTRAINT"
                && !CORRECTED
                    .iter()
                    .any(|(d, n, t, _)| &entry_key(d, n, t) == key)
                && !CORRECTED_DROPS
                    .iter()
                    .any(|(d, n, t, _)| &entry_key(d, n, t) == key)
                && !RECOVERED_COMMENTS
                    .iter()
                    .any(|(tag, _)| key == &entry_key("COMMENT", "test", tag))
                && !NEW_COLUMN_COMMENTS
                    .iter()
                    .any(|(tag, _)| key == &entry_key("COMMENT", "test", tag))
        })
        .cloned()
        .collect();
    actual.sort();
    assert_eq!(actual, expected, "non-deviant entries differ");

    // deviation 17: the three types whose owner statement pg_restore
    // builds from the stored drop statement carry no IF EXISTS
    for (desc, namespace, tag, expected_drop) in CORRECTED_DROPS {
        let key = entry_key(desc, namespace, tag);
        let drop = rust_tuples
            .iter()
            .find(|(k, ..)| k == &key)
            .map(|(_, _, _, drop, _)| drop.as_str())
            .unwrap_or_else(|| {
                panic!("missing entry {key:?} in Rust archive")
            });
        assert_eq!(drop, *expected_drop, "{key:?} drop statement");
        assert!(
            !drop.contains("IF EXISTS"),
            "{key:?} drop statement still carries IF EXISTS"
        );
    }

    // deviation 38: pg_restore fails on the owner of a role, a user or
    // a group, so their entries have none
    for (desc, namespace, tag) in OWNERLESS {
        let key = entry_key(desc, namespace, tag);
        let (_, owner, ..) = rust_tuples
            .iter()
            .find(|(k, ..)| k == &key)
            .unwrap_or_else(|| panic!("missing entry {key:?}"));
        assert_eq!(owner, "", "{key:?} has an owner");
    }

    // deviation 18: an entry with nothing to drop stores nothing
    for (key, _, _, drop, _) in &rust_tuples {
        assert_ne!(drop, ";\n", "{key:?} stores a bare drop statement");
    }

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

    // column comments Python's build.py dropped entirely (data loss)
    for (tag, comment) in NEW_COLUMN_COMMENTS {
        let key = entry_key("COMMENT", "test", tag);
        let defn = rust_tuples
            .iter()
            .find(|(k, ..)| k == &key)
            .map(|(_, _, defn, ..)| defn.as_str())
            .unwrap_or_else(|| panic!("missing column comment {key:?}"));
        assert!(defn.contains(comment), "{key:?} missing comment text");
    }

    // deviation 14: foreign keys are their own entries now, named and
    // ordered after every table so a circular reference can restore
    let fk = rust_tuples
        .iter()
        .find(|(key, ..)| key.0 == "FK CONSTRAINT")
        .expect("missing FK CONSTRAINT entry");
    assert_eq!(
        fk.0,
        entry_key("FK CONSTRAINT", "test", "addresses addresses_user_id")
    );
    assert_eq!(
        fk.2,
        "ALTER TABLE ONLY test.addresses ADD CONSTRAINT addresses_user_id \
         FOREIGN KEY (user_id) REFERENCES test.users (id) ON DELETE CASCADE \
         ON UPDATE CASCADE;\n"
    );
    assert_eq!(
        fk.3,
        "ALTER TABLE ONLY test.addresses DROP CONSTRAINT IF EXISTS \
         addresses_user_id;\n"
    );

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

    // 60 Python entries + 4 recovered text search comments + 12 new
    // column comments + 1 ACL + 1 FK CONSTRAINT (deviation 14)
    // - 1 create: false role
    assert_eq!(dump.entries().len(), 77);
}

/// The deviations that no test-project object shows, asserted on a
/// project of the objects that [`outside_items`] give
#[test]
fn corrects_objects_outside_the_test_project() {
    let project = project::Project {
        name: "outside".into(),
        superuser: "postgres".into(),
        default_schema: "public".into(),
        path: std::path::PathBuf::new(),
        inventory: outside_items(),
    };
    let output = build::assemble(&project).unwrap();
    for (desc, namespace, tag, defn, drop) in OUTSIDE_CORRECTED {
        let entry = output
            .dump
            .entries()
            .iter()
            .find(|e| {
                e.desc.as_str() == *desc
                    && e.namespace.as_deref().unwrap_or_default() == *namespace
                    && e.tag.as_deref().unwrap_or_default() == *tag
            })
            .unwrap_or_else(|| panic!("missing entry {desc} {tag}"));
        assert_eq!(entry.defn.as_deref(), Some(*defn), "{desc} {tag} defn");
        assert_eq!(
            entry.drop_stmt.as_deref().unwrap_or_default(),
            *drop,
            "{desc} {tag} drop"
        );
    }
    // deviation 40: the separate default and check come after the
    // function, and the function after the table
    let id = |desc: &str, tag: &str| {
        output
            .dump
            .entries()
            .iter()
            .find(|e| e.desc.as_str() == desc && e.tag.as_deref() == Some(tag))
            .map(|e| (e.dump_id, e.dependencies.clone()))
            .unwrap_or_else(|| panic!("missing entry {desc} {tag}"))
    };
    let (table, _) = id("TABLE", "tickets");
    let (function, function_deps) = id("FUNCTION", "next_ticket");
    assert_eq!(function_deps, [table]);
    for (desc, tag) in [
        ("DEFAULT", "tickets n"),
        ("CHECK CONSTRAINT", "tickets tickets_n_check"),
    ] {
        let (_, mut deps) = id(desc, tag);
        deps.sort_unstable();
        assert_eq!(deps, [table, function], "{desc} {tag} dependencies");
    }
    for (desc, tag, defns) in OUTSIDE_SETTINGS {
        let settings: Vec<&str> = output
            .dump
            .entries()
            .iter()
            .filter(|e| {
                e.desc.as_str() == *desc && e.tag.as_deref() == Some(*tag)
            })
            .filter_map(|e| e.defn.as_deref())
            .filter(|defn| defn.starts_with("ALTER "))
            .collect();
        assert_eq!(settings, *defns, "{desc} {tag} settings");
    }
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
    // the same 10 inventory edges the loader resolves, plus the edge
    // from the FK CONSTRAINT entry to its own table (Python recorded
    // no dependency edges at all; libpgdump's weighted toposort uses
    // these to order the archive). A foreign key needs no edge to the
    // table it references: FK CONSTRAINT is a post-data desc, so it
    // already sorts after every table.
    //
    // Deviation 41: the conversion and the event trigger wait for the
    // function that they call. Each names its function without the
    // argument types, and test-project names the function with them,
    // so the name found no function before.
    //
    // The text search objects of one schema's container are chained,
    // each after the one before, in the order they depend on each
    // other: parser, template, dictionary, configuration. Each one
    // also waits for the text search objects it names qualified; the
    // test project names them bare, and pg_restore resolves a bare
    // name in pg_catalog, so those names order nothing.
    assert_eq!(
        edges,
        vec![
            "AGGREGATE:test_agg -> \
             FUNCTION:test_aggregate(integer, integer)",
            "CONVERSION:myconv -> FUNCTION:utf8_to_latin1(integer, integer, \
             cstring, internal, integer)",
            "DOMAIN:bcp47_locale -> EXTENSION:citext",
            "EVENT TRIGGER:disable_alter_domain -> \
             FUNCTION:disable_alter_domain()",
            "FK CONSTRAINT:addresses addresses_user_id -> TABLE:addresses",
            "FUNCTION:utf8_to_latin1(integer, integer, cstring, internal, \
             integer) -> PROCEDURAL LANGUAGE:plpython3u",
            "MATERIALIZED VIEW:user_addresses -> \
             TABLE:addresses, TABLE:users",
            "SERVER:localhost -> EXTENSION:postgres_fdw",
            "TABLE:addresses -> TYPE:address_type",
            "TABLE:users -> DOMAIN:bcp47_locale, DOMAIN:email_address, \
             TYPE:user_state",
            "TEXT SEARCH CONFIGURATION:custom_english -> \
             TEXT SEARCH DICTIONARY:custom_simple",
            "TEXT SEARCH CONFIGURATION:custom_german -> \
             TEXT SEARCH CONFIGURATION:custom_english",
            "TEXT SEARCH DICTIONARY:custom_simple -> \
             TEXT SEARCH TEMPLATE:custom_snowball",
            "TEXT SEARCH TEMPLATE:custom_snowball -> \
             TEXT SEARCH PARSER:custom_default",
            "VIEW:user_addresses -> TABLE:addresses, TABLE:users",
        ]
    );
}
