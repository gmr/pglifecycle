//! Phase 1 gate: the full test-project loads, validates, and every
//! definition round-trips through its model to a semantically
//! identical value (the loader itself fails on round-trip mismatch)

use std::collections::HashMap;
use std::path::Path;

use pglifecycle::{project, yamlio};

#[test]
fn loads_the_full_test_project() {
    let project = project::load(Path::new("test-project")).unwrap();
    assert_eq!(project.name, "test-project");

    let mut counts: HashMap<&'static str, usize> = HashMap::new();
    for item in &project.inventory {
        *counts.entry(item.desc.as_str()).or_default() += 1;
    }
    // one inventory item per definition in test-project/
    assert_eq!(counts["EXTENSION"], 3);
    assert_eq!(counts["FOREIGN DATA WRAPPER"], 1);
    assert_eq!(counts["PROCEDURAL LANGUAGE"], 1);
    assert_eq!(counts["SCHEMA"], 1);
    assert_eq!(counts["AGGREGATE"], 1);
    assert_eq!(counts["CAST"], 1);
    assert_eq!(counts["COLLATION"], 1);
    assert_eq!(counts["CONVERSION"], 1);
    assert_eq!(counts["DOMAIN"], 2);
    assert_eq!(counts["EVENT TRIGGER"], 1);
    assert_eq!(counts["FUNCTION"], 3);
    assert_eq!(counts["GROUP"], 1);
    assert_eq!(counts["MATERIALIZED VIEW"], 1);
    assert_eq!(counts["OPERATOR"], 1);
    assert_eq!(counts["PUBLICATION"], 1);
    assert_eq!(counts["ROLE"], 1);
    assert_eq!(counts["SEQUENCE"], 1);
    assert_eq!(counts["SERVER"], 1);
    assert_eq!(counts["SUBSCRIPTION"], 1);
    assert_eq!(counts["TABLE"], 3);
    assert_eq!(counts["TABLESPACE"], 1);
    assert_eq!(counts["TEXT SEARCH"], 1);
    assert_eq!(counts["TYPE"], 4);
    assert_eq!(counts["USER"], 1);
    assert_eq!(counts["USER MAPPING"], 1);
    assert_eq!(counts["VIEW"], 1);
}

#[test]
fn resolves_dependencies() {
    let project = project::load(Path::new("test-project")).unwrap();
    let mut edges: Vec<String> = project
        .inventory
        .iter()
        .filter(|item| !item.dependencies.is_empty())
        .map(|item| {
            let mut parents: Vec<String> = item
                .dependencies
                .iter()
                .map(|dep| {
                    let parent = &project.inventory[*dep];
                    format!(
                        "{}:{}",
                        parent.desc.as_str(),
                        parent.definition.name()
                    )
                })
                .collect();
            parents.sort();
            format!(
                "{}:{} -> {}",
                item.desc.as_str(),
                item.definition.name(),
                parents.join(", ")
            )
        })
        .collect();
    edges.sort();
    // the exact edge set the Python implementation resolves, plus the
    // edge from a routine to its procedural language, and the edges
    // from the conversion and the event trigger to the functions that
    // they call (build deviation 41)
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
            "FUNCTION:utf8_to_latin1(integer, integer, cstring, internal, \
             integer) -> PROCEDURAL LANGUAGE:plpython3u",
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

#[test]
fn definitions_round_trip_through_yaml_emission() {
    let project = project::load(Path::new("test-project")).unwrap();
    for item in &project.inventory {
        let value = serde_json::to_value(&item.definition).unwrap();
        let emitted = yamlio::dump(&value);
        let parsed = yamlio::load_str(&emitted).unwrap_or_else(|e| {
            panic!(
                "emitted YAML for {} {} failed to parse: {e}\n{emitted}",
                item.desc.as_str(),
                item.definition.name()
            )
        });
        assert_eq!(
            parsed,
            value,
            "{} {} changed through emission",
            item.desc.as_str(),
            item.definition.name()
        );
    }
}

/// Load a project that has only `project.yaml`, with `extra` added to
/// it
fn load_project_file(extra: &str) -> Result<project::Project, String> {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("project.yaml"),
        format!("---\nname: settings\n{extra}"),
    )
    .unwrap();
    project::load(dir.path())
}

/// The text of a project is UTF-8 and has standard conforming strings,
/// and build always writes the archive so. The obsolete `encoding` and
/// `stdstrings` fields load only with these values, in any spelling
/// that PostgreSQL accepts
#[test]
fn loads_the_obsolete_session_fields_only_with_utf8_and_true() {
    for extra in [
        "",
        "encoding: UTF8\n",
        "encoding: UTF-8\n",
        "encoding: utf8\n",
        "encoding: Utf_8\n",
        "encoding: Unicode\n",
        "stdstrings: true\n",
    ] {
        load_project_file(extra)
            .unwrap_or_else(|e| panic!("{extra:?} did not load: {e}"));
    }
    for (extra, field) in [
        ("encoding: LATIN1\n", "encoding"),
        ("encoding: SQL_ASCII\n", "encoding"),
        ("stdstrings: false\n", "stdstrings"),
    ] {
        let error = load_project_file(extra)
            .err()
            .unwrap_or_else(|| panic!("{extra:?} loaded"));
        assert!(
            error.contains(field) && error.contains("Remove"),
            "the error for {extra:?} must name the field and the fix: {error}"
        );
    }
}

/// Load a project with `project.yaml` (with `extra` added to it) and
/// one table in schema `test` with a column of type `data_type`
fn load_table_project(
    extra: &str,
    data_type: &str,
) -> Result<project::Project, String> {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("project.yaml"),
        format!("---\nname: types\n{extra}"),
    )
    .unwrap();
    for (directory, file, text) in [
        ("schemata", "test.yaml", "---\nname: test\n".to_string()),
        (
            "types",
            "test.yaml",
            "---\nschema: test\ntypes:\n- name: mood\n  type: enum\n  \
             enum: [happy]\n"
                .to_string(),
        ),
        (
            "tables/test",
            "t.yaml",
            format!(
                "---\nname: t\ncolumns:\n- name: a\n  data_type: \
                 {data_type}\n"
            ),
        ),
    ] {
        let directory = dir.path().join(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(file), text).unwrap();
    }
    project::load(dir.path())
}

/// The restore and the deploy script run with an empty search_path.
/// A type name with no schema loads when it is a built-in type or a
/// type of the project in the schema of the object. Any other such
/// name fails the load, unless the project has an extension, which can
/// make the type: then the load gives a warning.
#[test]
fn refuses_an_unqualified_type_that_the_project_does_not_have() {
    let extension = "extensions:\n- name: citext\n  schema: public\n";
    for data_type in ["text", "mood", "test.mood", "public.citext"] {
        load_table_project("", data_type)
            .unwrap_or_else(|e| panic!("{data_type} did not load: {e}"));
    }
    assert!(load_table_project("", "citext").is_err());
    assert!(load_table_project("", "citext[]").is_err());
    load_table_project(extension, "citext")
        .unwrap_or_else(|e| panic!("citext with an extension: {e}"));
}
