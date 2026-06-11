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
    assert_eq!(project.encoding, "UTF-8");

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
    // the exact edge set the Python implementation resolves
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
