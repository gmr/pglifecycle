//! Shared constants (ports constants.py)

/// Project subdirectories, one per object-type path in the managed layout
/// (constants.PATHS in the Python implementation, deduplicated — foreign
/// tables share the tables directory)
pub const PROJECT_DIRS: &[&str] = &[
    "aggregates",
    "casts",
    "collations",
    "conversions",
    "dml",
    "domains",
    "event_triggers",
    "functions",
    "groups",
    "materialized_views",
    "operators",
    "procedures",
    "publications",
    "roles",
    "schemata",
    "sequences",
    "servers",
    "subscriptions",
    "tables",
    "tablespaces",
    "text_search",
    "types",
    "user_mappings",
    "users",
    "views",
];
