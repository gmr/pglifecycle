//! Default privileges (ALTER DEFAULT PRIVILEGES). One item holds the
//! declarations of one role. Deploy compares the privileges that the
//! declarations give, not the declarations as written: pg_dump writes
//! `ALL` for the full list, and a revoke from the owner as `REVOKE ALL`
//! and a `GRANT` of the others.
//!
//! A GRANT or a REVOKE changes only the objects that the role makes
//! later, and no data, so neither is destructive. The removal of the
//! default privileges of a role that the project does not have is
//! gated, as the drop of each database-only object is (see
//! [`removal`]).
//!
//! New in the Rust implementation: the Python implementation had no
//! `deploy` command, so no Python file ports to this module.

use std::collections::BTreeMap;

use crate::deploy::alter::{Alter, Resolution};
use crate::models::{DefaultPrivilege, DefaultPrivileges};
use crate::utils::{quote_ident, user_mapping_subject};

/// One privilege that the defaults give: (schema, object type, grantee,
/// privilege). The schema is empty for the defaults of every schema.
type Key = (String, String, String, String);

/// The privileges that a role's defaults give, each with its grant
/// option
pub(crate) type Acl = BTreeMap<Key, bool>;

/// The privileges of an object type, in the order PostgreSQL shows
/// them. `ALL` is this list.
fn all_privileges(object_type: &str) -> &'static [&'static str] {
    match object_type {
        "TABLES" => &[
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "TRUNCATE",
            "REFERENCES",
            "TRIGGER",
            "MAINTAIN",
        ],
        "SEQUENCES" => &["USAGE", "SELECT", "UPDATE"],
        "FUNCTIONS" => &["EXECUTE"],
        "TYPES" => &["USAGE"],
        "SCHEMAS" => &["USAGE", "CREATE"],
        "LARGE OBJECTS" => &["SELECT", "UPDATE"],
        _ => &[],
    }
}

/// The object type in upper case. ROUTINES is another name for
/// FUNCTIONS.
fn object_type(value: &str) -> String {
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    match value.to_uppercase().as_str() {
        "ROUTINES" => String::from("FUNCTIONS"),
        other => other.to_string(),
    }
}

/// PUBLIC in any case is the PUBLIC keyword. Other names are kept as
/// they are: build quotes them, so `App` and `app` are two roles.
fn grantee(value: &str) -> String {
    if value.eq_ignore_ascii_case("PUBLIC") {
        String::from("PUBLIC")
    } else {
        value.to_string()
    }
}

/// The privileges of a declaration in upper case, with `ALL` (or
/// `ALL PRIVILEGES`) as the full list of the object type
fn privileges(object_type: &str, privileges: &[String]) -> Vec<String> {
    let mut names = Vec::new();
    for privilege in privileges {
        let privilege = privilege
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_uppercase();
        if privilege == "ALL" || privilege == "ALL PRIVILEGES" {
            names.extend(
                all_privileges(object_type).iter().map(|p| p.to_string()),
            );
        } else {
            names.push(privilege);
        }
    }
    names
}

/// The object types that default privileges can name
const OBJECT_TYPES: [&str; 6] = [
    "TABLES",
    "SEQUENCES",
    "FUNCTIONS",
    "TYPES",
    "SCHEMAS",
    "LARGE OBJECTS",
];

/// The privileges that `defaults` give, for each schema and object
/// type. With no declarations, PostgreSQL gives all privileges to the
/// owner, and EXECUTE on functions and USAGE on types to PUBLIC. A
/// declaration for every schema changes these, and one for a schema
/// adds to them. As in build, the revocations apply first, then the
/// grants.
pub(crate) fn effective(defaults: &DefaultPrivileges) -> Acl {
    let role = grantee(&defaults.name);
    let mut acl = Acl::new();
    for kind in OBJECT_TYPES {
        let mut add = |grantee: &str, privilege: &str| {
            acl.insert(
                (
                    String::new(),
                    kind.to_string(),
                    grantee.to_string(),
                    privilege.to_string(),
                ),
                false,
            );
        };
        for privilege in all_privileges(kind) {
            add(&role, privilege);
        }
        match kind {
            "FUNCTIONS" => add("PUBLIC", "EXECUTE"),
            "TYPES" => add("PUBLIC", "USAGE"),
            _ => {}
        }
    }
    for declaration in defaults.revocations.iter().flatten() {
        for key in keys(declaration) {
            acl.remove(&key);
        }
    }
    for declaration in defaults.grants.iter().flatten() {
        let option = declaration.with_grant_option == Some(true);
        for key in keys(declaration) {
            *acl.entry(key).or_default() |= option;
        }
    }
    acl
}

/// The privileges that one declaration names
fn keys(declaration: &DefaultPrivilege) -> Vec<Key> {
    let schema = declaration.schema.clone().unwrap_or_default();
    let kind = object_type(&declaration.object_type);
    let grantee = grantee(&declaration.grantee);
    privileges(&kind, &declaration.privileges)
        .into_iter()
        .map(|privilege| {
            (schema.clone(), kind.clone(), grantee.clone(), privilege)
        })
        .collect()
}

/// The in-place statements that make the database's default privileges
/// of a role those of the project
pub(crate) fn default_privileges(
    repo: &DefaultPrivileges,
    db: &DefaultPrivileges,
) -> Resolution {
    Resolution::Statements(statements(
        &repo.name,
        &effective(repo),
        &effective(db),
    ))
}

/// The statements that take away the default privileges of a role that
/// the project does not have, so that the role has the built-in ones
/// again. The plan gates them, as it gates each drop. A REVOKE among
/// them fails open when it is withheld.
pub(crate) fn removal(db: &DefaultPrivileges) -> Vec<Alter> {
    let built_in = DefaultPrivileges {
        name: db.name.clone(),
        grants: None,
        revocations: None,
    };
    statements(&db.name, &effective(&built_in), &effective(db))
}

/// The GRANT and REVOKE statements that change `existing` to `wanted`,
/// one for each schema, object type, grantee and action. For one
/// grantee, a REVOKE comes before a GRANT, as pg_dump writes them.
fn statements(role: &str, wanted: &Acl, existing: &Acl) -> Vec<Alter> {
    #[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
    enum Action {
        Revoke,
        RevokeGrantOption,
        Grant,
        GrantWithGrantOption,
    }
    let mut groups: BTreeMap<(String, String, String, Action), Vec<String>> =
        BTreeMap::new();
    let mut add = |key: &Key, action: Action| {
        let (schema, kind, grantee, privilege) = key.clone();
        groups
            .entry((schema, kind, grantee, action))
            .or_default()
            .push(privilege);
    };
    for (key, option) in existing {
        match wanted.get(key) {
            None => add(key, Action::Revoke),
            Some(false) if *option => add(key, Action::RevokeGrantOption),
            Some(_) => {}
        }
    }
    for (key, option) in wanted {
        match (existing.get(key), option) {
            (None, false) => add(key, Action::Grant),
            (None, true) | (Some(false), true) => {
                add(key, Action::GrantWithGrantOption)
            }
            _ => {}
        }
    }
    groups
        .into_iter()
        .map(|((schema, kind, grantee, action), mut names)| {
            let order = all_privileges(&kind);
            names.sort_by_key(|p| {
                (
                    order.iter().position(|o| o == p).unwrap_or(order.len()),
                    p.clone(),
                )
            });
            let mut target = format!(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {}",
                quote_ident(role)
            );
            let mut label = format!("DEFAULT PRIVILEGES {role}");
            if !schema.is_empty() {
                target
                    .push_str(&format!(" IN SCHEMA {}", quote_ident(&schema)));
                label.push_str(&format!(" IN SCHEMA {schema}"));
            }
            label.push_str(&format!(" ON {kind}"));
            let names = names.join(", ");
            let grantee = user_mapping_subject(&grantee);
            let sql = match action {
                Action::Revoke => {
                    format!(
                        "{target} REVOKE {names} ON {kind} FROM {grantee};\n"
                    )
                }
                Action::RevokeGrantOption => format!(
                    "{target} REVOKE GRANT OPTION FOR {names} ON {kind} FROM \
                     {grantee};\n"
                ),
                Action::Grant => {
                    format!("{target} GRANT {names} ON {kind} TO {grantee};\n")
                }
                Action::GrantWithGrantOption => format!(
                    "{target} GRANT {names} ON {kind} TO {grantee} WITH GRANT \
                     OPTION;\n"
                ),
            };
            let revoke =
                matches!(action, Action::Revoke | Action::RevokeGrantOption);
            Alter {
                fails_open: revoke,
                schema: (!schema.is_empty()).then_some(schema),
                ..Alter::new(sql).labeled(&label)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults(value: serde_json::Value) -> DefaultPrivileges {
        serde_json::from_value(value).expect("default privileges")
    }

    fn sql(alters: &[Alter]) -> Vec<&str> {
        alters.iter().map(|a| a.sql.trim_end()).collect()
    }

    fn resolved(
        repo: &DefaultPrivileges,
        db: &DefaultPrivileges,
    ) -> Vec<Alter> {
        match default_privileges(repo, db) {
            Resolution::Statements(alters) => alters,
            _ => panic!("expected statements"),
        }
    }

    /// The fixture's defaults as pull writes them from pg_dump
    fn pulled() -> DefaultPrivileges {
        defaults(serde_json::json!({
            "name": "postgres",
            "grants": [
                {"schema": "test", "object_type": "TABLES",
                 "grantee": "PUBLIC", "privileges": ["SELECT"]},
                {"object_type": "TABLES", "grantee": "postgres",
                 "privileges": ["INSERT", "REFERENCES", "DELETE", "TRIGGER",
                                "MAINTAIN", "UPDATE", "SELECT"]},
                {"object_type": "TABLES", "grantee": "app",
                 "privileges": ["ALL"], "with_grant_option": true},
            ],
            "revocations": [
                {"object_type": "FUNCTIONS", "grantee": "PUBLIC",
                 "privileges": ["ALL"]},
                {"object_type": "TABLES", "grantee": "postgres",
                 "privileges": ["ALL"]},
            ],
        }))
    }

    #[test]
    fn hand_written_form_gives_the_same_privileges() {
        let hand_written = defaults(serde_json::json!({
            "name": "postgres",
            "grants": [
                {"object_type": "TABLES", "grantee": "app",
                 "privileges": ["trigger", "select", "insert", "update",
                                "delete", "truncate", "references",
                                "maintain"],
                 "with_grant_option": true},
                {"schema": "test", "object_type": "TABLES",
                 "grantee": "public", "privileges": ["Select"]},
            ],
            "revocations": [
                {"object_type": "ROUTINES", "grantee": "Public",
                 "privileges": ["EXECUTE"]},
                {"object_type": "TABLES", "grantee": "postgres",
                 "privileges": ["TRUNCATE"]},
            ],
        }));
        assert_eq!(effective(&hand_written), effective(&pulled()));
        assert!(resolved(&hand_written, &pulled()).is_empty());
    }

    #[test]
    fn all_privileges_is_the_full_list() {
        let grant = |privileges: serde_json::Value| {
            effective(&defaults(serde_json::json!({
                "name": "postgres",
                "grants": [{"schema": "s", "object_type": "SEQUENCES",
                            "grantee": "app", "privileges": privileges}],
            })))
        };
        assert_eq!(
            grant(serde_json::json!(["ALL PRIVILEGES"])),
            grant(serde_json::json!(["update", "USAGE", "select"]))
        );
    }

    #[test]
    fn declarations_for_other_object_types_keep_the_built_in_ones() {
        // a project with no TABLES declaration for every schema, and a
        // database with one for another grantee, differ only in that
        // grantee: the owner keeps its built-in privileges
        let repo = defaults(serde_json::json!({"name": "postgres"}));
        let db = defaults(serde_json::json!({
            "name": "postgres",
            "grants": [{"object_type": "TABLES", "grantee": "app",
                        "privileges": ["SELECT"]}],
        }));
        assert_eq!(
            sql(&resolved(&repo, &db)),
            vec![
                "ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE SELECT \
                 ON TABLES FROM app;"
            ]
        );
    }

    #[test]
    fn emits_the_difference_only() {
        let repo = defaults(serde_json::json!({
            "name": "Owner",
            "grants": [
                {"schema": "My Schema", "object_type": "TABLES",
                 "grantee": "app", "privileges": ["SELECT", "INSERT"]},
                {"object_type": "SCHEMAS", "grantee": "app",
                 "privileges": ["USAGE"], "with_grant_option": true},
                {"object_type": "TYPES", "grantee": "reader",
                 "privileges": ["USAGE"]},
            ],
        }));
        let db = defaults(serde_json::json!({
            "name": "Owner",
            "grants": [
                {"schema": "My Schema", "object_type": "TABLES",
                 "grantee": "app", "privileges": ["SELECT", "DELETE"]},
                {"object_type": "SCHEMAS", "grantee": "app",
                 "privileges": ["USAGE"]},
                {"object_type": "TYPES", "grantee": "reader",
                 "privileges": ["USAGE"], "with_grant_option": true},
            ],
            "revocations": [
                {"object_type": "FUNCTIONS", "grantee": "PUBLIC",
                 "privileges": ["EXECUTE"]},
            ],
        }));
        let alters = resolved(&repo, &db);
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER DEFAULT PRIVILEGES FOR ROLE \"Owner\" GRANT EXECUTE \
                 ON FUNCTIONS TO PUBLIC;",
                "ALTER DEFAULT PRIVILEGES FOR ROLE \"Owner\" GRANT USAGE ON \
                 SCHEMAS TO app WITH GRANT OPTION;",
                "ALTER DEFAULT PRIVILEGES FOR ROLE \"Owner\" REVOKE GRANT \
                 OPTION FOR USAGE ON TYPES FROM reader;",
                "ALTER DEFAULT PRIVILEGES FOR ROLE \"Owner\" IN SCHEMA \
                 \"My Schema\" REVOKE DELETE ON TABLES FROM app;",
                "ALTER DEFAULT PRIVILEGES FOR ROLE \"Owner\" IN SCHEMA \
                 \"My Schema\" GRANT INSERT ON TABLES TO app;",
            ]
        );
        // neither a GRANT nor a REVOKE is gated; a REVOKE fails open if
        // it is withheld
        assert!(alters.iter().all(|a| !a.destructive));
        assert_eq!(
            alters.iter().map(|a| a.fails_open).collect::<Vec<_>>(),
            vec![false, false, true, true, false]
        );
        assert_eq!(
            alters[3].label.as_deref(),
            Some("DEFAULT PRIVILEGES Owner IN SCHEMA My Schema ON TABLES")
        );
    }

    #[test]
    fn removal_gives_back_the_built_in_privileges() {
        let alters = removal(&pulled());
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT EXECUTE ON \
                 FUNCTIONS TO PUBLIC;",
                "ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE SELECT, \
                 INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, \
                 MAINTAIN ON TABLES FROM app;",
                "ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT TRUNCATE ON \
                 TABLES TO postgres;",
                "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA test \
                 REVOKE SELECT ON TABLES FROM PUBLIC;",
            ]
        );
        // a removal that only gives privileges back does not fail open
        let revoked_only = defaults(serde_json::json!({
            "name": "postgres",
            "revocations": [{"object_type": "TYPES", "grantee": "PUBLIC",
                             "privileges": ["USAGE"]}],
        }));
        let alters = removal(&revoked_only);
        assert_eq!(alters.len(), 1);
        assert!(!alters[0].fails_open);
    }
}
