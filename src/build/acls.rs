//! ACL entry emission (new in the Rust implementation — the Python
//! build loaded grants/revocations into models but never wrote them to
//! the archive)
//!
//! Grants and revocations from role, user, and group definitions are
//! grouped per target object and emitted as pg_dump-style ACL entries:
//! tag `<KIND> <name>` (e.g. `SCHEMA test`, `TABLE users`), the owning
//! object's namespace and owner, and a dependency edge on the object's
//! entry so the topological sort restores ACLs after their objects.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::constants::ObjectType;
use crate::models::{Acls, Definition, Item};
use crate::project::Project;
use crate::utils::quote_ident;

use super::Builder;

/// Marker appended to a privilege in acls.yml when it was granted with
/// grant option (e.g. `SELECT WITH GRANT OPTION`)
const GRANT_OPTION_SUFFIX: &str = " WITH GRANT OPTION";

/// PostgreSQL grants on views and materialized views use the same
/// TABLE/COLUMN syntax as tables, so relation ACLs may target any of
/// the three
const RELATIONS: &[ObjectType] = &[
    ObjectType::Table,
    ObjectType::View,
    ObjectType::MaterializedView,
];

/// `(Acls field, GRANT keyword, dependency object types)`
const SECTIONS: &[(&str, &str, &[ObjectType])] = &[
    ("columns", "TABLE", RELATIONS),
    ("databases", "DATABASE", &[]),
    ("domains", "DOMAIN", &[ObjectType::Domain]),
    (
        "foreign_data_wrappers",
        "FOREIGN DATA WRAPPER",
        &[ObjectType::ForeignDataWrapper],
    ),
    ("foreign_servers", "FOREIGN SERVER", &[ObjectType::Server]),
    ("functions", "FUNCTION", &[ObjectType::Function]),
    ("languages", "LANGUAGE", &[ObjectType::ProceduralLanguage]),
    ("large_objects", "LARGE OBJECT", &[]),
    ("schemata", "SCHEMA", &[ObjectType::Schema]),
    ("sequences", "SEQUENCE", &[ObjectType::Sequence]),
    ("tables", "TABLE", RELATIONS),
    ("tablespaces", "TABLESPACE", &[ObjectType::Tablespace]),
    ("types", "TYPE", &[ObjectType::Type]),
];

#[derive(Default)]
struct ObjectAcl {
    revokes: Vec<String>,
    grants: Vec<String>,
}

pub(super) fn dump_acls(
    builder: &mut Builder,
    project: &Project,
) -> Result<(), String> {
    // (section index, object) → statements; BTreeMap keeps the output
    // deterministic across runs
    let mut objects: BTreeMap<(usize, String), ObjectAcl> = BTreeMap::new();
    for item in &project.inventory {
        let (grants, revocations) = match &item.definition {
            Definition::Group(d) => (&d.grants, &d.revocations),
            Definition::Role(d) => (&d.grants, &d.revocations),
            Definition::User(d) => (&d.grants, &d.revocations),
            _ => continue,
        };
        let role = item.definition.name();
        if let Some(acls) = revocations {
            collect(&mut objects, acls, &role, true);
        }
        if let Some(acls) = grants {
            collect(&mut objects, acls, &role, false);
        }
    }
    for ((section, object), acl) in &objects {
        let (key, keyword, dep_types) = SECTIONS[*section];
        // column grants attach to their table's entry
        let target = match key {
            "columns" => match object.rsplit_once('.') {
                Some((table, _)) => table.to_string(),
                None => object.clone(),
            },
            _ => object.clone(),
        };
        let (namespace, name) = match target.split_once('.') {
            Some((schema, name)) => (schema, name),
            None => ("", target.as_str()),
        };
        let tag = match key {
            "columns" => format!(
                "COLUMN {}",
                object.split_once('.').map(|(_, n)| n).unwrap_or(object)
            ),
            _ => format!("{keyword} {name}"),
        };
        let mut owner = builder.superuser.clone();
        let mut dependencies = Vec::new();
        if !dep_types.is_empty() {
            match find_object(builder, project, dep_types, &target) {
                Some((dump_id, item_owner)) => {
                    dependencies.push(dump_id);
                    if let Some(item_owner) = item_owner {
                        owner = item_owner;
                    }
                }
                None => log::warn!(
                    "ACL target {keyword} {target} not found in the project"
                ),
            }
        }
        let mut statements = acl.revokes.clone();
        statements.extend(acl.grants.iter().cloned());
        let defn = format!("{}\n", statements.join("\n"));
        builder
            .dump
            .add_entry(
                libpgdump::ObjectType::Acl,
                Some(namespace),
                Some(&tag),
                Some(&owner),
                Some(&defn),
                None,
                None,
                &dependencies,
            )
            .map_err(|e| format!("failed to add ACL {tag}: {e}"))?;
    }
    Ok(())
}

/// Render one role's ACLs into the per-object statement map
fn collect(
    objects: &mut BTreeMap<(usize, String), ObjectAcl>,
    acls: &Acls,
    role: &str,
    revoke: bool,
) {
    for (index, (key, keyword, _)) in SECTIONS.iter().enumerate() {
        let Some(map) = section(acls, key) else {
            continue;
        };
        for (object, privileges) in map {
            let privileges: Vec<String> = privileges
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect();
            if privileges.is_empty() {
                continue;
            }
            // privileges carrying ` WITH GRANT OPTION` need their own
            // statement (the option applies to every privilege in it)
            let (grantable, plain): (Vec<String>, Vec<String>) = privileges
                .into_iter()
                .partition(|p| p.ends_with(GRANT_OPTION_SUFFIX));
            let grantable: Vec<String> = grantable
                .iter()
                .map(|p| p.trim_end_matches(GRANT_OPTION_SUFFIX).to_string())
                .collect();
            let entry = objects.entry((index, object.clone())).or_default();
            for (privileges, grant_option) in
                [(plain, false), (grantable, true)]
            {
                if privileges.is_empty() {
                    continue;
                }
                let statement = statement(
                    revoke,
                    keyword,
                    key,
                    object,
                    &privileges,
                    role,
                    grant_option,
                );
                if revoke {
                    entry.revokes.push(statement);
                } else {
                    entry.grants.push(statement);
                }
            }
        }
    }
}

/// One GRANT or REVOKE statement for a role on an object. With
/// `grant_option`, a GRANT gains a trailing `WITH GRANT OPTION` and a
/// REVOKE a leading `GRANT OPTION FOR`.
pub(crate) fn statement(
    revoke: bool,
    keyword: &str,
    section: &str,
    object: &str,
    privileges: &[String],
    role: &str,
    grant_option: bool,
) -> String {
    let (privileges, object) = match section {
        // `schema.table.column` → `SELECT(column) ON TABLE schema.table`
        "columns" => match object.rsplit_once('.') {
            Some((table, column)) => (
                privileges
                    .iter()
                    .map(|p| format!("{p}({})", quote_ident(column)))
                    .collect::<Vec<_>>()
                    .join(", "),
                quote_object(table),
            ),
            None => (privileges.join(", "), quote_object(object)),
        },
        // function signatures carry their argument list verbatim
        "functions" => (privileges.join(", "), object.to_string()),
        _ => (privileges.join(", "), quote_object(object)),
    };
    if revoke {
        let option = if grant_option {
            "GRANT OPTION FOR "
        } else {
            ""
        };
        format!(
            "REVOKE {option}{privileges} ON {keyword} {object} FROM {};",
            quote_role(role)
        )
    } else {
        let option = if grant_option {
            " WITH GRANT OPTION"
        } else {
            ""
        };
        format!(
            "GRANT {privileges} ON {keyword} {object} TO {}{option};",
            quote_role(role)
        )
    }
}

/// Find the granted-on object's entry id and owner
fn find_object(
    builder: &Builder,
    project: &Project,
    descs: &[ObjectType],
    object: &str,
) -> Option<(i32, Option<String>)> {
    let (schema, name) = match object.split_once('.') {
        Some((schema, name)) => (Some(schema), name),
        None => (None, object),
    };
    let item = if descs == [ObjectType::Function] {
        find_function(project, schema, name)
    } else {
        project.inventory.iter().find(|item| {
            descs.contains(&item.desc)
                && item.definition.name() == name
                && (item.desc.is_schemaless()
                    || item.definition.schema() == schema)
        })
    }?;
    let dump_id = builder.dump_id_map.get(&item.id)?;
    Some((*dump_id, item.definition.owner().map(str::to_string)))
}

/// Function ACLs are keyed `schema.name(args)`: match the identity
/// signature exactly, falling back to the bare name only when it is
/// unambiguous, so overloads never bind to the wrong entry
fn find_function<'a>(
    project: &'a Project,
    schema: Option<&str>,
    name: &str,
) -> Option<&'a Item> {
    let functions = project.inventory.iter().filter(|item| {
        item.desc == ObjectType::Function && item.definition.schema() == schema
    });
    if let Some(item) = functions.clone().find(|item| {
        matches!(&item.definition, Definition::Function(f)
            if f.identity() == name)
    }) {
        return Some(item);
    }
    let base = name.split('(').next().unwrap_or(name);
    let mut matches = functions.filter(|item| item.definition.name() == base);
    let item = matches.next()?;
    matches.next().is_none().then_some(item)
}

fn section<'a>(acls: &'a Acls, key: &str) -> Option<&'a Map<String, Value>> {
    match key {
        "columns" => acls.columns.as_ref(),
        "databases" => acls.databases.as_ref(),
        "domains" => acls.domains.as_ref(),
        "foreign_data_wrappers" => acls.foreign_data_wrappers.as_ref(),
        "foreign_servers" => acls.foreign_servers.as_ref(),
        "functions" => acls.functions.as_ref(),
        "languages" => acls.languages.as_ref(),
        "large_objects" => acls.large_objects.as_ref(),
        "schemata" => acls.schemata.as_ref(),
        "sequences" => acls.sequences.as_ref(),
        "tables" => acls.tables.as_ref(),
        "tablespaces" => acls.tablespaces.as_ref(),
        "types" => acls.types.as_ref(),
        other => unreachable!("unknown ACL section {other}"),
    }
}

/// Quote each part of a possibly-qualified object name
fn quote_object(object: &str) -> String {
    object
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

/// PUBLIC is a keyword, not an identifier
fn quote_role(role: &str) -> String {
    if role.eq_ignore_ascii_case("public") {
        String::from("PUBLIC")
    } else {
        quote_ident(role)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use crate::models::{self, Item};

    use super::*;

    fn project_with_acls() -> Project {
        let schema: models::Schema = serde_json::from_value(json!({
            "name": "test", "owner": "owner_role",
        }))
        .unwrap();
        let role: models::Role = serde_json::from_value(json!({
            "name": "PUBLIC",
            "create": false,
            "grants": {"schemata": {"test": ["USAGE"]}},
            "revocations": {"schemata": {"test": ["CREATE"]}},
        }))
        .unwrap();
        Project {
            name: String::from("acls"),
            encoding: String::from("UTF8"),
            stdstrings: true,
            superuser: String::from("postgres"),
            default_schema: String::from("public"),
            path: std::path::PathBuf::new(),
            inventory: vec![
                Item {
                    id: 0,
                    desc: ObjectType::Schema,
                    definition: Definition::Schema(schema),
                    dependencies: BTreeSet::new(),
                },
                Item {
                    id: 1,
                    desc: ObjectType::Role,
                    definition: Definition::Role(role),
                    dependencies: BTreeSet::new(),
                },
            ],
        }
    }

    #[test]
    fn emits_acl_entries_with_dependencies() {
        let project = project_with_acls();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("acls.dump");
        crate::build::build(&project, &path).unwrap();
        let dump = libpgdump::load(&path).unwrap();
        let acl = dump
            .entries()
            .iter()
            .find(|e| e.desc == libpgdump::ObjectType::Acl)
            .expect("ACL entry");
        assert_eq!(acl.tag.as_deref(), Some("SCHEMA test"));
        assert_eq!(acl.owner.as_deref(), Some("owner_role"));
        assert_eq!(
            acl.defn.as_deref(),
            Some(
                "REVOKE CREATE ON SCHEMA test FROM PUBLIC;\n\
                 GRANT USAGE ON SCHEMA test TO PUBLIC;\n"
            )
        );
        let schema = dump
            .entries()
            .iter()
            .find(|e| e.desc == libpgdump::ObjectType::Schema)
            .expect("schema entry");
        assert_eq!(acl.dependencies, vec![schema.dump_id]);
    }

    #[test]
    fn statement_renders_grant_option() {
        let privileges = [String::from("SELECT")];
        assert_eq!(
            statement(false, "TABLE", "tables", "t.x", &privileges, "r", true),
            "GRANT SELECT ON TABLE t.x TO r WITH GRANT OPTION;"
        );
        assert_eq!(
            statement(true, "TABLE", "tables", "t.x", &privileges, "r", true),
            "REVOKE GRANT OPTION FOR SELECT ON TABLE t.x FROM r;"
        );
    }

    #[test]
    fn grantable_privileges_get_their_own_grant() {
        // `INSERT WITH GRANT OPTION` splits off from the plain grant
        let role: models::Role = serde_json::from_value(json!({
            "name": "app",
            "create": false,
            "grants": {"tables": {"test.users":
                ["SELECT", "INSERT WITH GRANT OPTION"]}},
        }))
        .unwrap();
        let table: models::Table = serde_json::from_value(json!({
            "name": "users", "schema": "test", "owner": "postgres",
            "columns": [{"name": "id", "data_type": "uuid"}],
        }))
        .unwrap();
        let project = Project {
            name: String::from("acls"),
            encoding: String::from("UTF8"),
            stdstrings: true,
            superuser: String::from("postgres"),
            default_schema: String::from("public"),
            path: std::path::PathBuf::new(),
            inventory: vec![
                Item {
                    id: 0,
                    desc: ObjectType::Table,
                    definition: Definition::Table(table),
                    dependencies: BTreeSet::new(),
                },
                Item {
                    id: 1,
                    desc: ObjectType::Role,
                    definition: Definition::Role(role),
                    dependencies: BTreeSet::new(),
                },
            ],
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("acls.dump");
        crate::build::build(&project, &path).unwrap();
        let dump = libpgdump::load(&path).unwrap();
        let acl = dump
            .entries()
            .iter()
            .find(|e| e.desc == libpgdump::ObjectType::Acl)
            .expect("ACL entry");
        assert_eq!(
            acl.defn.as_deref(),
            Some(
                "GRANT SELECT ON TABLE test.users TO app;\n\
                 GRANT INSERT ON TABLE test.users TO app \
                 WITH GRANT OPTION;\n"
            )
        );
    }
}
