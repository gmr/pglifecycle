//! ACL entry emission (new in the Rust implementation — the Python
//! build loaded grants/revocations into models but never wrote them to
//! the archive)
//!
//! Grants and revocations from role, user, and group definitions are
//! grouped per target object and emitted as pg_dump-style ACL entries:
//! tag `<KIND> <name>` (e.g. `SCHEMA test`, `TABLE users`), the owning
//! object's namespace and owner, and a dependency edge on the object's
//! entry so the topological sort restores ACLs after their objects.

use std::collections::{BTreeMap, HashMap};

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

/// Membership ACL sections: `(Acls field, tag keyword)`. Both grant
/// role membership; `groups` exists for hand-authored projects since
/// pg_dumpall does not distinguish groups from roles on pull
const MEMBERSHIP_SECTIONS: &[(&str, &str)] =
    &[("groups", "GROUP"), ("roles", "ROLE")];

/// A granted role and its grantees may each be defined as a role,
/// group, or user file
const ROLES: &[ObjectType] =
    &[ObjectType::Role, ObjectType::Group, ObjectType::User];

#[derive(Default)]
struct ObjectAcl {
    revokes: Vec<String>,
    grants: Vec<String>,
}

#[derive(Default)]
struct MembershipAcl {
    revokes: Vec<String>,
    grants: Vec<String>,
    grantees: Vec<String>,
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
    // build the lookup index once rather than rescanning the whole
    // inventory for every ACL group below
    let index = ObjectIndex::build(project);
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
            match find_object(builder, &index, dep_types, &target) {
                Some((dump_id, item_owner)) => {
                    dependencies.push(dump_id);
                    if let Some(item_owner) = item_owner {
                        owner = item_owner;
                    }
                }
                // common and benign: grants on extension-owned,
                // foreign, or platform-managed (RDS) objects the project
                // does not track; the grant is still emitted
                None => log::debug!(
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
    dump_memberships(builder, project, &index)
}

/// Emit `GRANT role TO grantee` / `REVOKE role FROM grantee` entries
/// from the `roles` and `groups` ACL sections, grouped per granted
/// role like the object ACLs above. Each entry depends on the granted
/// role's and every grantee's create entry so the topological sort
/// restores memberships after the roles exist.
fn dump_memberships(
    builder: &mut Builder,
    project: &Project,
    index: &ObjectIndex,
) -> Result<(), String> {
    let mut memberships: BTreeMap<(usize, String), MembershipAcl> =
        BTreeMap::new();
    for item in &project.inventory {
        let (grants, revocations) = match &item.definition {
            Definition::Group(d) => (&d.grants, &d.revocations),
            Definition::Role(d) => (&d.grants, &d.revocations),
            Definition::User(d) => (&d.grants, &d.revocations),
            _ => continue,
        };
        let grantee = item.definition.name();
        for (acls, revoke) in [(revocations, true), (grants, false)] {
            let Some(acls) = acls else {
                continue;
            };
            for (section, (key, _)) in MEMBERSHIP_SECTIONS.iter().enumerate() {
                let roles = match *key {
                    "groups" => &acls.groups,
                    _ => &acls.roles,
                };
                for membership in roles.iter().flatten() {
                    let role = membership.role();
                    // PostgreSQL does not permit PUBLIC as either
                    // operand of a role membership grant
                    if role.eq_ignore_ascii_case("public")
                        || grantee.eq_ignore_ascii_case("public")
                    {
                        return Err(format!(
                            "invalid membership ACL ({role} to \
                             {grantee}): PostgreSQL does not permit \
                             PUBLIC in role memberships"
                        ));
                    }
                    let entry = memberships
                        .entry((section, role.to_string()))
                        .or_default();
                    let statement = if revoke {
                        // the options go with the membership itself
                        format!(
                            "REVOKE {} FROM {};",
                            quote_role(role),
                            quote_role(&grantee)
                        )
                    } else {
                        format!(
                            "GRANT {} TO {}{};",
                            quote_role(role),
                            quote_role(&grantee),
                            match membership.options_sql() {
                                Some(options) => format!(" WITH {options}"),
                                None => String::new(),
                            }
                        )
                    };
                    if revoke {
                        entry.revokes.push(statement);
                    } else {
                        entry.grants.push(statement);
                    }
                    if !entry.grantees.contains(&grantee) {
                        entry.grantees.push(grantee.clone());
                    }
                }
            }
        }
    }
    for ((section, role), acl) in &memberships {
        let (_, keyword) = MEMBERSHIP_SECTIONS[*section];
        let tag = format!("{keyword} {role}");
        let mut dependencies = Vec::new();
        match find_role(builder, index, role) {
            Some(dump_id) => dependencies.push(dump_id),
            // common and benign: predefined pg_* roles and
            // platform-managed (RDS) roles are never project files,
            // and create: false roles emit no create entry; the
            // membership grant is still emitted
            None => log::debug!(
                "Membership target {keyword} {role} not found in the \
                 project"
            ),
        }
        for grantee in &acl.grantees {
            if let Some(dump_id) = find_role(builder, index, grantee) {
                dependencies.push(dump_id);
            }
        }
        let mut statements = acl.revokes.clone();
        statements.extend(acl.grants.iter().cloned());
        let defn = format!("{}\n", statements.join("\n"));
        builder
            .dump
            .add_entry(
                libpgdump::ObjectType::Acl,
                Some(""),
                Some(&tag),
                Some(&builder.superuser),
                Some(&defn),
                None,
                None,
                &dependencies,
            )
            .map_err(|e| format!("failed to add ACL {tag}: {e}"))?;
    }
    Ok(())
}

/// Find a role/group/user's entry id. Membership targets are
/// schemaless, so the name is looked up whole (never schema-split as
/// in [`find_object`]); `None` also covers `create: false` roles,
/// which have no create entry.
fn find_role(
    builder: &Builder,
    index: &ObjectIndex,
    name: &str,
) -> Option<i32> {
    ROLES.iter().find_map(|desc| {
        let item = index.objects.get(&(*desc, None, name.to_string()))?;
        builder.dump_id_map.get(&item.id).copied()
    })
}

/// Render one role's ACLs into the per-object statement map
fn collect(
    objects: &mut BTreeMap<(usize, String), ObjectAcl>,
    acls: &Acls,
    role: &str,
    revoke: bool,
) {
    for (index, (key, keyword, _)) in SECTIONS.iter().enumerate() {
        // a section may draw from more than one Acls field; both feed
        // the same bucket so they coalesce into one entry per object
        let targets: Vec<(&String, &Value)> =
            [section(acls, key), coalesced(acls, key)]
                .into_iter()
                .flatten()
                .flat_map(|map| map.iter())
                .collect();
        for (object, privileges) in targets {
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

/// Lookup indexes over the project inventory, built once per build so
/// each ACL group's object lookup is O(1) instead of a full inventory
/// scan (previously repeated per group, and twice over for functions)
struct ObjectIndex<'a> {
    /// (desc, schema — `None` for schemaless descs, name) → item; a
    /// name collision across desc/schema is not possible for valid
    /// PostgreSQL objects, so keeping the first item seen is equivalent
    /// to the original full-scan `find`
    objects: HashMap<(ObjectType, Option<&'a str>, String), &'a Item>,
    /// (schema, identity signature) → item, for exact function matches
    functions_by_identity: HashMap<(Option<&'a str>, String), &'a Item>,
    /// (schema, bare name) → matching items, for the unambiguous
    /// bare-name fallback
    functions_by_name: HashMap<(Option<&'a str>, String), Vec<&'a Item>>,
}

impl<'a> ObjectIndex<'a> {
    fn build(project: &'a Project) -> Self {
        let mut objects = HashMap::new();
        let mut functions_by_identity = HashMap::new();
        let mut functions_by_name: HashMap<_, Vec<&Item>> = HashMap::new();
        for item in &project.inventory {
            let schema = item.definition.schema();
            let key_schema = if item.desc.is_schemaless() {
                None
            } else {
                schema
            };
            objects
                .entry((item.desc, key_schema, item.definition.name()))
                .or_insert(item);
            if let Definition::Function(f) = &item.definition {
                functions_by_identity
                    .entry((schema, f.identity()))
                    .or_insert(item);
                functions_by_name
                    .entry((schema, item.definition.name()))
                    .or_default()
                    .push(item);
            }
        }
        ObjectIndex {
            objects,
            functions_by_identity,
            functions_by_name,
        }
    }
}

/// Find the granted-on object's entry id and owner
fn find_object(
    builder: &Builder,
    index: &ObjectIndex,
    descs: &[ObjectType],
    object: &str,
) -> Option<(i32, Option<String>)> {
    let (schema, name) = match object.split_once('.') {
        Some((schema, name)) => (Some(schema), name),
        None => (None, object),
    };
    let item = if descs == [ObjectType::Function] {
        find_function(index, schema, name)
    } else {
        descs.iter().find_map(|desc| {
            let key_schema = if desc.is_schemaless() { None } else { schema };
            index
                .objects
                .get(&(*desc, key_schema, name.to_string()))
                .copied()
        })
    }?;
    let dump_id = builder.dump_id_map.get(&item.id)?;
    Some((*dump_id, item.definition.owner().map(str::to_string)))
}

/// Function ACLs are keyed `schema.name(args)`: match the identity
/// signature exactly, falling back to the bare name only when it is
/// unambiguous, so overloads never bind to the wrong entry
fn find_function<'a>(
    index: &ObjectIndex<'a>,
    schema: Option<&str>,
    name: &str,
) -> Option<&'a Item> {
    if let Some(item) =
        index.functions_by_identity.get(&(schema, name.to_string()))
    {
        return Some(*item);
    }
    let base = name.split('(').next().unwrap_or(name);
    let matches = index.functions_by_name.get(&(schema, base.to_string()))?;
    (matches.len() == 1).then(|| matches[0])
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

/// An additional [`Acls`] field emitted as part of `key`'s section.
/// PostgreSQL grants on views with TABLE syntax, so `views:` shares
/// the `tables:` section rather than forming entries of its own —
/// `pull` writes view grants under `tables:` for the same reason, and
/// a view named in both must produce a single ACL entry
fn coalesced<'a>(acls: &'a Acls, key: &str) -> Option<&'a Map<String, Value>> {
    match key {
        "tables" => acls.views.as_ref(),
        _ => None,
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

    fn project_with(inventory: Vec<Item>) -> Project {
        Project {
            name: String::from("memberships"),
            encoding: String::from("UTF8"),
            stdstrings: true,
            superuser: String::from("postgres"),
            default_schema: String::from("public"),
            path: std::path::PathBuf::new(),
            inventory,
        }
    }

    fn item(id: usize, desc: ObjectType, definition: Definition) -> Item {
        Item {
            id,
            desc,
            definition,
            dependencies: BTreeSet::new(),
        }
    }

    fn build_dump(project: &Project) -> libpgdump::Dump {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("memberships.dump");
        crate::build::build(project, &path).unwrap();
        libpgdump::load(&path).unwrap()
    }

    fn find_acl<'a>(
        dump: &'a libpgdump::Dump,
        tag: &str,
    ) -> &'a libpgdump::Entry {
        dump.entries()
            .iter()
            .find(|e| {
                e.desc == libpgdump::ObjectType::Acl
                    && e.tag.as_deref() == Some(tag)
            })
            .unwrap_or_else(|| panic!("missing ACL entry {tag}"))
    }

    fn view(name: &str) -> Definition {
        Definition::View(
            serde_json::from_value(json!({
                "name": name,
                "schema": "test",
                "owner": "owner_role",
                "query": "SELECT 1",
            }))
            .unwrap(),
        )
    }

    #[test]
    fn emits_grants_from_the_views_section() {
        // PostgreSQL grants on views with TABLE syntax, so a views:
        // grant must reach the archive as GRANT ... ON TABLE
        let reader: models::Role = serde_json::from_value(json!({
            "name": "reader",
            "grants": {"views": {"test.active": ["SELECT"]}},
        }))
        .unwrap();
        let project = project_with(vec![
            item(0, ObjectType::View, view("active")),
            item(1, ObjectType::Role, Definition::Role(reader)),
        ]);
        let dump = build_dump(&project);
        let acl = find_acl(&dump, "TABLE active");
        assert_eq!(acl.namespace.as_deref(), Some("test"));
        assert_eq!(acl.owner.as_deref(), Some("owner_role"));
        assert_eq!(
            acl.defn.as_deref(),
            Some("GRANT SELECT ON TABLE test.active TO reader;\n")
        );
        let entry = dump
            .entries()
            .iter()
            .find(|e| e.desc == libpgdump::ObjectType::View)
            .expect("view entry");
        assert_eq!(acl.dependencies, vec![entry.dump_id]);
    }

    #[test]
    fn views_and_tables_sections_coalesce_into_one_entry() {
        // the same view named under both keys is still one object, so
        // it must not produce two ACL entries with the same tag
        let reader: models::Role = serde_json::from_value(json!({
            "name": "reader",
            "grants": {
                "tables": {"test.active": ["SELECT"]},
                "views": {"test.active": ["ALL"]},
            },
        }))
        .unwrap();
        let project = project_with(vec![
            item(0, ObjectType::View, view("active")),
            item(1, ObjectType::Role, Definition::Role(reader)),
        ]);
        let dump = build_dump(&project);
        let entries: Vec<_> = dump
            .entries()
            .iter()
            .filter(|e| {
                e.desc == libpgdump::ObjectType::Acl
                    && e.tag.as_deref() == Some("TABLE active")
            })
            .collect();
        assert_eq!(entries.len(), 1, "expected one coalesced ACL entry");
        assert_eq!(
            entries[0].defn.as_deref(),
            Some(
                "GRANT SELECT ON TABLE test.active TO reader;\n\
                 GRANT ALL ON TABLE test.active TO reader;\n"
            )
        );
    }

    #[test]
    fn emits_role_membership_grants_with_dependencies() {
        let developers: models::Role = serde_json::from_value(json!({
            "name": "developers",
        }))
        .unwrap();
        let alice: models::User = serde_json::from_value(json!({
            "name": "alice",
            "grants": {"roles": ["developers"]},
        }))
        .unwrap();
        let project = project_with(vec![
            item(0, ObjectType::Role, Definition::Role(developers)),
            item(1, ObjectType::User, Definition::User(alice)),
        ]);
        let dump = build_dump(&project);
        let acl = find_acl(&dump, "ROLE developers");
        assert_eq!(acl.namespace.as_deref().unwrap_or_default(), "");
        assert_eq!(acl.owner.as_deref(), Some("postgres"));
        assert_eq!(acl.defn.as_deref(), Some("GRANT developers TO alice;\n"));
        // the membership sorts after both the granted role's and the
        // grantee's create entries
        let role = dump
            .entries()
            .iter()
            .find(|e| e.tag.as_deref() == Some("developers"))
            .expect("role entry");
        let user = dump
            .entries()
            .iter()
            .find(|e| e.tag.as_deref() == Some("alice"))
            .expect("user entry");
        assert_eq!(acl.dependencies, vec![role.dump_id, user.dump_id]);
    }

    /// A membership with options renders them, in the canonical
    /// order, on the GRANT
    #[test]
    fn emits_role_membership_options() {
        let developers: models::Role = serde_json::from_value(json!({
            "name": "developers",
        }))
        .unwrap();
        let alice: models::User = serde_json::from_value(json!({
            "name": "alice",
            "grants": {
                "roles": [
                    {"role": "developers", "admin": true, "inherit": false},
                ],
            },
            // a revoked membership takes its options with it
            "revocations": {"roles": [{"role": "developers"}]},
        }))
        .unwrap();
        let project = project_with(vec![
            item(0, ObjectType::Role, Definition::Role(developers)),
            item(1, ObjectType::User, Definition::User(alice)),
        ]);
        let dump = build_dump(&project);
        assert_eq!(
            find_acl(&dump, "ROLE developers").defn.as_deref(),
            Some(
                "REVOKE developers FROM alice;\n\
                 GRANT developers TO alice WITH ADMIN OPTION, \
                 INHERIT FALSE;\n"
            )
        );
    }

    #[test]
    fn emits_role_membership_revocations() {
        let developers: models::Role = serde_json::from_value(json!({
            "name": "developers",
        }))
        .unwrap();
        let alice: models::User = serde_json::from_value(json!({
            "name": "alice",
            "grants": {"roles": ["developers"]},
            "revocations": {"roles": ["developers"]},
        }))
        .unwrap();
        let project = project_with(vec![
            item(0, ObjectType::Role, Definition::Role(developers)),
            item(1, ObjectType::User, Definition::User(alice)),
        ]);
        let dump = build_dump(&project);
        let acl = find_acl(&dump, "ROLE developers");
        assert_eq!(
            acl.defn.as_deref(),
            Some(
                "REVOKE developers FROM alice;\n\
                 GRANT developers TO alice;\n"
            )
        );
    }

    #[test]
    fn emits_group_membership_grants() {
        let admins: models::Group = serde_json::from_value(json!({
            "name": "admins",
        }))
        .unwrap();
        let bob: models::Role = serde_json::from_value(json!({
            "name": "bob",
            "grants": {"groups": ["admins"]},
        }))
        .unwrap();
        let project = project_with(vec![
            item(0, ObjectType::Group, Definition::Group(admins)),
            item(1, ObjectType::Role, Definition::Role(bob)),
        ]);
        let dump = build_dump(&project);
        let acl = find_acl(&dump, "GROUP admins");
        assert_eq!(acl.defn.as_deref(), Some("GRANT admins TO bob;\n"));
        let group = dump
            .entries()
            .iter()
            .find(|e| e.desc == libpgdump::ObjectType::Group)
            .expect("group entry");
        assert!(acl.dependencies.contains(&group.dump_id));
    }

    #[test]
    fn emits_membership_grants_on_roles_absent_from_the_project() {
        // predefined pg_* roles are filtered on pull and can never be
        // project files; the membership grant must still be emitted
        let alice: models::User = serde_json::from_value(json!({
            "name": "alice",
            "grants": {"roles": ["pg_read_all_data"]},
        }))
        .unwrap();
        let project = project_with(vec![item(
            0,
            ObjectType::User,
            Definition::User(alice),
        )]);
        let dump = build_dump(&project);
        let acl = find_acl(&dump, "ROLE pg_read_all_data");
        assert_eq!(
            acl.defn.as_deref(),
            Some("GRANT pg_read_all_data TO alice;\n")
        );
        // only the grantee's create entry is resolvable
        let user = dump
            .entries()
            .iter()
            .find(|e| e.tag.as_deref() == Some("alice"))
            .expect("user entry");
        assert_eq!(acl.dependencies, vec![user.dump_id]);
    }

    fn build_error(project: &Project) -> String {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("memberships.dump");
        crate::build::build(project, &path).unwrap_err()
    }

    #[test]
    fn rejects_public_as_the_granted_role() {
        // PostgreSQL prohibits `GRANT PUBLIC TO alice`
        let alice: models::User = serde_json::from_value(json!({
            "name": "alice",
            "grants": {"roles": ["PUBLIC"]},
        }))
        .unwrap();
        let project = project_with(vec![item(
            0,
            ObjectType::User,
            Definition::User(alice),
        )]);
        let error = build_error(&project);
        assert!(
            error.contains("PUBLIC in role memberships"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_public_as_the_grantee() {
        // PostgreSQL prohibits `GRANT developers TO PUBLIC`
        let developers: models::Role = serde_json::from_value(json!({
            "name": "developers",
        }))
        .unwrap();
        let public: models::Role = serde_json::from_value(json!({
            "name": "PUBLIC",
            "create": false,
            "grants": {"roles": ["developers"]},
        }))
        .unwrap();
        let project = project_with(vec![
            item(0, ObjectType::Role, Definition::Role(developers)),
            item(1, ObjectType::Role, Definition::Role(public)),
        ]);
        let error = build_error(&project);
        assert!(
            error.contains("PUBLIC in role memberships"),
            "unexpected error: {error}"
        );
    }
}
