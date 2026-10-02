//! Privileges on objects (GRANT and REVOKE). Deploy compares the
//! privileges that an object has, not the statements as written: the
//! project and pg_dump write their statements relative to the built-in
//! privileges of the owner and of PUBLIC (`acldefault`), so each side
//! starts from these and applies its statements. Both sides are SQL
//! that the same parser reads: the statements that build writes from
//! the project, and the statements of the snapshot's ACL entries.
//!
//! Each object is compared as a set of (column, grantee, privilege)
//! items, each with its grant option. A GRANT is not destructive. A
//! REVOKE takes access away from a role now, thus it is destructive,
//! and it fails open when it is withheld.
//!
//! The connecting role makes each new object, thus the default
//! privileges of that role give the first ACL of the object, and
//! `ALTER ... OWNER TO` moves the role's items to the owner. The ACL
//! entry of the archive is relative to the built-in privileges of the
//! owner, so after the CREATE deploy first changes the ACL to these.
//!
//! New in the Rust implementation: the Python implementation had no
//! `deploy` command, so no Python file ports to this module.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::cli;
use crate::constants;
use crate::ddl::{self, AclTarget};
use crate::deploy::alter::default_privileges::effective;
use crate::deploy::alter::{Alter, Resolution};
use crate::deploy::diff::{Change, Diff, ObjectKey};
use crate::models::{DefaultPrivileges, Definition};
use crate::project::Project;
use crate::utils::user_mapping_subject;

/// One privilege: (column, grantee, privilege). The column is empty
/// for a privilege on the object.
type Key = (String, String, String);

/// The privileges of an object, each with its grant option
type Acl = BTreeMap<Key, bool>;

/// The kinds of objects that have privileges and that deploy compares.
/// A domain is a type: pg_dump writes `ON TYPE` for it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Kind {
    Table,
    Sequence,
    Function,
    Type,
    Schema,
    Language,
    ForeignDataWrapper,
    ForeignServer,
}

impl Kind {
    /// DATABASE, TABLESPACE and LARGE OBJECT have no project item
    fn of(target: AclTarget) -> Option<Self> {
        match target {
            AclTarget::Table => Some(Self::Table),
            AclTarget::Sequence => Some(Self::Sequence),
            AclTarget::Function => Some(Self::Function),
            AclTarget::Type | AclTarget::Domain => Some(Self::Type),
            AclTarget::Schema => Some(Self::Schema),
            AclTarget::Language => Some(Self::Language),
            AclTarget::ForeignDataWrapper => Some(Self::ForeignDataWrapper),
            AclTarget::ForeignServer => Some(Self::ForeignServer),
            AclTarget::Database
            | AclTarget::LargeObject
            | AclTarget::Tablespace => None,
        }
    }

    /// The privileges of the kind, in the order PostgreSQL shows
    /// them. `ALL` is this list.
    fn all(self, column: bool) -> &'static [&'static str] {
        match self {
            Self::Table if column => {
                &["SELECT", "INSERT", "UPDATE", "REFERENCES"]
            }
            Self::Table => &[
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
                "MAINTAIN",
            ],
            Self::Sequence => &["USAGE", "SELECT", "UPDATE"],
            Self::Function => &["EXECUTE"],
            Self::Schema => &["USAGE", "CREATE"],
            Self::Type
            | Self::Language
            | Self::ForeignDataWrapper
            | Self::ForeignServer => &["USAGE"],
        }
    }

    /// The object type of the default privileges that give a new
    /// object of this kind its first ACL
    fn defaults(self) -> Option<&'static str> {
        match self {
            Self::Table => Some("TABLES"),
            Self::Sequence => Some("SEQUENCES"),
            Self::Function => Some("FUNCTIONS"),
            Self::Type => Some("TYPES"),
            Self::Schema => Some("SCHEMAS"),
            Self::Language
            | Self::ForeignDataWrapper
            | Self::ForeignServer => None,
        }
    }
}

/// `acldefault`: all privileges for the owner, and EXECUTE on routines
/// and USAGE on types and languages for PUBLIC
fn built_in(kind: Kind, owner: &str) -> Acl {
    let mut acl = Acl::new();
    for privilege in kind.all(false) {
        acl.insert(key("", owner, privilege), false);
    }
    if matches!(kind, Kind::Function | Kind::Type | Kind::Language) {
        acl.insert(key("", "PUBLIC", kind.all(false)[0]), false);
    }
    acl
}

fn key(column: &str, grantee: &str, privilege: &str) -> Key {
    let grantee = if grantee.eq_ignore_ascii_case("PUBLIC") {
        "PUBLIC"
    } else {
        grantee
    };
    (
        column.to_string(),
        grantee.to_string(),
        privilege.to_string(),
    )
}

/// Apply the statements to `acl`: those on the object first, then
/// those on columns, as pg_dump writes them. A REVOKE on the object
/// also revokes the privilege on each column, as in PostgreSQL.
fn apply(acl: &mut Acl, kind: Kind, statements: &[ddl::Acl]) {
    let (columns, object): (Vec<&ddl::Acl>, Vec<&ddl::Acl>) =
        statements.iter().partition(|statement| {
            statement.privileges.iter().any(|p| p.columns.is_some())
        });
    for statement in object.into_iter().chain(columns) {
        for privilege in &statement.privileges {
            let columns = match &privilege.columns {
                Some(columns) => columns.clone(),
                None => vec![String::new()],
            };
            for column in &columns {
                let name = privilege
                    .name
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .to_uppercase();
                let names: Vec<&str> =
                    if name == "ALL" || name == "ALL PRIVILEGES" {
                        kind.all(!column.is_empty()).to_vec()
                    } else {
                        vec![name.as_str()]
                    };
                for role in &statement.roles {
                    for name in &names {
                        let item = key(column, role, name);
                        if !statement.revoke {
                            *acl.entry(item).or_default() |=
                                statement.with_grant_option;
                            continue;
                        }
                        let (_, grantee, _) = &item;
                        let cascade = column.is_empty();
                        for (key, option) in acl.iter_mut() {
                            let hit = &key.1 == grantee
                                && key.2 == *name
                                && (key.0 == *column || cascade);
                            if hit && statement.grant_option_for {
                                *option = false;
                            }
                        }
                        if !statement.grant_option_for {
                            acl.retain(|key, _| {
                                !(&key.1 == grantee
                                    && key.2 == *name
                                    && (key.0 == *column || cascade))
                            });
                        }
                    }
                }
            }
        }
    }
}

/// `ALTER ... OWNER TO` gives the new owner each privilege of the old
/// owner (`aclnewowner`)
fn reowned(acl: Acl, old: &str, new: &str) -> Acl {
    let mut moved = Acl::new();
    for ((column, grantee, privilege), option) in acl {
        let grantee = if grantee == old {
            new.to_string()
        } else {
            grantee
        };
        *moved.entry((column, grantee, privilege)).or_default() |= option;
    }
    moved
}

/// The GRANT and REVOKE statements that change `existing` to `wanted`
/// on the object `on` (`TABLE test.users`, for example). Statements on
/// the object come first, then those on each column. For each grantee
/// a REVOKE comes before a GRANT, as pg_dump writes them. A REVOKE on
/// the object also takes the privilege away from each column, so the
/// column statements give back what the project keeps there.
fn statements(
    kind: Kind,
    on: &str,
    label: &str,
    wanted: &Acl,
    existing: &Acl,
) -> Vec<Alter> {
    #[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
    enum Action {
        Revoke,
        RevokeGrantOption,
        Grant,
        GrantWithGrantOption,
    }
    let mut existing = existing.clone();
    let mut alters = Vec::new();
    for level in [false, true] {
        let on_level = |key: &Key| key.0.is_empty() != level;
        let mut groups: BTreeMap<(String, String, Action), Vec<String>> =
            BTreeMap::new();
        let mut add = |key: &Key, action: Action| {
            let (column, grantee, privilege) = key.clone();
            groups
                .entry((column, grantee, action))
                .or_default()
                .push(privilege);
        };
        for (key, option) in existing.iter().filter(|(k, _)| on_level(k)) {
            match wanted.get(key) {
                None => add(key, Action::Revoke),
                Some(false) if *option => add(key, Action::RevokeGrantOption),
                Some(_) => {}
            }
        }
        for (key, option) in wanted.iter().filter(|(k, _)| on_level(k)) {
            match (existing.get(key), option) {
                (None, false) => add(key, Action::Grant),
                (None, true) | (Some(false), true) => {
                    add(key, Action::GrantWithGrantOption)
                }
                _ => {}
            }
        }
        // the revokes on the object reach the columns
        if !level {
            let revokes: Vec<(String, String, Action)> = groups
                .iter()
                .filter(|((_, _, action), _)| *action < Action::Grant)
                .flat_map(|((_, grantee, action), names)| {
                    names
                        .iter()
                        .map(|name| (grantee.clone(), name.clone(), *action))
                })
                .collect();
            for (grantee, name, action) in revokes {
                match action {
                    Action::Revoke => existing.retain(|key, _| {
                        key.0.is_empty() || key.1 != grantee || key.2 != name
                    }),
                    _ => {
                        for (key, option) in existing.iter_mut() {
                            if key.1 == grantee && key.2 == name {
                                *option = false;
                            }
                        }
                    }
                }
            }
        }
        for ((column, grantee, action), mut names) in groups {
            let order = kind.all(level);
            names.sort_by_key(|p| {
                (
                    order.iter().position(|o| o == p).unwrap_or(order.len()),
                    p.clone(),
                )
            });
            let names = names
                .iter()
                .map(|name| match column.is_empty() {
                    true => name.clone(),
                    false => {
                        format!(
                            "{name}({})",
                            crate::utils::quote_ident(&column)
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            let grantee = user_mapping_subject(&grantee);
            let sql = match action {
                Action::Revoke => {
                    format!("REVOKE {names} ON {on} FROM {grantee};\n")
                }
                Action::RevokeGrantOption => format!(
                    "REVOKE GRANT OPTION FOR {names} ON {on} FROM {grantee};\n"
                ),
                Action::Grant => {
                    format!("GRANT {names} ON {on} TO {grantee};\n")
                }
                Action::GrantWithGrantOption => format!(
                    "GRANT {names} ON {on} TO {grantee} WITH GRANT OPTION;\n"
                ),
            };
            let revoke = action < Action::Grant;
            alters.push(Alter {
                destructive: revoke,
                fails_open: revoke,
                label: Some(label.to_string()),
                ..Alter::new(sql)
            });
        }
    }
    alters
}

/// The types of the items whose privileges deploy compares: the
/// targets of the ACL sections of build (`build::acls`)
const COMPARED: &[constants::ObjectType] = &[
    constants::ObjectType::Domain,
    constants::ObjectType::ForeignDataWrapper,
    constants::ObjectType::Function,
    constants::ObjectType::MaterializedView,
    constants::ObjectType::ProceduralLanguage,
    constants::ObjectType::Procedure,
    constants::ObjectType::Schema,
    constants::ObjectType::Sequence,
    constants::ObjectType::Server,
    constants::ObjectType::Table,
    constants::ObjectType::Type,
    constants::ObjectType::View,
];

/// The privilege statements of the plan
#[derive(Default)]
pub(crate) struct Privileges {
    /// The statements that come directly after the CREATE of an
    /// archive entry, in the same plan statement, by its dump id: they
    /// change the ACL that the default privileges of the connecting
    /// role give into the built-in one, which the ACL entry of the
    /// archive starts from. A REVOKE among them only takes away what
    /// the CREATE gave, thus it is not destructive
    pub after_create: HashMap<i32, String>,
    /// The statements for the objects that the database has, which
    /// come after all other statements of the plan
    pub existing: Vec<Alter>,
}

/// The statements of one target object on one side
struct Side {
    statements: Vec<ddl::Acl>,
    /// The owner of the object, from the ACL entry
    owner: Option<String>,
    /// The object as the statements name it, after ON
    on: String,
    /// The object for the plan labels
    label: String,
}

/// One object that has privileges. A routine is its item, as the two
/// sides give its name with other argument names; another object is
/// its kind and name, which also finds the identity sequences and the
/// partitions that a table item makes
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Target {
    Item(usize),
    Name(Kind, String),
}

/// The GRANT and REVOKE statements of the plan, with --no-privileges
/// none. `creator` is the role that runs the script, when it is
/// known. `implied` are the items of the sequences that serial columns
/// make (see `serial::expand`): the database privileges of such a
/// sequence are compared only when the project grants privileges on
/// it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan(
    project: &Project,
    implied: &BTreeSet<usize>,
    diff: &Diff,
    resolutions: &BTreeMap<usize, Resolution>,
    output: &crate::build::BuildOutput,
    snapshot: &libpgdump::Dump,
    args: &cli::Deploy,
    creator: Option<&str>,
) -> Result<Privileges, String> {
    let mut privileges = Privileges::default();
    if args.no_privileges {
        return Ok(privileges);
    }
    let mut parser = ddl::Parser::new()?;
    let items: HashMap<usize, &Definition> = project
        .inventory
        .iter()
        .map(|item| (item.id, &item.definition))
        .collect();
    // the snapshot entry of each project item, for its owner and for
    // the ACL entries that depend on it. Only the types that build
    // gives ACL entries are compared: pg_dump writes the ACL of an
    // aggregate ON FUNCTION, which the project cannot attach to it
    let keys: BTreeMap<ObjectKey, usize> = project
        .inventory
        .iter()
        .filter(|item| {
            COMPARED.contains(&item.desc) && !implied.contains(&item.id)
        })
        .map(|item| {
            let key = ObjectKey::new(item.desc, &item.definition);
            (super::drop_match_key(&key, &item.definition), item.id)
        })
        .collect();
    let mut snapshot_items: HashMap<i32, usize> = HashMap::new();
    let mut db_owners: HashMap<usize, String> = HashMap::new();
    for entry in snapshot.entries() {
        if let Some(id) = super::entry_key(entry).and_then(|k| keys.get(&k)) {
            snapshot_items.insert(entry.dump_id, *id);
            if let Some(owner) = entry.owner.as_deref()
                && !owner.is_empty()
            {
                db_owners.entry(*id).or_insert_with(|| owner.to_string());
            }
        }
    }
    let rebuilt = |id: &usize| {
        matches!(
            resolutions.get(id),
            Some(Resolution::Replace | Resolution::Rebuild { .. })
        )
    };
    let stays = |id: &usize| match diff.items.get(id) {
        Some(Change::Unchanged | Change::Undiffable) => true,
        Some(Change::Changed) => !rebuilt(id),
        _ => false,
    };
    // the project side: build's ACL entries, by their object item
    let entries_by_id: HashMap<i32, &libpgdump::Entry> = output
        .dump
        .entries()
        .iter()
        .map(|entry| (entry.dump_id, entry))
        .collect();
    let mut wanted: HashMap<Target, Side> = HashMap::new();
    let mut targets: HashMap<Target, usize> = HashMap::new();
    let mut skipped: HashSet<Target> = HashSet::new();
    for entry in output.dump.entries() {
        if entry.desc != libpgdump::ObjectType::Acl {
            continue;
        }
        let item = super::entry_owners(entry, output, &entries_by_id)
            .into_iter()
            .find(|id| diff.items.get(id) != Some(&Change::Skipped));
        let Some((target, side)) =
            side(&mut parser, entry, item, &mut skipped)
        else {
            continue;
        };
        if let Some(item) = item {
            targets.insert(target.clone(), item);
        }
        merge(&mut wanted, target, side);
    }
    // the database side: the snapshot's ACL entries, by the entry of
    // their object
    let mut existing: HashMap<Target, Side> = HashMap::new();
    for entry in snapshot.entries() {
        if entry.desc != libpgdump::ObjectType::Acl {
            continue;
        }
        let item = entry
            .dependencies
            .iter()
            .find_map(|dep| snapshot_items.get(dep).copied());
        let Some((target, side)) =
            side(&mut parser, entry, item, &mut skipped)
        else {
            continue;
        };
        if let Some(item) = item {
            targets.entry(target.clone()).or_insert(item);
        }
        merge(&mut existing, target, side);
    }
    let mut compared: Vec<&Target> = targets.keys().collect();
    compared.sort_by_key(|target| match target {
        Target::Item(id) => (0, *id, String::new()),
        Target::Name(_, name) => (1, targets[*target], name.clone()),
    });
    for target in compared {
        let item = targets[target];
        if !stays(&item)
            || skipped.contains(target)
            || skipped.contains(&Target::Item(item))
        {
            continue;
        }
        let (want, have) = (wanted.get(target), existing.get(target));
        let Some(kind) = [want, have]
            .into_iter()
            .flatten()
            .find_map(|side| Kind::of(side.statements.first()?.target))
        else {
            continue;
        };
        let db_owner = have
            .and_then(|side| side.owner.clone())
            .or_else(|| db_owners.get(&item).cloned());
        let project_owner = items
            .get(&item)
            .and_then(|definition| definition.owner())
            .filter(|owner| !owner.is_empty() && !args.no_owner);
        let Some(owner) =
            project_owner.map(str::to_string).or(db_owner.clone())
        else {
            continue;
        };
        let db_owner = db_owner.unwrap_or_else(|| owner.clone());
        let mut desired = built_in(kind, &owner);
        if let Some(side) = want {
            apply(&mut desired, kind, &side.statements);
        }
        let mut current = built_in(kind, &db_owner);
        if let Some(side) = have {
            apply(&mut current, kind, &side.statements);
        }
        let current = reowned(current, &db_owner, &owner);
        let (on, label) = match want.or(have) {
            Some(side) => (&side.on, &side.label),
            None => continue,
        };
        privileges
            .existing
            .extend(statements(kind, on, label, &desired, &current));
    }
    if let Some(creator) = creator {
        after_create(
            &mut privileges,
            project,
            diff,
            &rebuilt,
            output,
            args,
            creator,
        );
    }
    Ok(privileges)
}

/// Read the statements of an ACL entry. A target with a statement that
/// is not a GRANT or a REVOKE of a compared kind (pg_dump writes a
/// grant by another grantor in SET SESSION AUTHORIZATION) is not
/// compared.
fn side(
    parser: &mut ddl::Parser,
    entry: &libpgdump::Entry,
    item: Option<usize>,
    skipped: &mut HashSet<Target>,
) -> Option<(Target, Side)> {
    let defn = entry.defn.as_deref()?;
    let parsed = match parser.parse(defn) {
        Ok(parsed) => parsed,
        Err(error) => {
            log::warn!(
                "{}: deploy cannot read its privileges ({error}), thus it \
                 does not compare the privileges of its object",
                super::entry_label(entry)
            );
            if let Some(item) = item {
                skipped.insert(Target::Item(item));
            }
            return None;
        }
    };
    let mut statements = Vec::new();
    let mut readable = true;
    for statement in parsed {
        match statement {
            ddl::Statement::Acl(acl)
                if Kind::of(acl.target).is_some()
                    && acl.objects.len() == 1 =>
            {
                statements.push(acl)
            }
            _ => readable = false,
        }
    }
    let first = statements.first()?;
    let kind = Kind::of(first.target)?;
    let target = match (kind, item) {
        (Kind::Function, Some(item)) => Target::Item(item),
        (Kind::Function, None) => return None,
        (kind, _) => Target::Name(kind, first.objects[0].clone()),
    };
    if !readable {
        log::warn!(
            "{}: deploy cannot read its privileges, thus it does not \
             compare them",
            super::entry_label(entry)
        );
        skipped.insert(target);
        return None;
    }
    let keyword = first.on.split_whitespace().next().unwrap_or_default();
    let label = match kind {
        // a table grant can leave out TABLE
        Kind::Table => format!("TABLE {}", first.objects[0]),
        _ => format!("{} {}", keyword.to_uppercase(), first.objects[0]),
    };
    let on = match kind {
        Kind::Table if !keyword.eq_ignore_ascii_case("TABLE") => {
            format!("TABLE {}", first.on)
        }
        _ => first.on.clone(),
    };
    Some((
        target,
        Side {
            on,
            label,
            owner: entry.owner.clone().filter(|owner| !owner.is_empty()),
            statements,
        },
    ))
}

/// Add the statements of one more entry of a target: the column
/// entries of a table come apart from the entry of the table
fn merge(sides: &mut HashMap<Target, Side>, target: Target, side: Side) {
    match sides.get_mut(&target) {
        Some(existing) => {
            existing.statements.extend(side.statements);
            if existing.owner.is_none() {
                existing.owner = side.owner;
            }
        }
        None => {
            sides.insert(target, side);
        }
    }
}

/// The statements after the CREATE of each new or rebuilt object that
/// change the ACL that the default privileges of `creator` give it
/// into the built-in ACL of its owner
fn after_create(
    privileges: &mut Privileges,
    project: &Project,
    diff: &Diff,
    rebuilt: &dyn Fn(&usize) -> bool,
    output: &crate::build::BuildOutput,
    args: &cli::Deploy,
    creator: &str,
) {
    let built_in_defaults = DefaultPrivileges {
        name: creator.to_string(),
        grants: None,
        revocations: None,
    };
    // the default privileges of the creator when the script makes each
    // object: the project's ones when deploy changes them before the
    // objects, else the database's ones until the archive entry of the
    // project's ones
    let project_item =
        project
            .inventory
            .iter()
            .find_map(|item| match &item.definition {
                Definition::DefaultPrivileges(defaults)
                    if defaults.name == creator =>
                {
                    Some((item.id, defaults))
                }
                _ => None,
            });
    let removed = diff.removed.get(&ObjectKey {
        desc: constants::ObjectType::DefaultPrivileges,
        schema: String::new(),
        name: creator.to_string(),
    });
    let database = match removed {
        Some(Definition::DefaultPrivileges(defaults)) if !args.allow_drop => {
            defaults
        }
        _ => &built_in_defaults,
    };
    let mut in_effect = match project_item {
        Some((id, defaults))
            if diff.items.get(&id) != Some(&Change::Added) =>
        {
            defaults
        }
        _ => database,
    };
    let mut defaults = effective(in_effect);
    for entry in output.dump.entries() {
        let Some(id) = output.item_ids.get(&entry.dump_id) else {
            continue;
        };
        if let Some((project_id, project_defaults)) = project_item
            && *id == project_id
            && !std::ptr::eq(in_effect, project_defaults)
        {
            in_effect = project_defaults;
            defaults = effective(in_effect);
        }
        let change = diff.items.get(id);
        if change != Some(&Change::Added)
            && !(change == Some(&Change::Changed) && rebuilt(id))
        {
            continue;
        }
        let Some((kind, on, label)) = object(entry) else {
            continue;
        };
        let Some(object_type) = kind.defaults() else {
            continue;
        };
        let schema = match kind {
            Kind::Schema => "",
            _ => entry.namespace.as_deref().unwrap_or_default(),
        };
        let mut first = Acl::new();
        for ((scope, kind, grantee, privilege), option) in &defaults {
            if kind == object_type && (scope.is_empty() || scope == schema) {
                *first.entry(key("", grantee, privilege)).or_default() |=
                    *option;
            }
        }
        // a new object gets its owner after its CREATE (see `plan`)
        let owner = match entry.owner.as_deref() {
            Some(owner)
                if !owner.is_empty()
                    && !args.no_owner
                    && diff.owned.contains(id) =>
            {
                owner
            }
            _ => creator,
        };
        let first = reowned(first, creator, owner);
        let sql: String =
            statements(kind, &on, &label, &built_in(kind, owner), &first)
                .into_iter()
                .map(|alter| alter.sql)
                .collect();
        if !sql.is_empty() {
            privileges.after_create.insert(entry.dump_id, sql);
        }
    }
}

/// The kind of an archive entry's object, the object as a GRANT names
/// it, and its label. A routine is named by its DROP statement, which
/// has its signature.
fn object(entry: &libpgdump::Entry) -> Option<(Kind, String, String)> {
    use libpgdump::ObjectType as OT;
    let (kind, keyword) = match entry.desc {
        OT::Table | OT::View | OT::MaterializedView | OT::ForeignTable => {
            (Kind::Table, "TABLE")
        }
        OT::Sequence => (Kind::Sequence, "SEQUENCE"),
        OT::Function | OT::Procedure => {
            let drop = entry.drop_stmt.as_deref()?.strip_prefix("DROP ")?;
            let on = drop.trim_end_matches(['\n', ';']).to_string();
            return Some((Kind::Function, on.clone(), on));
        }
        OT::Type => (Kind::Type, "TYPE"),
        OT::Domain => (Kind::Type, "DOMAIN"),
        OT::Schema => (Kind::Schema, "SCHEMA"),
        _ => return None,
    };
    let tag = entry.tag.as_deref()?;
    let quoted = crate::utils::quote_ident(tag);
    let (name, label) = match entry.namespace.as_deref() {
        Some(namespace) if !namespace.is_empty() => (
            format!("{}.{quoted}", crate::utils::quote_ident(namespace)),
            format!("{namespace}.{tag}"),
        ),
        _ => (quoted, tag.to_string()),
    };
    Some((
        kind,
        format!("{keyword} {name}"),
        format!("{keyword} {label}"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(sql: &str) -> Vec<ddl::Acl> {
        ddl::Parser::new()
            .expect("parser")
            .parse(sql)
            .expect("parses")
            .into_iter()
            .map(|statement| match statement {
                ddl::Statement::Acl(acl) => acl,
                other => panic!("expected an ACL statement: {other:?}"),
            })
            .collect()
    }

    fn acl(kind: Kind, owner: &str, sql: &str) -> Acl {
        let mut acl = built_in(kind, owner);
        apply(&mut acl, kind, &parsed(sql));
        acl
    }

    fn sql(alters: &[Alter]) -> Vec<&str> {
        alters.iter().map(|a| a.sql.trim_end()).collect()
    }

    /// pg_dump's form and a hand-written form that build renders give
    /// the same privileges: ALL, case, order, the owner's own
    /// privileges, PUBLIC's built-in ones, and a revoke of a privilege
    /// that is not there
    #[test]
    fn written_forms_give_the_same_privileges() {
        let pulled = acl(
            Kind::Table,
            "postgres",
            "REVOKE ALL ON TABLE test.t FROM postgres;\n\
             GRANT SELECT,INSERT ON TABLE test.t TO postgres;\n\
             GRANT ALL ON TABLE test.t TO app WITH GRANT OPTION;\n\
             GRANT SELECT ON TABLE test.t TO PUBLIC;",
        );
        let hand_written = acl(
            Kind::Table,
            "postgres",
            "REVOKE delete, truncate, references, trigger, maintain, \
             update ON TABLE test.t FROM postgres;\n\
             REVOKE DELETE ON TABLE test.t FROM public;\n\
             GRANT insert ON test.t TO postgres;\n\
             GRANT select ON TABLE test.t TO Public;\n\
             GRANT trigger, select, insert, update, delete, truncate, \
             references, maintain ON TABLE test.t TO app WITH GRANT \
             OPTION;",
        );
        assert_eq!(pulled, hand_written);
        let function = acl(
            Kind::Function,
            "postgres",
            "GRANT EXECUTE ON ROUTINE public.f(integer) TO PUBLIC;",
        );
        assert_eq!(function, built_in(Kind::Function, "postgres"));
    }

    /// A REVOKE on the table also takes the privilege away from each
    /// column, in the simulation and in the statements
    #[test]
    fn revoke_on_the_table_reaches_the_columns() {
        let have = acl(
            Kind::Table,
            "o",
            "GRANT SELECT ON TABLE t TO app;\n\
             GRANT SELECT(a), UPDATE(a) ON TABLE t TO app;",
        );
        let want = acl(Kind::Table, "o", "GRANT SELECT(a) ON TABLE t TO app;");
        let alters =
            statements(Kind::Table, "TABLE t", "TABLE t", &want, &have);
        assert_eq!(
            sql(&alters),
            vec![
                "REVOKE SELECT ON TABLE t FROM app;",
                "REVOKE UPDATE(a) ON TABLE t FROM app;",
                "GRANT SELECT(a) ON TABLE t TO app;",
            ]
        );
        let mut applied = have.clone();
        apply(
            &mut applied,
            Kind::Table,
            &parsed(
                &alters.iter().map(|a| a.sql.as_str()).collect::<String>(),
            ),
        );
        assert_eq!(applied, want);
    }

    /// The order: on the object, then each column; for each grantee a
    /// REVOKE before a GRANT. A REVOKE fails open and is destructive, a
    /// GRANT is neither
    #[test]
    fn emits_the_difference_in_order() {
        let have = acl(
            Kind::Schema,
            "o",
            "GRANT USAGE ON SCHEMA s TO app WITH GRANT OPTION;\n\
             GRANT CREATE ON SCHEMA s TO \"Other\";",
        );
        let want = acl(
            Kind::Schema,
            "o",
            "GRANT USAGE ON SCHEMA s TO app;\n\
             GRANT USAGE, CREATE ON SCHEMA s TO \"Other\" WITH GRANT OPTION;\n\
             GRANT USAGE ON SCHEMA s TO PUBLIC;",
        );
        let alters =
            statements(Kind::Schema, "SCHEMA s", "SCHEMA s", &want, &have);
        assert_eq!(
            sql(&alters),
            vec![
                "GRANT USAGE, CREATE ON SCHEMA s TO \"Other\" WITH GRANT \
                 OPTION;",
                "GRANT USAGE ON SCHEMA s TO PUBLIC;",
                "REVOKE GRANT OPTION FOR USAGE ON SCHEMA s FROM app;",
            ]
        );
        assert_eq!(
            alters
                .iter()
                .map(|a| (a.destructive, a.fails_open))
                .collect::<Vec<_>>(),
            vec![(false, false), (false, false), (true, true)]
        );
        assert!(
            statements(Kind::Schema, "SCHEMA s", "SCHEMA s", &want, &want)
                .is_empty()
        );
    }

    /// REVOKE GRANT OPTION FOR keeps the privilege
    #[test]
    fn revoke_grant_option_keeps_the_privilege() {
        let acl = acl(
            Kind::Sequence,
            "o",
            "GRANT USAGE ON SEQUENCE s TO app WITH GRANT OPTION;\n\
             REVOKE GRANT OPTION FOR USAGE ON SEQUENCE s FROM app;",
        );
        assert_eq!(acl.get(&key("", "app", "USAGE")), Some(&false));
    }

    /// A new owner gets the privileges of the old one
    #[test]
    fn new_owner_gets_the_privileges_of_the_old_owner() {
        let have = reowned(
            acl(
                Kind::Function,
                "old",
                "REVOKE ALL ON FUNCTION f() FROM PUBLIC;\n\
                 GRANT EXECUTE ON FUNCTION f() TO new;",
            ),
            "old",
            "new",
        );
        let want = acl(
            Kind::Function,
            "new",
            "REVOKE ALL ON FUNCTION f() FROM PUBLIC;",
        );
        assert_eq!(have, want);
    }

    /// The archive names the object of a new entry with quotes, and a
    /// routine by its signature
    #[test]
    fn names_the_object_of_an_entry() {
        let mut dump = libpgdump::new("test", "UTF8", "18.0").expect("dump");
        dump.add_entry(
            libpgdump::ObjectType::View,
            Some("My Schema"),
            Some("V"),
            Some("o"),
            None,
            None,
            None,
            &[],
        )
        .expect("view");
        dump.add_entry(
            libpgdump::ObjectType::Procedure,
            Some("s"),
            Some("p(integer)"),
            Some("o"),
            None,
            Some("DROP PROCEDURE s.p(integer);\n"),
            None,
            &[],
        )
        .expect("procedure");
        let objects: Vec<_> =
            dump.entries().iter().filter_map(object).collect();
        assert_eq!(
            objects,
            vec![
                (
                    Kind::Table,
                    String::from("TABLE \"My Schema\".\"V\""),
                    String::from("TABLE My Schema.V"),
                ),
                (
                    Kind::Function,
                    String::from("PROCEDURE s.p(integer)"),
                    String::from("PROCEDURE s.p(integer)"),
                ),
            ]
        );
    }
}
