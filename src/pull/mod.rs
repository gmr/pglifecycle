//! The `pull` command: database/dump → project (replaces
//! generate_project.py)
//!
//! Unlike the Python `generate`, output is the structured YAML format
//! (the test-project/ shape) — entry DDL is parsed through the [`ddl`]
//! module into models and child entries (indexes, constraints,
//! triggers, comments, ACLs, OWNED BY) are merged into their owners.

mod update;
mod writer;

use std::collections::BTreeMap;
use std::fmt::Write;
use std::io::IsTerminal;
use std::path::Path;
use std::time::Duration;

use serde_json::{Map, Value};

use crate::ddl::{self, Acl, AclTarget, QualifiedName, RoleDef, Statement};
use crate::models;
use crate::{cli, diagnostics, pgdump, progress};

pub fn pull(args: &cli::Pull) -> Result<(), String> {
    if args.update {
        if !args.destination.join("project.yaml").exists() {
            return Err(format!(
                "--update requires an existing project; {} has no \
                 project.yaml",
                args.destination.display()
            ));
        }
    } else if args.destination.exists() && !args.force {
        return Err(format!("{} already exists", args.destination.display()));
    }
    if args.no_owner {
        return Err(String::from(
            "--no-owner is not supported by pull: owner metadata is \
             required to generate the project",
        ));
    }
    if args.connection.password && !std::io::stdin().is_terminal() {
        return Err(String::from(
            "--password requires an interactive terminal; set PGPASSWORD \
             or use a pgpass file instead",
        ));
    }
    let (verb, gerund) = if args.update {
        ("Updated", "Updating")
    } else {
        ("Created", "Creating")
    };
    println!(
        "pglifecycle v{} {gerund} {} → {}",
        env!("CARGO_PKG_VERSION"),
        source_label(args),
        args.destination.display(),
    );
    diagnostics::init(args.error_file.clone());
    let ddl = pgdump::DumpDdl {
        no_owner: args.no_owner,
        no_privileges: args.no_privileges,
        no_security_labels: args.no_security_labels,
        no_tablespaces: args.no_tablespaces,
        exclude_tables: args.exclude_table.clone(),
        exclude_schemas: args.exclude_schema.clone(),
        exclude_extensions: args.exclude_extension.clone(),
    };
    // roles/users come from pg_dumpall, which needs a live connection;
    // skip them when replaying a --dump file (and on --no-roles)
    let roles = if args.no_roles || args.dump.is_some() {
        if args.dump.is_some() && !args.no_roles {
            log::info!("Skipping role extraction: --dump has no live cluster");
        }
        None
    } else {
        Some(args.include_password_hashes)
    };
    let (assembly, _) = snapshot(
        args.dump.as_deref(),
        &args.connection,
        &ddl,
        roles,
        args.style,
    )?;
    let task = progress::spinner("Rendering project");
    let files = writer::render(&assembly, args)?;
    task.finish();
    let counts = assembly.counts_by_type();
    let objects = assembly.object_count();
    if args.update {
        update::merge(&files, args)?;
    } else {
        writer::write_bootstrap(&files, args)?;
    }
    let plural = if objects == 1 { "object" } else { "objects" };
    println!(
        "\n{verb} {} with {objects} {plural}:\n\n{}",
        args.destination.display(),
        count_grid(&counts),
    );
    Ok(())
}

/// Render the per-type counts as a three-column, column-major grid with
/// right-aligned counts, e.g.
///
/// ```text
///      37  schemas          278  sequences          523  functions
///      13  extensions      1734  tables             191  users
/// ```
fn count_grid(counts: &[(&'static str, usize)]) -> String {
    const COLS: usize = 3;
    let rows = counts.len().div_ceil(COLS);
    // column c holds counts[c * rows .. c * rows + rows]; size each
    // column to its own widest count and label
    let column: Vec<&[(&str, usize)]> = (0..COLS)
        .map(|c| {
            let start = (c * rows).min(counts.len());
            let end = (start + rows).min(counts.len());
            &counts[start..end]
        })
        .collect();
    let count_w: Vec<usize> = column
        .iter()
        .map(|cells| {
            cells
                .iter()
                .map(|(_, n)| n.to_string().len())
                .max()
                .unwrap_or(0)
        })
        .collect();
    let label_w: Vec<usize> = column
        .iter()
        .map(|cells| cells.iter().map(|(l, _)| l.len()).max().unwrap_or(0))
        .collect();
    let mut out = String::new();
    for r in 0..rows {
        let mut line = String::new();
        for c in 0..COLS {
            if let Some((label, count)) = column[c].get(r) {
                let (cw, lw) = (count_w[c], label_w[c]);
                let _ = write!(line, "  {count:>cw$}  {label:<lw$}");
            }
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out.trim_end().to_string()
}

/// The connection the dump is read from, for the startup banner: the
/// dump file when replaying one, otherwise `dbname@host` (or just the
/// host when no database name was given)
fn source_label(args: &cli::Pull) -> String {
    if let Some(dump) = &args.dump {
        return dump.display().to_string();
    }
    match &args.connection.dbname {
        Some(dbname) => format!("{dbname}@{}", args.connection.host),
        None => args.connection.host.clone(),
    }
}

/// Snapshot a database (or an existing dump file) into an [`Assembly`],
/// returning the loaded dump alongside it for callers that need the
/// archive's entry order.
///
/// `roles` controls cluster role/user extraction via pg_dumpall:
/// `None` skips it; `Some(include_passwords)` extracts roles, including
/// password hashes only when `true`.
pub fn snapshot(
    dump_path: Option<&Path>,
    conn: &cli::Connection,
    ddl: &pgdump::DumpDdl,
    roles: Option<bool>,
    style: libpgfmt::style::Style,
) -> Result<(Assembly, libpgdump::Dump), String> {
    let mut temp_dump: Option<tempfile::NamedTempFile> = None;
    let dump_path = match dump_path {
        Some(path) => path.to_path_buf(),
        None => {
            let file = tempfile::Builder::new()
                .prefix("pglifecycle-")
                .suffix(".dump")
                .tempfile()
                .map_err(|e| format!("failed to create temp file: {e}"))?;
            let task = progress::spinner("Dumping database");
            pgdump::dump(conn, ddl, file.path())?;
            task.finish();
            let path = file.path().to_path_buf();
            temp_dump = Some(file);
            path
        }
    };
    log::info!("Loading dump from {}", dump_path.display());
    let task = progress::spinner("Loading dump");
    let dump = libpgdump::load(&dump_path).map_err(|e| {
        format!("failed to load dump {}: {e}", dump_path.display())
    })?;
    task.finish();
    drop(temp_dump);
    let mut assembly = Assembly::default();
    assembly.ingest(&dump)?;
    if let Some(include_passwords) = roles {
        // role extraction is best-effort: a locked-down cluster (e.g.
        // RDS restricts pg_authid) should not abort the whole schema
        // export, so a failure is warned and skipped, not propagated
        if let Err(error) =
            extract_roles(conn, include_passwords, &mut assembly)
        {
            log::warn!(
                "Skipping roles and users: {error}. Use --no-roles to \
                 silence this, or connect with sufficient privileges."
            );
        }
    }
    assembly.format_sql(style);
    Ok((assembly, dump))
}

/// Dump cluster roles via pg_dumpall and merge them into `assembly`
fn extract_roles(
    conn: &cli::Connection,
    include_passwords: bool,
    assembly: &mut Assembly,
) -> Result<(), String> {
    let file = tempfile::Builder::new()
        .prefix("pglifecycle-roles-")
        .suffix(".sql")
        .tempfile()
        .map_err(|e| format!("failed to create temp file: {e}"))?;
    pgdump::dump_roles(conn, file.path(), include_passwords)?;
    let text = std::fs::read_to_string(file.path()).map_err(|e| {
        format!("failed to read roles dump {}: {e}", file.path().display())
    })?;
    assembly.ingest_roles(&text)
}

/// A dump entry that was not assembled into the project models
#[derive(Debug)]
pub struct Remaining {
    pub desc: String,
    pub namespace: Option<String>,
    pub tag: Option<String>,
    pub defn: Option<String>,
}

/// How a cluster role is written to the project
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleKind {
    /// A role with the LOGIN attribute → `users/`
    User,
    /// A NOLOGIN role (or an ACL-only grantee like PUBLIC) → `roles/`
    Role,
}

/// Classify a cluster role for the project, shared by the writer and
/// the pull summary. Reserved `pg_*` roles are cluster-managed (and
/// uncreatable), so they are excluded (`None`); a role with the LOGIN
/// attribute is a [`RoleKind::User`], everything else a
/// [`RoleKind::Role`] (pg_dumpall does not distinguish groups).
pub fn classify_role(name: &str, state: &RoleState) -> Option<RoleKind> {
    if name.starts_with("pg_") {
        return None;
    }
    if state.options.login == Some(true) {
        Some(RoleKind::User)
    } else {
        Some(RoleKind::Role)
    }
}

/// Per-role state accumulated from ACL entries and the pg_dumpall
/// roles dump; classified into a user or role file at write time
#[derive(Debug, Default)]
pub struct RoleState {
    /// Whether a CREATE ROLE statement was seen; grantee-only roles
    /// (e.g. PUBLIC) are written with `create: false`
    pub created: bool,
    pub options: models::RoleOptions,
    pub password: Option<String>,
    pub valid_until: Option<String>,
    pub settings: Map<String, Value>,
    pub grants: AclMaps,
    pub revocations: AclMaps,
}

/// Accumulates ACLs as `section → object → [privileges]` plus role
/// memberships, converted to [`models::Acls`] at write time
#[derive(Debug, Default)]
pub struct AclMaps {
    pub roles: Vec<String>,
    pub sections: BTreeMap<&'static str, Map<String, Value>>,
}

impl AclMaps {
    fn add(&mut self, section: &'static str, object: &str, privilege: &str) {
        let map = self.sections.entry(section).or_default();
        let list = map
            .entry(object.to_string())
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Value::Array(items) = list
            && !items.iter().any(|v| v == privilege)
        {
            items.push(Value::String(privilege.to_string()));
        }
    }

    fn is_empty(&self) -> bool {
        self.roles.is_empty() && self.sections.is_empty()
    }

    pub fn to_acls(&self) -> Option<models::Acls> {
        if self.is_empty() {
            return None;
        }
        let mut acls = models::Acls::default();
        if !self.roles.is_empty() {
            acls.roles = Some(self.roles.clone());
        }
        for (section, map) in &self.sections {
            *acls_section(&mut acls, section) = Some(map.clone());
        }
        Some(acls)
    }
}

fn acls_section<'a>(
    acls: &'a mut models::Acls,
    section: &str,
) -> &'a mut Option<Map<String, Value>> {
    match section {
        "columns" => &mut acls.columns,
        "databases" => &mut acls.databases,
        "domains" => &mut acls.domains,
        "foreign_data_wrappers" => &mut acls.foreign_data_wrappers,
        "foreign_servers" => &mut acls.foreign_servers,
        "functions" => &mut acls.functions,
        "languages" => &mut acls.languages,
        "large_objects" => &mut acls.large_objects,
        "schemata" => &mut acls.schemata,
        "sequences" => &mut acls.sequences,
        "tables" => &mut acls.tables,
        "tablespaces" => &mut acls.tablespaces,
        "types" => &mut acls.types,
        other => unreachable!("unknown ACL section {other}"),
    }
}

/// The [`models::Acls`] section for an ACL target kind
fn section_key(target: AclTarget) -> &'static str {
    match target {
        AclTarget::Database => "databases",
        AclTarget::Domain => "domains",
        AclTarget::ForeignDataWrapper => "foreign_data_wrappers",
        AclTarget::ForeignServer => "foreign_servers",
        AclTarget::Function => "functions",
        AclTarget::Language => "languages",
        AclTarget::LargeObject => "large_objects",
        AclTarget::Schema => "schemata",
        AclTarget::Sequence => "sequences",
        AclTarget::Table => "tables",
        AclTarget::Tablespace => "tablespaces",
        AclTarget::Type => "types",
    }
}

/// The project models assembled from a dump
#[derive(Debug, Default)]
pub struct Assembly {
    pub dbname: String,
    pub encoding: Option<String>,
    pub stdstrings: Option<bool>,
    pub extensions: Vec<models::Extension>,
    pub languages: Vec<models::Language>,
    pub schemas: Vec<models::Schema>,
    pub domains: Vec<models::Domain>,
    pub types: Vec<models::Type>,
    pub sequences: Vec<models::Sequence>,
    pub tables: Vec<models::Table>,
    pub views: Vec<models::View>,
    pub materialized_views: Vec<models::MaterializedView>,
    pub functions: Vec<models::Function>,
    pub foreign_data_wrappers: Vec<models::ForeignDataWrapper>,
    pub servers: Vec<models::Server>,
    pub user_mappings: Vec<models::UserMapping>,
    pub roles: BTreeMap<String, RoleState>,
    pub remaining: Vec<Remaining>,
    /// Indexes whose target relation had not yet been ingested when the
    /// index entry was seen (pg_dump sorts INDEX before MATERIALIZED
    /// VIEW), replayed after the entry loop completes
    deferred_indexes: Vec<(QualifiedName, models::Index)>,
}

impl Assembly {
    /// The modeled objects written to the project, by type, in a
    /// readable order and excluding empty categories (the things a user
    /// thinks of as schema objects; excludes unparsed `remaining`
    /// entries and database-level metadata)
    pub fn counts_by_type(&self) -> Vec<(&'static str, usize)> {
        let mut users = 0;
        let mut roles = 0;
        for (name, state) in &self.roles {
            match classify_role(name, state) {
                Some(RoleKind::User) => users += 1,
                Some(RoleKind::Role) => roles += 1,
                None => {}
            }
        }
        [
            ("schemas", self.schemas.len()),
            ("extensions", self.extensions.len()),
            ("languages", self.languages.len()),
            ("domains", self.domains.len()),
            ("types", self.types.len()),
            ("sequences", self.sequences.len()),
            ("tables", self.tables.len()),
            ("views", self.views.len()),
            ("materialized views", self.materialized_views.len()),
            ("functions", self.functions.len()),
            ("foreign data wrappers", self.foreign_data_wrappers.len()),
            ("servers", self.servers.len()),
            ("user mappings", self.user_mappings.len()),
            ("users", users),
            ("roles", roles),
        ]
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .collect()
    }

    /// Total modeled object count across all types
    pub fn object_count(&self) -> usize {
        self.counts_by_type().iter().map(|(_, count)| count).sum()
    }

    /// Parse every supported archive entry into the project models
    pub fn ingest(&mut self, dump: &libpgdump::Dump) -> Result<(), String> {
        use libpgdump::ObjectType as OT;
        let mut parser = ddl::Parser::new()?;
        self.dbname = dump.dbname().to_string();
        let entries = dump.entries();
        let task = progress::spinner("Ingesting entries");
        for entry in entries {
            task.set_message(format!(
                "Ingesting {} {}",
                entry.desc.as_str(),
                entry.tag.as_deref().unwrap_or_default()
            ));
            match &entry.desc {
                OT::Database
                | OT::SearchPath
                | OT::SequenceSet
                | OT::TableData => {}
                OT::Encoding => {
                    self.encoding = entry.defn.as_deref().and_then(set_value);
                }
                OT::StdStrings => {
                    self.stdstrings = entry
                        .defn
                        .as_deref()
                        .and_then(set_value)
                        .map(|v| v == "on");
                }
                OT::Extension => self.extensions.push(extension(entry)),
                OT::ProceduralLanguage => {
                    self.languages.push(models::Language {
                        name: entry.tag.clone().unwrap_or_default(),
                        replace: None,
                        trusted: None,
                        handler: None,
                        inline_handler: None,
                        validator: None,
                        comment: None,
                    });
                }
                OT::Schema
                | OT::Domain
                | OT::Type
                | OT::Table
                | OT::View
                | OT::MaterializedView
                | OT::Function
                | OT::Sequence
                | OT::SequenceOwnedBy
                | OT::Index
                | OT::Constraint
                | OT::FkConstraint
                | OT::CheckConstraint
                | OT::Trigger
                | OT::ForeignTable
                | OT::ForeignDataWrapper
                | OT::ForeignServer
                | OT::Server
                | OT::UserMapping
                | OT::Comment
                | OT::Acl => {
                    let Some(defn) = &entry.defn else { continue };
                    let label = format!(
                        "{} {}",
                        entry.desc.as_str(),
                        entry.tag.as_deref().unwrap_or_default()
                    );
                    diagnostics::enter(&label, defn);
                    let parsed = parser.parse(defn);
                    diagnostics::leave();
                    match parsed {
                        Ok(statements) => {
                            for statement in cancel_revokes(statements) {
                                self.apply(statement, entry);
                            }
                        }
                        Err(error) => {
                            log::warn!("Failed to parse {label}: {error}");
                            diagnostics::record_failure(
                                "FAILED TO PARSE",
                                &label,
                                &error,
                                defn,
                            );
                            self.push_remaining(entry);
                        }
                    }
                }
                _ => self.push_remaining(entry),
            }
        }
        self.apply_deferred_indexes();
        task.finish();
        Ok(())
    }

    /// Attach indexes whose target relation was not yet ingested when
    /// the index entry was seen; warn for any that remain unresolved
    fn apply_deferred_indexes(&mut self) {
        for (table, index) in std::mem::take(&mut self.deferred_indexes) {
            if let Some(table) = self.find_table(&table) {
                table.indexes.get_or_insert_default().push(index);
            } else if let Some(view) = self.find_materialized_view(&table) {
                view.indexes.get_or_insert_default().push(index);
            } else {
                log::warn!("Index on unknown relation {table}");
            }
        }
    }

    /// Parse a `pg_dumpall --roles-only` SQL dump, skipping comments,
    /// SET statements, and psql meta-commands (PG17 wraps the output
    /// in `\restrict` / `\unrestrict`)
    pub fn ingest_roles(&mut self, text: &str) -> Result<(), String> {
        let mut parser = ddl::Parser::new()?;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty()
                || line.starts_with("--")
                || line.starts_with("SET ")
                || line.starts_with('\\')
            {
                continue;
            }
            match parser.parse(line) {
                Ok(statements) => {
                    for statement in statements {
                        self.apply_role_statement(statement, line);
                    }
                }
                Err(error) => {
                    log::warn!("Failed to parse role line {line:?}: {error}");
                }
            }
        }
        Ok(())
    }

    fn apply(&mut self, statement: Statement, entry: &libpgdump::Entry) {
        let owner = entry
            .owner
            .clone()
            .filter(|o| !o.is_empty())
            .unwrap_or_else(|| String::from("postgres"));
        match statement {
            Statement::CreateSchema(mut schema) => {
                schema.owner = owner;
                self.schemas.push(schema);
            }
            Statement::CreateDomain(mut domain) => {
                domain.owner = owner;
                self.domains.push(domain);
            }
            Statement::CreateType(mut value) => {
                value.owner = owner;
                self.types.push(*value);
            }
            Statement::CreateTable(mut table) => {
                table.owner = owner;
                self.tables.push(*table);
            }
            Statement::CreateSequence(mut sequence) => {
                sequence.owner = owner;
                self.sequences.push(sequence);
            }
            Statement::AlterSequence(sequence) => {
                self.merge_sequence(sequence);
            }
            Statement::CreateView(mut view) => {
                view.owner = owner;
                self.views.push(view);
            }
            Statement::CreateMaterializedView(mut view) => {
                view.owner = owner;
                self.materialized_views.push(view);
            }
            Statement::CreateFunction(mut function) => {
                function.owner = owner;
                self.functions.push(*function);
            }
            Statement::CreateForeignDataWrapper(mut fdw) => {
                fdw.owner = owner;
                self.foreign_data_wrappers.push(fdw);
            }
            Statement::CreateServer(server) => self.servers.push(server),
            Statement::CreateUserMapping(mapping) => {
                // group mappings by user so one UserMapping carries all
                // its servers (matching the project model)
                match self
                    .user_mappings
                    .iter_mut()
                    .find(|m| m.name == mapping.name)
                {
                    Some(existing) => existing.servers.extend(mapping.servers),
                    None => self.user_mappings.push(mapping),
                }
            }
            Statement::CreateIndex { table, index } => {
                if let Some(table) = self.find_table(&table) {
                    table.indexes.get_or_insert_default().push(index);
                } else if let Some(view) = self.find_materialized_view(&table)
                {
                    view.indexes.get_or_insert_default().push(index);
                } else {
                    // the target relation may simply not be ingested
                    // yet (matview indexes sort before their matview);
                    // retry after the entry loop
                    self.deferred_indexes.push((table, index));
                }
            }
            Statement::AddConstraint {
                table,
                name,
                constraint,
            } => match self.find_table(&table) {
                Some(table) => ddl::apply_constraint(table, name, constraint),
                None => log::warn!("Constraint on unknown table {table}"),
            },
            Statement::CreateTrigger { table, trigger } => {
                match self.find_table(&table) {
                    Some(table) => {
                        table.triggers.get_or_insert_default().push(trigger);
                    }
                    None => log::warn!("Trigger on unknown table {table}"),
                }
            }
            Statement::Comment {
                on,
                target,
                comment,
            } => self.apply_comment(&on, &target, comment),
            Statement::Acl(acl) => self.apply_acl(&acl),
            Statement::RoleMembership { .. }
            | Statement::CreateRole(_)
            | Statement::AlterRole(_)
            | Statement::AlterRoleSetting { .. } => {
                self.apply_role_statement(
                    statement,
                    entry.defn.as_deref().unwrap_or_default(),
                );
            }
            Statement::Unsupported(kind) => {
                log::info!(
                    "Unsupported {} {:?}: {kind}",
                    entry.desc.as_str(),
                    entry.tag
                );
                self.push_remaining(entry);
            }
        }
    }

    fn apply_role_statement(&mut self, statement: Statement, source: &str) {
        match statement {
            Statement::CreateRole(def) => self.merge_role(def, true),
            Statement::AlterRole(def) => self.merge_role(def, false),
            Statement::AlterRoleSetting { role, name, value } => {
                // a single element stays a scalar; a list (e.g.
                // search_path) keeps its elements so it round-trips as
                // `SET search_path TO a, b` rather than one bogus value
                let value = match value.as_slice() {
                    [single] => Value::String(single.clone()),
                    _ => Value::Array(
                        value.into_iter().map(Value::String).collect(),
                    ),
                };
                self.role(&role).settings.insert(name, value);
            }
            Statement::RoleMembership {
                revoke,
                roles,
                members,
            } => {
                for member in &members {
                    let state = self.role(member);
                    let maps = if revoke {
                        &mut state.revocations
                    } else {
                        &mut state.grants
                    };
                    for role in &roles {
                        if !maps.roles.contains(role) {
                            maps.roles.push(role.clone());
                        }
                    }
                }
            }
            other => {
                log::warn!("Unexpected statement in roles dump: {other:?}");
                self.remaining.push(Remaining {
                    desc: String::from("ROLE"),
                    namespace: None,
                    tag: None,
                    defn: Some(source.to_string()),
                });
            }
        }
    }

    fn role(&mut self, name: &str) -> &mut RoleState {
        self.roles.entry(name.to_string()).or_default()
    }

    fn merge_role(&mut self, def: RoleDef, created: bool) {
        let state = self.role(&def.name);
        state.created |= created;
        if def.password.is_some() {
            state.password = def.password;
        }
        if def.valid_until.is_some() {
            state.valid_until = def.valid_until;
        }
        merge_options(&mut state.options, def.options);
    }

    fn apply_acl(&mut self, acl: &Acl) {
        let section = section_key(acl.target);
        for role in &acl.roles {
            let state = self.role(role);
            let maps = if acl.revoke {
                &mut state.revocations
            } else {
                &mut state.grants
            };
            for object in &acl.objects {
                for privilege in &acl.privileges {
                    // grant option is carried on the privilege string
                    // (acls.yml), e.g. `SELECT WITH GRANT OPTION`
                    let name = if acl.with_grant_option {
                        format!("{} WITH GRANT OPTION", privilege.name)
                    } else {
                        privilege.name.clone()
                    };
                    match &privilege.columns {
                        Some(columns) => {
                            for column in columns {
                                maps.add(
                                    "columns",
                                    &format!("{object}.{column}"),
                                    &name,
                                );
                            }
                        }
                        None => maps.add(section, object, &name),
                    }
                }
            }
        }
    }

    fn apply_comment(
        &mut self,
        on: &str,
        target: &QualifiedName,
        comment: String,
    ) {
        let schema = target.schema.clone().unwrap_or_default();
        let name = &target.name;
        let found = match on {
            "SCHEMA" => self
                .schemas
                .iter_mut()
                .find(|s| s.name == *name)
                .map(|s| s.comment = Some(comment.clone()))
                .is_some(),
            "EXTENSION" => self
                .extensions
                .iter_mut()
                .find(|e| e.name == *name)
                .map(|e| e.comment = Some(comment.clone()))
                .is_some(),
            "TABLE" | "FOREIGN TABLE" => self
                .find_table(target)
                .map(|t| t.comment = Some(comment.clone()))
                .is_some(),
            "COLUMN" => self.apply_column_comment(target, &comment),
            "DOMAIN" => self
                .domains
                .iter_mut()
                .find(|d| d.schema == schema && d.name == *name)
                .map(|d| d.comment = Some(comment.clone()))
                .is_some(),
            "TYPE" => self
                .types
                .iter_mut()
                .find(|t| t.schema == schema && t.name == *name)
                .map(|t| t.comment = Some(comment.clone()))
                .is_some(),
            "SEQUENCE" => self
                .sequences
                .iter_mut()
                .find(|s| s.schema == schema && s.name == *name)
                .map(|s| s.comment = Some(comment.clone()))
                .is_some(),
            "VIEW" => self
                .views
                .iter_mut()
                .find(|v| v.schema == schema && v.name == *name)
                .map(|v| v.comment = Some(comment.clone()))
                .is_some(),
            "MATERIALIZED VIEW" => self
                .materialized_views
                .iter_mut()
                .find(|v| v.schema == schema && v.name == *name)
                .map(|v| v.comment = Some(comment.clone()))
                .is_some(),
            "FUNCTION" => self.apply_function_comment(&schema, name, &comment),
            "INDEX" => {
                self.tables
                    .iter_mut()
                    .filter(|t| t.schema == schema)
                    .flat_map(|t| t.indexes.iter_mut().flatten())
                    .find(|i| i.name == *name)
                    .map(|i| i.comment = Some(comment.clone()))
                    .is_some()
                    || self
                        .materialized_views
                        .iter_mut()
                        .filter(|v| v.schema == schema)
                        .flat_map(|v| v.indexes.iter_mut().flatten())
                        .find(|i| i.name == *name)
                        .map(|i| i.comment = Some(comment.clone()))
                        .is_some()
                    || self
                        .deferred_indexes
                        .iter_mut()
                        .find(|(rel, i)| {
                            rel.schema.clone().unwrap_or_default() == schema
                                && i.name == *name
                        })
                        .map(|(_, i)| i.comment = Some(comment.clone()))
                        .is_some()
            }
            _ => false,
        };
        if !found {
            log::warn!("Comment on unmatched object: {on} {target}");
        }
    }

    /// `COMMENT ON FUNCTION schema.fn(args)` — match the full identity
    /// signature so overloaded functions are not conflated; fall back
    /// to the base name only when it is unambiguous
    fn apply_function_comment(
        &mut self,
        schema: &str,
        name: &str,
        comment: &str,
    ) -> bool {
        if let Some(function) = self
            .functions
            .iter_mut()
            .find(|f| f.schema == schema && f.identity() == name)
        {
            function.comment = Some(comment.to_string());
            return true;
        }
        let base = name.split('(').next().unwrap_or(name);
        let mut candidates = self
            .functions
            .iter_mut()
            .filter(|f| f.schema == schema && f.name == base);
        let first = candidates.next();
        if candidates.next().is_some() {
            return false;
        }
        first
            .map(|f| f.comment = Some(comment.to_string()))
            .is_some()
    }

    /// `COMMENT ON COLUMN schema.table.column` — the ddl layer puts
    /// everything before the column into `target.schema`
    fn apply_column_comment(
        &mut self,
        target: &QualifiedName,
        comment: &str,
    ) -> bool {
        let Some(relation) = &target.schema else {
            return false;
        };
        let (schema, table) = match relation.split_once('.') {
            Some((schema, table)) => (Some(schema.to_string()), table),
            None => (None, relation.as_str()),
        };
        let relation = QualifiedName {
            schema,
            name: table.to_string(),
        };
        let Some(table) = self.find_table(&relation) else {
            return false;
        };
        let Some(column) = table
            .columns
            .iter_mut()
            .flatten()
            .find(|c| c.name == target.name)
        else {
            return false;
        };
        column.comment = Some(comment.to_string());
        true
    }

    fn find_table(
        &mut self,
        name: &QualifiedName,
    ) -> Option<&mut models::Table> {
        let schema = name.schema.clone().unwrap_or_default();
        self.tables
            .iter_mut()
            .find(|t| t.schema == schema && t.name == name.name)
    }

    fn find_materialized_view(
        &mut self,
        name: &QualifiedName,
    ) -> Option<&mut models::MaterializedView> {
        let schema = name.schema.clone().unwrap_or_default();
        self.materialized_views
            .iter_mut()
            .find(|v| v.schema == schema && v.name == name.name)
    }

    /// Merge ALTER SEQUENCE options (including OWNED BY) into the
    /// sequence created by CREATE SEQUENCE
    fn merge_sequence(&mut self, sequence: models::Sequence) {
        let Some(existing) = self
            .sequences
            .iter_mut()
            .find(|s| s.schema == sequence.schema && s.name == sequence.name)
        else {
            self.sequences.push(sequence);
            return;
        };
        macro_rules! merge {
            ($($field:ident),+) => {
                $(if sequence.$field.is_some() {
                    existing.$field = sequence.$field;
                })+
            };
        }
        merge!(
            data_type,
            increment_by,
            min_value,
            max_value,
            start_with,
            cache,
            cycle,
            owned_by
        );
    }

    /// Format view queries and function bodies with libpgfmt (AWeber
    /// style). On a formatting error — or if a single statement exceeds
    /// [`FORMAT_TIMEOUT`] (a likely upstream hang) — the original text
    /// is kept and the statement is recorded to the diagnostics report.
    pub fn format_sql(&mut self, style: libpgfmt::style::Style) {
        let task = progress::spinner("Formatting SQL");
        for view in &mut self.views {
            if let Some(query) = &view.query {
                task.set_message(format!("Formatting view {}", view.name));
                let label = format!("view {}", view.name);
                if let Some(formatted) =
                    format_one(query, false, &label, style)
                {
                    view.query = Some(strip_trailing(&formatted));
                }
            }
        }
        for view in &mut self.materialized_views {
            if let Some(query) = &view.query {
                task.set_message(format!(
                    "Formatting materialized view {}",
                    view.name
                ));
                let label = format!("materialized view {}", view.name);
                if let Some(formatted) =
                    format_one(query, false, &label, style)
                {
                    view.query = Some(strip_trailing(&formatted));
                }
            }
        }
        for function in &mut self.functions {
            let Some(definition) = &function.definition else {
                continue;
            };
            task.set_message(format!("Formatting function {}", function.name));
            let plpgsql = match function.language.as_deref() {
                Some("plpgsql") => true,
                Some("sql") => false,
                _ => continue,
            };
            let label = format!("function {}", function.name);
            if let Some(formatted) =
                format_one(definition, plpgsql, &label, style)
            {
                function.definition = Some(formatted);
            }
        }
        task.finish();
    }

    fn push_remaining(&mut self, entry: &libpgdump::Entry) {
        self.remaining.push(Remaining {
            desc: entry.desc.as_str().to_string(),
            namespace: entry.namespace.clone().filter(|n| !n.is_empty()),
            tag: entry.tag.clone(),
            defn: entry.defn.clone(),
        });
    }
}

fn strip_trailing(formatted: &str) -> String {
    formatted.trim_end_matches(';').trim_end().to_string()
}

/// Per-statement formatting budget. libpgfmt occasionally loops forever
/// on a pathological statement; well-formed SQL formats far under this,
/// so a statement that exceeds it is treated as a hang — kept
/// unformatted and recorded to the diagnostics report for reproduction.
const FORMAT_TIMEOUT: Duration = Duration::from_millis(500);

/// Format one statement with libpgfmt, tracking it as the in-flight
/// statement so an interrupt or a timeout attributes to it. Returns the
/// formatted SQL, or `None` to mean "keep the original" — on a
/// formatting error or a [`FORMAT_TIMEOUT`] overrun, both recorded to
/// the diagnostics report so the offending DDL can be reproduced.
fn format_one(
    sql: &str,
    plpgsql: bool,
    label: &str,
    style: libpgfmt::style::Style,
) -> Option<String> {
    diagnostics::enter(label, sql);
    let owned = sql.to_string();
    let result = run_with_timeout(FORMAT_TIMEOUT, move || {
        let formatted = if plpgsql {
            libpgfmt::format_plpgsql(&owned, style)
        } else {
            libpgfmt::format(&owned, style)
        };
        formatted.map_err(|e| e.to_string())
    });
    diagnostics::leave();
    match result {
        Some(Ok(formatted)) => Some(formatted),
        Some(Err(error)) => {
            log::warn!("failed to format {label}: {error}");
            diagnostics::record_failure(
                "FAILED TO FORMAT",
                label,
                &error,
                sql,
            );
            None
        }
        None => {
            log::warn!(
                "formatting {label} exceeded {FORMAT_TIMEOUT:?}; keeping it \
                 unformatted"
            );
            diagnostics::record_failure(
                "TIMED OUT FORMATTING",
                label,
                &format!("libpgfmt did not finish within {FORMAT_TIMEOUT:?}"),
                sql,
            );
            None
        }
    }
}

/// Run `op` on a worker thread, returning its result, or `None` if it
/// did not finish within `timeout`. A timed-out worker is abandoned —
/// it keeps running until the process exits, which is the only way to
/// walk away from an upstream infinite loop; for a one-shot CLI the
/// leaked thread is acceptable.
fn run_with_timeout<T: Send + 'static>(
    timeout: Duration,
    op: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(op());
    });
    rx.recv_timeout(timeout).ok()
}

/// Merge ALTER ROLE options over CREATE ROLE options
fn merge_options(into: &mut models::RoleOptions, from: models::RoleOptions) {
    macro_rules! merge {
        ($($field:ident),+) => {
            $(if from.$field.is_some() {
                into.$field = from.$field;
            })+
        };
    }
    merge!(
        bypass_rls,
        connection_limit,
        create_db,
        create_role,
        inherit,
        login,
        replication,
        superuser
    );
}

/// Drop REVOKE statements cancelled by an identical GRANT in the same
/// entry (pg_dump pairs `REVOKE ALL` with the explicit grants;
/// generate_project.py `_maybe_ignore_revoke`)
fn cancel_revokes(statements: Vec<Statement>) -> Vec<Statement> {
    let grants: Vec<Acl> = statements
        .iter()
        .filter_map(|s| match s {
            Statement::Acl(acl) if !acl.revoke => Some(acl.clone()),
            _ => None,
        })
        .collect();
    statements
        .into_iter()
        .filter(|statement| match statement {
            Statement::Acl(acl) if acl.revoke => !grants.iter().any(|g| {
                g.privileges == acl.privileges
                    && g.target == acl.target
                    && g.objects == acl.objects
                    && g.roles == acl.roles
                    && g.with_grant_option == acl.with_grant_option
            }),
            _ => true,
        })
        .collect()
}

/// The quoted value from a `SET name = 'value';` entry definition
fn set_value(defn: &str) -> Option<String> {
    let start = defn.find('\'')? + 1;
    let end = defn.rfind('\'')?;
    (end > start).then(|| defn[start..end].to_string())
}

/// Build an extension model from its entry; the schema comes from the
/// `WITH SCHEMA` clause of the definition
fn extension(entry: &libpgdump::Entry) -> models::Extension {
    let schema = entry.defn.as_deref().and_then(|defn| {
        let mut words = defn.split_whitespace().peekable();
        while let Some(word) = words.next() {
            if word.eq_ignore_ascii_case("schema")
                && let Some(schema) = words.peek()
            {
                return Some(
                    schema.trim_end_matches(';').trim_matches('"').to_string(),
                );
            }
        }
        None
    });
    models::Extension {
        name: entry.tag.clone().unwrap_or_default(),
        schema,
        version: None,
        cascade: None,
        comment: None,
    }
}

#[cfg(test)]
mod tests {
    use libpgdump::ObjectType as OT;

    use super::*;

    fn add(
        dump: &mut libpgdump::Dump,
        desc: OT,
        namespace: &str,
        tag: &str,
        defn: &str,
    ) {
        dump.add_entry(
            desc,
            Some(namespace),
            Some(tag),
            Some("postgres"),
            Some(defn),
            None,
            None,
            &[],
        )
        .expect("add_entry failed");
    }

    fn fixture_dump() -> libpgdump::Dump {
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(
            &mut dump,
            OT::Encoding,
            "",
            "ENCODING",
            "SET client_encoding = 'UTF8';",
        );
        add(
            &mut dump,
            OT::StdStrings,
            "",
            "STDSTRINGS",
            "SET standard_conforming_strings = 'on';",
        );
        add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
        add(
            &mut dump,
            OT::Acl,
            "",
            "SCHEMA test",
            "REVOKE ALL ON SCHEMA test FROM postgres;\n\
             GRANT ALL ON SCHEMA test TO postgres;\n\
             GRANT USAGE ON SCHEMA test TO PUBLIC;\n",
        );
        add(
            &mut dump,
            OT::Extension,
            "",
            "citext",
            "CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;",
        );
        add(
            &mut dump,
            OT::Comment,
            "",
            "EXTENSION citext",
            "COMMENT ON EXTENSION citext IS 'case-insensitive text';",
        );
        add(
            &mut dump,
            OT::Type,
            "test",
            "user_state",
            "CREATE TYPE test.user_state AS ENUM ('unverified', \
             'verified');",
        );
        add(
            &mut dump,
            OT::Domain,
            "test",
            "email_address",
            "CREATE DOMAIN test.email_address AS public.citext CHECK \
             (VALUE ~ '@');",
        );
        add(
            &mut dump,
            OT::Table,
            "test",
            "users",
            "CREATE TABLE test.users (\n\
             id uuid DEFAULT public.uuid_generate_v4() NOT NULL,\n\
             email public.citext NOT NULL,\n\
             locale text DEFAULT 'en-US'::text NOT NULL\n);",
        );
        add(
            &mut dump,
            OT::Constraint,
            "test",
            "users users_pkey",
            "ALTER TABLE ONLY test.users ADD CONSTRAINT users_pkey \
             PRIMARY KEY (id);",
        );
        add(
            &mut dump,
            OT::Index,
            "test",
            "users_unique_email",
            "CREATE UNIQUE INDEX users_unique_email ON test.users USING \
             btree (email);",
        );
        add(
            &mut dump,
            OT::Table,
            "test",
            "addresses",
            "CREATE TABLE test.addresses (\n\
             id uuid NOT NULL,\n\
             user_id uuid NOT NULL\n);",
        );
        add(
            &mut dump,
            OT::FkConstraint,
            "test",
            "addresses addresses_user_id_fkey",
            "ALTER TABLE ONLY test.addresses ADD CONSTRAINT \
             addresses_user_id_fkey FOREIGN KEY (user_id) REFERENCES \
             test.users(id) ON DELETE CASCADE;",
        );
        add(
            &mut dump,
            OT::Comment,
            "test",
            "TABLE users",
            "COMMENT ON TABLE test.users IS 'User records';",
        );
        add(
            &mut dump,
            OT::Comment,
            "test",
            "COLUMN users.email",
            "COMMENT ON COLUMN test.users.email IS 'Email address';",
        );
        add(
            &mut dump,
            OT::Sequence,
            "test",
            "user_id_seq",
            "CREATE SEQUENCE test.user_id_seq START WITH 1 INCREMENT \
             BY 1 CACHE 1;",
        );
        add(
            &mut dump,
            OT::SequenceOwnedBy,
            "test",
            "user_id_seq",
            "ALTER SEQUENCE test.user_id_seq OWNED BY test.users.id;",
        );
        add(
            &mut dump,
            OT::View,
            "test",
            "us_users",
            "CREATE VIEW test.us_users AS SELECT id FROM test.users \
             WHERE (locale = 'en-US'::text);",
        );
        add(
            &mut dump,
            OT::Function,
            "test",
            "set_last_modified()",
            "CREATE FUNCTION test.set_last_modified() RETURNS trigger \
             LANGUAGE plpgsql AS $$ BEGIN NEW.last_modified_at = \
             CURRENT_TIMESTAMP; RETURN NEW; END; $$;",
        );
        dump
    }

    fn assembled() -> Assembly {
        let mut assembly = Assembly::default();
        assembly.ingest(&fixture_dump()).unwrap();
        assembly
    }

    #[test]
    fn function_comments_match_overloads() {
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
        add(
            &mut dump,
            OT::Function,
            "test",
            "fn(a integer)",
            "CREATE FUNCTION test.fn(a integer) RETURNS integer \
             LANGUAGE sql AS $$ SELECT a $$;",
        );
        add(
            &mut dump,
            OT::Function,
            "test",
            "fn(a text)",
            "CREATE FUNCTION test.fn(a text) RETURNS text \
             LANGUAGE sql AS $$ SELECT a $$;",
        );
        add(
            &mut dump,
            OT::Comment,
            "test",
            "FUNCTION fn(a text)",
            "COMMENT ON FUNCTION test.fn(a text) IS 'text variant';",
        );
        let mut assembly = Assembly::default();
        assembly.ingest(&dump).unwrap();
        assert_eq!(assembly.functions.len(), 2);
        let data_type = |f: &models::Function| {
            f.parameters.as_ref().unwrap()[0].data_type.clone()
        };
        for function in &assembly.functions {
            match data_type(function).as_str() {
                "integer" => assert_eq!(function.comment, None),
                "text" => assert_eq!(
                    function.comment.as_deref(),
                    Some("text variant")
                ),
                other => panic!("unexpected parameter type {other}"),
            }
        }
    }

    #[test]
    fn ingests_project_settings() {
        let assembly = assembled();
        assert_eq!(assembly.dbname, "fixtures");
        assert_eq!(assembly.encoding.as_deref(), Some("UTF8"));
        assert_eq!(assembly.stdstrings, Some(true));
    }

    #[test]
    fn ingests_extension_with_comment() {
        let assembly = assembled();
        assert_eq!(assembly.extensions.len(), 1);
        let extension = &assembly.extensions[0];
        assert_eq!(extension.name, "citext");
        assert_eq!(extension.schema.as_deref(), Some("public"));
        assert_eq!(
            extension.comment.as_deref(),
            Some("case-insensitive text")
        );
    }

    #[test]
    fn ingests_schema_objects() {
        let assembly = assembled();
        assert_eq!(assembly.schemas.len(), 1);
        assert_eq!(assembly.schemas[0].owner, "postgres");
        assert_eq!(assembly.domains.len(), 1);
        assert_eq!(assembly.types.len(), 1);
        assert_eq!(
            assembly.types[0].enum_values,
            Some(vec!["unverified".into(), "verified".into()])
        );
        assert_eq!(assembly.views.len(), 1);
        assert_eq!(assembly.functions.len(), 1);
        assert!(assembly.remaining.is_empty());
    }

    #[test]
    fn merges_table_children() {
        let assembly = assembled();
        let users = assembly
            .tables
            .iter()
            .find(|t| t.name == "users")
            .expect("users table");
        assert!(users.primary_key.is_some());
        assert_eq!(users.indexes.as_ref().map(Vec::len), Some(1));
        assert_eq!(users.comment.as_deref(), Some("User records"));
        let email = users
            .columns
            .iter()
            .flatten()
            .find(|c| c.name == "email")
            .expect("email column");
        assert_eq!(email.comment.as_deref(), Some("Email address"));
        let addresses = assembly
            .tables
            .iter()
            .find(|t| t.name == "addresses")
            .expect("addresses table");
        let fks = addresses.foreign_keys.as_ref().expect("foreign keys");
        assert_eq!(fks[0].name, "addresses_user_id_fkey");
        assert_eq!(fks[0].on_delete.as_deref(), Some("CASCADE"));
    }

    #[test]
    fn merges_sequence_owned_by() {
        let assembly = assembled();
        assert_eq!(assembly.sequences.len(), 1);
        let sequence = &assembly.sequences[0];
        assert_eq!(sequence.increment_by, Some(1));
        assert_eq!(sequence.owned_by.as_deref(), Some("test.users.id"));
    }

    #[test]
    fn cancels_matching_revokes_and_collects_acls() {
        let assembly = assembled();
        let postgres = &assembly.roles["postgres"];
        assert!(postgres.revocations.is_empty());
        let schemata = &postgres.grants.sections["schemata"];
        assert_eq!(schemata["test"], serde_json::json!(["ALL"]));
        let public = &assembly.roles["PUBLIC"];
        assert!(!public.created);
        let schemata = &public.grants.sections["schemata"];
        assert_eq!(schemata["test"], serde_json::json!(["USAGE"]));
    }

    #[test]
    fn formats_view_queries() {
        let mut assembly = assembled();
        assembly.format_sql(libpgfmt::style::Style::Aweber);
        let query = assembly.views[0].query.as_deref().unwrap();
        assert!(query.contains('\n'), "expected formatted query: {query}");
        assert!(!query.ends_with(';'));
    }

    #[test]
    fn run_with_timeout_returns_fast_results() {
        let value = run_with_timeout(Duration::from_secs(5), || 21 * 2);
        assert_eq!(value, Some(42));
    }

    #[test]
    fn run_with_timeout_abandons_a_hang() {
        // the worker outlives the timeout; the call must return None
        // promptly rather than block on it
        let value = run_with_timeout(Duration::from_millis(20), || {
            std::thread::sleep(Duration::from_secs(30));
            7
        });
        assert_eq!(value, None);
    }

    #[test]
    fn ingests_roles_dump() {
        let mut assembly = Assembly::default();
        assembly
            .ingest_roles(
                "--\n\
                 -- PostgreSQL database cluster dump\n\
                 --\n\
                 \\restrict abc123\n\
                 SET default_transaction_read_only = off;\n\
                 CREATE ROLE app;\n\
                 ALTER ROLE app WITH NOSUPERUSER INHERIT NOCREATEROLE \
                 NOCREATEDB LOGIN PASSWORD 'md5abc' VALID UNTIL \
                 'infinity';\n\
                 CREATE ROLE readonly;\n\
                 ALTER ROLE readonly WITH NOLOGIN;\n\
                 GRANT readonly TO app GRANTED BY postgres;\n\
                 ALTER ROLE app SET search_path TO test, public;\n\
                 ALTER ROLE app SET work_mem TO '64MB';\n\
                 \\unrestrict abc123\n",
            )
            .unwrap();
        let app = &assembly.roles["app"];
        assert!(app.created);
        assert_eq!(app.password.as_deref(), Some("md5abc"));
        assert_eq!(app.valid_until.as_deref(), Some("infinity"));
        assert_eq!(app.options.login, Some(true));
        assert_eq!(app.options.superuser, Some(false));
        assert_eq!(app.grants.roles, vec!["readonly"]);
        // a multi-element setting keeps its list shape; a scalar stays
        // a string
        assert_eq!(
            app.settings.get("search_path"),
            Some(&Value::Array(vec![
                Value::String("test".into()),
                Value::String("public".into()),
            ]))
        );
        assert_eq!(
            app.settings.get("work_mem"),
            Some(&Value::String("64MB".into()))
        );
        let readonly = &assembly.roles["readonly"];
        assert!(readonly.created);
        assert_eq!(readonly.options.login, Some(false));
    }

    /// LOGIN roles become users, NOLOGIN roles stay roles, and reserved
    /// pg_* roles are dropped from the project (the bootstrap superuser
    /// is kept)
    #[test]
    fn roles_split_by_login_and_filter_cluster_roles() {
        use clap::Parser;
        let mut assembly = Assembly::default();
        assembly
            .ingest_roles(
                "CREATE ROLE app_login;\n\
                 ALTER ROLE app_login WITH LOGIN;\n\
                 CREATE ROLE app_group;\n\
                 ALTER ROLE app_group WITH NOLOGIN;\n\
                 CREATE ROLE postgres;\n\
                 ALTER ROLE postgres WITH SUPERUSER LOGIN;\n\
                 CREATE ROLE pg_read_all_data;\n\
                 ALTER ROLE pg_read_all_data WITH NOLOGIN;\n",
            )
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("project");
        let dump = dir.path().join("unused.dump");
        let args = match cli::Cli::try_parse_from([
            "pglifecycle",
            "pull",
            "--dump",
            dump.to_str().unwrap(),
            dest.to_str().unwrap(),
        ])
        .unwrap()
        .action
        {
            cli::Action::Pull(args) => args,
            _ => unreachable!(),
        };
        let files = writer::render(&assembly, &args).unwrap();

        assert!(files.contains_key(Path::new("users/app_login.yaml")));
        assert!(files.contains_key(Path::new("roles/app_group.yaml")));
        // the bootstrap superuser is a LOGIN role, so it is kept as a
        // user; only the uncreatable pg_* reserved roles are filtered
        assert!(files.contains_key(Path::new("users/postgres.yaml")));
        assert!(!files.contains_key(Path::new("roles/pg_read_all_data.yaml")));
    }

    /// Role settings survive the full pull → write → load → build
    /// path: emitted in the schema's array-of-objects shape (so the
    /// project validates and loads), then rendered back as
    /// `ALTER ROLE ... SET` entries in the build archive
    #[test]
    fn role_settings_round_trip_through_build() {
        use clap::Parser;
        let mut assembly = Assembly {
            dbname: String::from("settings"),
            ..Assembly::default()
        };
        assembly
            .ingest_roles(
                "CREATE ROLE app;\n\
                 ALTER ROLE app SET search_path TO test, public;\n\
                 ALTER ROLE app SET work_mem TO '64MB';\n",
            )
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("project");
        let dump = dir.path().join("unused.dump");
        let args = match cli::Cli::try_parse_from([
            "pglifecycle",
            "pull",
            "--dump",
            dump.to_str().unwrap(),
            dest.to_str().unwrap(),
        ])
        .unwrap()
        .action
        {
            cli::Action::Pull(args) => args,
            _ => unreachable!(),
        };
        let files = writer::render(&assembly, &args).unwrap();
        writer::write_bootstrap(&files, &args).unwrap();

        // load validates each file against its schema, so a successful
        // load proves the emitted settings shape matches role.yml
        let project = crate::project::load(&dest).unwrap();
        let role = project
            .inventory
            .iter()
            .find_map(|item| match &item.definition {
                models::Definition::Role(role) if role.name == "app" => {
                    Some(role)
                }
                _ => None,
            })
            .expect("app role");
        assert_eq!(
            role.settings,
            Some(vec![
                serde_json::from_value(serde_json::json!({
                    "search_path": ["test", "public"]
                }))
                .unwrap(),
                serde_json::from_value(serde_json::json!({
                    "work_mem": "64MB"
                }))
                .unwrap(),
            ])
        );

        let archive = dir.path().join("settings.dump");
        crate::build::build(&project, &archive).unwrap();
        let built = libpgdump::load(&archive).unwrap();
        let settings: Vec<&str> = built
            .entries()
            .iter()
            .filter_map(|e| e.defn.as_deref())
            .filter(|defn| defn.starts_with("ALTER ROLE"))
            .collect();
        assert_eq!(
            settings,
            vec![
                "ALTER ROLE app SET search_path TO test, public;\n",
                "ALTER ROLE app SET work_mem TO '64MB';\n",
            ]
        );
    }
}
