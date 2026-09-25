//! The `pull` command: database/dump → project (replaces
//! generate_project.py)
//!
//! Unlike the Python `generate`, output is the structured YAML format
//! (the test-project/ shape) — entry DDL is parsed through the [`ddl`]
//! module into models and child entries (indexes, constraints,
//! triggers, comments, ACLs, OWNED BY) are merged into their owners.

mod update;
mod writer;

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::io::IsTerminal;
use std::path::Path;
use std::time::Duration;

use serde_json::{Map, Value};

use crate::ddl::{self, Acl, AclTarget, QualifiedName, RoleDef, Statement};
use crate::models;
use crate::utils::quote_ident;
use crate::{cli, diagnostics, pgdump, progress};

/// Every TOC entry type that pull models. pull parses the DDL of each
/// one, except for extension entries, which it models from the entry
/// itself. Every other type goes to
/// `remaining`. The parse-coverage gate (`bin/parse-coverage`) makes
/// sure that the fixtures exercise each type in this list.
pub const MODELED_DESCS: &[libpgdump::ObjectType] = {
    use libpgdump::ObjectType as OT;
    &[
        OT::Extension,
        OT::ProceduralLanguage,
        OT::Schema,
        OT::Domain,
        OT::Type,
        OT::Table,
        OT::View,
        OT::MaterializedView,
        OT::Function,
        OT::Sequence,
        OT::SequenceOwnedBy,
        OT::Index,
        OT::IndexAttach,
        OT::Constraint,
        OT::FkConstraint,
        OT::CheckConstraint,
        OT::Default,
        OT::TableAttach,
        OT::Trigger,
        OT::Policy,
        OT::RowSecurity,
        OT::Aggregate,
        OT::Cast,
        OT::Collation,
        OT::Conversion,
        OT::EventTrigger,
        OT::Publication,
        OT::PublicationTable,
        OT::PublicationTablesInSchema,
        OT::TextSearchConfiguration,
        OT::TextSearchDictionary,
        OT::TextSearchParser,
        OT::TextSearchTemplate,
        OT::DefaultAcl,
        OT::Statistics,
        OT::Rule,
        OT::Procedure,
        OT::Operator,
        OT::OperatorFamily,
        OT::OperatorClass,
        OT::AccessMethod,
        OT::ForeignTable,
        OT::ForeignDataWrapper,
        OT::ForeignServer,
        OT::Server,
        OT::UserMapping,
        OT::Comment,
        OT::Acl,
    ]
};

/// The root-level file holding dump entries that could not be modeled
pub(super) const REMAINING_FILE: &str = "remaining.yaml";

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
    report_unmodeled(&assembly, args)
}

/// Report entries `pull` could not model. They are always written to
/// remaining.yaml, so the project directory is left in place either
/// way; what differs is whether the command succeeds. Failing is the
/// default because a project that silently omits schema does not
/// reproduce the database it came from, and nothing downstream —
/// `build`, `deploy`, or a code review of the YAML — can tell that
/// something went missing.
fn report_unmodeled(
    assembly: &Assembly,
    args: &cli::Pull,
) -> Result<(), String> {
    let count = assembly.remaining.len();
    if count == 0 {
        return Ok(());
    }
    let plural = if count == 1 { "entry" } else { "entries" };
    let descs = assembly.unmodeled_descs().join(", ");
    let path = args.destination.join(REMAINING_FILE).display().to_string();
    if args.allow_unsupported {
        log::warn!(
            "{count} dump {plural} could not be modeled ({descs}); they \
             were preserved in {path}, but the project will not reproduce \
             the source database"
        );
        return Ok(());
    }
    Err(format!(
        "{count} dump {plural} could not be modeled ({descs}), so the \
         generated project would not reproduce the source database.\n\
         The {plural} {verb} preserved in {path}; re-run with \
         --allow-unsupported to accept the project as it is.",
        verb = if count == 1 { "was" } else { "were" }
    ))
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
    pub comment: Option<String>,
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
    pub roles: Vec<models::Membership>,
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
    pub aggregates: Vec<models::Aggregate>,
    pub casts: Vec<models::Cast>,
    pub collations: Vec<models::Collation>,
    pub conversions: Vec<models::Conversion>,
    pub event_triggers: Vec<models::EventTrigger>,
    pub publications: Vec<models::Publication>,
    /// One per schema, as the project stores them
    pub text_search: Vec<models::TextSearch>,
    pub default_privileges: Vec<models::DefaultPrivileges>,
    pub statistics: Vec<models::Statistics>,
    pub procedures: Vec<models::Procedure>,
    pub operators: Vec<models::Operator>,
    pub operator_families: Vec<models::OperatorFamily>,
    pub operator_classes: Vec<models::OperatorClass>,
    pub access_methods: Vec<models::AccessMethod>,
    pub roles: BTreeMap<String, RoleState>,
    pub remaining: Vec<Remaining>,
    /// Indexes whose target relation had not yet been ingested when the
    /// index entry was seen (pg_dump sorts INDEX before MATERIALIZED
    /// VIEW), replayed after the entry loop completes
    deferred_indexes: Vec<(QualifiedName, models::Index)>,
    /// PARTITION OF children whose parent table had not yet been
    /// ingested, replayed after the entry loop completes
    deferred_partitions: Vec<(QualifiedName, models::TablePartition)>,
    /// ALTER TABLE ... ALTER COLUMN ... SET DEFAULT statements whose
    /// table had not yet been ingested, replayed after the entry loop
    /// completes
    deferred_defaults: Vec<(QualifiedName, String, Value)>,
    /// ALTER TABLE ... ATTACH PARTITION children, replayed after the
    /// entry loop so both parent and child tables are fully ingested;
    /// each child is folded into its parent's `partitions` and its
    /// standalone table is removed
    attached_partitions: Vec<(QualifiedName, models::TablePartition)>,
    /// (schema, name) -> index into `tables`, kept in sync as tables
    /// are ingested so index/constraint/trigger/comment merges are
    /// O(1) instead of a linear scan per lookup
    table_index: HashMap<(String, String), usize>,
    /// (schema, name) -> index into `materialized_views`, mirroring
    /// `table_index`
    matview_index: HashMap<(String, String), usize>,
    /// (schema, index name) -> where the index currently lives, kept
    /// in sync as indexes are attached so `COMMENT ON INDEX` does not
    /// need to scan every index in every table
    index_location: HashMap<(String, String), IndexLocation>,
}

/// Where an ingested index currently lives, for `index_location`
/// lookups
#[derive(Debug, Clone, Copy)]
enum IndexLocation {
    Table(usize),
    MaterializedView(usize),
    Deferred(usize),
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
            ("aggregates", self.aggregates.len()),
            ("casts", self.casts.len()),
            ("collations", self.collations.len()),
            ("conversions", self.conversions.len()),
            (
                "text search objects",
                self.text_search
                    .iter()
                    .map(|t| {
                        t.configurations.as_ref().map_or(0, Vec::len)
                            + t.dictionaries.as_ref().map_or(0, Vec::len)
                            + t.parsers.as_ref().map_or(0, Vec::len)
                            + t.templates.as_ref().map_or(0, Vec::len)
                    })
                    .sum(),
            ),
            ("publications", self.publications.len()),
            ("event triggers", self.event_triggers.len()),
            ("default privileges", self.default_privileges.len()),
            ("statistics", self.statistics.len()),
            ("procedures", self.procedures.len()),
            ("operators", self.operators.len()),
            ("operator families", self.operator_families.len()),
            ("operator classes", self.operator_classes.len()),
            ("access methods", self.access_methods.len()),
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
                desc if MODELED_DESCS.contains(desc) => {
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
                _ => {
                    log::warn!("Cannot model {}", entry_label(entry));
                    self.push_remaining(entry);
                }
            }
        }
        self.apply_deferred_indexes();
        self.apply_deferred_partitions();
        self.apply_deferred_defaults();
        self.apply_attached_partitions();
        self.apply_default_row_security();
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

    /// Attach PARTITION OF children whose parent table was not yet
    /// ingested when the child entry was seen; warn for any that
    /// remain unresolved
    fn apply_deferred_partitions(&mut self) {
        for (parent, partition) in
            std::mem::take(&mut self.deferred_partitions)
        {
            match self.find_table(&parent) {
                Some(table) => {
                    table.partitions.get_or_insert_default().push(partition);
                }
                None => log::warn!("Partition of unknown table {parent}"),
            }
        }
    }

    /// Attach column defaults (`ALTER TABLE ... ALTER COLUMN ... SET
    /// DEFAULT`) whose table was not yet ingested when the statement
    /// was seen; warn for any that remain unresolved
    fn apply_deferred_defaults(&mut self) {
        for (table, column, default) in
            std::mem::take(&mut self.deferred_defaults)
        {
            match self.find_table(&table) {
                Some(t) => set_column_default(t, &column, default),
                None => {
                    log::warn!("Column default on unknown table {table}");
                }
            }
        }
    }

    /// Fold `ATTACH PARTITION` children into their parent tables: move
    /// each child's bounds (and any table comment picked up during the
    /// entry loop) into the parent's `partitions`, then drop the child's
    /// standalone table so partitions are modeled only under the parent
    fn apply_attached_partitions(&mut self) {
        let attaches = std::mem::take(&mut self.attached_partitions);
        if attaches.is_empty() {
            return;
        }
        let mut removed = std::collections::HashSet::new();
        let mut folds = Vec::with_capacity(attaches.len());
        for (parent, mut partition) in attaches {
            let key = (partition.schema.clone(), partition.name.clone());
            match self.table_index.get(&key).and_then(|&i| self.tables.get(i))
            {
                // a partition with properties of its own stays a table,
                // attached to its parent; one without folds into the
                // parent's partitions, modeled by its bounds
                Some(child) if child.has_own_partition_properties() => {
                    partition.attached = Some(true);
                }
                Some(child) => {
                    if partition.comment.is_none() {
                        partition.comment = child.comment.clone();
                    }
                    removed.insert(key);
                }
                None => log::warn!(
                    "ATTACH PARTITION of unknown child {}.{}",
                    partition.schema,
                    partition.name
                ),
            }
            folds.push((parent, partition));
        }
        self.tables.retain(|t| {
            !removed.contains(&(t.schema.clone(), t.name.clone()))
        });
        self.rebuild_table_index();
        for (parent, partition) in folds {
            match self.find_table(&parent) {
                Some(table) => {
                    table.partitions.get_or_insert_default().push(partition);
                }
                None => {
                    log::warn!("ATTACH PARTITION to unknown table {parent}")
                }
            }
        }
    }

    /// Record row security as disabled on each table that has none, so
    /// every pulled table states it. An absent state tells deploy the
    /// project does not manage it, which is right for a project written
    /// before row security was modeled, not for a fresh pull. A foreign
    /// table keeps its state only when the dump gave one.
    fn apply_default_row_security(&mut self) {
        for table in &mut self.tables {
            if table.server.is_none() {
                table.row_level_security.get_or_insert_default();
            }
        }
    }

    /// Rebuild `table_index` from the current `tables` order after a
    /// removal has shifted positions
    fn rebuild_table_index(&mut self) {
        self.table_index = self
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.clone(), t.name.clone()), i))
            .collect();
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
                table.access_method = table_access_method(entry);
                self.table_index.insert(
                    (table.schema.clone(), table.name.clone()),
                    self.tables.len(),
                );
                self.tables.push(*table);
            }
            Statement::CreateTablePartition { parent, partition } => {
                match self.find_table(&parent) {
                    Some(table) => {
                        table
                            .partitions
                            .get_or_insert_default()
                            .push(partition);
                    }
                    None => {
                        self.deferred_partitions.push((parent, partition));
                    }
                }
            }
            Statement::AttachPartition { parent, partition } => {
                self.attached_partitions.push((parent, partition));
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
                view.table_access_method = table_access_method(entry);
                self.matview_index.insert(
                    (view.schema.clone(), view.name.clone()),
                    self.materialized_views.len(),
                );
                self.materialized_views.push(view);
            }
            Statement::CreateProcedure(mut procedure) => {
                procedure.owner = owner;
                self.procedures.push(*procedure);
            }
            Statement::CreateOperator(mut operator) => {
                operator.owner = owner;
                self.operators.push(*operator);
            }
            Statement::AttachIndex { parent, child } => {
                self.attach_index(&parent, &child, entry);
            }
            Statement::CreateAccessMethod(method) => {
                self.access_methods.push(method);
            }
            Statement::CreateOperatorFamily(mut family) => {
                family.owner = owner;
                self.operator_families.push(family);
            }
            Statement::AlterOperatorFamily {
                family,
                method,
                operators,
                functions,
            } => {
                let schema = family.schema.clone().unwrap_or_default();
                match self.operator_families.iter_mut().find(|f| {
                    f.schema == schema
                        && f.name == family.name
                        && f.method == method
                }) {
                    Some(family) => {
                        if !operators.is_empty() {
                            family
                                .operators
                                .get_or_insert_default()
                                .extend(operators);
                        }
                        if !functions.is_empty() {
                            family
                                .functions
                                .get_or_insert_default()
                                .extend(functions);
                        }
                    }
                    None => {
                        log::warn!(
                            "Members of unknown operator family {family}"
                        );
                        self.push_remaining(entry);
                    }
                }
            }
            Statement::CreateOperatorClass(mut class) => {
                class.owner = owner;
                self.operator_classes.push(*class);
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
                let schema = table.schema.clone().unwrap_or_default();
                let location_key = (schema.clone(), index.name.clone());
                if let Some(&idx) =
                    self.table_index.get(&(schema.clone(), table.name.clone()))
                {
                    self.tables[idx]
                        .indexes
                        .get_or_insert_default()
                        .push(index);
                    self.index_location
                        .insert(location_key, IndexLocation::Table(idx));
                } else if let Some(&idx) =
                    self.matview_index.get(&(schema, table.name.clone()))
                {
                    self.materialized_views[idx]
                        .indexes
                        .get_or_insert_default()
                        .push(index);
                    self.index_location.insert(
                        location_key,
                        IndexLocation::MaterializedView(idx),
                    );
                } else {
                    // the target relation may simply not be ingested
                    // yet (matview indexes sort before their matview);
                    // retry after the entry loop
                    self.index_location.insert(
                        location_key,
                        IndexLocation::Deferred(self.deferred_indexes.len()),
                    );
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
            Statement::SetColumnAttribute {
                table,
                column,
                attribute,
            } => {
                let found = self.find_table(&table).and_then(|t| {
                    t.columns.iter_mut().flatten().find(|c| c.name == column)
                });
                match found {
                    Some(c) => match attribute {
                        ddl::ColumnAttribute::Storage(v) => {
                            c.storage = Some(v)
                        }
                        ddl::ColumnAttribute::Compression(v) => {
                            c.compression = Some(v);
                        }
                        ddl::ColumnAttribute::Statistics(v) => {
                            c.statistics = Some(v);
                        }
                        ddl::ColumnAttribute::Options(v) => {
                            c.options.get_or_insert_default().extend(v);
                        }
                    },
                    // an inherited column has no entry of its own to
                    // hold the attribute, so the entry is kept
                    None => {
                        log::warn!(
                            "Column attribute on unknown column \
                             {table}.{column}"
                        );
                        self.push_remaining(entry);
                    }
                }
            }
            Statement::SetColumnDefault {
                table,
                column,
                default,
            } => match self.find_table(&table) {
                Some(t) => set_column_default(t, &column, default),
                None => {
                    self.deferred_defaults.push((table, column, default));
                }
            },
            Statement::AddIdentity {
                table,
                column,
                generated,
            } => {
                let found = self.find_table(&table).and_then(|t| {
                    t.columns.iter_mut().flatten().find(|c| c.name == column)
                });
                match found {
                    Some(c) => c.generated = Some(generated),
                    // pg_dump writes the identity after its table, so a
                    // miss means something upstream went wrong. Keep the
                    // entry rather than drop the identity, so the pull
                    // fails instead of losing it silently.
                    None => {
                        log::warn!(
                            "Identity on unknown column {table}.{column}"
                        );
                        self.push_remaining(entry);
                    }
                }
            }
            Statement::CreateTrigger { table, trigger } => {
                match self.find_table(&table) {
                    Some(table) => {
                        table.triggers.get_or_insert_default().push(trigger);
                    }
                    None => log::warn!("Trigger on unknown table {table}"),
                }
            }
            Statement::CreateAggregate(mut aggregate) => {
                aggregate.owner = owner;
                self.aggregates.push(*aggregate);
            }
            // a cast has no owner of its own
            Statement::CreateCast(cast) => self.casts.push(cast),
            Statement::CreateCollation(mut collation) => {
                collation.owner = owner;
                self.collations.push(collation);
            }
            Statement::CreateLanguage(language) => {
                self.languages.push(language);
            }
            Statement::CreateConversion(mut conversion) => {
                conversion.owner = owner;
                self.conversions.push(conversion);
            }
            Statement::CreateEventTrigger(trigger) => {
                self.event_triggers.push(trigger);
            }
            Statement::AlterEventTrigger { name, enabled } => {
                match self.event_triggers.iter_mut().find(|t| t.name == name) {
                    Some(trigger) => trigger.enabled = enabled,
                    None => {
                        log::warn!("State of unknown event trigger {name}");
                        self.push_remaining(entry);
                    }
                }
            }
            Statement::CreatePublication(publication) => {
                self.publications.push(publication);
            }
            Statement::AddToPublication {
                name,
                tables,
                schemas,
            } => match self.publications.iter_mut().find(|p| p.name == name) {
                Some(publication) => {
                    if !tables.is_empty() {
                        publication
                            .tables
                            .get_or_insert_default()
                            .extend(tables);
                    }
                    if !schemas.is_empty() {
                        publication
                            .schemas
                            .get_or_insert_default()
                            .extend(schemas);
                    }
                }
                None => {
                    log::warn!("Tables added to unknown publication {name}");
                    self.push_remaining(entry);
                }
            },
            Statement::CreateTextSearch { schema, object } => {
                self.add_text_search(schema, object);
            }
            Statement::AddTextSearchMapping {
                configuration,
                tokens,
                dictionaries,
            } => {
                let schema = configuration.schema.clone().unwrap_or_default();
                let found = self
                    .text_search
                    .iter_mut()
                    .find(|t| t.schema == schema)
                    .and_then(|t| {
                        t.configurations
                            .iter_mut()
                            .flatten()
                            .find(|c| c.name == configuration.name)
                    });
                match found {
                    Some(config) => {
                        let mappings = config.mappings.get_or_insert_default();
                        for token in tokens {
                            mappings.insert(token, dictionaries.clone());
                        }
                    }
                    None => {
                        log::warn!(
                            "Mapping for unknown text search configuration \
                             {configuration}"
                        );
                        self.push_remaining(entry);
                    }
                }
            }
            Statement::DefaultPrivileges {
                roles,
                schemas,
                revoke,
                object_type,
                privileges,
                grantees,
                with_grant_option,
            } => {
                // pg_dump always names the role; the entry's owner is
                // the same role, for a statement that does not
                let roles = if roles.is_empty() { vec![owner] } else { roles };
                let schemas: Vec<Option<String>> = if schemas.is_empty() {
                    vec![None]
                } else {
                    schemas.into_iter().map(Some).collect()
                };
                for role in roles {
                    let index = match self
                        .default_privileges
                        .iter()
                        .position(|d| d.name == role)
                    {
                        Some(index) => index,
                        None => {
                            self.default_privileges.push(
                                models::DefaultPrivileges {
                                    name: role.clone(),
                                    grants: None,
                                    revocations: None,
                                },
                            );
                            self.default_privileges.len() - 1
                        }
                    };
                    let defaults = &mut self.default_privileges[index];
                    let list = if revoke {
                        defaults.revocations.get_or_insert_default()
                    } else {
                        defaults.grants.get_or_insert_default()
                    };
                    for schema in &schemas {
                        for grantee in &grantees {
                            list.push(models::DefaultPrivilege {
                                schema: schema.clone(),
                                object_type: object_type.clone(),
                                grantee: grantee.clone(),
                                privileges: privileges.clone(),
                                with_grant_option: with_grant_option
                                    .then_some(true),
                            });
                        }
                    }
                }
            }
            Statement::CreateStatistics(mut statistics) => {
                statistics.owner = owner;
                self.statistics.push(statistics);
            }
            Statement::AlterStatistics { name, target } => {
                let schema = name.schema.clone().unwrap_or_default();
                match self
                    .statistics
                    .iter_mut()
                    .find(|s| s.schema == schema && s.name == name.name)
                {
                    Some(statistics) => statistics.target = Some(target),
                    None => {
                        log::warn!("Target of unknown statistics {name}");
                        self.push_remaining(entry);
                    }
                }
            }
            Statement::CreateRule { relation, rule } => {
                self.add_rule(relation, rule, entry);
            }
            Statement::RuleState {
                relation,
                name,
                enabled,
            } => {
                let found = match self.rules_of(&relation) {
                    Some(rules) => rules.iter_mut().find(|r| r.name == name),
                    None => None,
                };
                match found {
                    Some(rule) => rule.enabled = enabled,
                    None => {
                        log::warn!("State of unknown rule {relation} {name}");
                        self.push_remaining(entry);
                    }
                }
            }
            Statement::CreatePolicy { table, policy } => {
                match self.find_table(&table) {
                    Some(table) => {
                        table.policies.get_or_insert_default().push(policy);
                    }
                    // a policy dropped here would leave the rebuilt table
                    // more open than the source, so keep the entry and
                    // let the pull fail
                    None => {
                        log::warn!("Policy on unknown table {table}");
                        self.push_remaining(entry);
                    }
                }
            }
            Statement::ReplicaIdentity { table, identity } => {
                match self.find_table(&table) {
                    Some(table) => table.replica_identity = identity,
                    None => {
                        log::warn!(
                            "Replica identity of unknown table {table}"
                        );
                        self.push_remaining(entry);
                    }
                }
            }
            Statement::RowSecurity {
                table,
                enabled,
                forced,
            } => match self.find_table(&table) {
                Some(table) => {
                    let state =
                        table.row_level_security.get_or_insert_default();
                    if let Some(enabled) = enabled {
                        state.enabled = enabled;
                    }
                    if let Some(forced) = forced {
                        state.forced = forced.then_some(true);
                    }
                }
                None => {
                    log::warn!("Row security on unknown table {table}");
                    self.push_remaining(entry);
                }
            },
            Statement::Comment {
                on,
                target,
                comment,
            } => {
                // a comment with nowhere to go would be lost from the
                // project, so its entry is kept and the pull fails
                if !self.apply_comment(&on, &target, comment) {
                    self.push_remaining(entry);
                }
            }
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
                log::warn!("Cannot model {}: {kind}", entry_label(entry));
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
                options,
            } => {
                for member in &members {
                    // an explicit INHERIT matching what the member role
                    // does anyway is what pg_dumpall writes for every
                    // membership from PostgreSQL 16 on, and says
                    // nothing; for a NOINHERIT member it is the only
                    // thing making the membership inherit, so it stays
                    let member_inherits =
                        self.role(member).options.inherit != Some(false);
                    let state = self.role(member);
                    let maps = if revoke {
                        &mut state.revocations
                    } else {
                        &mut state.grants
                    };
                    for role in &roles {
                        // a REVOKE removes the membership itself, so
                        // its options would only be noise there
                        let membership = if revoke {
                            models::Membership::Name(role.clone())
                        } else {
                            options.membership(role, member_inherits)
                        };
                        if !maps
                            .roles
                            .iter()
                            .any(|existing| existing.role() == role)
                        {
                            maps.roles.push(membership);
                        }
                    }
                }
            }
            Statement::Comment {
                on,
                target,
                comment,
            } if on == "ROLE" => {
                self.role(&target.name).comment = Some(comment);
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

    /// Set the comment on the object it names; `false` when no modeled
    /// object has that name
    fn apply_comment(
        &mut self,
        on: &str,
        target: &QualifiedName,
        comment: String,
    ) -> bool {
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
            "LANGUAGE" => self
                .languages
                .iter_mut()
                .find(|l| l.name == *name)
                .map(|l| l.comment = Some(comment.clone()))
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
            "PROCEDURE" => {
                self.apply_procedure_comment(&schema, name, &comment)
            }
            "OPERATOR" => self
                .operators
                .iter_mut()
                .find(|o| {
                    o.schema == schema
                        && format!(
                            "{}({}, {})",
                            o.name,
                            o.left_arg.as_deref().unwrap_or("NONE"),
                            o.right_arg.as_deref().unwrap_or("NONE")
                        ) == *name
                })
                .map(|o| o.comment = Some(comment.clone()))
                .is_some(),
            // named `name USING method`
            "OPERATOR FAMILY" => self
                .operator_families
                .iter_mut()
                .find(|f| {
                    f.schema == schema
                        && format!("{} USING {}", f.name, f.method) == *name
                })
                .map(|f| f.comment = Some(comment.clone()))
                .is_some(),
            "OPERATOR CLASS" => self
                .operator_classes
                .iter_mut()
                .find(|c| {
                    c.schema == schema
                        && format!("{} USING {}", c.name, c.method) == *name
                })
                .map(|c| c.comment = Some(comment.clone()))
                .is_some(),
            "ACCESS METHOD" => self
                .access_methods
                .iter_mut()
                .find(|m| m.name == *name)
                .map(|m| m.comment = Some(comment.clone()))
                .is_some(),
            "TRIGGER" => self.apply_trigger_comment(target, &comment),
            "POLICY" => self.apply_policy_comment(target, &comment),
            "RULE" => self.apply_rule_comment(target, &comment),
            "CONSTRAINT" => self.apply_constraint_comment(target, &comment),
            "AGGREGATE" => self
                .aggregates
                .iter_mut()
                .find(|a| {
                    a.schema == schema
                        && format!("{}{}", a.name, aggregate_signature(a))
                            == *name
                })
                .map(|a| a.comment = Some(comment.clone()))
                .is_some(),
            "CAST" => self
                .casts
                .iter_mut()
                .find(|c| {
                    models::Definition::Cast((*c).clone()).name() == *name
                })
                .map(|c| c.comment = Some(comment.clone()))
                .is_some(),
            "COLLATION" => self
                .collations
                .iter_mut()
                .find(|c| c.schema == schema && c.name == *name)
                .map(|c| c.comment = Some(comment.clone()))
                .is_some(),
            "STATISTICS" => self
                .statistics
                .iter_mut()
                .find(|s| s.schema == schema && s.name == *name)
                .map(|s| s.comment = Some(comment.clone()))
                .is_some(),
            "CONVERSION" => self
                .conversions
                .iter_mut()
                .find(|c| c.schema == schema && c.name == *name)
                .map(|c| c.comment = Some(comment.clone()))
                .is_some(),
            "EVENT TRIGGER" => self
                .event_triggers
                .iter_mut()
                .find(|t| t.name == *name)
                .map(|t| t.comment = Some(comment.clone()))
                .is_some(),
            "PUBLICATION" => self
                .publications
                .iter_mut()
                .find(|p| p.name == *name)
                .map(|p| p.comment = Some(comment.clone()))
                .is_some(),
            kind if kind.starts_with("TEXT SEARCH ") => {
                let kind = kind.trim_start_matches("TEXT SEARCH ");
                self.apply_text_search_comment(kind, &schema, name, &comment)
            }
            "INDEX" => {
                match self.index_location.get(&(schema, name.clone())) {
                    Some(&IndexLocation::Table(idx)) => self.tables[idx]
                        .indexes
                        .iter_mut()
                        .flatten()
                        .find(|i| i.name == *name)
                        .map(|i| i.comment = Some(comment.clone()))
                        .is_some(),
                    Some(&IndexLocation::MaterializedView(idx)) => self
                        .materialized_views[idx]
                        .indexes
                        .iter_mut()
                        .flatten()
                        .find(|i| i.name == *name)
                        .map(|i| i.comment = Some(comment.clone()))
                        .is_some(),
                    Some(&IndexLocation::Deferred(idx)) => self
                        .deferred_indexes
                        .get_mut(idx)
                        .map(|(_, i)| i.comment = Some(comment.clone()))
                        .is_some(),
                    None => false,
                }
            }
            _ => false,
        };
        if !found {
            log::warn!("Comment on unmatched object: {on} {target}");
        }
        found
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

    /// As [`Self::apply_function_comment`], for a procedure. pg_dump
    /// writes a procedure's IN modes, which the identity signature
    /// leaves out, as it does for a function.
    fn apply_procedure_comment(
        &mut self,
        schema: &str,
        name: &str,
        comment: &str,
    ) -> bool {
        let signature = name.replace("(IN ", "(").replace(", IN ", ", ");
        if let Some(procedure) = self
            .procedures
            .iter_mut()
            .find(|p| p.schema == schema && p.identity() == signature)
        {
            procedure.comment = Some(comment.to_string());
            return true;
        }
        let base = name.split('(').next().unwrap_or(name);
        let mut candidates = self
            .procedures
            .iter_mut()
            .filter(|p| p.schema == schema && p.name == base);
        let first = candidates.next();
        if candidates.next().is_some() {
            return false;
        }
        first
            .map(|p| p.comment = Some(comment.to_string()))
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

    /// `COMMENT ON TRIGGER trg ON schema.table` — the ddl layer puts
    /// the owning table's qualified name into `target.schema` (mirrors
    /// `apply_column_comment`'s two-name COMMENT shape)
    fn apply_trigger_comment(
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
        let Some(trigger) = table
            .triggers
            .iter_mut()
            .flatten()
            .find(|t| t.name.as_deref() == Some(target.name.as_str()))
        else {
            return false;
        };
        trigger.comment = Some(comment.to_string());
        true
    }

    /// A rule belongs to its table or view. A view's `_RETURN` rule is
    /// the view's query: pg_dump writes a view that way when its
    /// query depends on something that depends on the view, as a
    /// placeholder view and then the rule, so the rule's SELECT
    /// replaces the placeholder query and is never kept as a rule.
    fn add_rule(
        &mut self,
        relation: QualifiedName,
        rule: models::Rule,
        entry: &libpgdump::Entry,
    ) {
        let schema = relation.schema.clone().unwrap_or_default();
        if rule.name == "_RETURN" && rule.event == "SELECT" {
            let view = self
                .views
                .iter_mut()
                .find(|v| v.schema == schema && v.name == relation.name);
            match (view, rule.commands.as_deref()) {
                (Some(view), Some([query])) => {
                    view.query = Some(query.clone());
                }
                _ => {
                    log::warn!("Cannot model _RETURN rule on {relation}");
                    self.push_remaining(entry);
                }
            }
            return;
        }
        match self.rules_of(&relation) {
            Some(rules) => rules.push(rule),
            None => {
                log::warn!("Rule on unknown relation {relation}");
                self.push_remaining(entry);
            }
        }
    }

    /// The rule list of a table or view, created if absent
    fn rules_of(
        &mut self,
        relation: &QualifiedName,
    ) -> Option<&mut Vec<models::Rule>> {
        let schema = relation.schema.clone().unwrap_or_default();
        if self.find_table(relation).is_some() {
            return self
                .find_table(relation)
                .map(|t| t.rules.get_or_insert_default());
        }
        self.views
            .iter_mut()
            .find(|v| v.schema == schema && v.name == relation.name)
            .map(|v| v.rules.get_or_insert_default())
    }

    /// File a text search object under its schema's container
    fn add_text_search(
        &mut self,
        schema: String,
        object: ddl::TextSearchObject,
    ) {
        let index =
            match self.text_search.iter().position(|t| t.schema == schema) {
                Some(index) => index,
                None => {
                    self.text_search.push(models::TextSearch {
                        schema,
                        sql: None,
                        configurations: None,
                        dictionaries: None,
                        parsers: None,
                        templates: None,
                    });
                    self.text_search.len() - 1
                }
            };
        let container = &mut self.text_search[index];
        match object {
            ddl::TextSearchObject::Configuration(o) => {
                container.configurations.get_or_insert_default().push(o);
            }
            ddl::TextSearchObject::Dictionary(o) => {
                container.dictionaries.get_or_insert_default().push(o);
            }
            ddl::TextSearchObject::Parser(o) => {
                container.parsers.get_or_insert_default().push(o);
            }
            ddl::TextSearchObject::Template(o) => {
                container.templates.get_or_insert_default().push(o);
            }
        }
    }

    /// Set the comment of a text search object, which pull files under
    /// its schema's container
    fn apply_text_search_comment(
        &mut self,
        kind: &str,
        schema: &str,
        name: &str,
        comment: &str,
    ) -> bool {
        let Some(container) =
            self.text_search.iter_mut().find(|t| t.schema == schema)
        else {
            return false;
        };
        let slot = match kind {
            "CONFIGURATION" => container
                .configurations
                .iter_mut()
                .flatten()
                .find(|o| o.name == name)
                .map(|o| &mut o.comment),
            "DICTIONARY" => container
                .dictionaries
                .iter_mut()
                .flatten()
                .find(|o| o.name == name)
                .map(|o| &mut o.comment),
            "PARSER" => container
                .parsers
                .iter_mut()
                .flatten()
                .find(|o| o.name == name)
                .map(|o| &mut o.comment),
            "TEMPLATE" => container
                .templates
                .iter_mut()
                .flatten()
                .find(|o| o.name == name)
                .map(|o| &mut o.comment),
            _ => None,
        };
        slot.map(|slot| *slot = Some(comment.to_string())).is_some()
    }

    /// `COMMENT ON CONSTRAINT c ON schema.table`, the same two-name
    /// shape as [`Self::apply_trigger_comment`]. An exclusion
    /// constraint keeps its comment on itself; any other kind's goes
    /// into the table's `constraint_comments`.
    fn apply_constraint_comment(
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
        match table
            .exclude_constraints
            .iter_mut()
            .flatten()
            .find(|c| c.name == target.name)
        {
            Some(exclude) => exclude.comment = Some(comment.to_string()),
            None => {
                table
                    .constraint_comments
                    .get_or_insert_default()
                    .insert(target.name.clone(), comment.to_string());
            }
        }
        true
    }

    /// `COMMENT ON RULE r ON schema.relation`, the same two-name shape
    /// as [`Self::apply_trigger_comment`]
    fn apply_rule_comment(
        &mut self,
        target: &QualifiedName,
        comment: &str,
    ) -> bool {
        let Some(relation) = &target.schema else {
            return false;
        };
        let (schema, name) = match relation.split_once('.') {
            Some((schema, name)) => (Some(schema.to_string()), name),
            None => (None, relation.as_str()),
        };
        let relation = QualifiedName {
            schema,
            name: name.to_string(),
        };
        let Some(rule) = self.rules_of(&relation).and_then(|rules| {
            rules.iter_mut().find(|r| r.name == target.name)
        }) else {
            return false;
        };
        rule.comment = Some(comment.to_string());
        true
    }

    /// `COMMENT ON POLICY p ON schema.table`, the same two-name shape
    /// as [`Self::apply_trigger_comment`]
    fn apply_policy_comment(
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
        let Some(policy) = self.find_table(&relation).and_then(|table| {
            table
                .policies
                .iter_mut()
                .flatten()
                .find(|p| p.name == target.name)
        }) else {
            return false;
        };
        policy.comment = Some(comment.to_string());
        true
    }

    /// Record that the index of a partition belongs to an index of the
    /// partitioned table. pg_dump writes the INDEX ATTACH after both
    /// indexes. The index of a unique, primary key or exclusion
    /// constraint needs nothing: attaching the partition attaches it.
    fn attach_index(
        &mut self,
        parent: &QualifiedName,
        child: &QualifiedName,
        entry: &libpgdump::Entry,
    ) {
        let index_in = |tables: &[models::Table],
                        name: &QualifiedName|
         -> Option<(usize, usize)> {
            let schema = name.schema.as_deref().unwrap_or_default();
            tables.iter().enumerate().find_map(|(t, table)| {
                (table.schema == schema)
                    .then_some(table.indexes.as_deref()?)?
                    .iter()
                    .position(|index| index.name == name.name)
                    .map(|i| (t, i))
            })
        };
        match (
            index_in(&self.tables, child),
            index_in(&self.tables, parent),
        ) {
            (Some((t, i)), Some(_)) => {
                if let Some(indexes) = self.tables[t].indexes.as_mut() {
                    // quoted, so that a dot in a name is not taken
                    // for the one between the schema and the name
                    let name = quote_ident(&parent.name);
                    indexes[i].parent = Some(match &parent.schema {
                        Some(schema) => {
                            format!("{}.{name}", quote_ident(schema))
                        }
                        None => name,
                    });
                }
            }
            // a constraint's index attaches with its partition
            (None, None) => {}
            _ => {
                log::warn!(
                    "Cannot model the attach of index {child} to {parent}"
                );
                self.push_remaining(entry);
            }
        }
    }

    fn find_table(
        &mut self,
        name: &QualifiedName,
    ) -> Option<&mut models::Table> {
        let schema = name.schema.clone().unwrap_or_default();
        let idx = *self.table_index.get(&(schema, name.name.clone()))?;
        self.tables.get_mut(idx)
    }

    fn find_materialized_view(
        &mut self,
        name: &QualifiedName,
    ) -> Option<&mut models::MaterializedView> {
        let schema = name.schema.clone().unwrap_or_default();
        let idx = *self.matview_index.get(&(schema, name.name.clone()))?;
        self.materialized_views.get_mut(idx)
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
                _ => {
                    function.definition = Some(unwrap_body(definition));
                    continue;
                }
            };
            let label = format!("function {}", function.name);
            if let Some(formatted) =
                format_one(definition, plpgsql, &label, style)
            {
                function.definition = Some(formatted);
            }
        }
        // procedure bodies are formatted as function bodies are
        for procedure in &mut self.procedures {
            let Some(definition) = &procedure.definition else {
                continue;
            };
            task.set_message(format!(
                "Formatting procedure {}",
                procedure.name
            ));
            let plpgsql = match procedure.language.as_deref() {
                Some("plpgsql") => true,
                Some("sql") => false,
                _ => {
                    procedure.definition = Some(unwrap_body(definition));
                    continue;
                }
            };
            let label = format!("procedure {}", procedure.name);
            if let Some(formatted) =
                format_one(definition, plpgsql, &label, style)
            {
                procedure.definition = Some(formatted);
            }
        }
        task.finish();
    }

    /// Descriptions of the entries that could not be modeled, in dump
    /// order and deduplicated, for the summary `pull` prints and the
    /// error it fails with
    pub fn unmodeled_descs(&self) -> Vec<String> {
        let mut descs: Vec<String> = Vec::new();
        for entry in &self.remaining {
            if !descs.contains(&entry.desc) {
                descs.push(entry.desc.clone());
            }
        }
        descs
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

/// An aggregate's arguments as pg_dump writes them in `COMMENT ON
/// AGGREGATE`: `(integer)`, `(x integer ORDER BY integer)` or `(*)`.
/// pg_dump uses the identity arguments, which include the argument
/// names, so each argument is its mode, its name and its type.
fn aggregate_signature(aggregate: &models::Aggregate) -> String {
    let types = |args: &[models::Argument]| {
        args.iter()
            .map(|a| {
                a.mode
                    .iter()
                    .cloned()
                    .chain(a.name.as_deref().map(crate::utils::quote_ident))
                    .chain([a.data_type.clone()])
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let direct = types(&aggregate.arguments);
    match aggregate.order_by.as_deref() {
        Some(order_by) if direct.is_empty() => {
            format!("(ORDER BY {})", types(order_by))
        }
        Some(order_by) => format!("({direct} ORDER BY {})", types(order_by)),
        None if direct.is_empty() => String::from("(*)"),
        None => format!("({direct})"),
    }
}

/// `DESC namespace.tag` for an archive entry, for log lines and error
/// messages that have to name the object a user would recognize
fn entry_label(entry: &libpgdump::Entry) -> String {
    let tag = entry.tag.as_deref().unwrap_or("?");
    match entry.namespace.as_deref().filter(|n| !n.is_empty()) {
        Some(namespace) => {
            format!("{} {namespace}.{tag}", entry.desc.as_str())
        }
        None => format!("{} {tag}", entry.desc.as_str()),
    }
}

/// The table access method of a table or materialized view entry.
/// pg_dump does not write it in the DDL: it keeps it in the entry, and
/// pg_restore sets default_table_access_method before the statement.
/// pg_dump records `heap` for every table that uses it, and a project
/// omits the default.
fn table_access_method(entry: &libpgdump::Entry) -> Option<String> {
    entry
        .tableam
        .clone()
        .filter(|method| !method.is_empty() && method != "heap")
}

/// A body in a language that is not formatted, without the newline
/// after its opening `$$` and the one before its closing `$$`: build
/// writes those newlines around every body
fn unwrap_body(body: &str) -> String {
    body.strip_prefix('\n')
        .and_then(|body| body.strip_suffix('\n'))
        .unwrap_or(body)
        .to_string()
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

/// Set a table column's default expression. A column the table does
/// not declare locally is an inherited one — an inheritance child gets
/// its columns from its parents, so pg_dump has no column entry to
/// attach the default to and writes a standalone `ALTER TABLE ONLY`
/// instead; that default is kept in `column_defaults`
fn set_column_default(
    table: &mut models::Table,
    column: &str,
    default: Value,
) {
    match table
        .columns
        .iter_mut()
        .flatten()
        .find(|c| c.name == column)
    {
        Some(c) => c.default = Some(default),
        None => {
            let defaults = table.column_defaults.get_or_insert_default();
            let entry = models::ColumnDefault {
                column: column.to_string(),
                default,
            };
            match defaults.iter_mut().find(|d| d.column == column) {
                Some(existing) => *existing = entry,
                None => defaults.push(entry),
            }
        }
    }
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
    use serde_json::json;

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
    fn unwrap_body_removes_one_newline_at_each_end() {
        assert_eq!(unwrap_body("\nBEGIN\nEND;\n"), "BEGIN\nEND;");
        assert_eq!(unwrap_body("\n\nx\n\n"), "\nx\n");
        assert_eq!(unwrap_body("begin return 1; end"), "begin return 1; end");
        assert_eq!(unwrap_body("\nx"), "\nx");
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
    fn procedure_comments_fall_back_to_the_name() {
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
        add(
            &mut dump,
            OT::Procedure,
            "test",
            "archive(IN \"Days\" integer)",
            "CREATE PROCEDURE test.archive(IN \"Days\" integer) \
             LANGUAGE sql AS $$ SELECT 1 $$;",
        );
        // the quoted parameter name does not match the identity
        // signature, so the unambiguous name match applies
        add(
            &mut dump,
            OT::Comment,
            "test",
            "PROCEDURE archive(IN \"Days\" integer)",
            "COMMENT ON PROCEDURE test.archive(IN \"Days\" integer) \
             IS 'archives rows';",
        );
        let mut assembly = Assembly::default();
        assembly.ingest(&dump).unwrap();
        assert_eq!(assembly.procedures.len(), 1);
        assert_eq!(assembly.procedures[0].identity(), "archive(Days integer)");
        assert_eq!(
            assembly.procedures[0].comment.as_deref(),
            Some("archives rows")
        );
    }

    #[test]
    fn trigger_comments_attach_to_owning_table() {
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
        add(
            &mut dump,
            OT::Table,
            "test",
            "users",
            "CREATE TABLE test.users (id uuid NOT NULL);",
        );
        add(
            &mut dump,
            OT::Function,
            "test",
            "set_last_modified()",
            "CREATE FUNCTION test.set_last_modified() RETURNS trigger \
             LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;",
        );
        add(
            &mut dump,
            OT::Trigger,
            "test",
            "users trg_last_modified",
            "CREATE TRIGGER trg_last_modified BEFORE UPDATE ON test.users \
             FOR EACH ROW EXECUTE FUNCTION test.set_last_modified();",
        );
        add(
            &mut dump,
            OT::Comment,
            "test",
            "TRIGGER trg_last_modified ON users",
            "COMMENT ON TRIGGER trg_last_modified ON test.users IS \
             'keeps last_modified_at fresh';",
        );
        let mut assembly = Assembly::default();
        assembly.ingest(&dump).unwrap();
        let table = assembly
            .tables
            .iter()
            .find(|t| t.name == "users")
            .expect("users table");
        let trigger = table
            .triggers
            .as_ref()
            .unwrap()
            .iter()
            .find(|t| t.name.as_deref() == Some("trg_last_modified"))
            .expect("trigger");
        assert_eq!(
            trigger.comment.as_deref(),
            Some("keeps last_modified_at fresh")
        );
    }

    #[test]
    fn row_security_and_policies_fold_into_their_table() {
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
        // pg_dump writes FORCE inside the TABLE entry itself
        add(
            &mut dump,
            OT::Table,
            "test",
            "notes",
            "CREATE TABLE test.notes (id int);\n\n\
             ALTER TABLE ONLY test.notes FORCE ROW LEVEL SECURITY;",
        );
        add(
            &mut dump,
            OT::Table,
            "test",
            "plain",
            "CREATE TABLE test.plain (id int);",
        );
        add(
            &mut dump,
            OT::RowSecurity,
            "test",
            "notes",
            "ALTER TABLE test.notes ENABLE ROW LEVEL SECURITY;",
        );
        add(
            &mut dump,
            OT::Policy,
            "test",
            "notes own",
            "CREATE POLICY own ON test.notes USING ((id = 1));",
        );
        add(
            &mut dump,
            OT::Comment,
            "test",
            "POLICY own ON notes",
            "COMMENT ON POLICY own ON test.notes IS 'mine';",
        );
        let mut assembly = Assembly::default();
        assembly.ingest(&dump).unwrap();
        assert!(assembly.remaining.is_empty(), "{:?}", assembly.remaining);
        let table = |name: &str| {
            assembly
                .tables
                .iter()
                .find(|t| t.name == name)
                .expect("table")
                .clone()
        };
        let notes = table("notes");
        assert_eq!(
            notes.row_level_security,
            Some(models::RowLevelSecurity {
                enabled: true,
                forced: Some(true),
            })
        );
        let policies = notes.policies.expect("policies");
        assert_eq!(policies[0].using.as_deref(), Some("(id = 1)"));
        assert_eq!(policies[0].comment.as_deref(), Some("mine"));
        // every pulled table states its row security, so deploy manages
        // it rather than reading the absence as unmanaged
        assert_eq!(
            table("plain").row_level_security,
            Some(models::RowLevelSecurity::default())
        );
    }

    #[test]
    fn catalog_objects_fold_their_alters_and_comments() {
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(&mut dump, OT::Schema, "", "s", "CREATE SCHEMA s;");
        let entries = [
            (
                OT::Aggregate,
                "s",
                "total(integer)",
                "CREATE AGGREGATE s.total(integer) (SFUNC = int4pl, \
                 STYPE = integer);",
            ),
            (
                OT::Comment,
                "s",
                "AGGREGATE total(integer)",
                "COMMENT ON AGGREGATE s.total(integer) IS 'sums';",
            ),
            (
                OT::Aggregate,
                "s",
                "named(integer, integer)",
                "CREATE AGGREGATE s.named(\"Weird Name\" integer, \
                 other integer) (SFUNC = s.f, STYPE = integer);",
            ),
            (
                OT::Comment,
                "s",
                "AGGREGATE named(\"Weird Name\" integer, other integer)",
                "COMMENT ON AGGREGATE s.named(\"Weird Name\" integer, \
                 other integer) IS 'named';",
            ),
            (
                OT::Cast,
                "",
                "CAST (s.pair AS text)",
                "CREATE CAST (s.pair AS text) WITH INOUT;",
            ),
            (
                OT::Comment,
                "",
                "CAST (s.pair AS text)",
                "COMMENT ON CAST (s.pair AS text) IS 'as text';",
            ),
            (
                OT::EventTrigger,
                "",
                "et",
                "CREATE EVENT TRIGGER et ON sql_drop EXECUTE FUNCTION f();\n\n\
                 ALTER EVENT TRIGGER et DISABLE;",
            ),
            (
                OT::Publication,
                "",
                "pub",
                "CREATE PUBLICATION pub WITH (publish = 'insert');",
            ),
            (
                OT::PublicationTable,
                "s",
                "pub t",
                "ALTER PUBLICATION pub ADD TABLE ONLY s.t;",
            ),
            (
                OT::PublicationTablesInSchema,
                "s",
                "pub s",
                "ALTER PUBLICATION pub ADD TABLES IN SCHEMA s;",
            ),
            (
                OT::TextSearchConfiguration,
                "s",
                "cfg",
                "CREATE TEXT SEARCH CONFIGURATION s.cfg (\n    \
                 PARSER = pg_catalog.\"default\" );\n\n\
                 ALTER TEXT SEARCH CONFIGURATION s.cfg\n    \
                 ADD MAPPING FOR word WITH simple;",
            ),
        ];
        for (desc, namespace, tag, defn) in entries {
            add(&mut dump, desc, namespace, tag, defn);
        }
        let mut assembly = Assembly::default();
        assembly.ingest(&dump).unwrap();
        assert!(assembly.remaining.is_empty(), "{:?}", assembly.remaining);
        assert_eq!(assembly.aggregates[0].comment.as_deref(), Some("sums"));
        // pg_dump names the arguments in the comment's signature
        assert_eq!(assembly.aggregates[1].comment.as_deref(), Some("named"));
        assert_eq!(assembly.casts[0].comment.as_deref(), Some("as text"));
        assert_eq!(
            assembly.event_triggers[0].enabled.as_deref(),
            Some("DISABLED")
        );
        let publication = &assembly.publications[0];
        assert_eq!(publication.tables.as_ref().unwrap()[0].name(), "s.t");
        assert_eq!(publication.schemas, Some(vec![String::from("s")]));
        let configuration =
            &assembly.text_search[0].configurations.as_ref().unwrap()[0];
        assert_eq!(
            configuration.mappings.as_ref().unwrap()["word"],
            vec![String::from("simple")]
        );
    }

    /// A comment the model has no place for keeps its entry, so the
    /// pull fails instead of losing it
    #[test]
    fn unmatched_comment_is_kept_as_remaining() {
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(&mut dump, OT::Schema, "", "s", "CREATE SCHEMA s;");
        add(&mut dump, OT::Table, "s", "t", "CREATE TABLE s.t (id int);");
        add(
            &mut dump,
            OT::Comment,
            "s",
            "RULE r ON t",
            "COMMENT ON RULE r ON s.t IS 'a rule';",
        );
        let mut assembly = Assembly::default();
        assembly.ingest(&dump).unwrap();
        assert_eq!(assembly.remaining.len(), 1);
        assert_eq!(assembly.remaining[0].desc, "COMMENT");
    }

    #[test]
    fn set_default_attaches_to_existing_column() {
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
        add(
            &mut dump,
            OT::Sequence,
            "test",
            "t_id_seq",
            "CREATE SEQUENCE test.t_id_seq START WITH 1 INCREMENT BY 1 \
             CACHE 1;",
        );
        add(
            &mut dump,
            OT::Table,
            "test",
            "t",
            "CREATE TABLE test.t (id bigint NOT NULL);",
        );
        add(
            &mut dump,
            OT::Default,
            "test",
            "t id",
            "ALTER TABLE ONLY test.t ALTER COLUMN id SET DEFAULT \
             nextval('test.t_id_seq'::regclass);",
        );
        let mut assembly = Assembly::default();
        assembly.ingest(&dump).unwrap();
        let table = assembly
            .tables
            .iter()
            .find(|t| t.name == "t")
            .expect("t table");
        let column = table
            .columns
            .as_ref()
            .unwrap()
            .iter()
            .find(|c| c.name == "id")
            .expect("id column");
        assert_eq!(
            column.default,
            Some(json!("nextval('test.t_id_seq'::regclass)"))
        );
    }

    #[test]
    fn deferred_default_attaches_after_table_is_ingested() {
        // exercise the deferred-default retry path directly: the SET
        // DEFAULT statement arrives before its table has been ingested
        let mut assembly = Assembly::default();
        assembly.deferred_defaults.push((
            QualifiedName {
                schema: Some("test".into()),
                name: "t".into(),
            },
            "id".into(),
            json!("nextval('test.t_id_seq'::regclass)"),
        ));
        assembly.tables.push(models::Table {
            name: "t".into(),
            schema: "test".into(),
            owner: String::new(),
            column_defaults: None,
            sql: None,
            unlogged: None,
            from_type: None,
            parents: None,
            like_table: None,
            columns: Some(vec![models::Column {
                name: "id".into(),
                data_type: "bigint".into(),
                nullable: Some(false),
                not_null_constraint: None,
                default: None,
                collation: None,
                check_constraint: None,
                generated: None,
                storage: None,
                compression: None,
                statistics: None,
                options: None,
                comment: None,
            }]),
            indexes: None,
            primary_key: None,
            check_constraints: None,
            not_null_constraints: None,
            unique_constraints: None,
            foreign_keys: None,
            exclude_constraints: None,
            constraint_comments: None,
            triggers: None,
            rules: None,
            row_level_security: None,
            replica_identity: None,
            policies: None,
            partition: None,
            partitions: None,
            access_method: None,
            storage_parameters: None,
            tablespace: None,
            index_tablespace: None,
            server: None,
            options: None,
            comment: None,
        });
        assembly.table_index.insert(("test".into(), "t".into()), 0);
        assembly.apply_deferred_defaults();
        assert_eq!(
            assembly.tables[0].columns.as_ref().unwrap()[0].default,
            Some(json!("nextval('test.t_id_seq'::regclass)"))
        );
    }

    /// An inheritance child has no column entry for a column it
    /// inherits, so pg_dump's standalone `ALTER TABLE ONLY ... SET
    /// DEFAULT` is kept at the table level instead of being dropped
    #[test]
    fn default_on_an_inherited_column_is_kept() {
        let mut table: models::Table = serde_json::from_value(json!({
            "name": "child",
            "schema": "test",
            "owner": "postgres",
            "parents": ["test.parent"],
        }))
        .unwrap();
        set_column_default(&mut table, "recorded_at", json!("now()"));
        // a second statement for the same column replaces the first
        set_column_default(
            &mut table,
            "recorded_at",
            json!("CURRENT_TIMESTAMP"),
        );
        assert_eq!(
            table.column_defaults,
            Some(vec![models::ColumnDefault {
                column: String::from("recorded_at"),
                default: json!("CURRENT_TIMESTAMP"),
            }])
        );
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
    fn merges_partition_children() {
        // the child entry (events_2024) sorts before its parent
        // (events) in this dump, exercising the deferred-partition
        // retry path the same way deferred indexes are exercised
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
        add(
            &mut dump,
            OT::Table,
            "test",
            "events_2024",
            "CREATE TABLE test.events_2024 PARTITION OF test.events \
             FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');",
        );
        add(
            &mut dump,
            OT::Table,
            "test",
            "events",
            "CREATE TABLE test.events (id bigint, ts timestamp) \
             PARTITION BY RANGE (ts);",
        );
        let mut assembly = Assembly::default();
        assembly.ingest(&dump).unwrap();
        assert_eq!(assembly.tables.len(), 1);
        let events = &assembly.tables[0];
        assert_eq!(events.name, "events");
        let partitions = events.partitions.as_ref().expect("partitions");
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].name, "events_2024");
        assert_eq!(
            partitions[0].for_values_from,
            Some(serde_json::json!("2024-01-01"))
        );
    }

    #[test]
    fn folds_attached_partition_children() {
        // pg_dump's real form: the child is a standalone CREATE TABLE
        // and a separate `TABLE ATTACH` entry carries the bounds. The
        // child must be folded into the parent and dropped as a
        // top-level table.
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
        add(
            &mut dump,
            OT::Table,
            "test",
            "events",
            "CREATE TABLE test.events (id bigint, ts timestamp) \
             PARTITION BY RANGE (ts);",
        );
        add(
            &mut dump,
            OT::Table,
            "test",
            "events_2024",
            "CREATE TABLE test.events_2024 (id bigint, ts timestamp);",
        );
        add(
            &mut dump,
            OT::Comment,
            "test",
            "TABLE events_2024",
            "COMMENT ON TABLE test.events_2024 IS 'The 2024 slice';",
        );
        add(
            &mut dump,
            OT::TableAttach,
            "test",
            "events_2024",
            "ALTER TABLE ONLY test.events ATTACH PARTITION \
             test.events_2024 FOR VALUES FROM ('2024-01-01') \
             TO ('2025-01-01');",
        );
        let mut assembly = Assembly::default();
        assembly.ingest(&dump).unwrap();
        // the child no longer stands alone; only the parent remains
        assert_eq!(assembly.tables.len(), 1);
        let events = &assembly.tables[0];
        assert_eq!(events.name, "events");
        let partitions = events.partitions.as_ref().expect("partitions");
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].name, "events_2024");
        assert_eq!(
            partitions[0].for_values_from,
            Some(serde_json::json!("2024-01-01"))
        );
        assert_eq!(partitions[0].for_values_to, Some(json!("2025-01-01")));
        // a comment on the child carries onto the partition
        assert_eq!(partitions[0].comment.as_deref(), Some("The 2024 slice"));
    }

    #[test]
    fn partition_with_its_own_properties_stays_a_table() {
        // a partition with a replica identity, or anything else of its
        // own, stays a table attached to its parent, where a partition
        // modeled by its bounds alone would lose it
        let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
        add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
        add(
            &mut dump,
            OT::Table,
            "test",
            "events",
            "CREATE TABLE test.events (id bigint, ts timestamp) \
             PARTITION BY RANGE (ts);",
        );
        add(
            &mut dump,
            OT::Table,
            "test",
            "events_2024",
            "CREATE TABLE test.events_2024 (id bigint, ts timestamp);\n\
             ALTER TABLE ONLY test.events_2024 REPLICA IDENTITY FULL;",
        );
        add(
            &mut dump,
            OT::TableAttach,
            "test",
            "events_2024",
            "ALTER TABLE ONLY test.events ATTACH PARTITION \
             test.events_2024 FOR VALUES FROM ('2024-01-01') \
             TO ('2025-01-01');",
        );
        let mut assembly = Assembly::default();
        assembly.ingest(&dump).unwrap();
        assert!(assembly.remaining.is_empty(), "{:?}", assembly.remaining);
        let table = |name: &str| {
            assembly
                .tables
                .iter()
                .find(|t| t.name == name)
                .unwrap_or_else(|| panic!("no table {name}"))
        };
        assert_eq!(
            table("events_2024").replica_identity,
            Some(models::ReplicaIdentity::Mode(String::from("FULL")))
        );
        let partitions = table("events").partitions.as_ref().unwrap();
        assert_eq!(partitions[0].name, "events_2024");
        assert_eq!(partitions[0].attached, Some(true));
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
        assert_eq!(
            app.grants.roles,
            vec![models::Membership::Name(String::from("readonly"))]
        );
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

    /// `COMMENT ON ROLE` in the roles dump is captured on the role's
    /// state (rather than dropped with an "Unexpected statement"
    /// warning) and carried through to both the role and user models
    /// on write
    #[test]
    fn comment_on_role_is_captured_and_written() {
        use clap::Parser;
        let mut assembly = Assembly::default();
        assembly
            .ingest_roles(
                "CREATE ROLE app;\n\
                 ALTER ROLE app WITH LOGIN;\n\
                 COMMENT ON ROLE app IS 'application role';\n\
                 CREATE ROLE readonly;\n\
                 ALTER ROLE readonly WITH NOLOGIN;\n\
                 COMMENT ON ROLE readonly IS 'read-only role';\n",
            )
            .unwrap();
        assert_eq!(
            assembly.roles["app"].comment.as_deref(),
            Some("application role")
        );
        assert_eq!(
            assembly.roles["readonly"].comment.as_deref(),
            Some("read-only role")
        );

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

        let user = files.get(Path::new("users/app.yaml")).unwrap();
        assert!(user.contains("comment: application role"));
        let role = files.get(Path::new("roles/readonly.yaml")).unwrap();
        assert!(role.contains("comment: read-only role"));
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

    /// Role membership grants survive the full pull → write → load →
    /// build path: captured from the pg_dumpall roles dump into the
    /// grantee's `grants: {roles: [...]}`, then rendered back as
    /// `GRANT role TO grantee` ACL entries in the build archive —
    /// including grants on reserved pg_* roles, which are filtered
    /// from the project as files but remain valid membership targets
    #[test]
    fn role_membership_round_trips_through_build() {
        use clap::Parser;
        let mut assembly = Assembly {
            dbname: String::from("memberships"),
            ..Assembly::default()
        };
        assembly
            .ingest_roles(
                "CREATE ROLE developers;\n\
                 ALTER ROLE developers WITH NOLOGIN;\n\
                 CREATE ROLE alice;\n\
                 ALTER ROLE alice WITH LOGIN;\n\
                 GRANT developers TO alice GRANTED BY postgres;\n\
                 GRANT pg_read_all_data TO alice GRANTED BY postgres;\n",
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

        let project = crate::project::load(&dest).unwrap();
        let alice = project
            .inventory
            .iter()
            .find_map(|item| match &item.definition {
                models::Definition::User(user) if user.name == "alice" => {
                    Some(user)
                }
                _ => None,
            })
            .expect("alice user");
        assert_eq!(
            alice.grants.as_ref().and_then(|g| g.roles.clone()),
            Some(vec![
                models::Membership::Name(String::from("developers")),
                models::Membership::Name(String::from("pg_read_all_data")),
            ])
        );

        let archive = dir.path().join("memberships.dump");
        crate::build::build(&project, &archive).unwrap();
        let built = libpgdump::load(&archive).unwrap();
        let grants: Vec<&str> = built
            .entries()
            .iter()
            .filter_map(|e| e.defn.as_deref())
            .filter(|defn| defn.starts_with("GRANT"))
            .collect();
        assert_eq!(
            grants,
            vec![
                "GRANT developers TO alice;\n",
                "GRANT pg_read_all_data TO alice;\n",
            ]
        );
    }
}
