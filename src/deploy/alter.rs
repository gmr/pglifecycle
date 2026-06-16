//! In-place ALTER renderers for objects that exist in both the
//! project and the database but differ. Each resolver returns either
//! the statements that reconcile the database in place or
//! [`Resolution::Replace`], the gated drop+recreate fallback the
//! caller assembles from the build archive's entries.
//!
//! Index, trigger, and constraint drops are not gated: they lose no
//! data and the repo is authoritative. Only data-destructive
//! statements (DROP COLUMN, ALTER COLUMN TYPE) are.

use serde_json::{Map, Value};

use crate::build;
use crate::deploy::diff::canonical_type;
use crate::models::{
    CheckConstraint, Column, Definition, Domain, Extension,
    ForeignDataWrapper, ForeignKey, Index, Schema, Sequence, Server, Table,
    Trigger, Type, UserMapping, View, ViewColumn,
};
use crate::utils::{postgres_value, quote_ident, user_mapping_subject};

/// One reconciliation statement
pub(crate) struct Alter {
    pub sql: String,
    pub destructive: bool,
}

impl Alter {
    fn new(sql: String) -> Self {
        Self {
            sql,
            destructive: false,
        }
    }

    fn destructive(sql: String) -> Self {
        Self {
            sql,
            destructive: true,
        }
    }
}

/// The outcome of resolving a changed object
pub(crate) enum Resolution {
    /// Reconcile in place with these statements
    Statements(Vec<Alter>),
    /// Re-issue the object's CREATE as `CREATE OR REPLACE` (functions
    /// and views); non-destructive, the caller rewrites the entry's
    /// leading verb. `CREATE OR REPLACE` keeps the existing comment, so
    /// `comment` carries the `COMMENT ON` statement to run afterward
    /// when it changed (including the `IS NULL` form on removal); `None`
    /// means the comment is unchanged.
    OrReplace { comment: Option<String> },
    /// No in-place form exists (or is implemented yet): drop and
    /// recreate from the repo definition, gated behind --allow-drop
    Replace,
}

/// Resolve a changed object into in-place statements where supported
pub(crate) fn resolve(repo: &Definition, database: &Definition) -> Resolution {
    match (repo, database) {
        (Definition::Table(repo), Definition::Table(db)) => table(repo, db),
        (Definition::Sequence(repo), Definition::Sequence(db)) => {
            sequence(repo, db)
        }
        (Definition::Domain(repo), Definition::Domain(db)) => domain(repo, db),
        (Definition::Type(repo), Definition::Type(db)) => enum_type(repo, db),
        (Definition::Extension(repo), Definition::Extension(db)) => {
            extension(repo, db)
        }
        (Definition::Schema(repo), Definition::Schema(db)) => schema(repo, db),
        // CREATE OR REPLACE handles function bodies and view queries
        // in place; a function whose return type changed cannot be
        // replaced and must be dropped first
        (Definition::Function(repo), Definition::Function(db)) => {
            if repo.returns == db.returns {
                // function names carry their full identity signature,
                // so COMMENT ON FUNCTION takes the name verbatim
                let target =
                    format!("{}.{}", quote_ident(&repo.schema), repo.name);
                Resolution::OrReplace {
                    comment: comment_delta(
                        "FUNCTION",
                        &target,
                        &repo.comment,
                        &db.comment,
                    ),
                }
            } else {
                Resolution::Replace
            }
        }
        (Definition::View(repo), Definition::View(db)) => view(repo, db),
        (
            Definition::ForeignDataWrapper(repo),
            Definition::ForeignDataWrapper(db),
        ) => fdw(repo, db),
        (Definition::Server(repo), Definition::Server(db)) => server(repo, db),
        (Definition::UserMapping(repo), Definition::UserMapping(db)) => {
            user_mapping(repo, db)
        }
        _ => Resolution::Replace,
    }
}

fn qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}

/// CREATE OR REPLACE VIEW only succeeds when the new query's output
/// columns stay compatible with the existing view: same names in the
/// same order, with new columns added only at the end. A rename,
/// reorder, or removal must fall back to the gated drop+recreate path.
/// Column types can also force a drop, but the project model does not
/// carry view column types, so only the name-level guard is applied
/// here.
fn view(repo: &View, db: &View) -> Resolution {
    if view_columns_compatible(repo, db) {
        Resolution::OrReplace {
            comment: comment_delta(
                "VIEW",
                &qualified(&repo.schema, &repo.name),
                &repo.comment,
                &db.comment,
            ),
        }
    } else {
        Resolution::Replace
    }
}

fn view_column_name(column: &ViewColumn) -> &str {
    match column {
        ViewColumn::Name(name) => name,
        ViewColumn::Detailed { name, .. } => name,
    }
}

/// True when `repo`'s columns are the `db` columns optionally followed
/// by additional columns (the only mutation CREATE OR REPLACE VIEW
/// permits). Absent column metadata on either side is treated as
/// unknown, so the caller keeps the existing OR REPLACE behavior
/// rather than forcing an unnecessary drop.
fn view_columns_compatible(repo: &View, db: &View) -> bool {
    let (Some(repo_cols), Some(db_cols)) = (&repo.columns, &db.columns) else {
        return true;
    };
    if repo_cols.len() < db_cols.len() {
        return false;
    }
    repo_cols
        .iter()
        .zip(db_cols.iter())
        .all(|(r, d)| view_column_name(r) == view_column_name(d))
}

fn table(repo: &Table, db: &Table) -> Resolution {
    // foreign tables (a `server` on either side) reconcile through a
    // dedicated path: only OPTIONS and the comment are alterable in
    // place, everything else rebuilds
    if repo.server.is_some() || db.server.is_some() {
        return foreign_table(repo, db);
    }
    // properties only expressible by rebuilding the table
    if repo.sql != db.sql
        || repo.unlogged != db.unlogged
        || repo.from_type != db.from_type
        || repo.parents != db.parents
        || repo.like_table != db.like_table
        || repo.partition != db.partition
        || repo.partitions != db.partitions
        || repo.access_method != db.access_method
        || repo.storage_parameters != db.storage_parameters
        || repo.tablespace != db.tablespace
        || repo.index_tablespace != db.index_tablespace
        || repo.server != db.server
        || repo.options != db.options
    {
        return Resolution::Replace;
    }
    let name = qualified(&repo.schema, &repo.name);
    let mut alters = Vec::new();
    if !columns(&name, repo, db, &mut alters)
        || !constraints(&name, repo, db, &mut alters)
        || !triggers(&name, repo, db, &mut alters)
    {
        return Resolution::Replace;
    }
    indexes(&name, repo, db, &mut alters);
    if repo.comment != db.comment {
        alters.push(Alter::new(comment_on(
            "TABLE",
            &name,
            repo.comment.as_deref(),
        )));
    }
    Resolution::Statements(alters)
}

/// Column reconciliation; returns false where only a rebuild works
/// (reordered columns, collation/generation changes)
fn columns(
    table: &str,
    repo: &Table,
    db: &Table,
    alters: &mut Vec<Alter>,
) -> bool {
    let repo_columns = repo.columns.as_deref().unwrap_or_default();
    let db_columns = db.columns.as_deref().unwrap_or_default();
    // column position cannot be altered in place: the columns present
    // on both sides must appear in the same relative order
    let in_db: Vec<&str> = repo_columns
        .iter()
        .map(|c| c.name.as_str())
        .filter(|name| db_columns.iter().any(|c| c.name == *name))
        .collect();
    let in_repo: Vec<&str> = db_columns
        .iter()
        .map(|c| c.name.as_str())
        .filter(|name| repo_columns.iter().any(|c| c.name == *name))
        .collect();
    if in_db != in_repo {
        return false;
    }
    for column in repo_columns {
        match db_columns.iter().find(|c| c.name == column.name) {
            None => alters.push(Alter::new(format!(
                "ALTER TABLE {table} ADD COLUMN {};\n",
                build::render_table_column(column)
            ))),
            Some(existing) => {
                if !alter_column(table, column, existing, alters) {
                    return false;
                }
            }
        }
    }
    for column in db_columns {
        if !repo_columns.iter().any(|c| c.name == column.name) {
            alters.push(Alter::destructive(format!(
                "ALTER TABLE {table} DROP COLUMN {};\n",
                quote_ident(&column.name)
            )));
        }
    }
    true
}

fn alter_column(
    table: &str,
    repo: &Column,
    db: &Column,
    alters: &mut Vec<Alter>,
) -> bool {
    // collation, generation, and inline check changes require a
    // rebuild
    if repo.collation != db.collation
        || repo.generated != db.generated
        || repo.check_constraint != db.check_constraint
    {
        return false;
    }
    let column = quote_ident(&repo.name);
    if canonical_type(&repo.data_type) != canonical_type(&db.data_type) {
        // a type change may rewrite the table (and can fail outright
        // without a USING clause), so it is gated
        alters.push(Alter::destructive(format!(
            "ALTER TABLE {table} ALTER COLUMN {column} TYPE {};\n",
            repo.data_type
        )));
    }
    if repo.default != db.default {
        alters.push(Alter::new(match &repo.default {
            Some(default) => format!(
                "ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT \
                 {};\n",
                build::render_default(default)
            ),
            None => format!(
                "ALTER TABLE {table} ALTER COLUMN {column} DROP DEFAULT;\n"
            ),
        }));
    }
    if repo.nullable.unwrap_or(true) != db.nullable.unwrap_or(true) {
        alters.push(Alter::new(if repo.nullable == Some(false) {
            format!(
                "ALTER TABLE {table} ALTER COLUMN {column} SET NOT \
                     NULL;\n"
            )
        } else {
            format!(
                "ALTER TABLE {table} ALTER COLUMN {column} DROP NOT \
                     NULL;\n"
            )
        }));
    }
    if repo.comment != db.comment {
        alters.push(Alter::new(comment_on(
            "COLUMN",
            &format!("{table}.{column}"),
            repo.comment.as_deref(),
        )));
    }
    true
}

/// Constraint reconciliation. Check constraints and foreign keys are
/// named in the model and reconcile as DROP/ADD pairs; primary keys
/// and unique constraints are unnamed, so only additions are
/// expressible — removals and changes fall back to a rebuild
fn constraints(
    table: &str,
    repo: &Table,
    db: &Table,
    alters: &mut Vec<Alter>,
) -> bool {
    match (&repo.primary_key, &db.primary_key) {
        (repo_pk, db_pk) if repo_pk == db_pk => {}
        (Some(pk), None) => alters.push(Alter::new(format!(
            "ALTER TABLE {table} ADD {};\n",
            build::render_constraint("PRIMARY KEY", pk)
        ))),
        _ => return false,
    }
    let repo_unique = repo.unique_constraints.as_deref().unwrap_or_default();
    let db_unique = db.unique_constraints.as_deref().unwrap_or_default();
    if db_unique.iter().any(|u| !repo_unique.contains(u)) {
        return false;
    }
    for unique in repo_unique {
        if !db_unique.contains(unique) {
            alters.push(Alter::new(format!(
                "ALTER TABLE {table} ADD {};\n",
                build::render_constraint("UNIQUE", unique)
            )));
        }
    }
    let repo_checks = repo.check_constraints.as_deref().unwrap_or_default();
    let db_checks = db.check_constraints.as_deref().unwrap_or_default();
    named_pairs(
        alters,
        db_checks,
        repo_checks,
        |check: &CheckConstraint| check.name.clone(),
        |check| {
            format!(
                "ALTER TABLE {table} DROP CONSTRAINT {};\n",
                quote_ident(&check.name)
            )
        },
        |check| {
            format!(
                "ALTER TABLE {table} ADD CONSTRAINT {} CHECK ({});\n",
                quote_ident(&check.name),
                check.expression
            )
        },
    );
    let repo_fks = repo.foreign_keys.as_deref().unwrap_or_default();
    let db_fks = db.foreign_keys.as_deref().unwrap_or_default();
    named_pairs(
        alters,
        db_fks,
        repo_fks,
        |fk: &ForeignKey| fk.name.clone(),
        |fk| {
            format!(
                "ALTER TABLE {table} DROP CONSTRAINT {};\n",
                quote_ident(&fk.name)
            )
        },
        |fk| {
            format!(
                "ALTER TABLE {table} ADD CONSTRAINT {} {};\n",
                quote_ident(&fk.name),
                build::render_foreign_key(fk)
            )
        },
    );
    true
}

/// Reconcile named child objects: drop database-side entries that are
/// missing or different in the repo, then add the repo-side entries
/// the database is missing (a changed entry produces both)
fn named_pairs<T: PartialEq>(
    alters: &mut Vec<Alter>,
    database: &[T],
    repo: &[T],
    name: impl Fn(&T) -> String,
    drop: impl Fn(&T) -> String,
    add: impl Fn(&T) -> String,
) {
    for existing in database {
        match repo.iter().find(|t| name(t) == name(existing)) {
            Some(wanted) if wanted == existing => {}
            _ => alters.push(Alter::new(drop(existing))),
        }
    }
    for wanted in repo {
        match database.iter().find(|t| name(t) == name(wanted)) {
            Some(existing) if existing == wanted => {}
            _ => alters.push(Alter::new(add(wanted))),
        }
    }
}

fn indexes(table: &str, repo: &Table, db: &Table, alters: &mut Vec<Alter>) {
    let schema = quote_ident(&repo.schema);
    named_pairs(
        alters,
        db.indexes.as_deref().unwrap_or_default(),
        repo.indexes.as_deref().unwrap_or_default(),
        |index: &Index| index.name.clone(),
        |index| {
            format!(
                "DROP INDEX IF EXISTS {schema}.{};\n",
                quote_ident(&index.name)
            )
        },
        |index| {
            let mut sql =
                format!("{};\n", build::render_index(index, table).join(" "));
            if let Some(comment) = &index.comment {
                let name = format!("{schema}.{}", quote_ident(&index.name));
                sql.push_str(&comment_on("INDEX", &name, Some(comment)));
            }
            sql
        },
    );
}

/// Trigger reconciliation; triggers without names cannot be matched
/// or dropped, so a difference involving one falls back to a rebuild
fn triggers(
    table: &str,
    repo: &Table,
    db: &Table,
    alters: &mut Vec<Alter>,
) -> bool {
    let repo_triggers = repo.triggers.as_deref().unwrap_or_default();
    let db_triggers = db.triggers.as_deref().unwrap_or_default();
    if repo_triggers == db_triggers {
        return true;
    }
    if repo_triggers
        .iter()
        .chain(db_triggers)
        .any(|t| t.name.is_none())
    {
        return false;
    }
    named_pairs(
        alters,
        db_triggers,
        repo_triggers,
        |trigger: &Trigger| trigger.name.clone().unwrap_or_default(),
        |trigger| {
            format!(
                "DROP TRIGGER IF EXISTS {} ON {table};\n",
                quote_ident(trigger.name.as_deref().unwrap_or_default())
            )
        },
        |trigger| {
            let mut sql = format!(
                "{};\n",
                build::render_trigger(trigger, table).0.join(" ")
            );
            if let Some(comment) = &trigger.comment {
                let name = format!(
                    "{} ON {table}",
                    quote_ident(trigger.name.as_deref().unwrap_or_default())
                );
                sql.push_str(&comment_on("TRIGGER", &name, Some(comment)));
            }
            sql
        },
    );
    true
}

/// Sequence reconciliation: a single ALTER SEQUENCE of the changed
/// options, plus a comment delta. Every sequence property is
/// alterable in place, so this never falls back to a rebuild.
fn sequence(repo: &Sequence, db: &Sequence) -> Resolution {
    if repo.sql != db.sql {
        return Resolution::Replace;
    }
    let name = qualified(&repo.schema, &repo.name);
    let mut clauses: Vec<String> = Vec::new();
    if repo.data_type != db.data_type
        && let Some(data_type) = &repo.data_type
    {
        clauses.push(format!("AS {data_type}"));
    }
    if repo.increment_by != db.increment_by
        && let Some(increment) = repo.increment_by
    {
        clauses.push(format!("INCREMENT BY {increment}"));
    }
    if repo.min_value != db.min_value {
        clauses.push(match repo.min_value {
            Some(min) => format!("MINVALUE {min}"),
            None => "NO MINVALUE".into(),
        });
    }
    if repo.max_value != db.max_value {
        clauses.push(match repo.max_value {
            Some(max) => format!("MAXVALUE {max}"),
            None => "NO MAXVALUE".into(),
        });
    }
    if repo.start_with != db.start_with
        && let Some(start) = repo.start_with
    {
        clauses.push(format!("START WITH {start}"));
    }
    if repo.cache != db.cache
        && let Some(cache) = repo.cache
    {
        clauses.push(format!("CACHE {cache}"));
    }
    if repo.cycle != db.cycle {
        clauses.push(if repo.cycle == Some(true) {
            "CYCLE".into()
        } else {
            "NO CYCLE".into()
        });
    }
    if repo.owned_by != db.owned_by {
        clauses.push(match &repo.owned_by {
            Some(owner) => format!("OWNED BY {owner}"),
            None => "OWNED BY NONE".into(),
        });
    }
    let mut alters = Vec::new();
    if !clauses.is_empty() {
        alters.push(Alter::new(format!(
            "ALTER SEQUENCE {name} {};\n",
            clauses.join(" ")
        )));
    }
    if repo.comment != db.comment {
        alters.push(Alter::new(comment_on(
            "SEQUENCE",
            &name,
            repo.comment.as_deref(),
        )));
    }
    Resolution::Statements(alters)
}

/// Domain reconciliation: SET/DROP DEFAULT and a comment delta in
/// place. A base-type, collation, or constraint change rebuilds (the
/// domain's constraints are not all individually named).
fn domain(repo: &Domain, db: &Domain) -> Resolution {
    if repo.sql != db.sql
        || repo.data_type != db.data_type
        || repo.collation != db.collation
        || repo.check_constraints != db.check_constraints
    {
        return Resolution::Replace;
    }
    let name = qualified(&repo.schema, &repo.name);
    let mut alters = Vec::new();
    if repo.default != db.default {
        alters.push(Alter::new(match &repo.default {
            Some(default) => {
                format!("ALTER DOMAIN {name} SET DEFAULT {default};\n")
            }
            None => format!("ALTER DOMAIN {name} DROP DEFAULT;\n"),
        }));
    }
    if repo.comment != db.comment {
        alters.push(Alter::new(comment_on(
            "DOMAIN",
            &name,
            repo.comment.as_deref(),
        )));
    }
    Resolution::Statements(alters)
}

/// Enum reconciliation: append-only value additions via ALTER TYPE
/// ADD VALUE. Any other change (reordering, insertion, removal, or a
/// non-enum type kind) rebuilds.
fn enum_type(repo: &Type, db: &Type) -> Resolution {
    let (Some(repo_values), Some(db_values)) =
        (&repo.enum_values, &db.enum_values)
    else {
        return Resolution::Replace;
    };
    // the database values must be an unchanged prefix of the repo's;
    // only trailing additions can be expressed with ADD VALUE
    if !repo_values.starts_with(db_values) {
        return Resolution::Replace;
    }
    let name = qualified(&repo.schema, &repo.name);
    let mut alters: Vec<Alter> = repo_values[db_values.len()..]
        .iter()
        .map(|value| {
            let escaped = value.replace('\'', "''");
            Alter::new(format!("ALTER TYPE {name} ADD VALUE '{escaped}';\n"))
        })
        .collect();
    if repo.comment != db.comment {
        alters.push(Alter::new(comment_on(
            "TYPE",
            &name,
            repo.comment.as_deref(),
        )));
    }
    Resolution::Statements(alters)
}

/// Extension reconciliation: ALTER EXTENSION ... UPDATE/SET SCHEMA.
fn extension(repo: &Extension, db: &Extension) -> Resolution {
    let name = quote_ident(&repo.name);
    let mut alters = Vec::new();
    if repo.version != db.version
        && let Some(version) = &repo.version
    {
        alters.push(Alter::new(format!(
            "ALTER EXTENSION {name} UPDATE TO '{version}';\n"
        )));
    }
    if repo.schema != db.schema
        && let Some(schema) = &repo.schema
    {
        alters.push(Alter::new(format!(
            "ALTER EXTENSION {name} SET SCHEMA {};\n",
            quote_ident(schema)
        )));
    }
    if repo.comment != db.comment {
        alters.push(Alter::new(comment_on(
            "EXTENSION",
            &name,
            repo.comment.as_deref(),
        )));
    }
    Resolution::Statements(alters)
}

/// Schema reconciliation: only a comment delta is expressible in
/// place; an owner/authorization change rebuilds.
fn schema(repo: &Schema, db: &Schema) -> Resolution {
    if repo.authorization != db.authorization {
        return Resolution::Replace;
    }
    let mut alters = Vec::new();
    if repo.comment != db.comment {
        alters.push(Alter::new(comment_on(
            "SCHEMA",
            &quote_ident(&repo.name),
            repo.comment.as_deref(),
        )));
    }
    Resolution::Statements(alters)
}

/// Foreign-table reconciliation: only OPTIONS and the comment are
/// alterable in place. A different server or any column/structural
/// change rebuilds (gated behind --allow-drop). Foreign-table comments
/// use the FOREIGN TABLE object type, matching the build.
fn foreign_table(repo: &Table, db: &Table) -> Resolution {
    if repo.server != db.server
        || repo.columns != db.columns
        || repo.sql != db.sql
        || repo.check_constraints != db.check_constraints
    {
        return Resolution::Replace;
    }
    let name = qualified(&repo.schema, &repo.name);
    let mut alters = Vec::new();
    if let Some(clause) = options_delta(&repo.options, &db.options) {
        alters.push(Alter::new(format!(
            "ALTER FOREIGN TABLE {name} OPTIONS ({clause});\n"
        )));
    }
    if repo.comment != db.comment {
        alters.push(Alter::new(comment_on(
            "FOREIGN TABLE",
            &name,
            repo.comment.as_deref(),
        )));
    }
    Resolution::Statements(alters)
}

/// Foreign-data-wrapper reconciliation: handler, validator, OPTIONS,
/// and comment are all alterable in place.
fn fdw(repo: &ForeignDataWrapper, db: &ForeignDataWrapper) -> Resolution {
    let name = quote_ident(&repo.name);
    let mut alters = Vec::new();
    if repo.handler != db.handler {
        alters.push(Alter::new(match &repo.handler {
            Some(handler) => format!(
                "ALTER FOREIGN DATA WRAPPER {name} HANDLER {handler};\n"
            ),
            None => {
                format!("ALTER FOREIGN DATA WRAPPER {name} NO HANDLER;\n")
            }
        }));
    }
    if repo.validator != db.validator {
        alters.push(Alter::new(match &repo.validator {
            Some(validator) => format!(
                "ALTER FOREIGN DATA WRAPPER {name} VALIDATOR {validator};\n"
            ),
            None => {
                format!("ALTER FOREIGN DATA WRAPPER {name} NO VALIDATOR;\n")
            }
        }));
    }
    if let Some(clause) = options_delta(&repo.options, &db.options) {
        alters.push(Alter::new(format!(
            "ALTER FOREIGN DATA WRAPPER {name} OPTIONS ({clause});\n"
        )));
    }
    if repo.comment != db.comment {
        alters.push(Alter::new(comment_on(
            "FOREIGN DATA WRAPPER",
            &name,
            repo.comment.as_deref(),
        )));
    }
    Resolution::Statements(alters)
}

/// Server reconciliation: VERSION, OPTIONS, and comment alter in place.
/// The owning foreign-data wrapper and the server TYPE cannot be
/// altered, and VERSION cannot be cleared, so those changes rebuild.
fn server(repo: &Server, db: &Server) -> Resolution {
    if repo.foreign_data_wrapper != db.foreign_data_wrapper
        || repo.server_type != db.server_type
        || (repo.version.is_none() && db.version.is_some())
    {
        return Resolution::Replace;
    }
    let name = quote_ident(&repo.name);
    let mut alters = Vec::new();
    if repo.version != db.version
        && let Some(version) = &repo.version
    {
        alters.push(Alter::new(format!(
            "ALTER SERVER {name} VERSION {};\n",
            string_literal(version)
        )));
    }
    if let Some(clause) = options_delta(&repo.options, &db.options) {
        alters.push(Alter::new(format!(
            "ALTER SERVER {name} OPTIONS ({clause});\n"
        )));
    }
    if repo.comment != db.comment {
        alters.push(Alter::new(comment_on(
            "SERVER",
            &name,
            repo.comment.as_deref(),
        )));
    }
    Resolution::Statements(alters)
}

/// User-mapping reconciliation: each (user, server) mapping is a
/// distinct database object, so a mapping the repo adds is created, one
/// it drops is dropped, and a shared one's OPTIONS are altered in place.
fn user_mapping(repo: &UserMapping, db: &UserMapping) -> Resolution {
    let user = user_mapping_subject(&repo.name);
    let mut alters = Vec::new();
    for server in &repo.servers {
        let name = quote_ident(&server.name);
        match db.servers.iter().find(|s| s.name == server.name) {
            None => {
                let mut sql =
                    format!("CREATE USER MAPPING FOR {user} SERVER {name}");
                if let Some(clause) = options_clause(&server.options) {
                    sql.push_str(&format!(" OPTIONS ({clause})"));
                }
                sql.push_str(";\n");
                alters.push(Alter::new(sql));
            }
            Some(existing) => {
                // a redacted pull omits the password, so a project that
                // does not carry one must not drop the live credential
                let db_options =
                    keep_redacted_password(&server.options, &existing.options);
                if let Some(clause) =
                    options_delta(&server.options, &db_options)
                {
                    alters.push(Alter::new(format!(
                        "ALTER USER MAPPING FOR {user} SERVER {name} \
                         OPTIONS ({clause});\n"
                    )));
                }
            }
        }
    }
    for server in &db.servers {
        if !repo.servers.iter().any(|s| s.name == server.name) {
            alters.push(Alter::new(format!(
                "DROP USER MAPPING IF EXISTS FOR {user} SERVER {};\n",
                quote_ident(&server.name)
            )));
        }
    }
    Resolution::Statements(alters)
}

/// An `OPTIONS (...)` body reconciling `repo` against `db`: `ADD` for
/// keys only in the repo, `SET` for changed values, `DROP` for keys
/// only in the database. `None` when the option sets are equal. The
/// option set is not data, so a removed option is not gated.
fn options_delta(
    repo: &Option<Map<String, Value>>,
    db: &Option<Map<String, Value>>,
) -> Option<String> {
    let empty = Map::new();
    let repo = repo.as_ref().unwrap_or(&empty);
    let db = db.as_ref().unwrap_or(&empty);
    if repo == db {
        return None;
    }
    let mut parts = Vec::new();
    for (key, value) in repo {
        match db.get(key) {
            None => parts.push(format!("ADD {key} {}", postgres_value(value))),
            Some(existing) if existing != value => {
                parts.push(format!("SET {key} {}", postgres_value(value)))
            }
            _ => {}
        }
    }
    for key in db.keys() {
        if !repo.contains_key(key) {
            parts.push(format!("DROP {key}"));
        }
    }
    (!parts.is_empty()).then(|| parts.join(", "))
}

/// The database options with a `password` removed when the project does
/// not carry one (a redacted pull omits it), so the delta neither drops
/// nor changes a credential the project cannot see.
fn keep_redacted_password(
    repo: &Option<Map<String, Value>>,
    db: &Option<Map<String, Value>>,
) -> Option<Map<String, Value>> {
    let repo_has_password =
        repo.as_ref().is_some_and(|m| m.contains_key("password"));
    if repo_has_password {
        return db.clone();
    }
    let mut db = db.clone().unwrap_or_default();
    db.remove("password");
    (!db.is_empty()).then_some(db)
}

/// A `key 'value'` option list for a freshly created object (no diff)
fn options_clause(options: &Option<Map<String, Value>>) -> Option<String> {
    let options = options.as_ref().filter(|o| !o.is_empty())?;
    Some(
        options
            .iter()
            .map(|(key, value)| format!("{key} {}", postgres_value(value)))
            .collect::<Vec<_>>()
            .join(", "),
    )
}

/// A single-quoted SQL string literal with embedded quotes doubled
fn string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// The `COMMENT ON` statement to reconcile a comment that changed
/// between the repo and database, or `None` when it is unchanged. Used
/// for `CREATE OR REPLACE` objects, which preserve the existing comment
/// and so need it re-stated (or cleared with `IS NULL`) separately.
fn comment_delta(
    desc: &str,
    name: &str,
    repo: &Option<String>,
    db: &Option<String>,
) -> Option<String> {
    (repo != db).then(|| comment_on(desc, name, repo.as_deref()))
}

/// `COMMENT ON <desc> <name> IS ...` matching the build's comment
/// entry text shape; a removed comment becomes `IS NULL`
fn comment_on(desc: &str, name: &str, comment: Option<&str>) -> String {
    match comment {
        Some(comment) => {
            format!("COMMENT ON {desc} {name} IS $${comment}$$;\n")
        }
        None => format!("COMMENT ON {desc} {name} IS NULL;\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models;

    fn parse_table(value: serde_json::Value) -> Table {
        serde_json::from_value(value).expect("table deserializes")
    }

    fn base_table() -> serde_json::Value {
        serde_json::json!({
            "name": "users",
            "schema": "test",
            "owner": "postgres",
            "columns": [
                {"name": "id", "data_type": "uuid", "nullable": false},
                {"name": "email", "data_type": "text", "nullable": false},
            ],
        })
    }

    fn statements(resolution: Resolution) -> Vec<Alter> {
        match resolution {
            Resolution::Statements(alters) => alters,
            _ => panic!("expected in-place statements"),
        }
    }

    fn sql(alters: &[Alter]) -> Vec<&str> {
        alters.iter().map(|a| a.sql.as_str()).collect()
    }

    #[test]
    fn added_column_renders_add_column() {
        let mut repo = base_table();
        repo["columns"].as_array_mut().unwrap().push(
            serde_json::json!({"name": "nickname", "data_type": "text"}),
        );
        let alters =
            statements(table(&parse_table(repo), &parse_table(base_table())));
        assert_eq!(
            sql(&alters),
            vec!["ALTER TABLE test.users ADD COLUMN nickname text;\n"]
        );
        assert!(!alters[0].destructive);
    }

    #[test]
    fn removed_column_is_destructive() {
        let mut db = base_table();
        db["columns"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"name": "legacy", "data_type": "text"}));
        let alters =
            statements(table(&parse_table(base_table()), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec!["ALTER TABLE test.users DROP COLUMN legacy;\n"]
        );
        assert!(alters[0].destructive);
    }

    #[test]
    fn type_change_is_destructive() {
        let mut repo = base_table();
        repo["columns"][1]["data_type"] = "character varying".into();
        let alters =
            statements(table(&parse_table(repo), &parse_table(base_table())));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users ALTER COLUMN email TYPE character \
                 varying;\n"
            ]
        );
        assert!(alters[0].destructive);
    }

    #[test]
    fn aliased_type_is_not_a_change() {
        let mut repo = base_table();
        repo["columns"][1]["data_type"] = "varchar".into();
        let mut db = base_table();
        db["columns"][1]["data_type"] = "character varying".into();
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert!(alters.is_empty(), "alias must not diff: {:?}", sql(&alters));
    }

    #[test]
    fn default_and_nullability_toggle() {
        let mut repo = base_table();
        repo["columns"][1]["default"] = "'unknown'::text".into();
        repo["columns"][1]["nullable"] = true.into();
        let alters =
            statements(table(&parse_table(repo), &parse_table(base_table())));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users ALTER COLUMN email SET DEFAULT \
                 'unknown'::text;\n",
                "ALTER TABLE test.users ALTER COLUMN email DROP NOT NULL;\n",
            ]
        );
        assert!(alters.iter().all(|a| !a.destructive));
    }

    #[test]
    fn reordered_columns_require_replace() {
        let mut repo = base_table();
        repo["columns"].as_array_mut().unwrap().reverse();
        assert!(matches!(
            table(&parse_table(repo), &parse_table(base_table())),
            Resolution::Replace
        ));
    }

    #[test]
    fn primary_key_addition_and_removal() {
        let mut repo = base_table();
        repo["primary_key"] = serde_json::json!(["id"]);
        let alters =
            statements(table(&parse_table(repo), &parse_table(base_table())));
        assert_eq!(
            sql(&alters),
            vec!["ALTER TABLE test.users ADD PRIMARY KEY (id);\n"]
        );
        // removal cannot name the constraint → rebuild
        let mut db = base_table();
        db["primary_key"] = serde_json::json!(["id"]);
        assert!(matches!(
            table(&parse_table(base_table()), &parse_table(db)),
            Resolution::Replace
        ));
    }

    #[test]
    fn check_constraints_reconcile_as_pairs() {
        let mut repo = base_table();
        repo["check_constraints"] = serde_json::json!([
            {"name": "email_has_at", "expression": "email ~ '@'"},
        ]);
        let mut db = base_table();
        db["check_constraints"] = serde_json::json!([
            {"name": "email_has_at", "expression": "email <> ''"},
        ]);
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users DROP CONSTRAINT email_has_at;\n",
                "ALTER TABLE test.users ADD CONSTRAINT email_has_at CHECK \
                 (email ~ '@');\n",
            ]
        );
    }

    #[test]
    fn foreign_keys_reconcile_as_pairs() {
        let mut repo = base_table();
        repo["foreign_keys"] = serde_json::json!([{
            "name": "users_org_fk",
            "columns": ["org_id"],
            "references": {"name": "test.orgs", "columns": ["id"]},
            "on_delete": "CASCADE",
        }]);
        let alters =
            statements(table(&parse_table(repo), &parse_table(base_table())));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users ADD CONSTRAINT users_org_fk FOREIGN \
                 KEY (org_id) REFERENCES test.orgs (id) ON DELETE CASCADE;\n"
            ]
        );
    }

    #[test]
    fn indexes_reconcile_without_gating() {
        let mut repo = base_table();
        repo["indexes"] = serde_json::json!([{
            "name": "users_email_idx",
            "unique": true,
            "columns": [{"name": "email"}],
        }]);
        let mut db = base_table();
        db["indexes"] = serde_json::json!([{
            "name": "users_legacy_idx",
            "columns": [{"name": "id"}],
        }]);
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec![
                "DROP INDEX IF EXISTS test.users_legacy_idx;\n",
                "CREATE UNIQUE INDEX users_email_idx ON test.users ( email \
                 );\n",
            ]
        );
        assert!(alters.iter().all(|a| !a.destructive));
    }

    #[test]
    fn recreated_index_emits_its_comment() {
        let mut repo = base_table();
        repo["indexes"] = serde_json::json!([{
            "name": "users_email_idx",
            "columns": [{"name": "email"}],
            "comment": "lookup by email",
        }]);
        let mut db = base_table();
        db["indexes"] = serde_json::json!([{
            "name": "users_email_idx",
            "columns": [{"name": "id"}],
        }]);
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec![
                "DROP INDEX IF EXISTS test.users_email_idx;\n",
                "CREATE INDEX users_email_idx ON test.users ( email );\n\
                 COMMENT ON INDEX test.users_email_idx IS \
                 $$lookup by email$$;\n",
            ]
        );
    }

    #[test]
    fn triggers_reconcile_by_name() {
        let mut repo = base_table();
        repo["triggers"] = serde_json::json!([{
            "name": "set_last_modified",
            "when": "BEFORE",
            "events": ["UPDATE"],
            "for_each": "ROW",
            "function": "test.set_last_modified()",
        }]);
        let alters =
            statements(table(&parse_table(repo), &parse_table(base_table())));
        assert_eq!(
            sql(&alters),
            vec![
                "CREATE TRIGGER set_last_modified BEFORE UPDATE ON \
                 test.users FOR EACH ROW EXECUTE FUNCTION \
                 test.set_last_modified();\n"
            ]
        );
    }

    #[test]
    fn recreated_trigger_emits_its_comment() {
        let mut repo = base_table();
        repo["triggers"] = serde_json::json!([{
            "name": "set_last_modified",
            "when": "BEFORE",
            "events": ["UPDATE"],
            "for_each": "ROW",
            "function": "test.set_last_modified()",
            "comment": "stamp updates",
        }]);
        let mut db = base_table();
        db["triggers"] = serde_json::json!([{
            "name": "set_last_modified",
            "when": "BEFORE",
            "events": ["INSERT"],
            "for_each": "ROW",
            "function": "test.set_last_modified()",
        }]);
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec![
                "DROP TRIGGER IF EXISTS set_last_modified ON test.users;\n",
                "CREATE TRIGGER set_last_modified BEFORE UPDATE ON \
                 test.users FOR EACH ROW EXECUTE FUNCTION \
                 test.set_last_modified();\n\
                 COMMENT ON TRIGGER set_last_modified ON test.users IS \
                 $$stamp updates$$;\n",
            ]
        );
    }

    #[test]
    fn comment_changes_render_comment_on() {
        let mut repo = base_table();
        repo["comment"] = "User records".into();
        repo["columns"][1]["comment"] = "Email address".into();
        let alters = statements(table(
            &parse_table(repo.clone()),
            &parse_table(base_table()),
        ));
        assert_eq!(
            sql(&alters),
            vec![
                "COMMENT ON COLUMN test.users.email IS $$Email address$$;\n",
                "COMMENT ON TABLE test.users IS $$User records$$;\n",
            ]
        );
        let alters =
            statements(table(&parse_table(base_table()), &parse_table(repo)));
        assert_eq!(
            sql(&alters),
            vec![
                "COMMENT ON COLUMN test.users.email IS NULL;\n",
                "COMMENT ON TABLE test.users IS NULL;\n",
            ]
        );
    }

    #[test]
    fn storage_parameter_changes_require_replace() {
        let mut repo = base_table();
        repo["storage_parameters"] = serde_json::json!({"fillfactor": 70});
        assert!(matches!(
            table(&parse_table(repo), &parse_table(base_table())),
            Resolution::Replace
        ));
    }

    #[test]
    fn unsupported_definitions_replace() {
        // a materialized view has no in-place form
        let mview: models::MaterializedView =
            serde_json::from_value(serde_json::json!({
                "name": "m", "schema": "test", "owner": "postgres",
                "query": "SELECT 1",
            }))
            .unwrap();
        assert!(matches!(
            resolve(
                &Definition::MaterializedView(mview.clone()),
                &Definition::MaterializedView(mview)
            ),
            Resolution::Replace
        ));
    }

    #[test]
    fn function_body_change_uses_or_replace() {
        let f = |body: &str| -> Definition {
            Definition::Function(
                serde_json::from_value(serde_json::json!({
                    "name": "f", "schema": "test", "owner": "postgres",
                    "returns": "integer", "language": "sql",
                    "definition": body,
                }))
                .unwrap(),
            )
        };
        assert!(matches!(
            resolve(&f("SELECT 2"), &f("SELECT 1")),
            Resolution::OrReplace { .. }
        ));
    }

    #[test]
    fn function_return_type_change_replaces() {
        let f = |returns: &str| -> Definition {
            Definition::Function(
                serde_json::from_value(serde_json::json!({
                    "name": "f", "schema": "test", "owner": "postgres",
                    "returns": returns, "language": "sql",
                    "definition": "SELECT 1",
                }))
                .unwrap(),
            )
        };
        assert!(matches!(
            resolve(&f("bigint"), &f("integer")),
            Resolution::Replace
        ));
    }

    #[test]
    fn view_change_uses_or_replace() {
        let v = |query: &str| -> Definition {
            Definition::View(
                serde_json::from_value(serde_json::json!({
                    "name": "v", "schema": "test", "owner": "postgres",
                    "query": query,
                }))
                .unwrap(),
            )
        };
        assert!(matches!(
            resolve(&v("SELECT 2"), &v("SELECT 1")),
            Resolution::OrReplace { .. }
        ));
    }

    fn or_replace_comment(resolution: Resolution) -> Option<String> {
        match resolution {
            Resolution::OrReplace { comment } => comment,
            _ => panic!("expected OR REPLACE"),
        }
    }

    #[test]
    fn function_comment_removal_clears_it() {
        let f = |comment: Option<&str>| -> Definition {
            let mut value = serde_json::json!({
                "name": "f(integer)", "schema": "test", "owner": "postgres",
                "returns": "integer", "language": "sql",
                "definition": "SELECT 1",
            });
            if let Some(comment) = comment {
                value["comment"] = comment.into();
            }
            Definition::Function(serde_json::from_value(value).unwrap())
        };
        // removal emits IS NULL so a re-deploy converges
        assert_eq!(
            or_replace_comment(resolve(&f(None), &f(Some("old")))),
            Some("COMMENT ON FUNCTION test.f(integer) IS NULL;\n".into())
        );
        // a set comment is re-stated, an unchanged one is left alone
        assert_eq!(
            or_replace_comment(resolve(&f(Some("new")), &f(Some("old")))),
            Some("COMMENT ON FUNCTION test.f(integer) IS $$new$$;\n".into())
        );
        assert_eq!(
            or_replace_comment(resolve(&f(Some("same")), &f(Some("same")))),
            None
        );
    }

    #[test]
    fn view_comment_removal_clears_it() {
        let v = |comment: Option<&str>| -> Definition {
            let mut value = serde_json::json!({
                "name": "v", "schema": "test", "owner": "postgres",
                "query": "SELECT 1",
            });
            if let Some(comment) = comment {
                value["comment"] = comment.into();
            }
            Definition::View(serde_json::from_value(value).unwrap())
        };
        assert_eq!(
            or_replace_comment(resolve(&v(None), &v(Some("old")))),
            Some("COMMENT ON VIEW test.v IS NULL;\n".into())
        );
    }

    fn view_with_columns(columns: serde_json::Value) -> Definition {
        Definition::View(
            serde_json::from_value(serde_json::json!({
                "name": "v", "schema": "test", "owner": "postgres",
                "query": "SELECT 1", "columns": columns,
            }))
            .unwrap(),
        )
    }

    #[test]
    fn view_appended_column_uses_or_replace() {
        assert!(matches!(
            resolve(
                &view_with_columns(serde_json::json!(["a", "b"])),
                &view_with_columns(serde_json::json!(["a"])),
            ),
            Resolution::OrReplace { .. }
        ));
    }

    #[test]
    fn view_renamed_column_replaces() {
        assert!(matches!(
            resolve(
                &view_with_columns(serde_json::json!(["a", "c"])),
                &view_with_columns(serde_json::json!(["a", "b"])),
            ),
            Resolution::Replace
        ));
    }

    #[test]
    fn view_reordered_column_replaces() {
        assert!(matches!(
            resolve(
                &view_with_columns(serde_json::json!(["b", "a"])),
                &view_with_columns(serde_json::json!(["a", "b"])),
            ),
            Resolution::Replace
        ));
    }

    #[test]
    fn view_removed_column_replaces() {
        assert!(matches!(
            resolve(
                &view_with_columns(serde_json::json!(["a"])),
                &view_with_columns(serde_json::json!(["a", "b"])),
            ),
            Resolution::Replace
        ));
    }

    fn parse_sequence(value: serde_json::Value) -> Sequence {
        serde_json::from_value(value).expect("sequence deserializes")
    }

    #[test]
    fn sequence_options_render_one_alter() {
        let base = serde_json::json!({
            "name": "s", "schema": "test", "owner": "postgres",
            "increment_by": 1, "cache": 1,
        });
        let mut repo = base.clone();
        repo["increment_by"] = 2.into();
        repo["max_value"] = 100.into();
        repo["cycle"] = true.into();
        let alters =
            statements(sequence(&parse_sequence(repo), &parse_sequence(base)));
        assert_eq!(
            sql(&alters),
            vec!["ALTER SEQUENCE test.s INCREMENT BY 2 MAXVALUE 100 CYCLE;\n"]
        );
        assert!(alters.iter().all(|a| !a.destructive));
    }

    #[test]
    fn enum_append_adds_values() {
        let db: Type = serde_json::from_value(serde_json::json!({
            "name": "state", "schema": "test", "owner": "postgres",
            "type": "enum", "enum": ["a", "b"],
        }))
        .unwrap();
        let mut repo = db.clone();
        repo.enum_values = Some(vec!["a".into(), "b".into(), "c".into()]);
        let alters = statements(enum_type(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec!["ALTER TYPE test.state ADD VALUE 'c';\n"]
        );
    }

    #[test]
    fn enum_append_escapes_single_quotes() {
        let db: Type = serde_json::from_value(serde_json::json!({
            "name": "state", "schema": "test", "owner": "postgres",
            "type": "enum", "enum": ["a"],
        }))
        .unwrap();
        let mut repo = db.clone();
        repo.enum_values = Some(vec!["a".into(), "can't".into()]);
        let alters = statements(enum_type(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec!["ALTER TYPE test.state ADD VALUE 'can''t';\n"]
        );
    }

    #[test]
    fn enum_reorder_replaces() {
        let db: Type = serde_json::from_value(serde_json::json!({
            "name": "state", "schema": "test", "owner": "postgres",
            "type": "enum", "enum": ["a", "b"],
        }))
        .unwrap();
        let mut repo = db.clone();
        repo.enum_values = Some(vec!["b".into(), "a".into()]);
        assert!(matches!(enum_type(&repo, &db), Resolution::Replace));
    }

    #[test]
    fn extension_version_change_updates() {
        let db: Extension = serde_json::from_value(serde_json::json!({
            "name": "citext", "version": "1.0",
        }))
        .unwrap();
        let mut repo = db.clone();
        repo.version = Some("1.6".into());
        let alters = statements(extension(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec!["ALTER EXTENSION citext UPDATE TO '1.6';\n"]
        );
    }

    #[test]
    fn domain_default_changes_in_place_base_type_replaces() {
        let db: Domain = serde_json::from_value(serde_json::json!({
            "name": "d", "schema": "test", "owner": "postgres",
            "data_type": "text",
        }))
        .unwrap();
        let mut repo = db.clone();
        repo.default = Some("'x'".into());
        let alters = statements(domain(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec!["ALTER DOMAIN test.d SET DEFAULT 'x';\n"]
        );
        let mut retyped = db.clone();
        retyped.data_type = Some("citext".into());
        assert!(matches!(domain(&retyped, &db), Resolution::Replace));
    }

    #[test]
    fn schema_comment_changes_in_place() {
        let db: Schema = serde_json::from_value(serde_json::json!({
            "name": "test", "owner": "postgres",
        }))
        .unwrap();
        let mut repo = db.clone();
        repo.comment = Some("App schema".into());
        let alters = statements(schema(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec!["COMMENT ON SCHEMA test IS $$App schema$$;\n"]
        );
    }

    fn parse_fdw(value: serde_json::Value) -> ForeignDataWrapper {
        serde_json::from_value(value).expect("fdw deserializes")
    }

    #[test]
    fn fdw_handler_options_and_comment_alter_in_place() {
        let db = parse_fdw(serde_json::json!({
            "name": "wh", "owner": "postgres",
            "options": {"debug": "false"},
        }));
        let mut repo = db.clone();
        repo.handler = Some("postgres_fdw_handler".into());
        repo.options = Some(
            serde_json::from_value(serde_json::json!({"debug": "true"}))
                .unwrap(),
        );
        repo.comment = Some("warehouse".into());
        let alters = statements(fdw(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER FOREIGN DATA WRAPPER wh HANDLER \
                 postgres_fdw_handler;\n",
                "ALTER FOREIGN DATA WRAPPER wh OPTIONS (SET debug 'true');\n",
                "COMMENT ON FOREIGN DATA WRAPPER wh IS $$warehouse$$;\n",
            ]
        );
        assert!(alters.iter().all(|a| !a.destructive));
    }

    #[test]
    fn fdw_handler_removal_emits_no_handler() {
        let db = parse_fdw(serde_json::json!({
            "name": "wh", "owner": "postgres",
            "handler": "h", "validator": "v",
        }));
        let mut repo = db.clone();
        repo.handler = None;
        repo.validator = None;
        let alters = statements(fdw(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER FOREIGN DATA WRAPPER wh NO HANDLER;\n",
                "ALTER FOREIGN DATA WRAPPER wh NO VALIDATOR;\n",
            ]
        );
    }

    fn parse_server(value: serde_json::Value) -> Server {
        serde_json::from_value(value).expect("server deserializes")
    }

    #[test]
    fn server_version_and_options_alter_in_place() {
        let db = parse_server(serde_json::json!({
            "name": "wh", "foreign_data_wrapper": "postgres_fdw",
            "version": "14", "options": {"host": "old", "port": "5432"},
        }));
        let mut repo = db.clone();
        repo.version = Some("17".into());
        repo.options = Some(
            serde_json::from_value(
                serde_json::json!({"host": "new", "dbname": "w"}),
            )
            .unwrap(),
        );
        let alters = statements(server(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER SERVER wh VERSION '17';\n",
                "ALTER SERVER wh OPTIONS (SET host 'new', ADD dbname 'w', \
                 DROP port);\n",
            ]
        );
    }

    #[test]
    fn server_type_change_replaces() {
        let db = parse_server(serde_json::json!({
            "name": "wh", "foreign_data_wrapper": "postgres_fdw",
            "type": "oracle",
        }));
        let mut repo = db.clone();
        repo.server_type = Some("mysql".into());
        assert!(matches!(server(&repo, &db), Resolution::Replace));
    }

    fn parse_user_mapping(value: serde_json::Value) -> UserMapping {
        serde_json::from_value(value).expect("user mapping deserializes")
    }

    #[test]
    fn user_mapping_adds_alters_and_drops_per_server() {
        let db = parse_user_mapping(serde_json::json!({
            "name": "app",
            "servers": [
                {"name": "keep", "options": {"user": "old"}},
                {"name": "gone", "options": {"user": "x"}},
            ],
        }));
        let repo = parse_user_mapping(serde_json::json!({
            "name": "app",
            "servers": [
                {"name": "keep", "options": {"user": "new"}},
                {"name": "fresh", "options": {"user": "y"}},
            ],
        }));
        let alters = statements(user_mapping(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER USER MAPPING FOR app SERVER keep OPTIONS \
                 (SET user 'new');\n",
                "CREATE USER MAPPING FOR app SERVER fresh OPTIONS \
                 (user 'y');\n",
                "DROP USER MAPPING IF EXISTS FOR app SERVER gone;\n",
            ]
        );
        assert!(alters.iter().all(|a| !a.destructive));
    }

    #[test]
    fn user_mapping_public_subject_is_unquoted() {
        let db = parse_user_mapping(serde_json::json!({
            "name": "PUBLIC",
            "servers": [{"name": "gone", "options": {"user": "x"}}],
        }));
        let repo = parse_user_mapping(serde_json::json!({
            "name": "PUBLIC",
            "servers": [{"name": "fresh", "options": {"user": "y"}}],
        }));
        let alters = statements(user_mapping(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec![
                "CREATE USER MAPPING FOR PUBLIC SERVER fresh OPTIONS \
                 (user 'y');\n",
                "DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER gone;\n",
            ]
        );
    }

    #[test]
    fn user_mapping_keeps_redacted_password() {
        // the project (redacted pull) carries only `user`; the database
        // mapping also has a password — deploy must not drop it
        let db = parse_user_mapping(serde_json::json!({
            "name": "app",
            "servers": [{
                "name": "wh",
                "options": {"user": "remote", "password": "secret"},
            }],
        }));
        let repo = parse_user_mapping(serde_json::json!({
            "name": "app",
            "servers": [{"name": "wh", "options": {"user": "remote"}}],
        }));
        let alters = statements(user_mapping(&repo, &db));
        assert!(
            alters.is_empty(),
            "a redacted password must not diff: {:?}",
            sql(&alters)
        );
    }

    fn foreign_table_value(
        options: serde_json::Value,
        comment: Option<&str>,
    ) -> serde_json::Value {
        let mut value = serde_json::json!({
            "name": "remote", "schema": "test", "owner": "postgres",
            "columns": [{"name": "id", "data_type": "integer"}],
            "server": "wh", "options": options,
        });
        if let Some(comment) = comment {
            value["comment"] = comment.into();
        }
        value
    }

    #[test]
    fn foreign_table_options_alter_in_place() {
        let repo = parse_table(foreign_table_value(
            serde_json::json!({"schema_name": "public", "table_name": "t"}),
            Some("remote orders"),
        ));
        let db = parse_table(foreign_table_value(
            serde_json::json!({"schema_name": "public", "table_name": "old"}),
            None,
        ));
        let alters = statements(table(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER FOREIGN TABLE test.remote OPTIONS (SET table_name \
                 't');\n",
                "COMMENT ON FOREIGN TABLE test.remote IS $$remote orders$$;\n",
            ]
        );
        assert!(alters.iter().all(|a| !a.destructive));
    }

    #[test]
    fn foreign_table_server_change_replaces() {
        let repo = parse_table(foreign_table_value(
            serde_json::json!({"table_name": "t"}),
            None,
        ));
        let mut db = repo.clone();
        db.server = Some("other".into());
        assert!(matches!(table(&repo, &db), Resolution::Replace));
    }
}
