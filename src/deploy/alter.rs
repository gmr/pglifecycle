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
    CheckConstraint, Column, ColumnDefault, ColumnGenerated, ColumnNotNull,
    Definition, Domain, ExcludeConstraint, Extension, ForeignDataWrapper,
    ForeignKey, Function, Index, NotNullConstraint, Policy, ReplicaIdentity,
    Schema, Sequence, SequenceOptions, Server, Table, Trigger, Type,
    UserMapping, View, ViewColumn,
};
use crate::utils::{
    dollar_quote, postgres_value, quote_ident, user_mapping_subject,
};

/// One reconciliation statement
pub(crate) struct Alter {
    pub sql: String,
    pub destructive: bool,
    /// The object the statement reconciles, when it is a child of the
    /// object being altered that the script names on its own (a
    /// policy, or the table's row security)
    pub label: Option<String>,
    /// Withholding this statement can leave the database allowing
    /// access that the project does not
    pub fails_open: bool,
}

impl Alter {
    fn new(sql: String) -> Self {
        Self {
            sql,
            destructive: false,
            label: None,
            fails_open: false,
        }
    }

    fn destructive(sql: String) -> Self {
        Self {
            destructive: true,
            ..Self::new(sql)
        }
    }

    fn labeled(self, label: &str) -> Self {
        Self {
            label: Some(label.to_string()),
            ..self
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
        // CREATE OR REPLACE handles function bodies and view queries in
        // place; a function whose return type or output-parameter
        // signature changed cannot be replaced (Postgres rejects an
        // OR REPLACE that alters the output) and must be dropped first
        (Definition::Function(repo), Definition::Function(db)) => {
            if returns_equal(repo, db)
                && out_parameters(repo) == out_parameters(db)
            {
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

/// True when two functions' return types are the same modulo type
/// aliasing (`int4` vs `integer`)
fn returns_equal(repo: &Function, db: &Function) -> bool {
    match (&repo.returns, &db.returns) {
        (Some(r), Some(d)) => canonical_type(r) == canonical_type(d),
        (r, d) => r == d,
    }
}

/// The `OUT`/`TABLE`-mode parameters that make up a function's output
/// signature, with types canonicalized so an alias does not spuriously
/// diff. `CREATE OR REPLACE FUNCTION` cannot change this signature, so
/// callers must fall back to a drop+recreate when it differs.
fn out_parameters(function: &Function) -> Vec<(String, String, String)> {
    function
        .parameters
        .iter()
        .flatten()
        .filter(|p| p.mode == "OUT" || p.mode == "TABLE")
        .map(|p| {
            (
                p.mode.clone(),
                p.name.clone().unwrap_or_default(),
                canonical_type(&p.data_type),
            )
        })
        .collect()
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
    // Validating a NOT NULL on a local column changes how it is
    // written, not only its state, so it is found before the two sides
    // are made canonical: afterwards the repo's copy has moved onto the
    // column while the database's NOT VALID copy has not
    let name = qualified(&repo.schema, &repo.name);
    let mut validations = Vec::new();
    let db = &validate_local_not_nulls(&name, repo, db, &mut validations);
    // reconcile canonical forms; see Table::canonical
    let (repo, db) = (&repo.canonical(), &db.canonical());
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
    let mut alters = validations;
    if !columns(&name, repo, db, &mut alters)
        || !constraints(&name, repo, db, &mut alters)
        || !triggers(&name, repo, db, &mut alters)
    {
        return Resolution::Replace;
    }
    indexes(&name, repo, db, &mut alters);
    // after the indexes, which a USING INDEX identity may name. A
    // rebuilt identity index loses its mark, so the identity is set
    // again although both sides name the same index.
    if repo.replica_identity != db.replica_identity
        || identity_index_rebuilt(repo, db)
    {
        let sql = build::render_replica_identity(
            repo.replica_identity.as_ref(),
            &name,
            |_| true,
        )
        .unwrap_or_else(|| {
            format!("ALTER TABLE ONLY {name} REPLICA IDENTITY DEFAULT")
        });
        alters.push(Alter::new(format!("{sql};\n")));
    }
    row_security(&name, repo, db, &mut alters);
    policies(&name, repo, db, &mut alters);
    push_comment(&mut alters, "TABLE", &name, &repo.comment, &db.comment);
    Resolution::Statements(alters)
}

/// True when [`indexes`] drops and creates the index that the repo's
/// USING INDEX replica identity names. PostgreSQL clears the identity
/// mark when it drops the index, and the new index does not get it.
fn identity_index_rebuilt(repo: &Table, db: &Table) -> bool {
    let Some(ReplicaIdentity::Index { index }) = &repo.replica_identity else {
        return false;
    };
    let find = |table: &Table| {
        table
            .indexes
            .iter()
            .flatten()
            .find(|i| &i.name == index)
            .cloned()
    };
    matches!((find(repo), find(db)), (Some(r), Some(d)) if r != d)
}

/// Row security reconciliation. A statement that turns protection on
/// is always included; one that turns it off opens the table's rows
/// to more roles, so it is gated. Withholding it leaves the database
/// the stricter of the two, so it does not fail open.
///
/// The database side has no state when the repo leaves row security
/// unmanaged (see [`Table::without_unmanaged_security`]).
fn row_security(
    table: &str,
    repo: &Table,
    db: &Table,
    alters: &mut Vec<Alter>,
) {
    let Some(wanted) = &repo.row_level_security else {
        return;
    };
    let existing = db.row_level_security.clone().unwrap_or_default();
    let label = format!("ROW SECURITY {}.{}", repo.schema, repo.name);
    if wanted.enabled != existing.enabled {
        let sql = format!(
            "ALTER TABLE {table} {} ROW LEVEL SECURITY;\n",
            if wanted.enabled { "ENABLE" } else { "DISABLE" }
        );
        alters.push(if wanted.enabled {
            Alter::new(sql).labeled(&label)
        } else {
            Alter::destructive(sql).labeled(&label)
        });
    }
    let forced = wanted.forced == Some(true);
    if forced != (existing.forced == Some(true)) {
        let sql = format!(
            "ALTER TABLE ONLY {table} {} ROW LEVEL SECURITY;\n",
            if forced { "FORCE" } else { "NO FORCE" }
        );
        alters.push(if forced {
            Alter::new(sql).labeled(&label)
        } else {
            Alter::destructive(sql).labeled(&label)
        });
    }
}

/// Policy reconciliation, one labeled statement per policy.
///
/// Included: a new policy, which the repo adds explicitly; the drop of
/// a permissive policy; a role change that narrows access (fewer roles
/// on a permissive policy, more on a restrictive one); and a comment.
/// Gated, as reconciliation that can open access: the drop of a
/// restrictive policy and every other change. Whether a changed
/// expression allows more rows or fewer cannot be decided in general.
/// A withheld change is marked to fail open unless it provably opens
/// access, since the old policy stays and can allow more than the
/// project does.
fn policies(table: &str, repo: &Table, db: &Table, alters: &mut Vec<Alter>) {
    let wanted = repo.policies.as_deref().unwrap_or_default();
    let existing = db.policies.as_deref().unwrap_or_default();
    let label = |policy: &Policy| {
        format!("POLICY {}.{} {}", repo.schema, repo.name, policy.name)
    };
    let target =
        |policy: &Policy| format!("{} ON {table}", quote_ident(&policy.name));
    let drop = |policy: &Policy| {
        format!("DROP POLICY IF EXISTS {};\n", target(policy))
    };
    let create = |policy: &Policy| {
        let mut sql = format!("{};\n", build::render_policy(policy, table));
        if let Some(comment) = &policy.comment {
            sql.push_str(&comment_on(
                "POLICY",
                &target(policy),
                Some(comment),
            ));
        }
        sql
    };
    for old in existing {
        if !wanted.iter().any(|p| p.name == old.name) {
            let sql = drop(old);
            alters.push(if old.restrictive == Some(true) {
                Alter::destructive(sql).labeled(&label(old))
            } else {
                Alter::new(sql).labeled(&label(old))
            });
        }
    }
    for new in wanted {
        let Some(old) = existing.iter().find(|p| p.name == new.name) else {
            alters.push(Alter::new(create(new)).labeled(&label(new)));
            continue;
        };
        let without_comment = |p: &Policy| Policy {
            comment: None,
            ..p.clone()
        };
        if without_comment(new) == without_comment(old) {
            if new.comment != old.comment {
                alters.push(
                    Alter::new(comment_on(
                        "POLICY",
                        &target(new),
                        new.comment.as_deref(),
                    ))
                    .labeled(&label(new)),
                );
            }
            continue;
        }
        let only_roles = Policy {
            roles: old.roles.clone(),
            ..without_comment(new)
        } == without_comment(old);
        let restrictive = new.restrictive == Some(true);
        let fewer = fewer_roles(new.roles.as_deref(), old.roles.as_deref());
        let more = fewer_roles(old.roles.as_deref(), new.roles.as_deref());
        // a permissive policy grants access to its roles and a
        // restrictive one limits them, so fewer roles narrows the first
        // and more roles narrows the second
        let narrows = only_roles && if restrictive { more } else { fewer };
        let opens = (only_roles && if restrictive { fewer } else { more })
            || Policy {
                restrictive: old.restrictive,
                ..without_comment(new)
            } == without_comment(old)
                && !restrictive;
        let mut sql = if narrows {
            let roles = match new.roles.as_deref() {
                Some(roles) => {
                    roles.iter().map(|r| user_mapping_subject(r)).collect()
                }
                None => vec![String::from("PUBLIC")],
            };
            format!("ALTER POLICY {} TO {};\n", target(new), roles.join(", "))
        } else {
            format!("{}{}", drop(old), create(new))
        };
        if narrows && new.comment != old.comment {
            sql.push_str(&comment_on(
                "POLICY",
                &target(new),
                new.comment.as_deref(),
            ));
        }
        // withholding a change that provably opens access leaves the
        // database stricter; any other withheld change can leave it
        // more open than the project
        alters.push(if narrows {
            Alter::new(sql).labeled(&label(new))
        } else {
            Alter {
                fails_open: !opens,
                ..Alter::destructive(sql).labeled(&label(new))
            }
        });
    }
}

/// Whether the `wanted` roles are a strict subset of the `existing`
/// ones; no roles means PUBLIC, which includes every role
fn fewer_roles(
    wanted: Option<&[String]>,
    existing: Option<&[String]>,
) -> bool {
    match (wanted, existing) {
        (Some(wanted), None) => !wanted.is_empty(),
        (Some(wanted), Some(existing)) => {
            wanted.len() < existing.len()
                && wanted.iter().all(|r| existing.contains(r))
        }
        (None, _) => false,
    }
}

/// `VALIDATE CONSTRAINT` each NOT VALID table-level NOT NULL on one of
/// the table's own columns where the repo has the same constraint,
/// valid; return the database side as it will be once validated.
///
/// A valid NOT NULL on a local column is written on the column, so the
/// repo's copy is compared there, against a database copy that is
/// still table-level because it is NOT VALID. Left to the comparison,
/// that pair reads as a new constraint plus a dropped one, and the ADD
/// collides with the name the existing constraint already holds.
fn validate_local_not_nulls(
    table: &str,
    repo: &Table,
    db: &Table,
    alters: &mut Vec<Alter>,
) -> Table {
    let wanted = repo.canonical();
    let mut db = db.clone();
    for not_null in db.not_null_constraints.iter_mut().flatten() {
        if not_null.not_valid != Some(true) {
            continue;
        }
        let matches = wanted
            .columns
            .iter()
            .flatten()
            .find(|c| c.name == not_null.column)
            .is_some_and(|c| {
                let constraint =
                    c.not_null_constraint.clone().unwrap_or(ColumnNotNull {
                        name: None,
                        no_inherit: None,
                    });
                c.nullable == Some(false)
                    && constraint.name == not_null.name
                    && constraint.no_inherit == not_null.no_inherit
            });
        if matches {
            let name = not_null.name.clone().unwrap_or_else(|| {
                format!("{}_{}_not_null", db.name, not_null.column)
            });
            alters.push(Alter::new(format!(
                "ALTER TABLE {table} VALIDATE CONSTRAINT {};\n",
                quote_ident(&name)
            )));
            not_null.not_valid = None;
        }
    }
    db
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
                if !alter_column(table, &repo.name, column, existing, alters) {
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

/// Whether a column's NOT NULL constraint is NO INHERIT
fn no_inherit(not_null: &Option<ColumnNotNull>) -> bool {
    not_null.as_ref().and_then(|c| c.no_inherit) == Some(true)
}

/// The NOT NULL constraint's name on `relation`, falling back to the
/// name PostgreSQL generates when the model records none (the model
/// carries a name only where it differs from the generated one)
fn not_null_name(relation: &str, column: &Column) -> String {
    match column
        .not_null_constraint
        .as_ref()
        .and_then(|c| c.name.as_ref())
    {
        Some(name) => name.clone(),
        None => format!("{relation}_{}_not_null", column.name),
    }
}

fn alter_column(
    table: &str,
    relation: &str,
    repo: &Column,
    db: &Column,
    alters: &mut Vec<Alter>,
) -> bool {
    // collation, expression generation, and inline check changes
    // require a rebuild. An identity reconciles in place at the end of
    // this function: falling back to a rebuild there would drop and
    // recreate the table, and its rows, to change a sequence option.
    let identities = is_identity_or_none(&repo.generated)
        && is_identity_or_none(&db.generated);
    if repo.collation != db.collation
        || (!identities && repo.generated != db.generated)
        || repo.check_constraint != db.check_constraint
    {
        return false;
    }
    let column = quote_ident(&repo.name);
    // DROP IDENTITY goes first: PostgreSQL rejects SET DEFAULT and DROP
    // NOT NULL on a column that is still an identity. The statements
    // that depend on the drop are gated with it, so a script without
    // --allow-drop does not keep them and fail.
    let drops_identity =
        identities && repo.generated.is_none() && db.generated.is_some();
    let dependent = |sql: String| {
        if drops_identity {
            Alter::destructive(sql)
        } else {
            Alter::new(sql)
        }
    };
    if drops_identity {
        // Gated even though it keeps every row: the sequence and its
        // position go with it, so adding the identity back restarts the
        // numbering and collides with existing keys. A project pulled
        // before identity columns were modeled has none on any column,
        // so this is also what keeps a deploy of such a project from
        // stripping every identity in the database.
        alters.push(Alter::destructive(format!(
            "ALTER TABLE {table} ALTER COLUMN {column} DROP IDENTITY;\n"
        )));
    }
    if canonical_type(&repo.data_type) != canonical_type(&db.data_type) {
        // a type change may rewrite the table (and can fail outright
        // without a USING clause), so it is gated
        alters.push(Alter::destructive(format!(
            "ALTER TABLE {table} ALTER COLUMN {column} TYPE {};\n",
            repo.data_type
        )));
    }
    if repo.default != db.default {
        alters.push(dependent(match &repo.default {
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
    // NOT NULL is a named constraint from PostgreSQL 18 on, so a
    // rename or a NO INHERIT change reconciles even when nullability
    // itself is unchanged. DROP is always the plain ALTER COLUMN form,
    // which needs no name; ADD carries CONSTRAINT only when the model
    // records a name to restore
    let repo_not_null = repo.nullable == Some(false);
    let db_not_null = db.nullable == Some(false);
    if repo_not_null != db_not_null {
        if db_not_null {
            alters.push(dependent(format!(
                "ALTER TABLE {table} ALTER COLUMN {column} DROP NOT \
                 NULL;\n"
            )));
        }
        if repo_not_null {
            alters.push(Alter::new(match repo.not_null_constraint.as_ref() {
                Some(not_null) => format!(
                    "ALTER TABLE {table} ADD {}NOT NULL {column}{};\n",
                    match &not_null.name {
                        Some(name) =>
                            format!("CONSTRAINT {} ", quote_ident(name)),
                        None => String::new(),
                    },
                    if not_null.no_inherit == Some(true) {
                        " NO INHERIT"
                    } else {
                        ""
                    }
                ),
                None => format!(
                    "ALTER TABLE {table} ALTER COLUMN {column} SET NOT \
                         NULL;\n"
                ),
            }));
        }
    } else if repo_not_null
        && repo.not_null_constraint != db.not_null_constraint
    {
        // both sides are NOT NULL and only the constraint's name or its
        // inheritance differs. Reconcile it in place: DROP NOT NULL is
        // rejected outright on a primary-key column, and pg_dump does
        // write a named NOT NULL there
        let repo_name = not_null_name(relation, repo);
        let db_name = not_null_name(relation, db);
        if repo_name != db_name {
            alters.push(Alter::new(format!(
                "ALTER TABLE {table} RENAME CONSTRAINT {} TO {};\n",
                quote_ident(&db_name),
                quote_ident(&repo_name)
            )));
        }
        let repo_no_inherit = no_inherit(&repo.not_null_constraint);
        if repo_no_inherit != no_inherit(&db.not_null_constraint) {
            alters.push(Alter::new(format!(
                "ALTER TABLE {table} ALTER CONSTRAINT {} {}INHERIT;\n",
                quote_ident(&repo_name),
                if repo_no_inherit { "NO " } else { "" }
            )));
        }
    }
    // last, because ADD GENERATED needs the column NOT NULL and free of
    // a default, which the statements above may be what establishes
    if identities && !identity(table, &column, repo, db, alters) {
        return false;
    }
    push_comment(
        alters,
        "COLUMN",
        &format!("{table}.{column}"),
        &repo.comment,
        &db.comment,
    );
    true
}

/// Whether a column's generation is absent or an identity, the two
/// states [`identity`] reconciles in place. A pulled identity carries
/// only its behavior; an older project file names a separate sequence
/// instead.
fn is_identity_or_none(generated: &Option<ColumnGenerated>) -> bool {
    generated.as_ref().is_none_or(|g| {
        g.expression.is_none()
            && (g.sequence_behavior.is_some() || g.sequence.is_some())
    })
}

/// Reconcile an identity column in place: add it, drop it, or change
/// its behavior and sequence options. Returns false only for a change
/// of sequence name, which ALTER COLUMN cannot express.
fn identity(
    table: &str,
    column: &str,
    repo: &Column,
    db: &Column,
    alters: &mut Vec<Alter>,
) -> bool {
    let default = SequenceOptions::default();
    match (&repo.generated, &db.generated) {
        (None, None) => true,
        (Some(repo), None) => {
            alters.push(Alter::new(format!(
                "ALTER TABLE {table} ALTER COLUMN {column} ADD {};\n",
                build::render_identity(repo)
            )));
            true
        }
        // alter_column emits the DROP IDENTITY ahead of the default
        // and nullability statements, which PostgreSQL rejects on an
        // identity column
        (None, Some(_)) => true,
        (Some(repo), Some(db)) => {
            let repo_options =
                repo.sequence_options.as_ref().unwrap_or(&default);
            let db_options = db.sequence_options.as_ref().unwrap_or(&default);
            if repo_options.name != db_options.name {
                return false;
            }
            let mut sets = Vec::new();
            if let Some(behavior) = &repo.sequence_behavior
                && repo.sequence_behavior != db.sequence_behavior
            {
                sets.push(format!("SET GENERATED {behavior}"));
            }
            sets.extend(sequence_option_changes(repo_options, db_options));
            if !sets.is_empty() {
                alters.push(Alter::new(format!(
                    "ALTER TABLE {table} ALTER COLUMN {column} {};\n",
                    sets.join(" ")
                )));
            }
            true
        }
    }
}

/// `SET` clauses taking a sequence from `db` to `repo`. An option the
/// repo leaves out is PostgreSQL's default, so it is set back to that
/// explicitly rather than skipped; START WITH changes only the value a
/// RESTART uses, so no existing key is renumbered.
fn sequence_option_changes(
    repo: &SequenceOptions,
    db: &SequenceOptions,
) -> Vec<String> {
    let mut sets = Vec::new();
    let ascending = repo.increment_by.is_none_or(|by| by > 0);
    if repo.start_with != db.start_with {
        let start = repo.start_with.unwrap_or(if ascending {
            repo.min_value.unwrap_or(1)
        } else {
            repo.max_value.unwrap_or(-1)
        });
        sets.push(format!("SET START WITH {start}"));
    }
    if repo.increment_by != db.increment_by {
        sets.push(format!(
            "SET INCREMENT BY {}",
            repo.increment_by.unwrap_or(1)
        ));
    }
    if repo.min_value != db.min_value {
        sets.push(match repo.min_value {
            Some(min) => format!("SET MINVALUE {min}"),
            None => String::from("SET NO MINVALUE"),
        });
    }
    if repo.max_value != db.max_value {
        sets.push(match repo.max_value {
            Some(max) => format!("SET MAXVALUE {max}"),
            None => String::from("SET NO MAXVALUE"),
        });
    }
    if repo.cache != db.cache {
        sets.push(format!("SET CACHE {}", repo.cache.unwrap_or(1)));
    }
    if repo.cycle != db.cycle {
        sets.push(if repo.cycle == Some(true) {
            String::from("SET CYCLE")
        } else {
            String::from("SET NO CYCLE")
        });
    }
    sets
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
    let db_checks = validations(
        table,
        db.check_constraints.as_deref().unwrap_or_default(),
        repo_checks,
        |check: &CheckConstraint| check.name.clone(),
        |check| check.not_valid == Some(true),
        |check| CheckConstraint {
            not_valid: None,
            ..check.clone()
        },
        |check| check.name.clone(),
        alters,
    );
    named_pairs(
        alters,
        &db_checks,
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
                "ALTER TABLE {table} ADD {};\n",
                build::render_check_constraint(check)
            )
        },
    );
    // Table-level NOT NULL (PostgreSQL 18+) keys on the column, not
    // the constraint name: a column carries at most one, and the name
    // is absent from the model whenever PostgreSQL generated it. DROP
    // goes through ALTER COLUMN for the same reason — it needs no name
    let repo_not_null =
        repo.not_null_constraints.as_deref().unwrap_or_default();
    let db_not_null = validations(
        table,
        db.not_null_constraints.as_deref().unwrap_or_default(),
        repo_not_null,
        |not_null: &NotNullConstraint| not_null.column.clone(),
        |not_null| not_null.not_valid == Some(true),
        |not_null| NotNullConstraint {
            not_valid: None,
            ..not_null.clone()
        },
        // an unnamed one carries the name PostgreSQL generates
        |not_null| {
            not_null.name.clone().unwrap_or_else(|| {
                format!("{}_{}_not_null", repo.name, not_null.column)
            })
        },
        alters,
    );
    named_pairs(
        alters,
        &db_not_null,
        repo_not_null,
        |not_null: &NotNullConstraint| not_null.column.clone(),
        |not_null| {
            format!(
                "ALTER TABLE {table} ALTER COLUMN {} DROP NOT NULL;\n",
                quote_ident(&not_null.column)
            )
        },
        |not_null| {
            format!(
                "ALTER TABLE {table} ADD {};\n",
                build::render_not_null_constraint(not_null)
            )
        },
    );
    // Defaults on inherited columns, keyed on the column: a column
    // carries at most one default, and `ALTER TABLE ONLY` keeps the
    // change off this table's own descendants, as pg_dump writes it
    named_pairs(
        alters,
        db.column_defaults.as_deref().unwrap_or_default(),
        repo.column_defaults.as_deref().unwrap_or_default(),
        |column_default: &ColumnDefault| column_default.column.clone(),
        |column_default| {
            format!(
                "ALTER TABLE ONLY {table} ALTER COLUMN {} DROP DEFAULT;\n",
                quote_ident(&column_default.column)
            )
        },
        |column_default| {
            format!(
                "ALTER TABLE ONLY {table} ALTER COLUMN {} \
                 SET DEFAULT {};\n",
                quote_ident(&column_default.column),
                build::render_default(&column_default.default)
            )
        },
    );
    let repo_fks = repo.foreign_keys.as_deref().unwrap_or_default();
    let db_fks = validations(
        table,
        db.foreign_keys.as_deref().unwrap_or_default(),
        repo_fks,
        |fk: &ForeignKey| fk.name.clone(),
        |fk| fk.not_valid == Some(true),
        |fk| ForeignKey {
            not_valid: None,
            ..fk.clone()
        },
        |fk| fk.name.clone(),
        alters,
    );
    named_pairs(
        alters,
        &db_fks,
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
    exclude_constraints(table, repo, db, alters);
    true
}

/// Exclusion constraint reconciliation. The constraints pair without
/// their comments, so a changed comment alone is set with COMMENT ON
/// CONSTRAINT instead of rebuilding the index behind the constraint.
fn exclude_constraints(
    table: &str,
    repo: &Table,
    db: &Table,
    alters: &mut Vec<Alter>,
) {
    let without_comment = |list: &Option<Vec<ExcludeConstraint>>| {
        list.iter()
            .flatten()
            .map(|c| ExcludeConstraint {
                comment: None,
                ..c.clone()
            })
            .collect::<Vec<_>>()
    };
    let wanted = without_comment(&repo.exclude_constraints);
    let existing = without_comment(&db.exclude_constraints);
    named_pairs(
        alters,
        &existing,
        &wanted,
        |c: &ExcludeConstraint| c.name.clone(),
        |c| {
            format!(
                "ALTER TABLE {table} DROP CONSTRAINT {};\n",
                quote_ident(&c.name)
            )
        },
        |c| {
            format!(
                "ALTER TABLE {table} ADD {};\n",
                build::render_exclude_constraint(c)
            )
        },
    );
    // the comment of each repo constraint, against what the database
    // will have once the pairing above has run: a re-added constraint
    // has none
    for constraint in repo.exclude_constraints.iter().flatten() {
        let bare = ExcludeConstraint {
            comment: None,
            ..constraint.clone()
        };
        let current = match db
            .exclude_constraints
            .iter()
            .flatten()
            .find(|c| c.name == constraint.name)
        {
            Some(c) if existing.contains(&bare) => c.comment.clone(),
            _ => None,
        };
        let target = format!("{} ON {table}", quote_ident(&constraint.name));
        push_comment(
            alters,
            "CONSTRAINT",
            &target,
            &constraint.comment,
            &current,
        );
    }
}

/// Emit `VALIDATE CONSTRAINT` for each database constraint that is NOT
/// VALID where the repo's is otherwise identical and valid, and return
/// the database side with those counted as matching, so the pairing
/// that follows leaves them alone.
///
/// Dropping and re-adding would reach the same state, but a NOT VALID
/// constraint exists so a large table need not be scanned and locked
/// all at once. The ADD rescans the whole table under a heavier lock
/// than VALIDATE takes, which is the cost the NOT VALID was avoiding.
#[allow(clippy::too_many_arguments)]
fn validations<T: Clone + PartialEq>(
    table: &str,
    db: &[T],
    repo: &[T],
    key: impl Fn(&T) -> String,
    is_not_valid: impl Fn(&T) -> bool,
    as_valid: impl Fn(&T) -> T,
    name: impl Fn(&T) -> String,
    alters: &mut Vec<Alter>,
) -> Vec<T> {
    db.iter()
        .map(|existing| {
            let target = is_not_valid(existing)
                .then(|| repo.iter().find(|r| key(r) == key(existing)))
                .flatten()
                .filter(|r| !is_not_valid(r) && **r == as_valid(existing));
            match target {
                Some(target) => {
                    alters.push(Alter::new(format!(
                        "ALTER TABLE {table} VALIDATE CONSTRAINT {};\n",
                        quote_ident(&name(existing))
                    )));
                    target.clone()
                }
                None => existing.clone(),
            }
        })
        .collect()
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
    push_comment(&mut alters, "SEQUENCE", &name, &repo.comment, &db.comment);
    Resolution::Statements(alters)
}

/// Domain reconciliation: SET/DROP DEFAULT and a comment delta in
/// place. A base-type, collation, or constraint change rebuilds (the
/// domain's constraints are not all individually named).
fn domain(repo: &Domain, db: &Domain) -> Resolution {
    let data_type_changed = match (&repo.data_type, &db.data_type) {
        (Some(r), Some(d)) => canonical_type(r) != canonical_type(d),
        (r, d) => r != d,
    };
    if repo.sql != db.sql
        || data_type_changed
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
    push_comment(&mut alters, "DOMAIN", &name, &repo.comment, &db.comment);
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
            Alter::new(format!(
                "ALTER TYPE {name} ADD VALUE {};\n",
                string_literal(value)
            ))
        })
        .collect();
    push_comment(&mut alters, "TYPE", &name, &repo.comment, &db.comment);
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
            "ALTER EXTENSION {name} UPDATE TO {};\n",
            string_literal(version)
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
    push_comment(&mut alters, "EXTENSION", &name, &repo.comment, &db.comment);
    Resolution::Statements(alters)
}

/// Schema reconciliation: only a comment delta is expressible in
/// place; an owner/authorization change rebuilds.
fn schema(repo: &Schema, db: &Schema) -> Resolution {
    if repo.authorization != db.authorization {
        return Resolution::Replace;
    }
    let mut alters = Vec::new();
    push_comment(
        &mut alters,
        "SCHEMA",
        &quote_ident(&repo.name),
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

/// Foreign-table reconciliation: only OPTIONS and the comment are
/// alterable in place. A different server or any column/structural
/// change rebuilds (gated behind --allow-drop). Foreign-table comments
/// use the FOREIGN TABLE object type, matching the build.
fn foreign_table(repo: &Table, db: &Table) -> Resolution {
    if repo.server != db.server
        || canonicalize_columns(&repo.columns)
            != canonicalize_columns(&db.columns)
        || repo.sql != db.sql
        || repo.parents != db.parents
        || repo.check_constraints != db.check_constraints
        || repo.not_null_constraints != db.not_null_constraints
        || repo.column_defaults != db.column_defaults
    {
        return Resolution::Replace;
    }
    let name = qualified(&repo.schema, &repo.name);
    let mut alters = Vec::new();
    push_options(
        &mut alters,
        &format!("ALTER FOREIGN TABLE {name}"),
        &repo.options,
        &db.options,
    );
    push_comment(
        &mut alters,
        "FOREIGN TABLE",
        &name,
        &repo.comment,
        &db.comment,
    );
    Resolution::Statements(alters)
}

/// A copy of `columns` with each data type canonicalized, so an alias
/// (`int4` vs `integer`) does not force an unnecessary rebuild
fn canonicalize_columns(columns: &Option<Vec<Column>>) -> Vec<Column> {
    columns
        .iter()
        .flatten()
        .cloned()
        .map(|mut column| {
            column.data_type = canonical_type(&column.data_type);
            column
        })
        .collect()
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
    push_options(
        &mut alters,
        &format!("ALTER FOREIGN DATA WRAPPER {name}"),
        &repo.options,
        &db.options,
    );
    push_comment(
        &mut alters,
        "FOREIGN DATA WRAPPER",
        &name,
        &repo.comment,
        &db.comment,
    );
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
    push_options(
        &mut alters,
        &format!("ALTER SERVER {name}"),
        &repo.options,
        &db.options,
    );
    push_comment(&mut alters, "SERVER", &name, &repo.comment, &db.comment);
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
                push_options(
                    &mut alters,
                    &format!("ALTER USER MAPPING FOR {user} SERVER {name}"),
                    &server.options,
                    &db_options,
                );
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
            format!("COMMENT ON {desc} {name} IS {};\n", dollar_quote(comment))
        }
        None => format!("COMMENT ON {desc} {name} IS NULL;\n"),
    }
}

/// Push a `COMMENT ON` alter for `desc`/`name` when `repo` and `db`
/// comments differ. Shared by every resolver whose only comment
/// handling is this exact comparison.
fn push_comment(
    alters: &mut Vec<Alter>,
    desc: &str,
    name: &str,
    repo: &Option<String>,
    db: &Option<String>,
) {
    if repo != db {
        alters.push(Alter::new(comment_on(desc, name, repo.as_deref())));
    }
}

/// Push an `ALTER ... OPTIONS (...)` alter when `repo` and `db`
/// options differ. `prefix` is the full `ALTER <TYPE> <target>` clause
/// preceding `OPTIONS`.
fn push_options(
    alters: &mut Vec<Alter>,
    prefix: &str,
    repo: &Option<Map<String, Value>>,
    db: &Option<Map<String, Value>>,
) {
    if let Some(clause) = options_delta(repo, db) {
        alters.push(Alter::new(format!("{prefix} OPTIONS ({clause});\n")));
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

    /// A change of enforcement alone drops and re-adds the check; the
    /// re-add must keep NOT ENFORCED, or deploy never converges
    #[test]
    fn not_enforced_check_keeps_its_clause() {
        let mut repo = base_table();
        repo["check_constraints"] = serde_json::json!([
            {"name": "email_has_at", "expression": "email ~ '@'",
             "enforced": false},
        ]);
        let mut db = base_table();
        db["check_constraints"] = serde_json::json!([
            {"name": "email_has_at", "expression": "email ~ '@'"},
        ]);
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users DROP CONSTRAINT email_has_at;\n",
                "ALTER TABLE test.users ADD CONSTRAINT email_has_at CHECK \
                 (email ~ '@') NOT ENFORCED;\n",
            ]
        );
    }

    /// Validating a NOT VALID constraint keeps it and runs VALIDATE,
    /// rather than dropping and re-adding it, which rescans the whole
    /// table under a heavier lock than VALIDATE takes
    #[test]
    fn not_valid_constraints_validate_in_place() {
        let tables = |not_valid: Option<bool>| {
            let mut t = base_table();
            t["columns"] = serde_json::json!([
                {"name": "id", "data_type": "uuid", "nullable": false},
                {"name": "email", "data_type": "text"},
                {"name": "owner", "data_type": "uuid"},
            ]);
            t["check_constraints"] = serde_json::json!([
                {"name": "email_has_at", "expression": "email ~ '@'",
                 "not_valid": not_valid},
            ]);
            t["foreign_keys"] = serde_json::json!([
                {"name": "users_owner", "columns": ["owner"],
                 "references": {"name": "test.users", "columns": ["id"]},
                 "not_valid": not_valid},
            ]);
            // a local column's NOT NULL: table-level only while NOT VALID
            if not_valid == Some(true) {
                t["not_null_constraints"] = serde_json::json!([
                    {"name": "users_email_nn", "column": "email",
                     "not_valid": true},
                ]);
            } else {
                t["columns"][1]["nullable"] = serde_json::json!(false);
                t["columns"][1]["not_null_constraint"] =
                    serde_json::json!({"name": "users_email_nn"});
            }
            // null not_valid means absent, as a pull writes it
            let mut value = t;
            for key in ["check_constraints", "foreign_keys"] {
                for c in value[key].as_array_mut().unwrap() {
                    if c["not_valid"].is_null() {
                        c.as_object_mut().unwrap().remove("not_valid");
                    }
                }
            }
            parse_table(value)
        };
        let alters = statements(table(&tables(None), &tables(Some(true))));
        let mut got = sql(&alters);
        got.sort_unstable();
        assert_eq!(
            got,
            vec![
                "ALTER TABLE test.users VALIDATE CONSTRAINT email_has_at;\n",
                "ALTER TABLE test.users VALIDATE CONSTRAINT users_email_nn;\n",
                "ALTER TABLE test.users VALIDATE CONSTRAINT users_owner;\n",
            ]
        );
        assert!(alters.iter().all(|a| !a.destructive));
    }

    /// A valid table-level NOT NULL on a local column is the constraint
    /// the database reports on the column, written the other way, so
    /// the two are not a change
    #[test]
    fn local_not_null_written_either_way_is_not_a_change() {
        let mut repo = base_table();
        repo["columns"] = serde_json::json!([
            {"name": "id", "data_type": "uuid", "nullable": false},
            {"name": "email", "data_type": "text"},
        ]);
        repo["not_null_constraints"] = serde_json::json!([
            {"name": "users_email_nn", "column": "email"},
        ]);
        let mut db = base_table();
        db["columns"] = serde_json::json!([
            {"name": "id", "data_type": "uuid", "nullable": false},
            {"name": "email", "data_type": "text", "nullable": false,
             "not_null_constraint": {"name": "users_email_nn"}},
        ]);
        assert!(
            statements(table(&parse_table(repo), &parse_table(db))).is_empty()
        );
    }

    /// `base_table` with its `id` column's generation set, or cleared
    /// when `generated` is null
    fn with_id_generation(generated: serde_json::Value) -> Table {
        let mut t = base_table();
        let mut id = serde_json::json!(
            {"name": "id", "data_type": "integer", "nullable": false}
        );
        if !generated.is_null() {
            id["generated"] = generated;
        }
        t["columns"] = serde_json::json!([id]);
        parse_table(t)
    }

    /// An identity changes in place. Before this, any difference in
    /// `generated` rebuilt the table, which drops its rows to change a
    /// sequence option.
    #[test]
    fn identity_changes_in_place() {
        let repo = with_id_generation(serde_json::json!({
            "sequence_behavior": "BY DEFAULT",
            "sequence_options": {"start_with": 100, "increment_by": 5,
                                 "cycle": true},
        }));
        let db = with_id_generation(
            serde_json::json!({"sequence_behavior": "ALWAYS"}),
        );
        let alters = statements(table(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users ALTER COLUMN id SET GENERATED BY \
                 DEFAULT SET START WITH 100 SET INCREMENT BY 5 SET CYCLE;\n"
            ]
        );
        assert!(!alters[0].destructive);
    }

    /// An option the repo leaves out is PostgreSQL's default, so it is
    /// set back explicitly rather than left as the database has it
    #[test]
    fn identity_resets_an_omitted_option_to_its_default() {
        let repo = with_id_generation(
            serde_json::json!({"sequence_behavior": "ALWAYS"}),
        );
        let db = with_id_generation(serde_json::json!({
            "sequence_behavior": "ALWAYS",
            "sequence_options": {"start_with": 100, "max_value": 900,
                                 "cache": 20, "cycle": true},
        }));
        assert_eq!(
            sql(&statements(table(&repo, &db))),
            vec![
                "ALTER TABLE test.users ALTER COLUMN id SET START WITH 1 SET \
                 NO MAXVALUE SET CACHE 1 SET NO CYCLE;\n"
            ]
        );
    }

    #[test]
    fn identity_is_added_in_place() {
        let repo = with_id_generation(serde_json::json!({
            "sequence_behavior": "ALWAYS",
            "sequence_options": {"start_with": 10},
        }));
        let db = with_id_generation(serde_json::Value::Null);
        let alters = statements(table(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users ALTER COLUMN id ADD GENERATED ALWAYS \
                 AS IDENTITY (START WITH 10);\n"
            ]
        );
        assert!(!alters[0].destructive);
    }

    /// Dropping an identity keeps every row but loses the sequence and
    /// its position, so it is gated. It is also what a project pulled
    /// before identity columns were modeled asks for on every one of
    /// them, and the gate is what stops a deploy of such a project from
    /// stripping them all.
    #[test]
    fn identity_drop_is_gated() {
        let repo = with_id_generation(serde_json::Value::Null);
        let db = with_id_generation(
            serde_json::json!({"sequence_behavior": "ALWAYS"}),
        );
        let alters = statements(table(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec!["ALTER TABLE test.users ALTER COLUMN id DROP IDENTITY;\n"]
        );
        assert!(alters[0].destructive);
    }

    /// PostgreSQL rejects SET DEFAULT and DROP NOT NULL on an identity
    /// column, so the drop comes first, and both are gated with it
    #[test]
    fn identity_drop_precedes_default_and_nullability() {
        let mut repo = with_id_generation(serde_json::Value::Null);
        let column = &mut repo.columns.as_mut().unwrap()[0];
        column.nullable = None;
        column.default =
            Some(serde_json::json!("nextval('test.users_id'::regclass)"));
        let db = with_id_generation(
            serde_json::json!({"sequence_behavior": "ALWAYS"}),
        );
        let alters = statements(table(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users ALTER COLUMN id DROP IDENTITY;\n",
                "ALTER TABLE test.users ALTER COLUMN id SET DEFAULT \
                 nextval('test.users_id'::regclass);\n",
                "ALTER TABLE test.users ALTER COLUMN id DROP NOT NULL;\n",
            ]
        );
        assert!(alters.iter().all(|alter| alter.destructive));
    }

    /// An older project file names a separately managed sequence in
    /// `sequence` and never renders it, so it matches a pulled identity
    /// with the same behavior
    #[test]
    fn legacy_identity_sequence_name_is_not_a_change() {
        let repo = with_id_generation(serde_json::json!({
            "sequence": "users_id",
            "sequence_behavior": "ALWAYS",
        }));
        let db = with_id_generation(
            serde_json::json!({"sequence_behavior": "ALWAYS"}),
        );
        assert!(statements(table(&repo, &db)).is_empty());
    }

    /// A NOT NULL rename reconciles even though nullability itself is
    /// unchanged: from PostgreSQL 18 the constraint is named, so the
    /// name is part of the state deploy has to converge. It renames in
    /// place rather than dropping and re-adding, because DROP NOT NULL
    /// is rejected on a primary-key column. The database side carries
    /// no name, so it holds PostgreSQL's generated one
    #[test]
    fn column_not_null_rename_reconciles() {
        let mut repo = base_table();
        repo["columns"] = serde_json::json!([
            {"name": "email", "data_type": "text", "nullable": false,
             "not_null_constraint": {"name": "email_nn"}},
        ]);
        let mut db = base_table();
        db["columns"] = serde_json::json!([
            {"name": "email", "data_type": "text", "nullable": false},
        ]);
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users RENAME CONSTRAINT \
                 users_email_not_null TO email_nn;\n",
            ]
        );
    }

    /// A NO INHERIT change alone reconciles through ALTER CONSTRAINT,
    /// under the name the repository records
    #[test]
    fn column_not_null_no_inherit_reconciles() {
        let mut repo = base_table();
        repo["columns"] = serde_json::json!([
            {"name": "email", "data_type": "text", "nullable": false,
             "not_null_constraint": {"name": "email_nn",
                                     "no_inherit": true}},
        ]);
        let mut db = base_table();
        db["columns"] = serde_json::json!([
            {"name": "email", "data_type": "text", "nullable": false,
             "not_null_constraint": {"name": "email_nn"}},
        ]);
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users ALTER CONSTRAINT email_nn NO \
                 INHERIT;\n",
            ]
        );
    }

    /// Adding NOT NULL where the database has none still goes through
    /// ADD ... NOT NULL, carrying the name and NO INHERIT
    #[test]
    fn column_not_null_added_with_name() {
        let mut repo = base_table();
        repo["columns"] = serde_json::json!([
            {"name": "email", "data_type": "text", "nullable": false,
             "not_null_constraint": {"name": "email_nn",
                                     "no_inherit": true}},
        ]);
        let mut db = base_table();
        db["columns"] = serde_json::json!([
            {"name": "email", "data_type": "text"},
        ]);
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users ADD CONSTRAINT email_nn NOT NULL \
                 email NO INHERIT;\n",
            ]
        );
    }

    /// A default on an inherited column reconciles through
    /// `ALTER TABLE ONLY`, keyed on the column: changed defaults
    /// re-SET, and one the repo dropped is DROPped
    #[test]
    fn column_defaults_reconcile_by_column() {
        let mut repo = base_table();
        repo["column_defaults"] = serde_json::json!([
            {"column": "recorded_at", "default": "CURRENT_TIMESTAMP"},
        ]);
        let mut db = base_table();
        db["column_defaults"] = serde_json::json!([
            {"column": "recorded_at", "default": "now()"},
            {"column": "detail", "default": "'none'::text"},
        ]);
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE ONLY test.users ALTER COLUMN recorded_at \
                 DROP DEFAULT;\n",
                "ALTER TABLE ONLY test.users ALTER COLUMN detail \
                 DROP DEFAULT;\n",
                "ALTER TABLE ONLY test.users ALTER COLUMN recorded_at \
                 SET DEFAULT CURRENT_TIMESTAMP;\n",
            ]
        );
    }

    /// Table-level NOT NULL keys on the column rather than the
    /// constraint name, so an unnamed one still reconciles; DROP needs
    /// no name, and ADD carries one only when the model holds it
    #[test]
    fn not_null_constraints_reconcile_by_column() {
        let mut repo = base_table();
        repo["not_null_constraints"] = serde_json::json!([
            {"name": "sku_nn", "column": "sku", "no_inherit": true},
            {"column": "qty"},
        ]);
        let mut db = base_table();
        db["not_null_constraints"] = serde_json::json!([
            {"column": "sku"},
        ]);
        let alters = statements(table(&parse_table(repo), &parse_table(db)));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users ALTER COLUMN sku DROP NOT NULL;\n",
                "ALTER TABLE test.users ADD CONSTRAINT sku_nn NOT NULL sku \
                 NO INHERIT;\n",
                "ALTER TABLE test.users ADD NOT NULL qty;\n",
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
    fn function_returns_alias_uses_or_replace() {
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
        // a repo `returns: int4` against the server's `integer` must
        // not force a drop+recreate
        assert!(matches!(
            resolve(&f("int4"), &f("integer")),
            Resolution::OrReplace { .. }
        ));
    }

    #[test]
    fn function_out_parameter_change_replaces() {
        let f = |data_type: &str| -> Definition {
            Definition::Function(
                serde_json::from_value(serde_json::json!({
                    "name": "f", "schema": "test", "owner": "postgres",
                    "returns": "record", "language": "sql",
                    "definition": "SELECT 1",
                    "parameters": [
                        {"mode": "OUT", "name": "x", "data_type": data_type},
                    ],
                }))
                .unwrap(),
            )
        };
        // CREATE OR REPLACE FUNCTION cannot change the output
        // signature, so a changed OUT parameter must fall back to a
        // gated drop+recreate rather than OrReplace
        assert!(matches!(
            resolve(&f("bigint"), &f("integer")),
            Resolution::Replace
        ));
    }

    #[test]
    fn function_out_parameter_alias_uses_or_replace() {
        let f = |data_type: &str| -> Definition {
            Definition::Function(
                serde_json::from_value(serde_json::json!({
                    "name": "f", "schema": "test", "owner": "postgres",
                    "returns": "record", "language": "sql",
                    "definition": "SELECT 1",
                    "parameters": [
                        {"mode": "OUT", "name": "x", "data_type": data_type},
                    ],
                }))
                .unwrap(),
            )
        };
        // an aliased OUT parameter type must not force a rebuild
        assert!(matches!(
            resolve(&f("int4"), &f("integer")),
            Resolution::OrReplace { .. }
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
    fn domain_type_alias_is_not_replace() {
        let db: Domain = serde_json::from_value(serde_json::json!({
            "name": "d", "schema": "test", "owner": "postgres",
            "data_type": "integer",
        }))
        .unwrap();
        let mut repo = db.clone();
        repo.data_type = Some("int4".into());
        assert!(matches!(
            domain(&repo, &db),
            Resolution::Statements(ref alters) if alters.is_empty()
        ));
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
    fn foreign_table_column_type_alias_is_not_replace() {
        let repo = parse_table(foreign_table_value(
            serde_json::json!({"table_name": "t"}),
            None,
        ));
        let mut db = repo.clone();
        db.columns = Some(vec![Column {
            name: "id".into(),
            data_type: "int4".into(),
            nullable: None,
            not_null_constraint: None,
            default: None,
            collation: None,
            check_constraint: None,
            generated: None,
            comment: None,
        }]);
        assert!(matches!(table(&repo, &db), Resolution::Statements(_)));
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

    fn with_security(
        state: serde_json::Value,
        policies: serde_json::Value,
    ) -> Table {
        let mut table = base_table();
        if !state.is_null() {
            table["row_level_security"] = state;
        }
        if !policies.is_null() {
            table["policies"] = policies;
        }
        parse_table(table)
    }

    fn gated(alters: &[Alter]) -> Vec<bool> {
        alters.iter().map(|a| a.destructive).collect()
    }

    #[test]
    fn enabling_row_security_is_included_and_disabling_is_gated() {
        let off = with_security(
            serde_json::json!({"enabled": false}),
            serde_json::Value::Null,
        );
        let on = with_security(
            serde_json::json!({"enabled": true, "forced": true}),
            serde_json::Value::Null,
        );
        let alters = statements(table(&on, &off));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users ENABLE ROW LEVEL SECURITY;\n",
                "ALTER TABLE ONLY test.users FORCE ROW LEVEL SECURITY;\n",
            ]
        );
        assert_eq!(gated(&alters), vec![false, false]);
        assert_eq!(
            alters[0].label.as_deref(),
            Some("ROW SECURITY test.users")
        );
        let alters = statements(table(&off, &on));
        assert_eq!(
            sql(&alters),
            vec![
                "ALTER TABLE test.users DISABLE ROW LEVEL SECURITY;\n",
                "ALTER TABLE ONLY test.users NO FORCE ROW LEVEL SECURITY;\n",
            ]
        );
        assert_eq!(gated(&alters), vec![true, true]);
        // the stricter state is kept when these are withheld
        assert!(alters.iter().all(|a| !a.fails_open));
    }

    #[test]
    fn policies_reconcile_one_labeled_statement_each() {
        let on = serde_json::json!({"enabled": true});
        let db = with_security(
            on.clone(),
            serde_json::json!([
                {"name": "open", "using": "true"},
                {"name": "only_mine", "restrictive": true,
                 "using": "(owner = CURRENT_USER)"},
                {"name": "wide", "using": "true"},
                {"name": "edited", "using": "(a = 1)"},
            ]),
        );
        let repo = with_security(
            on,
            serde_json::json!([
                {"name": "wide", "roles": ["alice"], "using": "true"},
                {"name": "edited", "using": "(a = 2)", "comment": "c"},
                {"name": "fresh", "command": "select", "using": "true"},
            ]),
        );
        let alters = statements(table(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec![
                "DROP POLICY IF EXISTS only_mine ON test.users;\n",
                "DROP POLICY IF EXISTS open ON test.users;\n",
                "DROP POLICY IF EXISTS edited ON test.users;\n\
                 CREATE POLICY edited ON test.users USING ((a = 2));\n\
                 COMMENT ON POLICY edited ON test.users IS $$c$$;\n",
                "CREATE POLICY fresh ON test.users FOR SELECT USING (true);\n",
                "ALTER POLICY wide ON test.users TO alice;\n",
            ]
        );
        // dropping a permissive policy and narrowing one tighten access;
        // dropping a restrictive one and editing an expression can open it
        assert_eq!(gated(&alters), vec![true, false, true, false, false]);
        let fails_open: Vec<bool> =
            alters.iter().map(|a| a.fails_open).collect();
        assert_eq!(fails_open, vec![false, false, true, false, false]);
        assert_eq!(alters[1].label.as_deref(), Some("POLICY test.users open"));
    }

    #[test]
    fn widening_roles_is_gated() {
        let on = serde_json::json!({"enabled": true});
        let db = with_security(
            on.clone(),
            serde_json::json!([{"name": "p", "roles": ["alice"]}]),
        );
        let repo = with_security(
            on,
            serde_json::json!([{"name": "p", "roles": ["alice", "bob"]}]),
        );
        let alters = statements(table(&repo, &db));
        assert_eq!(gated(&alters), vec![true]);
        // withheld, the database keeps the narrower policy
        assert!(!alters[0].fails_open);
    }

    #[test]
    fn widening_a_restrictive_policy_is_included() {
        let on = serde_json::json!({"enabled": true});
        let db = with_security(
            on.clone(),
            serde_json::json!([
                {"name": "p", "restrictive": true, "roles": ["alice"]}
            ]),
        );
        let repo = with_security(
            on,
            serde_json::json!([{"name": "p", "restrictive": true}]),
        );
        let alters = statements(table(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec!["ALTER POLICY p ON test.users TO PUBLIC;\n"]
        );
        assert_eq!(gated(&alters), vec![false]);
    }

    #[test]
    fn making_a_policy_restrictive_fails_open_when_withheld() {
        let on = serde_json::json!({"enabled": true});
        let db = with_security(on.clone(), serde_json::json!([{"name": "p"}]));
        let repo = with_security(
            on,
            serde_json::json!([{"name": "p", "restrictive": true}]),
        );
        let alters = statements(table(&repo, &db));
        assert_eq!(gated(&alters), vec![true]);
        assert!(alters[0].fails_open);
        // and the reverse opens access, so withholding it does not
        let alters = statements(table(&db, &repo));
        assert!(!alters[0].fails_open);
    }

    #[test]
    fn policy_comment_alone_is_altered_in_place() {
        let on = serde_json::json!({"enabled": true});
        let db = with_security(
            on.clone(),
            serde_json::json!([{"name": "p", "comment": "old"}]),
        );
        let repo = with_security(on, serde_json::json!([{"name": "p"}]));
        let alters = statements(table(&repo, &db));
        assert_eq!(
            sql(&alters),
            vec!["COMMENT ON POLICY p ON test.users IS NULL;\n"]
        );
        assert_eq!(gated(&alters), vec![false]);
    }

    #[test]
    fn written_defaults_and_order_are_not_a_change() {
        let on = serde_json::json!({"enabled": true, "forced": false});
        let repo = with_security(
            on,
            serde_json::json!([
                {"name": "b", "command": "ALL", "roles": ["public"],
                 "restrictive": false},
                {"name": "a"},
            ]),
        );
        let db = with_security(
            serde_json::json!({"enabled": true}),
            serde_json::json!([{"name": "a"}, {"name": "b"}]),
        );
        assert!(sql(&statements(table(&repo, &db))).is_empty());
    }

    fn with_key(key: &str, value: serde_json::Value) -> Table {
        let mut table = base_table();
        table[key] = value;
        parse_table(table)
    }

    #[test]
    fn exclude_constraint_comment_alone_is_set_in_place() {
        let constraint = |comment: Option<&str>| {
            let mut c = serde_json::json!({
                "name": "no_overlap", "method": "gist",
                "elements": [{"name": "email", "operator": "="}],
            });
            if let Some(comment) = comment {
                c["comment"] = serde_json::json!(comment);
            }
            serde_json::json!([c])
        };
        let db = with_key("exclude_constraints", constraint(Some("old")));
        let repo = with_key("exclude_constraints", constraint(Some("new")));
        assert_eq!(
            sql(&statements(table(&repo, &db))),
            vec![
                "COMMENT ON CONSTRAINT no_overlap ON test.users IS $$new$$;\n"
            ]
        );
        // a changed element rebuilds the constraint, and the comment is
        // set on the new one
        let mut changed = constraint(Some("new"));
        changed[0]["elements"][0]["operator"] = serde_json::json!("<>");
        let repo = with_key("exclude_constraints", changed);
        assert_eq!(
            sql(&statements(table(&repo, &db))),
            vec![
                "ALTER TABLE test.users DROP CONSTRAINT no_overlap;\n",
                "ALTER TABLE test.users ADD CONSTRAINT no_overlap EXCLUDE \
                 USING gist (email WITH <>);\n",
                "COMMENT ON CONSTRAINT no_overlap ON test.users IS $$new$$;\n",
            ]
        );
    }

    #[test]
    fn replica_identity_is_altered_in_place() {
        let full = with_key("replica_identity", serde_json::json!("FULL"));
        let index = with_key(
            "replica_identity",
            serde_json::json!({"index": "users_email"}),
        );
        let default = parse_table(base_table());
        assert_eq!(
            sql(&statements(table(&index, &full))),
            vec![
                "ALTER TABLE ONLY test.users REPLICA IDENTITY USING INDEX \
                 users_email;\n"
            ]
        );
        assert_eq!(
            sql(&statements(table(&default, &full))),
            vec!["ALTER TABLE ONLY test.users REPLICA IDENTITY DEFAULT;\n"]
        );
        // DEFAULT written out is the default
        let written =
            with_key("replica_identity", serde_json::json!("default"));
        assert!(sql(&statements(table(&written, &default))).is_empty());
    }

    #[test]
    fn rebuilt_identity_index_sets_the_identity_again() {
        let indexed = |unique: bool| {
            let mut table = base_table();
            table["indexes"] = serde_json::json!([{
                "name": "users_email", "unique": unique,
                "columns": [{"name": "email"}],
            }]);
            table["replica_identity"] =
                serde_json::json!({"index": "users_email"});
            parse_table(table)
        };
        let (repo, db) = (indexed(true), indexed(false));
        let alters = statements(table(&repo, &db));
        let rendered = sql(&alters);
        assert_eq!(rendered.len(), 3, "{rendered:?}");
        assert!(rendered[0].starts_with("DROP INDEX"));
        assert!(rendered[1].starts_with("CREATE UNIQUE INDEX"));
        assert_eq!(
            rendered[2],
            "ALTER TABLE ONLY test.users REPLICA IDENTITY USING INDEX \
             users_email;\n"
        );
        // an unchanged identity index is left alone
        assert!(sql(&statements(table(&repo, &repo))).is_empty());
    }

    #[test]
    fn exclude_constraint_without_method_is_btree() {
        let constraint = |method: Option<&str>| {
            let mut c = serde_json::json!({
                "name": "no_overlap",
                "elements": [{"name": "email", "operator": "="}],
            });
            if let Some(method) = method {
                c["method"] = serde_json::json!(method);
            }
            serde_json::json!([c])
        };
        let db = with_key("exclude_constraints", constraint(Some("btree")));
        let repo = with_key("exclude_constraints", constraint(None));
        assert!(sql(&statements(table(&repo, &db))).is_empty());
    }
}
