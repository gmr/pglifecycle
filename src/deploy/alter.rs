//! In-place ALTER renderers for objects that exist in both the
//! project and the database but differ. Each resolver returns either
//! the statements that reconcile the database in place or
//! [`Resolution::Replace`], the gated drop+recreate fallback the
//! caller assembles from the build archive's entries.
//!
//! Index, trigger, and constraint drops are not gated: they lose no
//! data and the repo is authoritative. Only data-destructive
//! statements (DROP COLUMN, ALTER COLUMN TYPE) are.

use crate::build;
use crate::deploy::diff::canonical_type;
use crate::models::{
    CheckConstraint, Column, Definition, ForeignKey, Index, Table, Trigger,
};
use crate::utils::quote_ident;

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
    /// No in-place form exists (or is implemented yet): drop and
    /// recreate from the repo definition, gated behind --allow-drop
    Replace,
}

/// Resolve a changed object into in-place statements where supported
pub(crate) fn resolve(repo: &Definition, database: &Definition) -> Resolution {
    match (repo, database) {
        (Definition::Table(repo), Definition::Table(database)) => {
            table(repo, database)
        }
        _ => Resolution::Replace,
    }
}

fn qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}

fn table(repo: &Table, db: &Table) -> Resolution {
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
        |index| format!("{};\n", build::render_index(index, table).join(" ")),
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
            format!("{};\n", build::render_trigger(trigger, table).0.join(" "))
        },
    );
    true
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
            Resolution::Replace => panic!("expected in-place statements"),
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
    fn non_table_definitions_replace() {
        let schema: models::Schema =
            serde_json::from_value(serde_json::json!(
                {"name": "test", "owner": "postgres"}
            ))
            .unwrap();
        assert!(matches!(
            resolve(
                &Definition::Schema(schema.clone()),
                &Definition::Schema(schema)
            ),
            Resolution::Replace
        ));
    }
}
