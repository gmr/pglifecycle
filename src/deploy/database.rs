//! The comment, the security labels and the settings of the database,
//! and the settings of each role in the database: the `comment`,
//! `security_labels`, `settings` and `role_settings` of `project.yaml`
//! compared with the COMMENT, SECURITY LABEL and DATABASE PROPERTIES
//! entries of the snapshot.
//!
//! A comment that is different or that only the project has gets
//! `COMMENT ON DATABASE`, and a comment that only the database has
//! gets `IS NULL`, as for the comment of an object. The removal is not
//! destructive: it loses no data. The security labels change in the
//! same way.
//!
//! A setting that is different or that only the project has gets
//! `SET`, and a setting that only the database has gets `RESET`. A
//! RESET is not destructive: it loses no data, and a changed value
//! replaces the value of the database as much as a RESET does. The
//! statements name the database of the snapshot, because a statement
//! cannot name the current database in another way. They come after
//! all other statements: a setting changes only the sessions that
//! start later, and each object that a value names then exists.

use serde_json::{Map, Value};

use super::Statement;
use super::alter::comment_on;
use super::alter::security_label;
use crate::models::canonical_settings;
use crate::project::DatabaseSettings;
use crate::pull::Assembly;
use crate::utils::{quote_ident, setting_value};

/// The statements that make the comment, the security labels and the
/// settings of the database `assembly` those of the project, and each setting that
/// they reset, as `name of label`
pub(super) fn statements(
    project: &DatabaseSettings,
    assembly: &Assembly,
) -> (Vec<Statement>, Vec<String>) {
    let database = quote_ident(&assembly.dbname);
    let label = format!("DATABASE {}", assembly.dbname);
    let mut resets = Vec::new();
    let mut statements = Vec::new();
    if project.comment != assembly.comment {
        statements.push(Statement {
            label: label.clone(),
            sql: comment_on("DATABASE", &database, project.comment.as_deref()),
            fails_open: false,
        });
    }
    statements.extend(
        security_label::changes(
            "DATABASE",
            &database,
            project.security_labels.as_ref(),
            assembly.security_labels.as_ref(),
        )
        .into_iter()
        .map(|sql| Statement {
            label: label.clone(),
            sql,
            fails_open: false,
        }),
    );
    statements.extend(changes(
        &label,
        &format!("ALTER DATABASE {database}"),
        &flatten(&project.database),
        &assembly.settings,
        &mut resets,
    ));
    let empty = Map::new();
    let mut roles: Vec<&String> = project
        .roles
        .keys()
        .chain(assembly.role_settings.keys())
        .collect();
    roles.sort();
    roles.dedup();
    for role in roles {
        let wanted = project.roles.get(role).map(|l| flatten(l));
        statements.extend(changes(
            &format!("ROLE {role} IN DATABASE {}", assembly.dbname),
            &format!(
                "ALTER ROLE {} IN DATABASE {database}",
                quote_ident(role)
            ),
            wanted.as_ref().unwrap_or(&empty),
            assembly.role_settings.get(role).unwrap_or(&empty),
            &mut resets,
        ));
    }
    (statements, resets)
}

/// The `{ name: value }` objects of a `settings` list as one map. For
/// a name that is in the list more than one time, the last value is
/// the value, as PostgreSQL keeps it
fn flatten(list: &[Map<String, Value>]) -> Map<String, Value> {
    list.iter()
        .flatten()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// SET for each setting of `wanted` that `existing` does not have in
/// the same form, and RESET for each setting that only `existing` has.
/// The names and the values compare as [`canonical_settings`] gives
/// them. Each name is quoted as pg_dump quotes it. Each reset goes in
/// `resets`
fn changes(
    label: &str,
    prefix: &str,
    wanted: &Map<String, Value>,
    existing: &Map<String, Value>,
    resets: &mut Vec<String>,
) -> Vec<Statement> {
    let canonical_wanted = canonical_settings(wanted);
    let canonical_existing = canonical_settings(existing);
    let statement = |sql: String| Statement {
        label: label.to_string(),
        sql,
        fails_open: false,
    };
    let mut statements = Vec::new();
    for (name, value) in wanted {
        let key = name.to_lowercase();
        if canonical_existing.get(&key) != canonical_wanted.get(&key) {
            statements.push(statement(format!(
                "{prefix} SET {} TO {};\n",
                quote_ident(name),
                setting_value(value)
            )));
        }
    }
    for name in existing.keys() {
        if !canonical_wanted.contains_key(&name.to_lowercase()) {
            statements.push(statement(format!(
                "{prefix} RESET {};\n",
                quote_ident(name)
            )));
            resets.push(format!("{name} of {label}"));
        }
    }
    statements
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn settings(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    fn assembly(dbname: &str) -> Assembly {
        let mut assembly = Assembly::default();
        assembly.dbname = dbname.to_string();
        assembly
    }

    fn sql(statements: &[Statement]) -> Vec<&str> {
        statements.iter().map(|s| s.sql.as_str()).collect()
    }

    /// A changed value and a setting that only the project has get
    /// SET, a setting that only the database has gets RESET, and a
    /// value in another form that PostgreSQL keeps the same is not a
    /// change
    #[test]
    fn database_settings_set_and_reset() {
        let project = DatabaseSettings {
            database: vec![
                settings(json!({"work_mem": "64MB"})),
                settings(json!({"TimeZone": "UTC"})),
                settings(json!({"search_path": ["$user", "public"]})),
                settings(json!({"enable_seqscan": false})),
                settings(json!({"statement_timeout": 1000})),
            ],
            ..Default::default()
        };
        let mut assembly = assembly("App DB");
        assembly.settings = settings(json!({
            "work_mem": "32MB",
            "timezone": "UTC",
            "enable_seqscan": "false",
            "statement_timeout": "1000",
            "gate.stray": "x",
        }));
        let (statements, resets) = statements(&project, &assembly);
        assert_eq!(
            sql(&statements),
            [
                "ALTER DATABASE \"App DB\" SET work_mem TO '64MB';\n",
                "ALTER DATABASE \"App DB\" SET search_path TO '$user', \
                 'public';\n",
                "ALTER DATABASE \"App DB\" RESET \"gate.stray\";\n",
            ]
        );
        assert!(statements.iter().all(|s| s.label == "DATABASE App DB"));
        assert_eq!(resets, ["gate.stray of DATABASE App DB"]);
    }

    /// The settings of a role in the database compare for each role
    /// that the project or the database has
    #[test]
    fn role_settings_set_and_reset() {
        let mut project = DatabaseSettings::default();
        project.roles.insert(
            String::from("App User"),
            vec![settings(json!({"statement_timeout": "5s"}))],
        );
        let mut assembly = assembly("app");
        assembly
            .role_settings
            .insert(String::from("old"), settings(json!({"work_mem": "1MB"})));
        let (statements, resets) = statements(&project, &assembly);
        assert_eq!(
            sql(&statements),
            [
                "ALTER ROLE \"App User\" IN DATABASE app SET \
                 statement_timeout TO '5s';\n",
                "ALTER ROLE old IN DATABASE app RESET work_mem;\n",
            ]
        );
        assert_eq!(statements[0].label, "ROLE App User IN DATABASE app");
        assert_eq!(resets, ["work_mem of ROLE old IN DATABASE app"]);
    }

    /// A changed comment, and a comment that only the project has, get
    /// COMMENT ON DATABASE, and a comment that only the database has
    /// gets IS NULL. The same comment is not a change
    #[test]
    fn database_comment_set_and_removed() {
        let comment = |project: Option<&str>, database: Option<&str>| {
            let project = DatabaseSettings {
                comment: project.map(String::from),
                ..Default::default()
            };
            let mut assembly = assembly("App DB");
            assembly.comment = database.map(String::from);
            let (statements, resets) = statements(&project, &assembly);
            assert!(resets.is_empty());
            assert!(statements.iter().all(|s| s.label == "DATABASE App DB"));
            statements.into_iter().map(|s| s.sql).collect::<Vec<_>>()
        };
        assert_eq!(
            comment(Some("it's new"), Some("old")),
            ["COMMENT ON DATABASE \"App DB\" IS $$it's new$$;\n"]
        );
        assert_eq!(
            comment(Some("new"), None),
            ["COMMENT ON DATABASE \"App DB\" IS $$new$$;\n"]
        );
        assert_eq!(
            comment(None, Some("old")),
            ["COMMENT ON DATABASE \"App DB\" IS NULL;\n"]
        );
        assert!(comment(Some("same"), Some("same")).is_empty());
        assert!(comment(None, None).is_empty());
    }

    /// The security labels of the database change as its comment does
    #[test]
    fn database_security_labels_set_and_removed() {
        let labels = |project: &[(&str, &str)], database: &[(&str, &str)]| {
            let map = |pairs: &[(&str, &str)]| {
                (!pairs.is_empty()).then(|| {
                    pairs
                        .iter()
                        .map(|(p, l)| (p.to_string(), l.to_string()))
                        .collect()
                })
            };
            let project = DatabaseSettings {
                security_labels: map(project),
                ..Default::default()
            };
            let mut assembly = assembly("App DB");
            assembly.security_labels = map(database);
            let (statements, _) = statements(&project, &assembly);
            assert!(statements.iter().all(|s| s.label == "DATABASE App DB"));
            statements.into_iter().map(|s| s.sql).collect::<Vec<_>>()
        };
        assert_eq!(
            labels(&[("dummy", "new")], &[("dummy", "old"), ("gone", "x")]),
            [
                "SECURITY LABEL FOR dummy ON DATABASE \"App DB\" IS $$new$$;\n",
                "SECURITY LABEL FOR gone ON DATABASE \"App DB\" IS NULL;\n",
            ]
        );
        assert!(labels(&[("dummy", "same")], &[("dummy", "same")]).is_empty());
        assert!(labels(&[], &[]).is_empty());
    }

    #[test]
    fn same_settings_have_no_statements() {
        let mut project = DatabaseSettings {
            database: vec![settings(json!({"work_mem": "64MB"}))],
            ..Default::default()
        };
        project
            .roles
            .insert(String::from("app"), vec![settings(json!({"x.y": "1"}))]);
        let mut assembly = assembly("app");
        assembly.settings = settings(json!({"work_mem": "64MB"}));
        assembly
            .role_settings
            .insert(String::from("app"), settings(json!({"x.y": "1"})));
        let (statements, resets) = statements(&project, &assembly);
        assert!(statements.is_empty());
        assert!(resets.is_empty());
    }

    /// A setting name is quoted as pg_dump quotes it: a name with a
    /// part that is a keyword, as `app.user`, does not parse bare
    #[test]
    fn setting_names_are_quoted() {
        let mut project = DatabaseSettings {
            database: vec![
                settings(json!({"app.user": "a"})),
                settings(json!({"app.\"q": "b"})),
            ],
            ..Default::default()
        };
        project
            .roles
            .insert(String::from("app"), vec![settings(json!({"x.y": "1"}))]);
        let mut assembly = assembly("app");
        assembly.settings =
            settings(json!({"DateStyle": "ISO, MDY", "x.\"r": "1"}));
        assembly
            .role_settings
            .insert(String::from("app"), settings(json!({"app.order": "1"})));
        let (statements, _) = statements(&project, &assembly);
        assert_eq!(
            sql(&statements),
            [
                "ALTER DATABASE app SET \"app.user\" TO 'a';\n",
                // a double quote in the name is doubled
                "ALTER DATABASE app SET \"app.\"\"q\" TO 'b';\n",
                "ALTER DATABASE app RESET \"DateStyle\";\n",
                "ALTER DATABASE app RESET \"x.\"\"r\";\n",
                "ALTER ROLE app IN DATABASE app SET \"x.y\" TO '1';\n",
                "ALTER ROLE app IN DATABASE app RESET \"app.order\";\n",
            ]
        );
    }
}
