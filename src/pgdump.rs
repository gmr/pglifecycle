//! pg_dump / pg_dumpall subprocess wrappers (ports pgdump.py)

use std::path::Path;
use std::process::{Command, Stdio};

use crate::cli;

/// DDL suppression flags and object exclusions passed through to
/// pg_dump
#[derive(Default)]
pub struct DumpDdl {
    pub no_owner: bool,
    pub no_privileges: bool,
    pub no_security_labels: bool,
    pub no_tablespaces: bool,
    /// `--exclude-table` patterns (also match views, materialized
    /// views, and sequences, as in pg_dump)
    pub exclude_tables: Vec<String>,
    /// `--exclude-schema` patterns
    pub exclude_schemas: Vec<String>,
    /// `--exclude-extension` patterns
    pub exclude_extensions: Vec<String>,
}

/// Dump the database schema described by the connection options to
/// `path` as a custom-format archive
pub fn dump(
    conn: &cli::Connection,
    ddl: &DumpDdl,
    path: &Path,
) -> Result<(), String> {
    let mut command = Command::new("pg_dump");
    connection_args(&mut command, conn);
    if let Some(dbname) = &conn.dbname {
        command.arg("-d").arg(dbname);
    }
    command.arg("-f").arg(path);
    command.arg("-Fc");
    command.arg("--schema-only");
    command.args(ddl_args(ddl));
    execute(command)
}

/// Dump cluster roles to `path` as SQL via `pg_dumpall --roles-only`
pub fn dump_roles(conn: &cli::Connection, path: &Path) -> Result<(), String> {
    let mut command = Command::new("pg_dumpall");
    connection_args(&mut command, conn);
    command.arg("-f").arg(path);
    command.arg("-r");
    execute(command)
}

/// Apply a SQL script to the database in a single transaction via
/// `psql`, aborting on the first error. Returns psql's stderr on
/// failure so the caller can map it back to a statement.
pub fn apply(conn: &cli::Connection, script: &Path) -> Result<(), String> {
    let mut command = Command::new("psql");
    connection_args(&mut command, conn);
    // connection_args may add -W when a password prompt is requested;
    // inherit stdin so psql can read the prompted password (output()
    // otherwise closes stdin)
    command.stdin(Stdio::inherit());
    if let Some(dbname) = &conn.dbname {
        command.arg("-d").arg(dbname);
    }
    command.arg("-X");
    command.arg("-q");
    command.arg("--single-transaction");
    command.arg("-v").arg("ON_ERROR_STOP=1");
    command.arg("-f").arg(script);
    log::debug!("Executing {command:?}");
    let output = command.output().map_err(|e| {
        format!("failed to run {:?}: {e}", command.get_program())
    })?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr)
            .trim()
            .to_string());
    }
    Ok(())
}

/// The DDL-suppression and object-exclusion flags for a pg_dump
/// invocation, in a stable order
fn ddl_args(ddl: &DumpDdl) -> Vec<String> {
    let mut args = Vec::new();
    for (flag, enabled) in [
        ("--no-owner", ddl.no_owner),
        ("--no-privileges", ddl.no_privileges),
        ("--no-security-labels", ddl.no_security_labels),
        ("--no-tablespaces", ddl.no_tablespaces),
    ] {
        if enabled {
            args.push(flag.to_string());
        }
    }
    for (flag, patterns) in [
        ("--exclude-table", &ddl.exclude_tables),
        ("--exclude-schema", &ddl.exclude_schemas),
        ("--exclude-extension", &ddl.exclude_extensions),
    ] {
        for pattern in patterns {
            args.push(flag.to_string());
            args.push(pattern.clone());
        }
    }
    args
}

fn connection_args(command: &mut Command, conn: &cli::Connection) {
    command.arg("-h").arg(&conn.host);
    command.arg("-p").arg(conn.port.to_string());
    if let Some(username) = &conn.username {
        command.arg("-U").arg(username);
    }
    if conn.no_password {
        command.arg("-w");
    }
    if conn.password {
        command.arg("-W");
    }
    if let Some(role) = &conn.role {
        command.arg("--role").arg(role);
    }
}

fn execute(mut command: Command) -> Result<(), String> {
    log::debug!("Executing {command:?}");
    let output = command.output().map_err(|e| {
        format!("failed to run {:?}: {e}", command.get_program())
    })?;
    if !output.status.success() {
        return Err(format!(
            "Failed to dump ({}): {}",
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_args_emits_suppressions_and_exclusions() {
        let ddl = DumpDdl {
            no_owner: true,
            no_privileges: false,
            no_security_labels: false,
            no_tablespaces: true,
            exclude_tables: vec!["public.big".into(), "report.*_vw".into()],
            exclude_schemas: vec!["pgq".into()],
            exclude_extensions: vec!["pg_cron".into()],
        };
        assert_eq!(
            ddl_args(&ddl),
            vec![
                "--no-owner",
                "--no-tablespaces",
                "--exclude-table",
                "public.big",
                "--exclude-table",
                "report.*_vw",
                "--exclude-schema",
                "pgq",
                "--exclude-extension",
                "pg_cron",
            ]
        );
    }

    #[test]
    fn ddl_args_empty_by_default() {
        assert!(ddl_args(&DumpDdl::default()).is_empty());
    }
}
