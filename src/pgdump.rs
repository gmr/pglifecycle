//! pg_dump / pg_dumpall subprocess wrappers (ports pgdump.py)

use std::path::Path;
use std::process::Command;

use crate::cli;

/// DDL suppression flags passed through to pg_dump
#[derive(Default)]
pub struct DumpDdl {
    pub no_owner: bool,
    pub no_privileges: bool,
    pub no_security_labels: bool,
    pub no_tablespaces: bool,
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
    for (flag, enabled) in [
        ("--no-owner", ddl.no_owner),
        ("--no-privileges", ddl.no_privileges),
        ("--no-security-labels", ddl.no_security_labels),
        ("--no-tablespaces", ddl.no_tablespaces),
    ] {
        if enabled {
            command.arg(flag);
        }
    }
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
