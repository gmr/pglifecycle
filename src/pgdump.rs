//! pg_dump / pg_dumpall subprocess wrappers (ports pgdump.py)

use std::path::Path;
use std::process::Command;

use crate::cli;

/// Dump the database schema described by the connection options to
/// `path` as a custom-format archive
pub fn dump(args: &cli::Pull, path: &Path) -> Result<(), String> {
    let mut command = Command::new("pg_dump");
    connection_args(&mut command, args);
    if let Some(dbname) = &args.dbname {
        command.arg("-d").arg(dbname);
    }
    command.arg("-f").arg(path);
    command.arg("-Fc");
    command.arg("--schema-only");
    for (flag, enabled) in [
        ("--no-owner", args.no_owner),
        ("--no-privileges", args.no_privileges),
        ("--no-security-labels", args.no_security_labels),
        ("--no-tablespaces", args.no_tablespaces),
    ] {
        if enabled {
            command.arg(flag);
        }
    }
    execute(command)
}

/// Dump cluster roles to `path` as SQL via `pg_dumpall --roles-only`
pub fn dump_roles(args: &cli::Pull, path: &Path) -> Result<(), String> {
    let mut command = Command::new("pg_dumpall");
    connection_args(&mut command, args);
    command.arg("-f").arg(path);
    command.arg("-r");
    execute(command)
}

fn connection_args(command: &mut Command, args: &cli::Pull) {
    command.arg("-h").arg(&args.host);
    command.arg("-p").arg(args.port.to_string());
    if let Some(username) = &args.username {
        command.arg("-U").arg(username);
    }
    if args.no_password {
        command.arg("-w");
    }
    if args.password {
        command.arg("-W");
    }
    if let Some(role) = &args.role {
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
