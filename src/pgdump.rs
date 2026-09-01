//! pg_dump / pg_dumpall subprocess wrappers (ports pgdump.py)

use std::ffi::OsString;
use std::io::IsTerminal;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use crate::{cli, progress};

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
    let mut args = connection_args(conn);
    if let Some(dbname) = &conn.dbname {
        args.push("-d".into());
        args.push(dbname.into());
    }
    args.push("-f".into());
    args.push(path.into());
    args.push("-Fc".into());
    args.push("--schema-only".into());
    args.extend(ddl_args(ddl).into_iter().map(OsString::from));
    execute("pg_dump", args, conn)
}

/// Dump cluster roles to `path` as SQL via `pg_dumpall --roles-only`.
/// Password hashes are omitted (`--no-role-passwords`) unless
/// `include_passwords` is set, to keep secrets out of the project.
///
/// Reading hashes requires `pg_authid`, which managed platforms (e.g.
/// RDS) deny to non-superusers. When passwords were requested and the
/// dump fails on that restriction, retry without passwords so role and
/// user extraction still succeeds (minus hashes) rather than aborting.
pub fn dump_roles(
    conn: &cli::Connection,
    path: &Path,
    include_passwords: bool,
) -> Result<(), String> {
    match run_dump_roles(conn, path, include_passwords) {
        Err(error)
            if should_retry_without_passwords(include_passwords, &error) =>
        {
            log::warn!(
                "Cannot read password hashes ({error}); retrying roles \
                 without passwords"
            );
            run_dump_roles(conn, path, false)
        }
        result => result,
    }
}

fn run_dump_roles(
    conn: &cli::Connection,
    path: &Path,
    include_passwords: bool,
) -> Result<(), String> {
    let mut args = connection_args(conn);
    args.push("-f".into());
    args.push(path.into());
    args.push("-r".into());
    if !include_passwords {
        args.push("--no-role-passwords".into());
    }
    execute("pg_dumpall", args, conn)
}

/// Whether a failed password-included roles dump should be retried
/// without passwords: the failure is a `pg_authid` access restriction
fn should_retry_without_passwords(
    include_passwords: bool,
    error: &str,
) -> bool {
    include_passwords && error.contains("pg_authid")
}

/// Apply a SQL script to the database in a single transaction via
/// `psql`, aborting on the first error. Returns psql's stderr on
/// failure so the caller can map it back to a statement.
pub fn apply(conn: &cli::Connection, script: &Path) -> Result<(), String> {
    let mut args = connection_args(conn);
    if let Some(dbname) = &conn.dbname {
        args.push("-d".into());
        args.push(dbname.into());
    }
    args.push("-X".into());
    args.push("-q".into());
    args.push("--single-transaction".into());
    args.push("-v".into());
    args.push("ON_ERROR_STOP=1".into());
    args.push("-f".into());
    args.push(script.into());
    let output = run("psql", &args, conn)?;
    if !output.status.success() {
        return Err(stderr_of(&output));
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

/// The connection flags shared by every client tool. `-w` is always
/// passed: the tools prompt on /dev/tty, where a live progress bar
/// overwrites the prompt, so pglifecycle prompts itself instead (see
/// [`run`]) and hands the password over in the environment.
fn connection_args(conn: &cli::Connection) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        "-h".into(),
        conn.host.clone().into(),
        "-p".into(),
        conn.port.to_string().into(),
    ];
    if let Some(username) = &conn.username {
        args.push("-U".into());
        args.push(username.into());
    }
    args.push("-w".into());
    if let Some(role) = &conn.role {
        args.push("--role".into());
        args.push(role.into());
    }
    args
}

/// Run a client tool, supplying a password when one is needed.
///
/// `-W` prompts up front; otherwise the tool runs with whatever
/// PGPASSWORD or pgpass already provides, and only a "no password
/// supplied" failure triggers a prompt and one retry. Prompting is
/// pglifecycle's own (bars suspended, echo off), so it cannot be
/// erased by a redrawing spinner the way the tools' own /dev/tty
/// prompt is.
fn run(
    program: &str,
    args: &[OsString],
    conn: &cli::Connection,
) -> Result<Output, String> {
    let mut password = match conn.password {
        true => Some(prompt_password(program, conn)?),
        false => None,
    };
    loop {
        let mut command = Command::new(program);
        command.args(args);
        // no stdin of our own to give it; the password comes from the
        // environment and the prompt is read by us, not the child
        command.stdin(Stdio::null());
        if let Some(password) = &password {
            command.env("PGPASSWORD", password);
        }
        log::debug!("Executing {command:?}");
        let output = command.output().map_err(|e| {
            format!("failed to run {:?}: {e}", command.get_program())
        })?;
        if output.status.success()
            || password.is_some()
            || !needs_password(&stderr_of(&output))
            || !can_prompt(conn)
        {
            return Ok(output);
        }
        password = Some(prompt_password(program, conn)?);
    }
}

/// Read a password from the terminal with the progress bars hidden and
/// echo off
fn prompt_password(
    program: &str,
    conn: &cli::Connection,
) -> Result<String, String> {
    let user = conn.username.as_deref().unwrap_or_default();
    let prompt = format!("Password for {program} as {user}: ");
    progress::suspend(|| rpassword::prompt_password(prompt))
        .map_err(|e| format!("failed to read password: {e}"))
}

/// Whether the failure is the server asking for a password pglifecycle
/// has not supplied yet
fn needs_password(stderr: &str) -> bool {
    stderr.contains("no password supplied")
}

/// Whether a password can be prompted for: `-w` forbids it, and a
/// non-interactive stdin has nobody to answer
fn can_prompt(conn: &cli::Connection) -> bool {
    !conn.no_password && std::io::stdin().is_terminal()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).trim().to_string()
}

/// Run a dump command, reporting a non-zero exit as an error and
/// naming the ways to supply a password when that was the cause and
/// no prompt was possible
fn execute(
    program: &str,
    args: Vec<OsString>,
    conn: &cli::Connection,
) -> Result<(), String> {
    let output = run(program, &args, conn)?;
    if !output.status.success() {
        let stderr = stderr_of(&output);
        let hint = if needs_password(&stderr) {
            "\nSet PGPASSWORD, add a ~/.pgpass entry, or run in a \
             terminal (without -w) to be prompted."
        } else {
            ""
        };
        return Err(format!(
            "Failed to dump ({}): {stderr}{hint}",
            output.status.code().unwrap_or(-1),
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

    #[test]
    fn retries_without_passwords_on_pg_authid_denial() {
        let error = "Failed to dump (1): pg_dumpall: error: query failed: \
                     ERROR: permission denied for table pg_authid";
        assert!(should_retry_without_passwords(true, error));
    }

    #[test]
    fn does_not_retry_when_passwords_not_requested() {
        let error = "permission denied for table pg_authid";
        assert!(!should_retry_without_passwords(false, error));
    }

    #[test]
    fn never_lets_the_client_tools_prompt() {
        let args = connection_args(&connection(false));
        assert!(args.contains(&OsString::from("-w")));
        assert!(!args.contains(&OsString::from("-W")));
        // -W is pglifecycle's own prompt, not a flag passed through
        let args = connection_args(&connection(true));
        assert!(args.contains(&OsString::from("-w")));
        assert!(!args.contains(&OsString::from("-W")));
    }

    #[test]
    fn detects_a_missing_password() {
        assert!(needs_password(
            "pg_dump: error: connection to server failed: fe_sendauth: \
             no password supplied"
        ));
        assert!(!needs_password("permission denied for table pg_authid"));
    }

    #[test]
    fn no_password_forbids_prompting() {
        let mut conn = connection(false);
        conn.no_password = true;
        assert!(!can_prompt(&conn));
    }

    fn connection(password: bool) -> cli::Connection {
        cli::Connection {
            dbname: Some("app".into()),
            host: "localhost".into(),
            port: 5432,
            username: Some("postgres".into()),
            no_password: false,
            password,
            role: None,
        }
    }

    #[test]
    fn does_not_retry_on_unrelated_failure() {
        let error = "Failed to dump (2): connection refused";
        assert!(!should_retry_without_passwords(true, error));
    }
}
