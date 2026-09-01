//! pg_dump / pg_dumpall subprocess wrappers (ports pgdump.py)

use std::path::Path;
use std::process::{Command, Stdio};

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
    let mut command = Command::new("pg_dump");
    connection_args(&mut command, conn);
    if let Some(dbname) = &conn.dbname {
        command.arg("-d").arg(dbname);
    }
    command.arg("-f").arg(path);
    command.arg("-Fc");
    command.arg("--schema-only");
    command.args(ddl_args(ddl));
    execute(command, conn.password)
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
    let mut command = Command::new("pg_dumpall");
    connection_args(&mut command, conn);
    command.arg("-f").arg(path);
    command.arg("-r");
    if !include_passwords {
        command.arg("--no-role-passwords");
    }
    execute(command, conn.password)
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
    // Without -W, pg_dump/pg_dumpall prompt for a password on
    // /dev/tty, which a live progress bar immediately overwrites: the
    // command looks hung with no prompt in sight. Only prompt when it
    // was asked for; otherwise fail fast and say how to supply the
    // password.
    if conn.password {
        command.arg("-W");
    } else {
        command.arg("-w");
    }
    if let Some(role) = &conn.role {
        command.arg("--role").arg(role);
    }
}

/// Run a dump command. When `prompt` is set the caller asked for a
/// password prompt (-W), so stdin is inherited and the progress bars
/// are hidden for the duration, leaving the prompt readable.
fn execute(mut command: Command, prompt: bool) -> Result<(), String> {
    log::debug!("Executing {command:?}");
    if prompt {
        command.stdin(Stdio::inherit());
    }
    let mut run = || {
        command.output().map_err(|e| {
            format!("failed to run {:?}: {e}", command.get_program())
        })
    };
    let output = if prompt {
        progress::suspend(run)
    } else {
        run()
    }?;
    if !output.status.success() {
        let stderr =
            String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!(
            "Failed to dump ({}): {}{}",
            output.status.code().unwrap_or(-1),
            stderr,
            password_hint(&stderr),
        ));
    }
    Ok(())
}

/// Advice appended to a failure caused by the missing password that
/// -w turned into an error instead of a hidden prompt
fn password_hint(stderr: &str) -> &'static str {
    if stderr.contains("no password supplied") {
        "\nSet PGPASSWORD, add a ~/.pgpass entry, or pass -W to be \
         prompted."
    } else {
        ""
    }
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
    fn defaults_to_no_password_prompt() {
        let mut command = Command::new("pg_dump");
        connection_args(&mut command, &connection(false));
        let args: Vec<_> = command.get_args().collect();
        assert!(args.contains(&"-w".as_ref()));
        assert!(!args.contains(&"-W".as_ref()));
    }

    #[test]
    fn prompts_only_when_requested() {
        let mut command = Command::new("pg_dump");
        connection_args(&mut command, &connection(true));
        let args: Vec<_> = command.get_args().collect();
        assert!(args.contains(&"-W".as_ref()));
        assert!(!args.contains(&"-w".as_ref()));
    }

    #[test]
    fn hints_at_password_sources_when_none_supplied() {
        let stderr = "pg_dump: error: connection to server failed: \
                      fe_sendauth: no password supplied";
        assert!(password_hint(stderr).contains("PGPASSWORD"));
        assert!(password_hint("permission denied").is_empty());
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
