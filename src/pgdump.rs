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
    execute("pg_dump", dump_args(conn, ddl, path), Vec::new(), conn)
}

/// The pg_dump arguments for [`dump`]. `-E UTF8` makes the archive
/// UTF8 whatever the encoding of the database, the role or the
/// environment is: pg_dump writes the archive in the client encoding,
/// and libpgdump reads each string as UTF-8.
fn dump_args(
    conn: &cli::Connection,
    ddl: &DumpDdl,
    path: &Path,
) -> Vec<OsString> {
    let mut args = connection_args(conn);
    if let Some(dbname) = &conn.dbname {
        args.push("-d".into());
        args.push(dbname.into());
    }
    args.push("-f".into());
    args.push(path.into());
    args.push("-Fc".into());
    args.push("--schema-only".into());
    args.extend(["-E".into(), "UTF8".into()]);
    args.extend(ddl_args(ddl).into_iter().map(OsString::from));
    args
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
    let args = dump_roles_args(conn, path, include_passwords);
    execute("pg_dumpall", args, dump_roles_env(conn), conn)
}

/// Whether pg_dumpall gets the connection string of `--dbname`. Its
/// `--dbname` is only a connection string, thus a plain database name
/// is not given: the roles are the same in each database.
fn roles_connection_string(conn: &cli::Connection) -> Option<&str> {
    conn.dbname
        .as_deref()
        .filter(|dbname| is_connection_string(dbname))
}

/// The pg_dumpall arguments for [`run_dump_roles`]. `-E UTF8` makes the
/// SQL UTF8, the encoding that pull reads it in.
///
/// pg_dumpall gives `-h`, `-p` and `-U` priority over the values of
/// its connection string, but pg_dump and psql give the connection
/// string priority. Thus with a connection string, pg_dumpall does not
/// get these flags. In `keyword=value` form, their values go in front
/// of the connection string, because a keyword that occurs again
/// replaces the value. Thus the connection string has priority over
/// them, and they have priority over a service, as in pg_dump. A URI
/// cannot have other keywords in front, thus [`dump_roles_env`] gives
/// the values in the environment.
fn dump_roles_args(
    conn: &cli::Connection,
    path: &Path,
    include_passwords: bool,
) -> Vec<OsString> {
    let mut args = connection_args(conn);
    if let Some(connection) = roles_connection_string(conn) {
        args = without_server_flags(args);
        args.push("-d".into());
        args.push(match is_uri(connection) {
            true => connection.into(),
            false => format!("{} {connection}", server_keywords(conn)).into(),
        });
    }
    args.push("-f".into());
    args.push(path.into());
    args.push("-r".into());
    args.extend(["-E".into(), "UTF8".into()]);
    if !include_passwords {
        args.push("--no-role-passwords".into());
    }
    args
}

/// The environment of pg_dumpall for a URI: the values of `-h`, `-p`
/// and `-U` that [`dump_roles_args`] does not give
fn dump_roles_env(conn: &cli::Connection) -> Vec<(&'static str, OsString)> {
    if !roles_connection_string(conn).is_some_and(is_uri) {
        return Vec::new();
    }
    let mut env = vec![
        ("PGHOST", OsString::from(&conn.host)),
        ("PGPORT", OsString::from(conn.port.to_string())),
    ];
    if let Some(username) = &conn.username {
        env.push(("PGUSER", OsString::from(username)));
    }
    env
}

/// The values of `-h`, `-p` and `-U` as `keyword='value'` pairs
fn server_keywords(conn: &cli::Connection) -> String {
    let quote = |value: &str| {
        format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
    };
    let mut pairs = vec![
        format!("host={}", quote(&conn.host)),
        format!("port={}", quote(&conn.port.to_string())),
    ];
    if let Some(username) = &conn.username {
        pairs.push(format!("user={}", quote(username)));
    }
    pairs.join(" ")
}

/// `args` without the `-h`, `-p` and `-U` flags and their values
fn without_server_flags(args: Vec<OsString>) -> Vec<OsString> {
    let mut kept = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if ["-h", "-p", "-U"].iter().any(|flag| arg == *flag) {
            args.next();
        } else {
            kept.push(arg);
        }
    }
    kept
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
    let output = run("psql", &args, &[], conn)?;
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
/// prompt is. `env` is more environment of the tool.
fn run(
    program: &str,
    args: &[OsString],
    env: &[(&str, OsString)],
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
        command.envs(env.iter().map(|(name, value)| (name, value)));
        log::debug!("Executing {}", command_line(program, args, env, conn));
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

/// The PGOPTIONS of a dump: the options of the caller, and then
/// `standard_conforming_strings` on. pg_dump and pg_dumpall write a
/// string literal in the form that this setting selects, and pull reads
/// each literal with the setting on, as the build and the deploy script
/// write it. When a setting occurs two times, the server uses the last
/// value, thus this value replaces the value of the database, the role
/// and the caller. libpq does not use PGOPTIONS when the connection
/// string or its service sets `options`; then pull refuses a dump with
/// the setting off (see `pull::check_session`).
fn dump_options(caller: Option<OsString>) -> OsString {
    let mut options = caller.unwrap_or_default();
    if !options.is_empty() {
        options.push(" ");
    }
    options.push("-c standard_conforming_strings=on");
    options
}

/// Run a dump command, reporting a non-zero exit as an error and
/// naming the ways to supply a password when that was the cause and
/// no prompt was possible
fn execute(
    program: &str,
    args: Vec<OsString>,
    mut env: Vec<(&'static str, OsString)>,
    conn: &cli::Connection,
) -> Result<(), String> {
    env.insert(
        0,
        ("PGOPTIONS", dump_options(std::env::var_os("PGOPTIONS"))),
    );
    let output = run(program, &args, &env, conn)?;
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

/// The label of a connection for the banners, the logs and the header
/// of the deploy script, when pglifecycle cannot read the connection
/// string. The label does not show the text, because the text can have
/// a password.
const UNREADABLE: &str = "(unreadable connection string)";

/// The URI schemes of a libpq connection string
const URI_SCHEMES: [&str; 2] = ["postgresql://", "postgres://"];

/// A label of the connection that has no password:
/// `dbname@host:port`, or `host:port` when no database name is given.
/// When `--dbname` is a connection string, its values have priority
/// over `--host` and `--port`, as in pg_dump and psql. A service in
/// pg_service.conf is not read, and the label does not show the user.
pub fn label(conn: &cli::Connection) -> String {
    let target = match conn.dbname.as_deref() {
        None => Target::default(),
        Some(dbname) if !is_connection_string(dbname) => Target {
            dbname: Some(dbname.to_string()),
            ..Target::default()
        },
        Some(dbname) => match Target::parse(dbname) {
            Some(target) => target,
            None => return UNREADABLE.to_string(),
        },
    };
    let host = target
        .host
        .or(target.hostaddr)
        .unwrap_or_else(|| conn.host.clone());
    let port = target.port.unwrap_or_else(|| conn.port.to_string());
    match target.dbname {
        Some(dbname) => format!("{dbname}@{host}:{port}"),
        None => format!("{host}:{port}"),
    }
}

/// Whether libpq reads a `--dbname` value as a connection string: a
/// URI, or `keyword=value` pairs
fn is_connection_string(dbname: &str) -> bool {
    is_uri(dbname) || dbname.contains('=')
}

/// Whether a connection string is a URI
fn is_uri(connection: &str) -> bool {
    URI_SCHEMES
        .iter()
        .any(|scheme| connection.starts_with(scheme))
}

/// The values of a connection string that a label shows. An empty
/// value is not set.
#[derive(Default)]
struct Target {
    dbname: Option<String>,
    host: Option<String>,
    hostaddr: Option<String>,
    port: Option<String>,
}

impl Target {
    /// Read a connection string. Returns `None` when the text is not a
    /// connection string that this can read.
    fn parse(connection: &str) -> Option<Self> {
        let pairs = match URI_SCHEMES
            .iter()
            .find_map(|scheme| connection.strip_prefix(scheme))
        {
            Some(rest) => uri_pairs(rest)?,
            None => keyword_pairs(connection)?,
        };
        let mut target = Target::default();
        // a keyword that occurs again replaces the value, as in libpq
        for (key, value) in pairs {
            let slot = match key.as_str() {
                "dbname" => &mut target.dbname,
                "host" => &mut target.host,
                "hostaddr" => &mut target.hostaddr,
                "port" => &mut target.port,
                _ => continue,
            };
            *slot = (!value.is_empty()).then_some(value);
        }
        Some(target)
    }
}

/// The `keyword = value` pairs of a connection string. A value can
/// be in single quotes, and a backslash escapes the next character.
fn keyword_pairs(connection: &str) -> Option<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    let mut chars = connection.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        if chars.peek().is_none() {
            return Some(pairs);
        }
        let mut key = String::new();
        while let Some(c) = chars.next_if(|c| *c != '=' && !c.is_whitespace())
        {
            key.push(c);
        }
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        if key.is_empty() || chars.next_if_eq(&'=').is_none() {
            return None;
        }
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let quoted = chars.next_if_eq(&'\'').is_some();
        let mut value = String::new();
        loop {
            match chars.next() {
                None if quoted => return None,
                None => break,
                Some('\'') if quoted => break,
                Some(c) if c.is_whitespace() && !quoted => break,
                Some('\\') => value.push(chars.next()?),
                Some(c) => value.push(c),
            }
        }
        pairs.push((key, value));
    }
}

/// The pairs of a connection URI (the text after the scheme):
/// `[user[:password]@][host][:port][,...][/dbname][?keyword=value&...]`.
/// The user and the password are not read.
fn uri_pairs(rest: &str) -> Option<Vec<(String, String)>> {
    let rest = match rest.find(['@', '/']) {
        Some(at) if rest[at..].starts_with('@') => &rest[at + 1..],
        _ => rest,
    };
    let (address, query) = match rest.split_once('?') {
        Some((address, query)) => (address, query),
        None => (rest, ""),
    };
    let (hostspec, dbname) = match address.split_once('/') {
        Some((hostspec, dbname)) => (hostspec, dbname),
        None => (address, ""),
    };
    let mut pairs = Vec::new();
    if !hostspec.is_empty() {
        let mut hosts = Vec::new();
        let mut ports = Vec::new();
        for item in hostspec.split(',') {
            // an IPv6 address is in brackets: [::1]:5432
            let (host, port) = match item.strip_prefix('[') {
                Some(item) => {
                    let (host, after) = item.split_once(']')?;
                    match after {
                        "" => (host, ""),
                        _ => (host, after.strip_prefix(':')?),
                    }
                }
                None => item.split_once(':').unwrap_or((item, "")),
            };
            hosts.push(percent_decode(host)?);
            ports.push(percent_decode(port)?);
        }
        pairs.push((String::from("host"), hosts.join(",")));
        if ports.iter().any(|port| !port.is_empty()) {
            pairs.push((String::from("port"), ports.join(",")));
        }
    }
    if !dbname.is_empty() {
        pairs.push((String::from("dbname"), percent_decode(dbname)?));
    }
    for parameter in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = parameter.split_once('=')?;
        pairs.push((percent_decode(key)?, percent_decode(value)?));
    }
    Some(pairs)
}

/// Decode the `%XX` escapes of a URI part. Returns `None` for an
/// incorrect escape, or for a result that is not UTF-8.
fn percent_decode(text: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(text.len());
    let mut rest = text.as_bytes();
    while let Some((&byte, after)) = rest.split_first() {
        if byte == b'%' {
            let hex = after
                .get(..2)
                .filter(|hex| hex.iter().all(u8::is_ascii_hexdigit))?;
            let hex = std::str::from_utf8(hex).ok()?;
            bytes.push(u8::from_str_radix(hex, 16).ok()?);
            rest = &after[2..];
        } else {
            bytes.push(byte);
            rest = after;
        }
    }
    String::from_utf8(bytes).ok()
}

/// The command line of a client tool for the debug log: the
/// environment that pglifecycle sets, the program and the arguments. A
/// connection string can have a password, thus the label replaces each
/// argument that contains it. PGPASSWORD is not in `env`, thus the log
/// does not show it.
fn command_line(
    program: &str,
    args: &[OsString],
    env: &[(&str, OsString)],
    conn: &cli::Connection,
) -> String {
    let secret = conn
        .dbname
        .as_deref()
        .filter(|dbname| is_connection_string(dbname));
    let mut words: Vec<String> = env
        .iter()
        .map(|(name, value)| format!("{name}={value:?}"))
        .collect();
    words.push(program.to_string());
    for arg in args {
        let arg = arg.to_string_lossy();
        words.push(match secret {
            Some(dbname) if arg.contains(dbname) => {
                format!("(connection string to {})", label(conn))
            }
            _ => arg.into_owned(),
        });
    }
    words.join(" ")
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

    /// Whether `flag` and then `value` are in `args`
    fn has_pair(args: &[OsString], flag: &str, value: &str) -> bool {
        args.windows(2)
            .any(|pair| pair[0] == flag && pair[1] == value)
    }

    #[test]
    fn dumps_in_utf8() {
        let path = Path::new("schema.dump");
        let args = dump_args(&connection(false), &DumpDdl::default(), path);
        assert!(has_pair(&args, "-E", "UTF8"), "{args:?}");
        for include_passwords in [false, true] {
            let args =
                dump_roles_args(&connection(false), path, include_passwords);
            assert!(has_pair(&args, "-E", "UTF8"), "{args:?}");
        }
    }

    #[test]
    fn dump_options_set_standard_strings_after_the_caller() {
        assert_eq!(dump_options(None), "-c standard_conforming_strings=on");
        assert_eq!(
            dump_options(Some(OsString::new())),
            "-c standard_conforming_strings=on"
        );
        assert_eq!(
            dump_options(Some("-c standard_conforming_strings=off".into())),
            "-c standard_conforming_strings=off \
             -c standard_conforming_strings=on"
        );
    }

    #[test]
    fn does_not_retry_on_unrelated_failure() {
        let error = "Failed to dump (2): connection refused";
        assert!(!should_retry_without_passwords(true, error));
    }

    /// A connection with `dbname` as its --dbname value
    fn connection_to(dbname: Option<&str>) -> cli::Connection {
        cli::Connection {
            dbname: dbname.map(String::from),
            ..connection(false)
        }
    }

    /// The label of `dbname`. The test fails when the label has the
    /// password text "s3cret".
    fn label_of(dbname: Option<&str>) -> String {
        let label = label(&connection_to(dbname));
        assert!(!label.contains("s3cret"), "{dbname:?}: {label}");
        label
    }

    #[test]
    fn label_of_a_uri_has_no_password() {
        for (uri, expected) in [
            (
                "postgresql://u:s3cret@db.example.com:6543/app?sslmode=require",
                "app@db.example.com:6543",
            ),
            ("postgres://u@db/my%20app?password=s3cret", "my app@db:5432"),
            (
                "postgresql:///app?host=/tmp&port=6543&password=s3cret",
                "app@/tmp:6543",
            ),
            ("postgresql://u:s3cret@h1:1,h2:2/app", "app@h1,h2:1,2"),
            ("postgresql://[::1]:6543/app", "app@::1:6543"),
            ("postgresql://u:s3cret@db", "db:5432"),
        ] {
            assert_eq!(label_of(Some(uri)), expected, "{uri}");
        }
    }

    #[test]
    fn label_of_a_conninfo_has_no_quoted_password() {
        for (conninfo, expected) in [
            (
                "host=db port = 6543 password='s3cret \\' x' dbname='my app'",
                "my app@db:6543",
            ),
            ("dbname=app password='s3cret'", "app@localhost:5432"),
            ("password=s3cret hostaddr=10.0.0.1", "10.0.0.1:5432"),
            ("service=prod password=s3cret", "localhost:5432"),
        ] {
            assert_eq!(label_of(Some(conninfo)), expected, "{conninfo}");
        }
    }

    #[test]
    fn label_of_a_plain_name_has_the_host_and_port() {
        assert_eq!(label_of(Some("app")), "app@localhost:5432");
        assert_eq!(label_of(None), "localhost:5432");
    }

    #[test]
    fn label_of_an_unreadable_connection_string_is_a_placeholder() {
        for dbname in [
            "host=db password='s3cret",
            "host=db password s3cret",
            "=s3cret",
            "postgresql://u@db/%zz?password=s3cret",
            "postgresql://u@db/app?password",
            "postgresql://[::1/app?password=s3cret",
        ] {
            assert_eq!(label_of(Some(dbname)), UNREADABLE, "{dbname}");
        }
    }

    #[test]
    fn dumps_the_roles_of_the_server_of_a_connection_string() {
        let path = Path::new("roles.sql");
        let mut conn = connection_to(None);
        conn.username = Some(String::from("o'k\\"));
        for dbname in ["host=db port=6543 dbname=app", "service=prod"] {
            conn.dbname = Some(dbname.to_string());
            let args = dump_roles_args(&conn, path, false);
            let expected = format!(
                "host='localhost' port='5432' user='o\\'k\\\\' {dbname}"
            );
            assert!(has_pair(&args, "-d", &expected), "{args:?}");
            assert_no_server_flags(&args);
            assert!(dump_roles_env(&conn).is_empty());
        }
        let uri = "postgresql://db:6543/app";
        conn.dbname = Some(uri.to_string());
        let args = dump_roles_args(&conn, path, false);
        assert!(has_pair(&args, "-d", uri), "{args:?}");
        assert_no_server_flags(&args);
        assert_eq!(
            dump_roles_env(&conn),
            vec![
                ("PGHOST", OsString::from("localhost")),
                ("PGPORT", OsString::from("5432")),
                ("PGUSER", OsString::from("o'k\\")),
            ]
        );
    }

    /// pg_dumpall gives -h, -p and -U priority over the values of the
    /// connection string; pg_dump and psql do not
    fn assert_no_server_flags(args: &[OsString]) {
        for flag in ["-h", "-p", "-U"] {
            assert!(!args.contains(&OsString::from(flag)), "{args:?}");
        }
    }

    #[test]
    fn dumps_the_roles_of_a_plain_name_with_the_flags() {
        let conn = connection_to(Some("app"));
        let args = dump_roles_args(&conn, Path::new("roles.sql"), false);
        // pg_dumpall reads -d only as a connection string
        assert!(!args.contains(&OsString::from("-d")), "{args:?}");
        assert!(has_pair(&args, "-h", "localhost"), "{args:?}");
        assert!(has_pair(&args, "-p", "5432"), "{args:?}");
        assert!(dump_roles_env(&conn).is_empty());
    }

    #[test]
    fn command_line_has_no_password() {
        let conn = connection_to(Some("host=db password=s3cret dbname=app"));
        let args = dump_args(&conn, &DumpDdl::default(), Path::new("f"));
        let env = [("PGOPTIONS", OsString::from("-c a=b"))];
        let line = command_line("pg_dump", &args, &env, &conn);
        assert!(!line.contains("s3cret"), "{line}");
        assert!(
            line.starts_with("PGOPTIONS=\"-c a=b\" pg_dump -h"),
            "{line}"
        );
        assert!(
            line.contains(" -d (connection string to app@db:5432) "),
            "{line}"
        );
        let args = dump_roles_args(&conn, Path::new("f"), false);
        let line = command_line("pg_dumpall", &args, &[], &conn);
        assert!(!line.contains("s3cret"), "{line}");
        assert!(
            line.contains(" -d (connection string to app@db:5432) "),
            "{line}"
        );
    }
}
