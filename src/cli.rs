//! clap definitions mirroring the Python cli.py interface

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "pglifecycle",
    about = "PostgreSQL Schema Management",
    version
)]
pub struct Cli {
    /// Log to the specified filename. If not specified, log output is sent
    /// to STDOUT
    #[arg(short = 'L', long, global = true, help_heading = "Logging Options")]
    pub log_file: Option<PathBuf>,

    /// Increase output verbosity
    #[arg(short, long, global = true, help_heading = "Logging Options")]
    pub verbose: bool,

    /// Extra verbose debug logging
    #[arg(long, global = true, help_heading = "Logging Options")]
    pub debug: bool,

    #[command(subcommand)]
    pub action: Action,
}

#[derive(Subcommand)]
pub enum Action {
    /// Generate a pg_restore -Fc compatible archive of the project
    Build(Build),
    /// Create a skeleton project
    Create(Create),
    /// Generate the DDL to make a database match the project
    Deploy(Deploy),
    /// Create or update a project from a database or dump
    Pull(Pull),
}

impl Action {
    pub fn name(&self) -> &'static str {
        match self {
            Action::Build(_) => "build",
            Action::Create(_) => "create",
            Action::Deploy(_) => "deploy",
            Action::Pull(_) => "pull",
        }
    }
}

#[derive(Args)]
pub struct Build {
    /// The path to the pglifecycle project
    #[arg(value_name = "PROJECT")]
    pub project: PathBuf,

    /// The path to save the build artifact to
    #[arg(value_name = "DEST")]
    pub destination: PathBuf,
}

#[derive(Args)]
pub struct Create {
    /// Specify the database encoding
    #[arg(long, default_value = "UTF-8")]
    pub encoding: String,

    /// Write to destination path even if it already exists
    #[arg(long)]
    pub force: bool,

    /// Override the default project name
    #[arg(long)]
    pub name: Option<String>,

    /// Do not create .gitkeep files
    #[arg(long)]
    pub no_gitkeep: bool,

    /// Turn off standard conforming strings (< Postgres 9.1 behavior)
    #[arg(long)]
    pub no_stdstrings: bool,

    /// Specify the superuser name
    #[arg(long, default_value = "postgres")]
    pub superuser: String,

    /// The path to create the skeleton project in
    #[arg(value_name = "DEST")]
    pub destination: PathBuf,
}

#[derive(Args)]
#[command(disable_help_flag = true)]
pub struct Deploy {
    /// Print help
    #[arg(long, action = clap::ArgAction::Help)]
    help: Option<bool>,

    /// Compare against a pre-existing pg_dump file instead of
    /// connecting to a database
    #[arg(short = 'D', long)]
    pub dump: Option<PathBuf>,

    /// Write the DDL script to a file instead of STDOUT
    #[arg(short = 'o', long)]
    pub output: Option<PathBuf>,

    /// Execute the script against the database in a single transaction
    /// via psql instead of only printing it
    #[arg(long, conflicts_with = "dump")]
    pub apply: bool,

    /// Include destructive statements (DROP, drop+recreate fallbacks)
    /// in the script
    #[arg(long)]
    pub allow_drop: bool,

    /// do not include privileges (grant/revoke)
    #[arg(short = 'x', long)]
    pub no_privileges: bool,

    #[command(flatten)]
    pub connection: Connection,

    /// The path to the pglifecycle project
    #[arg(value_name = "PROJECT")]
    pub project: PathBuf,
}

/// PostgreSQL connection options shared by commands that talk to a
/// database; mirrors the client tools and their PG* environment
/// variables
#[derive(Args)]
pub struct Connection {
    /// database name to connect to
    #[arg(
        short,
        long,
        env = "PGDATABASE",
        help_heading = "Connection Options"
    )]
    pub dbname: Option<String>,

    /// database server host or socket directory
    #[arg(
        short = 'h',
        long,
        env = "PGHOST",
        default_value = "localhost",
        help_heading = "Connection Options"
    )]
    pub host: String,

    /// database server port number
    #[arg(
        short,
        long,
        env = "PGPORT",
        default_value_t = 5432,
        help_heading = "Connection Options"
    )]
    pub port: u16,

    /// The PostgreSQL username to operate as
    #[arg(
        short = 'U',
        long,
        env = "PGUSER",
        help_heading = "Connection Options"
    )]
    pub username: Option<String>,

    /// never prompt for password
    #[arg(short = 'w', long, help_heading = "Connection Options")]
    pub no_password: bool,

    /// force password prompt (should happen automatically)
    #[arg(short = 'W', long, help_heading = "Connection Options")]
    pub password: bool,

    /// Role to assume when connecting to a database
    #[arg(long, help_heading = "Connection Options")]
    pub role: Option<String>,
}

#[derive(Args)]
#[command(disable_help_flag = true)]
pub struct Pull {
    /// Print help
    #[arg(long, action = clap::ArgAction::Help)]
    help: Option<bool>,

    /// Use a pre-existing pg_dump file instead of connecting to a database
    #[arg(short = 'D', long)]
    pub dump: Option<PathBuf>,

    /// Extract roles (and users) from an existing cluster
    #[arg(short = 'r', long)]
    pub extract_roles: bool,

    /// Specify a file with files to skip writing
    #[arg(short, long)]
    pub ignore: Option<PathBuf>,

    /// Write to destination path even if it already exists
    #[arg(long)]
    pub force: bool,

    /// Merge into an existing project, rewriting only changed files
    #[arg(long, conflicts_with = "force")]
    pub update: bool,

    /// With --update, delete project files whose objects no longer
    /// exist in the database
    #[arg(long, requires = "update")]
    pub prune: bool,

    /// Create a .gitkeep file in empty directories
    #[arg(long, conflicts_with = "remove_empty_dirs")]
    pub gitkeep: bool,

    /// Remove empty directories after generation
    #[arg(long)]
    pub remove_empty_dirs: bool,

    /// Save any unparsed/unprocessed dump items to remaining.yaml
    #[arg(long)]
    pub save_remaining: bool,

    /// File to record DDL that fails to parse or format, and the
    /// statement in flight if interrupted (for reproducing hangs)
    #[arg(long, default_value = "pglifecycle-errors.log")]
    pub error_file: PathBuf,

    /// Exclude tables matching PATTERN (also matches views,
    /// materialized views, and sequences); repeatable. Conflicts with
    /// --dump
    #[arg(
        short = 'T',
        long = "exclude-table",
        value_name = "PATTERN",
        conflicts_with = "dump"
    )]
    pub exclude_table: Vec<String>,

    /// Exclude schemas matching PATTERN; repeatable. Conflicts with --dump
    #[arg(
        short = 'N',
        long = "exclude-schema",
        value_name = "PATTERN",
        conflicts_with = "dump"
    )]
    pub exclude_schema: Vec<String>,

    /// Exclude extensions matching PATTERN; repeatable. Conflicts with
    /// --dump
    #[arg(
        long = "exclude-extension",
        value_name = "PATTERN",
        conflicts_with = "dump"
    )]
    pub exclude_extension: Vec<String>,

    #[command(flatten)]
    pub connection: Connection,

    /// skip restoration of object ownership
    #[arg(short = 'O', long, help_heading = "DDL Options")]
    pub no_owner: bool,

    /// do not include privileges (grant/revoke)
    #[arg(short = 'x', long, help_heading = "DDL Options")]
    pub no_privileges: bool,

    /// do not include security label assignments
    #[arg(long, help_heading = "DDL Options")]
    pub no_security_labels: bool,

    /// do not include tablespace assignments
    #[arg(long, help_heading = "DDL Options")]
    pub no_tablespaces: bool,

    /// Destination directory for the project
    #[arg(value_name = "DEST")]
    pub destination: PathBuf,
}
