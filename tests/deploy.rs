//! `deploy` integration: projects pulled from synthesized archives are
//! compared against other archives offline via `--dump`; the emitted
//! script must contain exactly the expected CREATE/DROP statements and
//! honor the `--allow-drop` gate

mod common;

use clap::Parser;
use common::{fixture_archive, mutated_archive};
use pglifecycle::{cli, deploy, pull};

fn pull_project(archive: &std::path::Path, dest: &std::path::Path) {
    let argv = vec![
        "pglifecycle",
        "pull",
        "--dump",
        archive.to_str().unwrap(),
        dest.to_str().unwrap(),
    ];
    let parsed = cli::Cli::try_parse_from(argv).expect("failed to parse args");
    let cli::Action::Pull(args) = parsed.action else {
        unreachable!()
    };
    pull::pull(&args).expect("pull failed");
}

/// Run deploy against `archive` and return the script written via -o
fn deploy_script(
    project: &std::path::Path,
    archive: &std::path::Path,
    extra: &[&str],
) -> String {
    let output = project.with_extension("sql");
    let mut argv = vec![
        "pglifecycle",
        "deploy",
        "--dump",
        archive.to_str().unwrap(),
        "-o",
        output.to_str().unwrap(),
    ];
    argv.extend_from_slice(extra);
    argv.push(project.to_str().unwrap());
    let parsed = cli::Cli::try_parse_from(argv).expect("failed to parse args");
    let cli::Action::Deploy(args) = parsed.action else {
        unreachable!()
    };
    deploy::deploy(&args).expect("deploy failed");
    std::fs::read_to_string(&output).expect("script must exist")
}

#[test]
fn matching_database_is_an_empty_plan() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let project = dir.path().join("project");
    pull_project(&archive, &project);

    let script = deploy_script(&project, &archive, &[]);

    assert!(
        script.contains("-- no changes"),
        "expected an empty plan, got:\n{script}"
    );
    for verb in ["CREATE", "DROP", "ALTER"] {
        assert!(
            !script.contains(&format!("\n{verb} ")),
            "unexpected {verb} statement in:\n{script}"
        );
    }
}

#[test]
fn missing_objects_are_created() {
    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("fixtures.dump");
    fixture_archive(&baseline);
    let mutated = dir.path().join("mutated.dump");
    mutated_archive(&mutated);
    // the project has the view; the "database" (mutated) does not
    let project = dir.path().join("project");
    pull_project(&baseline, &project);

    let script = deploy_script(&project, &mutated, &[]);

    assert!(
        script.contains("CREATE VIEW test.us_users"),
        "missing view CREATE in:\n{script}"
    );
    // the users table and function differ (nickname column, function
    // body): in-place ALTER is not supported yet, so without
    // --allow-drop nothing destructive may appear
    assert!(!script.contains("DROP TABLE"), "gated drop in:\n{script}");
    assert!(
        !script.contains("CREATE TABLE"),
        "drop+recreate must be gated:\n{script}"
    );
    assert!(script.contains("excluded"), "header must note exclusions");
}

#[test]
fn changed_objects_replace_with_allow_drop() {
    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("fixtures.dump");
    fixture_archive(&baseline);
    let mutated = dir.path().join("mutated.dump");
    mutated_archive(&mutated);
    let project = dir.path().join("project");
    pull_project(&baseline, &project);

    let script = deploy_script(&project, &mutated, &["--allow-drop"]);

    // the changed table is dropped and recreated from the repo
    assert!(
        script.contains("DROP TABLE IF EXISTS test.users"),
        "missing table drop in:\n{script}"
    );
    assert!(
        script.contains("CREATE TABLE test.users"),
        "missing table recreate in:\n{script}"
    );
    assert!(
        !script.contains("nickname"),
        "the repo (without nickname) is authoritative:\n{script}"
    );
    assert!(
        script.contains("DROP FUNCTION IF EXISTS"),
        "missing function replace in:\n{script}"
    );
    assert!(
        script.contains("CURRENT_TIMESTAMP"),
        "recreated function must use the repo body:\n{script}"
    );
    // the table's primary key, index, and comment ride along with the
    // recreate
    assert!(
        script.contains("users_unique_email"),
        "missing index recreate in:\n{script}"
    );
    assert!(
        script.contains("COMMENT ON TABLE test.users"),
        "missing comment recreate in:\n{script}"
    );
}

#[test]
fn database_only_objects_drop_with_allow_drop() {
    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("fixtures.dump");
    fixture_archive(&baseline);
    let mutated = dir.path().join("mutated.dump");
    mutated_archive(&mutated);
    // the project (from mutated) has no view; the "database" does
    let project = dir.path().join("project");
    pull_project(&mutated, &project);

    let gated = deploy_script(&project, &baseline, &[]);
    assert!(
        !gated.contains("DROP VIEW"),
        "view drop must be gated:\n{gated}"
    );
    assert!(gated.contains("excluded"), "header must note exclusions");

    let script = deploy_script(&project, &baseline, &["--allow-drop"]);
    assert!(
        script.contains("DROP VIEW IF EXISTS test.us_users"),
        "missing view drop in:\n{script}"
    );
}

#[test]
fn script_header_is_self_describing() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let project = dir.path().join("project");
    pull_project(&archive, &project);

    let script = deploy_script(&project, &archive, &[]);

    assert!(script.starts_with("-- pglifecycle deploy\n"));
    assert!(script.contains("-- project: fixtures\n"));
    assert!(script.contains("-- source: dump "));
    assert!(script.contains("-- destructive statements: included\n"));
}

#[test]
fn deploy_requires_a_project() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let project = dir.path().join("missing");
    let argv = vec![
        "pglifecycle",
        "deploy",
        "--dump",
        archive.to_str().unwrap(),
        project.to_str().unwrap(),
    ];
    let parsed = cli::Cli::try_parse_from(argv).expect("failed to parse args");
    let cli::Action::Deploy(args) = parsed.action else {
        unreachable!()
    };
    assert!(deploy::deploy(&args).is_err());
}
