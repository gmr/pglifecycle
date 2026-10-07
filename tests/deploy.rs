//! `deploy` integration: projects pulled from synthesized archives are
//! compared against other archives offline via `--dump`; the emitted
//! script must contain exactly the expected CREATE/DROP statements and
//! honor the `--allow-drop` gate

mod common;

use clap::Parser;
use common::{
    fixture_archive, foreign_archive, labeled_archive, mutated_archive,
    mutated_foreign_archive,
};
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
    // the database has an extra column (nickname) and a changed
    // function: the column drop and the function replace are both
    // destructive, so without --allow-drop neither may appear
    assert!(!script.contains("DROP TABLE"), "gated drop in:\n{script}");
    assert!(
        !script.contains("DROP COLUMN"),
        "destructive column change must be gated:\n{script}"
    );
    assert!(script.contains("excluded"), "header must note exclusions");
}

#[test]
fn table_alters_in_place_function_or_replaces() {
    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("fixtures.dump");
    fixture_archive(&baseline);
    let mutated = dir.path().join("mutated.dump");
    mutated_archive(&mutated);
    let project = dir.path().join("project");
    pull_project(&baseline, &project);

    let script = deploy_script(&project, &mutated, &["--allow-drop"]);

    // the table is reconciled in place — the database's extra column
    // is dropped, not the whole table
    assert!(
        !script.contains("DROP TABLE"),
        "table must be altered, not replaced:\n{script}"
    );
    assert!(
        script.contains("ALTER TABLE test.users DROP COLUMN nickname;"),
        "missing in-place column drop in:\n{script}"
    );
    // the function (same signature, changed body) is replaced in place
    // with CREATE OR REPLACE, not dropped
    assert!(
        !script.contains("DROP FUNCTION"),
        "function must use CREATE OR REPLACE, not drop:\n{script}"
    );
    assert!(
        script.contains("CREATE OR REPLACE FUNCTION test.set_last_modified"),
        "missing function OR REPLACE in:\n{script}"
    );
    assert!(
        script.contains("CURRENT_TIMESTAMP"),
        "replaced function must use the repo body:\n{script}"
    );
}

#[test]
fn added_column_reconciles_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("fixtures.dump");
    fixture_archive(&baseline);
    let mutated = dir.path().join("mutated.dump");
    mutated_archive(&mutated);
    // project has the nickname column (from mutated); the database
    // (baseline) does not — deploy should ADD it, non-destructively
    let project = dir.path().join("project");
    pull_project(&mutated, &project);

    // no --allow-drop: a column add is not destructive, so it is
    // included; its presence in the script proves that
    let script = deploy_script(&project, &baseline, &[]);

    assert!(
        script.contains("ALTER TABLE test.users ADD COLUMN nickname text;"),
        "missing in-place column add in:\n{script}"
    );
    assert!(
        !script.contains("DROP TABLE"),
        "an added column must not trigger a replace:\n{script}"
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
    assert!(script.contains("-- destructive statements: none\n"));
}

#[test]
fn foreign_objects_alter_in_place_and_drop_when_gated() {
    let dir = tempfile::tempdir().unwrap();
    let baseline = dir.path().join("foreign.dump");
    foreign_archive(&baseline);
    let mutated = dir.path().join("mutated.dump");
    mutated_foreign_archive(&mutated);
    // the project is the baseline; the "database" has diverged options
    // and an extra server
    let project = dir.path().join("project");
    pull_project(&baseline, &project);

    let gated = deploy_script(&project, &mutated, &[]);

    // the FDW, server, and foreign-table option drift reconciles in
    // place — no rebuilds
    assert!(
        gated.contains(
            "ALTER FOREIGN DATA WRAPPER local_files OPTIONS (SET debug \
             'true');"
        ),
        "missing FDW options alter in:\n{gated}"
    );
    assert!(
        gated.contains("ALTER SERVER wh OPTIONS (SET host 'db.example');"),
        "missing server options alter in:\n{gated}"
    );
    assert!(
        gated.contains(
            "ALTER FOREIGN TABLE test.remote_orders OPTIONS (SET table_name \
             'orders');"
        ),
        "missing foreign-table options alter in:\n{gated}"
    );
    // the redacted user-mapping password must not be dropped
    assert!(
        !gated.contains("DROP password"),
        "deploy must not drop a redacted password:\n{gated}"
    );
    // the database-only server is a gated drop
    assert!(
        !gated.contains("DROP SERVER"),
        "server drop must be gated:\n{gated}"
    );
    assert!(gated.contains("excluded"), "header must note exclusions");

    let script = deploy_script(&project, &mutated, &["--allow-drop"]);
    assert!(
        script.contains("DROP SERVER IF EXISTS orphan;"),
        "missing database-only server drop in:\n{script}"
    );
}

#[test]
fn matching_foreign_objects_are_an_empty_plan() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("foreign.dump");
    foreign_archive(&archive);
    let project = dir.path().join("project");
    pull_project(&archive, &project);

    let script = deploy_script(&project, &archive, &[]);

    assert!(
        script.contains("-- no changes"),
        "expected an empty plan, got:\n{script}"
    );
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

/// deploy refuses a dump that pull cannot read as pg_dump wrote it: an
/// archive in LATIN1 (its text is ASCII, thus libpgdump can load it),
/// and an archive with standard_conforming_strings off
#[test]
fn deploy_refuses_a_dump_with_other_session_settings() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let project = dir.path().join("project");
    pull_project(&archive, &project);
    for (encoding, stdstrings, expected) in [
        ("LATIN1", "on", "in the LATIN1 encoding"),
        ("UTF8", "off", "standard_conforming_strings off"),
    ] {
        let other = dir.path().join("other.dump");
        common::session_archive(&other, encoding, stdstrings, false);
        let argv = vec![
            "pglifecycle",
            "deploy",
            "--dump",
            other.to_str().unwrap(),
            project.to_str().unwrap(),
        ];
        let parsed =
            cli::Cli::try_parse_from(argv).expect("failed to parse args");
        let cli::Action::Deploy(args) = parsed.action else {
            unreachable!()
        };
        let error = deploy::deploy(&args).unwrap_err();
        assert!(error.contains(expected), "unexpected error: {error}");
    }
}

/// Security labels pulled from an archive give an empty plan against
/// that archive. A label that is different, or that only the project
/// has, is set in place. No object is made again.
#[test]
fn security_labels_change_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let labeled = dir.path().join("labeled.dump");
    labeled_archive(&labeled, Some("secret"));
    let project = dir.path().join("project");
    pull_project(&labeled, &project);
    let table = std::fs::read_to_string(project.join("tables/test/t.yaml"))
        .expect("table file");
    assert!(
        table.contains("security_labels:\n  dummy: secret\n"),
        "missing table labels in:\n{table}"
    );

    let script = deploy_script(&project, &labeled, &[]);
    assert!(
        script.contains("-- no changes"),
        "expected an empty plan, got:\n{script}"
    );

    let other = dir.path().join("other.dump");
    labeled_archive(&other, Some("public"));
    let script = deploy_script(&project, &other, &[]);
    for statement in [
        "SECURITY LABEL FOR dummy ON SCHEMA test IS $$secret$$;",
        "SECURITY LABEL FOR dummy ON TABLE test.t IS $$secret$$;",
        "SECURITY LABEL FOR dummy ON COLUMN test.t.secret IS $$secret$$;",
        "SECURITY LABEL FOR dummy ON FUNCTION test.f IS $$secret$$;",
        "SECURITY LABEL FOR dummy ON DATABASE labels IS $$secret$$;",
        "SECURITY LABEL FOR dummy ON COLUMN test.v.c IS $$secret$$;",
        "SECURITY LABEL FOR dummy ON COLUMN test.m.c IS $$secret$$;",
        "SECURITY LABEL FOR dummy ON EVENT TRIGGER et IS $$secret$$;",
    ] {
        assert!(
            script.contains(statement),
            "missing {statement} in:\n{script}"
        );
    }
    for verb in ["CREATE", "DROP"] {
        assert!(
            !script.contains(&format!("\n{verb} ")),
            "unexpected {verb} statement in:\n{script}"
        );
    }
}

/// A project object with no `security_labels` does not manage its
/// labels: deploy leaves the labels of the database alone. An explicit
/// map is compared, so a label that it does not have gets IS NULL.
#[test]
fn security_labels_are_managed_only_when_given() {
    let dir = tempfile::tempdir().unwrap();
    let labeled = dir.path().join("labeled.dump");
    labeled_archive(&labeled, Some("secret"));
    let unlabeled = dir.path().join("unlabeled.dump");
    labeled_archive(&unlabeled, None);
    let project = dir.path().join("project");
    pull_project(&unlabeled, &project);

    let script = deploy_script(&project, &labeled, &[]);
    assert!(
        script.contains("-- no changes"),
        "labels with no field must be left alone, got:\n{script}"
    );

    // an explicit, empty map on the table removes its labels; its
    // column and the other objects still have no field
    let path = project.join("tables/test/t.yaml");
    let table = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, format!("{table}security_labels: {{}}\n")).unwrap();
    let script = deploy_script(&project, &labeled, &[]);
    let labels: Vec<&str> = script
        .lines()
        .filter(|line| line.starts_with("SECURITY LABEL"))
        .collect();
    assert_eq!(
        labels,
        ["SECURITY LABEL FOR dummy ON TABLE test.t IS NULL;"],
        "in:\n{script}"
    );
}
