//! `pull` integration: a synthesized pg_dump archive is pulled into a
//! project tree which must load and validate via `project::load` (the
//! front half of the Phase 3 round-trip gate)

mod common;

use clap::Parser;
use common::{fixture_archive, mutated_archive};
use pglifecycle::{cli, project, pull};

fn pull_args(archive: &std::path::Path, dest: &std::path::Path) -> cli::Pull {
    pull_args_with(archive, dest, &["--gitkeep"])
}

fn pull_args_with(
    archive: &std::path::Path,
    dest: &std::path::Path,
    extra: &[&str],
) -> cli::Pull {
    let mut argv =
        vec!["pglifecycle", "pull", "--dump", archive.to_str().unwrap()];
    argv.extend_from_slice(extra);
    argv.push(dest.to_str().unwrap());
    let parsed = cli::Cli::try_parse_from(argv).expect("failed to parse args");
    match parsed.action {
        cli::Action::Pull(args) => args,
        _ => unreachable!(),
    }
}

/// Relative path → (content, mtime) for every file under `root`
fn snapshot(
    root: &std::path::Path,
) -> std::collections::BTreeMap<String, (String, std::time::SystemTime)> {
    let mut files = std::collections::BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .to_string();
            let content = std::fs::read_to_string(&path).unwrap();
            let mtime = path.metadata().unwrap().modified().unwrap();
            files.insert(relative, (content, mtime));
        }
    }
    files
}

#[test]
fn pulled_project_loads_and_validates() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let dest = dir.path().join("project");

    pull::pull(&pull_args(&archive, &dest)).expect("pull failed");

    assert!(dest.join("project.yaml").exists());
    assert!(dest.join("schemata/test.yaml").exists());
    assert!(dest.join("tables/test/users.yaml").exists());
    assert!(dest.join("types/test.yaml").exists());
    assert!(dest.join("views/test/us_users.yaml").exists());
    assert!(dest.join("functions/test/set_last_modified.yaml").exists());
    assert!(dest.join("sequences/test/user_id_seq.yaml").exists());
    assert!(dest.join("domains/test/email_address.yaml").exists());
    assert!(dest.join("roles/PUBLIC.yaml").exists());
    // empty managed directories keep their .gitkeep, populated ones
    // lose it
    assert!(dest.join("casts/.gitkeep").exists());
    assert!(!dest.join("tables/.gitkeep").exists());

    let project = project::load(&dest).expect("pulled project must load");
    assert_eq!(project.name, "fixtures");
    let kinds: Vec<&str> =
        project.inventory.iter().map(|i| i.desc.as_str()).collect();
    for expected in [
        "SCHEMA",
        "TYPE",
        "DOMAIN",
        "TABLE",
        "SEQUENCE",
        "FUNCTION",
        "VIEW",
        "EXTENSION",
        "ROLE",
    ] {
        assert!(kinds.contains(&expected), "missing {expected}: {kinds:?}");
    }
}

#[test]
fn update_after_bootstrap_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let dest = dir.path().join("project");
    pull::pull(&pull_args(&archive, &dest)).expect("bootstrap failed");
    let before = snapshot(&dest);

    pull::pull(&pull_args_with(&archive, &dest, &["--update"]))
        .expect("update failed");

    let after = snapshot(&dest);
    assert_eq!(
        before, after,
        "update from an identical dump must be a no-op"
    );
}

#[test]
fn update_rewrites_only_changed_files() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let mutated = dir.path().join("mutated.dump");
    mutated_archive(&mutated);
    let dest = dir.path().join("project");
    pull::pull(&pull_args(&archive, &dest)).expect("bootstrap failed");
    let before = snapshot(&dest);

    pull::pull(&pull_args_with(&mutated, &dest, &["--update"]))
        .expect("update failed");

    let after = snapshot(&dest);
    let changed: Vec<&String> = after
        .iter()
        .filter(|(path, state)| before.get(*path) != Some(state))
        .map(|(path, _)| path)
        .collect();
    assert_eq!(
        changed,
        vec![
            "functions/test/set_last_modified.yaml",
            "tables/test/users.yaml"
        ],
        "only the mutated objects' files may change"
    );
    assert!(after["tables/test/users.yaml"].0.contains("nickname"));
    assert!(
        after["functions/test/set_last_modified.yaml"]
            .0
            .contains("clock_timestamp"),
    );
    // the dropped view's file is stale but preserved without --prune
    assert!(dest.join("views/test/us_users.yaml").exists());

    pull::pull(&pull_args_with(&mutated, &dest, &["--update", "--prune"]))
        .expect("prune failed");
    assert!(!dest.join("views/test/us_users.yaml").exists());
    assert!(
        !dest.join("views/test").exists(),
        "emptied schema directory must be removed"
    );
    assert!(dest.join("views").exists(), "top-level layout is preserved");

    let project = project::load(&dest).expect("updated project must load");
    assert_eq!(project.name, "fixtures");
}

#[test]
fn prune_removes_stale_remaining_file() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let dest = dir.path().join("project");
    pull::pull(&pull_args(&archive, &dest)).expect("bootstrap failed");
    std::fs::write(dest.join("remaining.yaml"), "[]\n").unwrap();

    pull::pull(&pull_args_with(&archive, &dest, &["--update", "--prune"]))
        .expect("prune failed");

    assert!(
        !dest.join("remaining.yaml").exists(),
        "stale remaining.yaml must be pruned"
    );
}

#[cfg(unix)]
#[test]
fn prune_does_not_follow_symlinked_directories() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let dest = dir.path().join("project");
    pull::pull(&pull_args(&archive, &dest)).expect("bootstrap failed");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    let outside_file = outside.join("other.yaml");
    std::fs::write(&outside_file, "outside: true\n").unwrap();
    std::os::unix::fs::symlink(&outside, dest.join("views/linked")).unwrap();

    pull::pull(&pull_args_with(&archive, &dest, &["--update", "--prune"]))
        .expect("prune failed");

    assert!(
        outside_file.exists(),
        "files behind a symlinked directory must not be pruned"
    );
}

#[test]
fn update_requires_existing_project() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let dest = dir.path().join("missing");
    let error = pull::pull(&pull_args_with(&archive, &dest, &["--update"]))
        .unwrap_err();
    assert!(error.contains("project.yaml"), "unexpected error: {error}");
}

#[test]
fn update_flag_conflicts() {
    for argv in [
        vec!["pglifecycle", "pull", "--update", "--force", "/tmp/x"],
        vec!["pglifecycle", "pull", "--prune", "/tmp/x"],
    ] {
        assert!(
            cli::Cli::try_parse_from(&argv).is_err(),
            "expected {argv:?} to be rejected"
        );
    }
}

#[test]
fn pull_refuses_existing_destination() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("fixtures.dump");
    fixture_archive(&archive);
    let dest = dir.path().join("project");
    std::fs::create_dir_all(&dest).unwrap();
    let error = pull::pull(&pull_args(&archive, &dest)).unwrap_err();
    assert!(
        error.contains("already exists"),
        "unexpected error: {error}"
    );
}
