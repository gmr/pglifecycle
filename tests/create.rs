use std::fs;
use std::path::Path;
use std::process::Command;

fn run_create(dest: &Path, extra: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_pglifecycle"))
        .arg("create")
        .args(extra)
        .arg(dest)
        .output()
        .expect("failed to run pglifecycle")
}

#[test]
fn creates_skeleton_project() {
    let parent = tempfile::tempdir().unwrap();
    let tmp = parent.path().join("pglc-test-create");
    let output = run_create(&tmp, &[]);
    assert!(output.status.success());
    for subdir in ["tables", "functions", "schemata", "views", "dml"] {
        assert!(tmp.join(subdir).is_dir(), "missing {subdir}");
        assert!(tmp.join(subdir).join(".gitkeep").is_file());
    }
    let yaml = fs::read_to_string(tmp.join("project.yaml")).unwrap();
    assert_eq!(
        yaml,
        "---\n\
         name: pglc-test-create\nsuperuser: postgres\n"
    );
}

#[test]
fn create_includes_mode_headers_when_requested() {
    let parent = tempfile::tempdir().unwrap();
    let tmp = parent.path().join("pglc-test-create-headers");
    let output = run_create(&tmp, &["--include-mode-headers"]);
    assert!(output.status.success());
    let yaml = fs::read_to_string(tmp.join("project.yaml")).unwrap();
    assert_eq!(
        yaml,
        "# -*- mode: pglifecycle -*-\n# pglifecycle: project\n---\n\
         name: pglc-test-create-headers\nsuperuser: postgres\n"
    );
}

#[test]
fn refuses_existing_destination_without_force() {
    let parent = tempfile::tempdir().unwrap();
    let tmp = parent.path().join("pglc-test-create-exists");
    fs::create_dir_all(&tmp).unwrap();
    let output = run_create(&tmp, &[]);
    assert!(!output.status.success());
    let output = run_create(&tmp, &["--force"]);
    assert!(output.status.success());
}

#[test]
fn create_honors_options() {
    let parent = tempfile::tempdir().unwrap();
    let tmp = parent.path().join("pglc-test-create-opts");
    let output = run_create(
        &tmp,
        &["--name", "example", "--superuser", "admin", "--no-gitkeep"],
    );
    assert!(output.status.success());
    assert!(!tmp.join("tables").join(".gitkeep").exists());
    let yaml = fs::read_to_string(tmp.join("project.yaml")).unwrap();
    assert_eq!(
        yaml,
        "---\n\
         name: example\nsuperuser: admin\n"
    );
}

/// The build always writes the archive in UTF8 with standard conforming
/// strings, so create has no options to change them
#[test]
fn create_has_no_session_options() {
    let parent = tempfile::tempdir().unwrap();
    for option in [&["--encoding", "LATIN1"][..], &["--no-stdstrings"]] {
        let tmp = parent.path().join("pglc-test-create-session");
        let output = run_create(&tmp, option);
        assert!(!output.status.success(), "create accepted {option:?}");
        assert!(!tmp.exists(), "create made a project for {option:?}");
    }
}
