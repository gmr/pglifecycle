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
        "# pglifecycle: project\n---\n\
         name: pglc-test-create\nencoding: UTF-8\n\
         stdstrings: true\nsuperuser: postgres\n"
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
        &[
            "--name",
            "example",
            "--encoding",
            "LATIN1",
            "--no-stdstrings",
            "--superuser",
            "admin",
            "--no-gitkeep",
        ],
    );
    assert!(output.status.success());
    assert!(!tmp.join("tables").join(".gitkeep").exists());
    let yaml = fs::read_to_string(tmp.join("project.yaml")).unwrap();
    assert_eq!(
        yaml,
        "# pglifecycle: project\n---\n\
         name: example\nencoding: LATIN1\n\
         stdstrings: false\nsuperuser: admin\n"
    );
}
