//! `pull --update`: merge the rendered file set into an existing
//! project, writing only files whose content changed so `git diff`
//! shows exactly what changed in the database. Files for objects no
//! longer in the database are warned about, or removed with `--prune`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::cli;
use crate::pull::writer;
use crate::yamlio;

/// The directories `pull` writes object files into; staleness (and
/// `--prune`) is confined to these — `dml/` and the other project
/// directories are never touched
const MANAGED_DIRS: &[&str] = &[
    "domains",
    "functions",
    "materialized_views",
    "roles",
    "schemata",
    "sequences",
    "tables",
    "types",
    "users",
    "views",
];

pub fn merge(
    files: &BTreeMap<PathBuf, String>,
    args: &cli::Pull,
) -> Result<(), String> {
    let root = &args.destination;
    log::info!("Updating project at {}", root.display());
    let ignore = writer::read_ignore(args.ignore.as_deref())?;
    let mut written = 0usize;
    for (relative, content) in files {
        let path = root.join(relative);
        if let Ok(existing) = std::fs::read_to_string(&path)
            && existing == *content
        {
            continue;
        }
        let verb = if path.exists() {
            "Updating"
        } else {
            "Creating"
        };
        log::info!("{verb} {}", relative.display());
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!("failed to create {}: {e}", parent.display())
            })?;
        }
        std::fs::write(&path, content)
            .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
        written += 1;
    }
    let stale = stale_files(root, files, &ignore, args)?;
    if args.prune {
        for relative in &stale {
            log::info!("Removing {}", relative.display());
            let path = root.join(relative);
            std::fs::remove_file(&path).map_err(|e| {
                format!("failed to remove {}: {e}", path.display())
            })?;
        }
        remove_emptied_directories(root)?;
    } else {
        for relative in &stale {
            log::warn!(
                "{} has no matching database object; re-run with --prune \
                 to remove it",
                relative.display()
            );
        }
    }
    log::info!(
        "Update complete: {written} file(s) written, {} stale",
        stale.len()
    );
    Ok(())
}

/// The root-level file `--save-remaining` emits; managed for
/// staleness like the directories above
const REMAINING_FILE: &str = "remaining.yaml";

/// Whether this run extracted cluster roles/users via `pg_dumpall`.
/// Mirrors the gate in `pull::pull` (`roles = None` when `--no-roles`
/// or `--dump` is given): if it did not run, the render produced no
/// role/user files, and staleness must not be judged against `roles/`
/// and `users/` or every pre-existing file there would be pruned
fn roles_extracted(args: &cli::Pull) -> bool {
    !args.no_roles && args.dump.is_none()
}

/// The schema an on-disk managed-directory file belongs to, so it can
/// be checked against an active `--exclude-schema` filter. `dir` is
/// the top-level managed directory (e.g. `"tables"`) and `relative`
/// the file's path under `root`
fn schema_of(dir: &str, relative: &Path) -> Option<String> {
    match dir {
        "schemata" | "types" => relative
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned()),
        "domains" | "functions" | "materialized_views" | "sequences"
        | "tables" | "views" => relative
            .strip_prefix(dir)
            .ok()
            .and_then(|rest| rest.iter().next())
            .map(|s| s.to_string_lossy().into_owned()),
        _ => None,
    }
}

/// The unqualified object name for a file under a directory
/// `--exclude-table` applies to (tables, views, materialized views,
/// sequences), for checking against an active `--exclude-table` filter
fn table_name_of(dir: &str, relative: &Path) -> Option<String> {
    match dir {
        "materialized_views" | "sequences" | "tables" | "views" => relative
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned()),
        _ => None,
    }
}

/// Whether `text` matches a `pg_dump`-style pattern (`*` and `?`
/// wildcards; `--exclude-schema`/`--exclude-table` patterns)
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0, 0);
    let (mut star, mut resume) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            resume = ti;
            pi += 1;
        } else if let Some(si) = star {
            pi = si + 1;
            resume += 1;
            ti = resume;
        } else {
            return false;
        }
    }
    while p.get(pi) == Some(&'*') {
        pi += 1;
    }
    pi == p.len()
}

/// Whether a file the render did not produce is still covered by an
/// active `--exclude-schema`/`--exclude-table` filter — such objects
/// were deliberately left out of this run's dump, so their files are
/// not stale
fn filtered_out(dir: &str, relative: &Path, args: &cli::Pull) -> bool {
    if let Some(schema) = schema_of(dir, relative)
        && args.exclude_schema.iter().any(|p| glob_match(p, &schema))
    {
        return true;
    }
    if let Some(name) = table_name_of(dir, relative) {
        let schema = schema_of(dir, relative).unwrap_or_default();
        let qualified = format!("{schema}.{name}");
        if args
            .exclude_table
            .iter()
            .any(|p| glob_match(p, &qualified) || glob_match(p, &name))
        {
            return true;
        }
    }
    false
}

/// YAML files on disk under the managed directories (plus the
/// root-level `remaining.yaml`) that the render did not produce and
/// the ignore file does not cover
fn stale_files(
    root: &Path,
    files: &BTreeMap<PathBuf, String>,
    ignore: &BTreeSet<String>,
    args: &cli::Pull,
) -> Result<Vec<PathBuf>, String> {
    let mut stale = Vec::new();
    let remaining = PathBuf::from(REMAINING_FILE);
    if root.join(&remaining).is_file()
        && !files.contains_key(&remaining)
        && !ignore.contains(REMAINING_FILE)
    {
        stale.push(remaining);
    }
    let roles_extracted = roles_extracted(args);
    for dir in MANAGED_DIRS {
        if (*dir == "roles" || *dir == "users") && !roles_extracted {
            continue;
        }
        let top = root.join(dir);
        if !top.is_dir() {
            continue;
        }
        let mut pending = vec![top];
        while let Some(dir_path) = pending.pop() {
            for entry in std::fs::read_dir(&dir_path).map_err(|e| {
                format!("failed to read {}: {e}", dir_path.display())
            })? {
                let entry =
                    entry.map_err(|e| format!("failed to read entry: {e}"))?;
                let path = entry.path();
                let file_type = entry.file_type().map_err(|e| {
                    format!("failed to read type for {}: {e}", path.display())
                })?;
                if file_type.is_symlink() {
                    continue;
                }
                if file_type.is_dir() {
                    pending.push(path);
                    continue;
                }
                if !yamlio::is_yaml(&path) {
                    continue;
                }
                let relative = path
                    .strip_prefix(root)
                    .expect("walked path under root")
                    .to_path_buf();
                if files.contains_key(&relative)
                    || ignore.contains(&relative.to_string_lossy().to_string())
                    || filtered_out(dir, &relative, args)
                {
                    continue;
                }
                stale.push(relative);
            }
        }
    }
    stale.sort();
    Ok(stale)
}

/// After pruning, drop directories left empty below the managed
/// top-level directories (the top-level layout itself is preserved)
fn remove_emptied_directories(root: &Path) -> Result<(), String> {
    for dir in MANAGED_DIRS {
        let top = root.join(dir);
        if !top.is_dir() {
            continue;
        }
        let mut directories = Vec::new();
        let mut pending = vec![top];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir).map_err(|e| {
                format!("failed to read {}: {e}", dir.display())
            })? {
                let entry =
                    entry.map_err(|e| format!("failed to read entry: {e}"))?;
                let path = entry.path();
                let file_type = entry.file_type().map_err(|e| {
                    format!("failed to read type for {}: {e}", path.display())
                })?;
                if file_type.is_symlink() {
                    continue;
                }
                if file_type.is_dir() {
                    directories.push(path.clone());
                    pending.push(path);
                }
            }
        }
        directories.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
        for dir in directories {
            let empty = std::fs::read_dir(&dir)
                .map_err(|e| format!("failed to read {}: {e}", dir.display()))?
                .next()
                .is_none();
            if empty {
                std::fs::remove_dir(&dir).map_err(|e| {
                    format!("failed to remove {}: {e}", dir.display())
                })?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use clap::Parser;

    use super::*;

    /// Build `cli::Pull` args for `pull --update --prune <extra> <dest>`
    /// the way the binary would parse them
    fn pull_args(extra: &[&str], dest: &Path) -> cli::Pull {
        let mut argv = vec!["pglifecycle", "pull", "--update", "--prune"];
        argv.extend_from_slice(extra);
        let dest = dest.to_str().unwrap();
        argv.push(dest);
        match cli::Cli::try_parse_from(argv).unwrap().action {
            cli::Action::Pull(args) => args,
            _ => unreachable!(),
        }
    }

    fn touch(root: &Path, relative: &str) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "").unwrap();
    }

    /// `--dump` skips pg_dumpall (mirrors the gate in `pull::pull`), so
    /// a render from a dump produces no role/user files; staleness must
    /// not treat pre-existing `roles/`/`users/` files as stale, or
    /// `--prune` would wipe the entire cluster role inventory
    #[test]
    fn dump_source_does_not_stale_roles_or_users() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        touch(root, "roles/app_group.yaml");
        touch(root, "users/app_login.yaml");
        let dump = root.join("unused.dump");
        let args = pull_args(
            &["--dump", dump.to_str().unwrap()],
            &root.join("project"),
        );
        let stale =
            stale_files(root, &BTreeMap::new(), &BTreeSet::new(), &args)
                .unwrap();
        assert!(stale.is_empty(), "expected no stale files, got {stale:?}");
    }

    /// `--no-roles` also skips role extraction on a live connection, so
    /// it must be treated the same as `--dump`
    #[test]
    fn no_roles_does_not_stale_roles_or_users() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        touch(root, "roles/app_group.yaml");
        touch(root, "users/app_login.yaml");
        let args = pull_args(&["--no-roles"], &root.join("project"));
        let stale =
            stale_files(root, &BTreeMap::new(), &BTreeSet::new(), &args)
                .unwrap();
        assert!(stale.is_empty(), "expected no stale files, got {stale:?}");
    }

    /// Without `--dump`/`--no-roles`, a live pull does extract roles, so
    /// an unmatched file under `roles/`/`users/` is still stale — the
    /// protection above must not blanket-exempt those directories
    #[test]
    fn live_source_still_stales_roles_and_users() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        touch(root, "roles/app_group.yaml");
        touch(root, "users/app_login.yaml");
        let args = pull_args(&[], &root.join("project"));
        let stale =
            stale_files(root, &BTreeMap::new(), &BTreeSet::new(), &args)
                .unwrap();
        assert_eq!(
            stale,
            vec![
                PathBuf::from("roles/app_group.yaml"),
                PathBuf::from("users/app_login.yaml"),
            ]
        );
    }

    /// A schema excluded via `--exclude-schema` was deliberately left
    /// out of the dump, so its files are not stale even though the
    /// render did not produce them
    #[test]
    fn excluded_schema_is_not_staled() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        touch(root, "schemata/reporting.yaml");
        touch(root, "tables/reporting/big.yaml");
        let args =
            pull_args(&["--exclude-schema", "report*"], &root.join("project"));
        let stale =
            stale_files(root, &BTreeMap::new(), &BTreeSet::new(), &args)
                .unwrap();
        assert!(stale.is_empty(), "expected no stale files, got {stale:?}");
    }

    /// A table excluded via `--exclude-table` (schema-qualified or bare)
    /// is not stale, but other tables in the same schema still are
    #[test]
    fn excluded_table_is_not_staled() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        touch(root, "tables/public/big.yaml");
        touch(root, "tables/public/small.yaml");
        let args = pull_args(
            &["--exclude-table", "public.big"],
            &root.join("project"),
        );
        let stale =
            stale_files(root, &BTreeMap::new(), &BTreeSet::new(), &args)
                .unwrap();
        assert_eq!(stale, vec![PathBuf::from("tables/public/small.yaml")]);
    }

    #[test]
    fn glob_match_wildcards() {
        assert!(glob_match("report*", "reporting"));
        assert!(glob_match("*_vw", "orders_vw"));
        assert!(glob_match("public.big", "public.big"));
        assert!(!glob_match("public.big", "public.small"));
        assert!(glob_match("pub??c.big", "public.big"));
    }
}
