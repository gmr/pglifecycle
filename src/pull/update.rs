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
    let stale = stale_files(root, files, &ignore)?;
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

/// YAML files on disk under the managed directories (plus the
/// root-level `remaining.yaml`) that the render did not produce and
/// the ignore file does not cover
fn stale_files(
    root: &Path,
    files: &BTreeMap<PathBuf, String>,
    ignore: &BTreeSet<String>,
) -> Result<Vec<PathBuf>, String> {
    let mut stale = Vec::new();
    let remaining = PathBuf::from(REMAINING_FILE);
    if root.join(&remaining).is_file()
        && !files.contains_key(&remaining)
        && !ignore.contains(REMAINING_FILE)
    {
        stale.push(remaining);
    }
    for dir in MANAGED_DIRS {
        let top = root.join(dir);
        if !top.is_dir() {
            continue;
        }
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
