//! The `create` command: scaffold an empty project directory

use std::fmt::Write as _;
use std::fs;

use crate::cli;
use crate::constants;

pub fn create(args: &cli::Create) -> Result<(), String> {
    let dest = &args.destination;
    if dest.exists() && !args.force {
        return Err(format!("{} already exists", dest.display()));
    }
    let name = match &args.name {
        Some(name) => name.clone(),
        None => dest
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .ok_or_else(|| {
                format!(
                    "can not derive a project name from {}",
                    dest.display()
                )
            })?,
    };
    fs::create_dir_all(dest).map_err(|e| e.to_string())?;
    for subdir in constants::PROJECT_DIRS {
        let path = dest.join(subdir);
        fs::create_dir_all(&path).map_err(|e| e.to_string())?;
        if !args.no_gitkeep {
            fs::write(path.join(".gitkeep"), "").map_err(|e| e.to_string())?;
        }
    }
    let mut yaml = String::from("---\n");
    let _ = writeln!(yaml, "name: {name}");
    let _ = writeln!(yaml, "encoding: {}", args.encoding);
    let _ = writeln!(yaml, "stdstrings: {}", !args.no_stdstrings);
    let _ = writeln!(yaml, "superuser: {}", args.superuser);
    fs::write(dest.join("project.yaml"), yaml).map_err(|e| e.to_string())
}
