//! Parse-coverage check for bin/parse-coverage: read pg_dump archives
//! and print one line for each gap.
//!
//!     cargo run --example parse_coverage -- ARCHIVE...
//!
//! `UNEXERCISED <desc>` is a type in `pull::MODELED_DESCS` that no
//! archive contains, so no gate tests it. `UNPARSED <desc> <tag>` is an
//! entry of a modeled type that has no DDL, or whose DDL does not parse
//! into a supported statement. pull models extensions from the entry itself, so their
//! DDL is not parsed.

use std::collections::BTreeSet;

use libpgdump::ObjectType as OT;
use pglifecycle::{ddl, pull};

fn main() {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    assert!(!paths.is_empty(), "usage: parse_coverage ARCHIVE...");
    let mut parser = ddl::Parser::new().expect("parser init failed");
    let mut seen = BTreeSet::new();
    for path in &paths {
        let dump = libpgdump::load(path).expect("failed to load archive");
        for entry in dump.entries() {
            if !pull::MODELED_DESCS.contains(&entry.desc) {
                continue;
            }
            seen.insert(entry.desc.as_str().to_string());
            if entry.desc == OT::Extension {
                continue;
            }
            // pull skips an entry that has no DDL, so it is a gap
            let supported = entry.defn.as_deref().is_some_and(|defn| {
                parser.parse(defn).is_ok_and(|statements| {
                    !statements
                        .iter()
                        .any(|s| matches!(s, ddl::Statement::Unsupported(_)))
                })
            });
            if !supported {
                println!(
                    "UNPARSED {} {}",
                    entry.desc.as_str(),
                    entry.tag.as_deref().unwrap_or_default()
                );
            }
        }
    }
    let mut unexercised: Vec<&str> = pull::MODELED_DESCS
        .iter()
        .map(|desc| desc.as_str())
        .filter(|desc| !seen.contains(*desc))
        .collect();
    unexercised.sort_unstable();
    for desc in unexercised {
        println!("UNEXERCISED {desc}");
    }
}
