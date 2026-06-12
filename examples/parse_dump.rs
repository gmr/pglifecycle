//! Development helper: parse every DDL entry in a pg_dump archive with
//! the ddl module and report what is supported, unsupported, or fails.
//!
//!     cargo run --example parse_dump -- /path/to/archive.dump

use pglifecycle::ddl;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: parse_dump <archive>");
    let dump = libpgdump::load(&path).expect("failed to load archive");
    let mut parser = ddl::Parser::new().expect("parser init failed");
    let (mut parsed, mut unsupported, mut errors) = (0, 0, 0);
    for entry in dump.entries() {
        let Some(defn) = entry.defn.as_deref() else {
            continue;
        };
        let trimmed = defn.trim();
        if trimmed.is_empty()
            || trimmed.to_ascii_uppercase().starts_with("SET ")
        {
            continue;
        }
        match parser.parse(defn) {
            Ok(statements) => {
                for statement in statements {
                    if let ddl::Statement::Unsupported(kind) = statement {
                        unsupported += 1;
                        println!(
                            "UNSUPPORTED {kind}: {} {}.{}",
                            entry.desc.as_str(),
                            entry.namespace.as_deref().unwrap_or_default(),
                            entry.tag.as_deref().unwrap_or_default(),
                        );
                    } else {
                        parsed += 1;
                    }
                }
            }
            Err(error) => {
                errors += 1;
                println!(
                    "ERROR for {} {}.{}: {error}",
                    entry.desc.as_str(),
                    entry.namespace.as_deref().unwrap_or_default(),
                    entry.tag.as_deref().unwrap_or_default(),
                );
            }
        }
    }
    println!("parsed: {parsed}, unsupported: {unsupported}, errors: {errors}");
}
