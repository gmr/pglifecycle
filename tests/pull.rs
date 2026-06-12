//! `pull` integration: a synthesized pg_dump archive is pulled into a
//! project tree which must load and validate via `project::load` (the
//! front half of the Phase 3 round-trip gate)

use clap::Parser;
use libpgdump::ObjectType as OT;
use pglifecycle::{cli, project, pull};

fn add(
    dump: &mut libpgdump::Dump,
    desc: OT,
    namespace: &str,
    tag: &str,
    defn: &str,
) {
    dump.add_entry(
        desc,
        Some(namespace),
        Some(tag),
        Some("postgres"),
        Some(defn),
        None,
        None,
        &[],
    )
    .expect("add_entry failed");
}

/// A miniature of the fixtures database dump
fn fixture_archive(path: &std::path::Path) {
    let mut dump = libpgdump::new("fixtures", "UTF8", "18.0").unwrap();
    add(
        &mut dump,
        OT::Encoding,
        "",
        "ENCODING",
        "SET client_encoding = 'UTF8';",
    );
    add(
        &mut dump,
        OT::StdStrings,
        "",
        "STDSTRINGS",
        "SET standard_conforming_strings = 'on';",
    );
    add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
    add(
        &mut dump,
        OT::Acl,
        "",
        "SCHEMA test",
        "GRANT USAGE ON SCHEMA test TO PUBLIC;",
    );
    add(
        &mut dump,
        OT::Extension,
        "",
        "citext",
        "CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;",
    );
    add(
        &mut dump,
        OT::Type,
        "test",
        "user_state",
        "CREATE TYPE test.user_state AS ENUM ('unverified', 'verified', \
         'suspended');",
    );
    add(
        &mut dump,
        OT::Domain,
        "test",
        "email_address",
        "CREATE DOMAIN test.email_address AS public.citext CHECK (VALUE \
         ~ '@');",
    );
    add(
        &mut dump,
        OT::Table,
        "test",
        "users",
        "CREATE TABLE test.users (\n\
         id uuid DEFAULT public.uuid_generate_v4() NOT NULL,\n\
         state test.user_state DEFAULT 'unverified'::test.user_state \
         NOT NULL,\n\
         email public.citext NOT NULL,\n\
         locale text DEFAULT 'en-US'::text NOT NULL\n);",
    );
    add(
        &mut dump,
        OT::Constraint,
        "test",
        "users users_pkey",
        "ALTER TABLE ONLY test.users ADD CONSTRAINT users_pkey PRIMARY \
         KEY (id);",
    );
    add(
        &mut dump,
        OT::Index,
        "test",
        "users_unique_email",
        "CREATE UNIQUE INDEX users_unique_email ON test.users USING \
         btree (email);",
    );
    add(
        &mut dump,
        OT::Comment,
        "test",
        "TABLE users",
        "COMMENT ON TABLE test.users IS 'User records';",
    );
    add(
        &mut dump,
        OT::Sequence,
        "test",
        "user_id_seq",
        "CREATE SEQUENCE test.user_id_seq START WITH 1 INCREMENT BY 1 \
         CACHE 1;",
    );
    add(
        &mut dump,
        OT::SequenceOwnedBy,
        "test",
        "user_id_seq",
        "ALTER SEQUENCE test.user_id_seq OWNED BY test.users.id;",
    );
    add(
        &mut dump,
        OT::View,
        "test",
        "us_users",
        "CREATE VIEW test.us_users AS SELECT id, email FROM test.users \
         WHERE (locale = 'en-US'::text);",
    );
    add(
        &mut dump,
        OT::Function,
        "test",
        "set_last_modified()",
        "CREATE FUNCTION test.set_last_modified() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN NEW.last_modified_at = \
         CURRENT_TIMESTAMP; RETURN NEW; END; $$;",
    );
    dump.save(path).expect("failed to save archive");
}

fn pull_args(archive: &std::path::Path, dest: &std::path::Path) -> cli::Pull {
    let parsed = cli::Cli::try_parse_from([
        "pglifecycle",
        "pull",
        "--dump",
        archive.to_str().unwrap(),
        "--gitkeep",
        dest.to_str().unwrap(),
    ])
    .expect("failed to parse args");
    match parsed.action {
        cli::Action::Pull(args) => args,
        _ => unreachable!(),
    }
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
