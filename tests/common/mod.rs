//! Shared integration-test helpers: synthesized pg_dump archives
//! mirroring a miniature of the fixtures database

// each test binary compiles its own copy; not every binary uses every
// helper
#![allow(dead_code)]

use libpgdump::ObjectType as OT;

pub fn add(
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
pub fn fixture_archive(path: &std::path::Path) {
    build_archive(path, false);
}

/// The fixtures dump after a migration: a column added to users, the
/// function body changed, and the view dropped
pub fn mutated_archive(path: &std::path::Path) {
    build_archive(path, true);
}

pub fn build_archive(path: &std::path::Path, mutated: bool) {
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
    let users = if mutated {
        "CREATE TABLE test.users (\n\
         id uuid DEFAULT public.uuid_generate_v4() NOT NULL,\n\
         state test.user_state DEFAULT 'unverified'::test.user_state \
         NOT NULL,\n\
         email public.citext NOT NULL,\n\
         nickname text,\n\
         locale text DEFAULT 'en-US'::text NOT NULL\n);"
    } else {
        "CREATE TABLE test.users (\n\
         id uuid DEFAULT public.uuid_generate_v4() NOT NULL,\n\
         state test.user_state DEFAULT 'unverified'::test.user_state \
         NOT NULL,\n\
         email public.citext NOT NULL,\n\
         locale text DEFAULT 'en-US'::text NOT NULL\n);"
    };
    add(&mut dump, OT::Table, "test", "users", users);
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
    if !mutated {
        add(
            &mut dump,
            OT::View,
            "test",
            "us_users",
            "CREATE VIEW test.us_users AS SELECT id, email FROM \
             test.users WHERE (locale = 'en-US'::text);",
        );
    }
    add(
        &mut dump,
        OT::MaterializedView,
        "test",
        "user_states",
        "CREATE MATERIALIZED VIEW test.user_states AS SELECT state, \
         count(*) AS total FROM test.users GROUP BY state WITH DATA;",
    );
    add(
        &mut dump,
        OT::Index,
        "test",
        "user_states_state",
        "CREATE UNIQUE INDEX user_states_state ON test.user_states \
         USING btree (state);",
    );
    let function = if mutated {
        "CREATE FUNCTION test.set_last_modified() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN NEW.last_modified_at = \
         clock_timestamp(); RETURN NEW; END; $$;"
    } else {
        "CREATE FUNCTION test.set_last_modified() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN NEW.last_modified_at = \
         CURRENT_TIMESTAMP; RETURN NEW; END; $$;"
    };
    add(
        &mut dump,
        OT::Function,
        "test",
        "set_last_modified()",
        function,
    );
    dump.save(path).expect("failed to save archive");
}

/// A small archive exercising all four foreign-object types (FDW,
/// server, user mapping with a secret, and a foreign table), used by
/// the pull tests
pub fn foreign_archive(path: &std::path::Path) {
    let mut dump = libpgdump::new("foreign", "UTF8", "18.0").unwrap();
    add(&mut dump, OT::Schema, "", "test", "CREATE SCHEMA test;");
    add(
        &mut dump,
        OT::ForeignDataWrapper,
        "",
        "local_files",
        "CREATE FOREIGN DATA WRAPPER local_files OPTIONS (debug 'true');",
    );
    add(
        &mut dump,
        OT::Server,
        "",
        "wh",
        "CREATE SERVER wh FOREIGN DATA WRAPPER postgres_fdw OPTIONS \
         (host 'db.example', dbname 'warehouse');",
    );
    add(
        &mut dump,
        OT::UserMapping,
        "",
        "postgres",
        "CREATE USER MAPPING FOR postgres SERVER wh OPTIONS \
         (user 'remote_app', password 'sup3rsecret');",
    );
    add(
        &mut dump,
        OT::ForeignTable,
        "test",
        "remote_orders",
        "CREATE FOREIGN TABLE test.remote_orders (id integer NOT NULL, \
         total numeric) SERVER wh OPTIONS (schema_name 'public', \
         table_name 'orders');",
    );
    add(
        &mut dump,
        OT::Comment,
        "test",
        "remote_orders",
        "COMMENT ON FOREIGN TABLE test.remote_orders IS 'Remote orders';",
    );
    dump.save(path).expect("failed to save archive");
}
