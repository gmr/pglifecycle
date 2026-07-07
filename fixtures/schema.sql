-- Fixture Schema For Testing

CREATE EXTENSION citext;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA test;
GRANT USAGE ON SCHEMA test TO PUBLIC;

SET search_path = test, public, pg_catalog;

CREATE TABLE empty_table(
    id               UUID                     NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_at TIMESTAMP WITH TIME ZONE,
    column_name      TEXT
);

CREATE DOMAIN test.email_address AS citext
        CHECK ( value ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$' );

-- Simplified locale check, doesn't fully conform to BCP-47
CREATE DOMAIN test.bcp47_locale AS TEXT
        CHECK ( value ~ '^[a-z]{2}-[A-Z]{2,3}$' );

CREATE TYPE user_state AS ENUM ('unverified', 'verified', 'suspended');

CREATE TABLE users (
    id               UUID                     NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_at TIMESTAMP WITH TIME ZONE,
    state            user_state               NOT NULL DEFAULT 'unverified',
    email            email_address            NOT NULL,
    name             TEXT                     NOT NULL,
    surname          TEXT                     NOT NULL,
    display_name     TEXT,
    locale           bcp47_locale             NOT NULL DEFAULT 'en-US',
    password_salt    TEXT                     NOT NULL,
    password         TEXT                     NOT NULL,
    signup_ip        INET                     NOT NULL,
    icon             OID
);

CREATE UNIQUE INDEX users_unique_email ON users (email);

CREATE TYPE address_type AS ENUM ('billing', 'delivery');

CREATE TABLE addresses (
    id               UUID                     NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_modified_at TIMESTAMP WITH TIME ZONE,
    user_id          UUID                     NOT NULL REFERENCES users (id) ON DELETE CASCADE ON UPDATE CASCADE,
    type             address_type             NOT NULL,
    address1         TEXT                     NOT NULL,
    address2         TEXT,
    address3         TEXT,
    locality         TEXT                     NOT NULL,
    region           TEXT,
    postal_code      TEXT                     NOT NULL,
    country          TEXT                     NOT NULL
);

-- Materialized view with an index, exercising matview index round-trip
CREATE MATERIALIZED VIEW user_states AS
    SELECT state, count(*) AS total FROM users GROUP BY state;

CREATE UNIQUE INDEX user_states_state ON user_states (state);

-- Range-partitioned table, exercising the parent's PARTITION BY
-- clause round-trip.
--
-- Partition children (`CREATE TABLE child PARTITION OF parent FOR
-- VALUES ...`, incl. a DEFAULT partition) were attempted here but are
-- skipped: they uncovered a real, previously-unknown product bug.
-- `pg_dump` never emits the inline `PARTITION OF ... FOR VALUES` form
-- pglifecycle's parser handles (src/ddl/table.rs's `create_table`,
-- matched via `kw_partition && kw_of`); it always splits partition
-- attachment into a separate `ALTER TABLE ONLY parent ATTACH
-- PARTITION child FOR VALUES ...` statement (TOC entry "Type: TABLE
-- ATTACH"). No code anywhere in src/ddl/ or src/pull/ recognizes that
-- ALTER TABLE subform, so it is silently dropped: the child comes
-- back from `pull` as a plain, unattached table with no partition
-- bound, and `build` has nothing to re-emit the ATTACH from. The
-- existing pull unit test `merges_partition_children`
-- (src/pull/mod.rs) only exercises the inline `PARTITION OF ... FOR
-- VALUES` form, so it never caught this since that form doesn't
-- occur in real `pg_dump` output.
CREATE TABLE events (
    id         BIGINT NOT NULL,
    created_at DATE   NOT NULL,
    payload    TEXT
) PARTITION BY RANGE (created_at);

-- Typed table: composite type + CREATE TABLE OF.
--
-- A column constraint (`CREATE TABLE locations OF point_2d
-- (CONSTRAINT locations_x_check CHECK (x IS NOT NULL))`) was
-- attempted here but is skipped: it uncovered another real product
-- bug. `pg_dump` keeps a typed table's column constraint inline in
-- the `CREATE TABLE ... OF type (...)` statement (unlike partition
-- attachment above), but `create_table` (src/ddl/table.rs) only
-- walks `TableElement` children to find constraints/columns; a typed
-- table's parenthesized element list uses a different grammar
-- production, so the CHECK constraint is silently dropped by pull
-- and the round-trip loses it entirely.
CREATE TYPE point_2d AS (
    x DOUBLE PRECISION,
    y DOUBLE PRECISION
);

CREATE TABLE locations OF point_2d;

-- Table-level CHECK constraints
CREATE TABLE products (
    id       UUID    NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    price    NUMERIC NOT NULL,
    quantity INTEGER NOT NULL,
    CONSTRAINT products_price_positive CHECK (price >= 0),
    CONSTRAINT products_quantity_nonneg CHECK (quantity >= 0)
);

-- View with security_barrier and check_option.
--
-- Named to sort alphabetically after `users`: build's dependency
-- graph never records an edge from a view to the tables/views it
-- queries (only tables get FK-derived `dependencies`, per
-- src/pull/writer.rs's table_dependencies()), so a view with no
-- recorded dependency is ordered purely by libpgdump's default
-- same-priority (namespace, tag) sort -- Table, Sequence, View, and
-- ForeignTable all share priority 22. A view alphabetically before
-- its underlying table (e.g. "active_users" before "users") is
-- restored first and pg_restore fails with "relation ... does not
-- exist". This is a real, pre-existing gap (see commit message); it
-- happens to be masked for user_states below because
-- MATERIALIZED VIEW has its own, later, priority tier.
CREATE VIEW verified_users
    WITH (security_barrier = true, check_option = 'local') AS
    SELECT id, name, surname FROM users WHERE state = 'verified';

-- Per-object COMMENT: column
--
-- A trigger + trigger-comment addition was attempted here but is
-- skipped: it uncovered two real product bugs rather than a fixture
-- issue:
--   1. src/pull/mod.rs `apply_comment` has no match arm for TRIGGER
--      (or RULE/POLICY/CONSTRAINT), so `COMMENT ON TRIGGER ... ON t`
--      always logs "Comment on unmatched object" and the comment is
--      silently dropped.
--   2. src/build/mod.rs `dump_function` (and the pull-side
--      src/ddl/function.rs, which no longer appends `()` to a
--      zero-parameter function's name as the Python tokenizer did)
--      renders zero-argument functions as `CREATE FUNCTION name
--      RETURNS ...` with no parameter list, which is invalid SQL and
--      fails pg_restore. This affects any zero-arg function,
--      including the trigger functions PostgreSQL requires.
COMMENT ON COLUMN users.display_name IS
    'Optional user-facing display name';

-- Bare `public` schema reference, exercising case-folding of an
-- unquoted `public` identifier.
--
-- Uses a plain integer primary key rather than SERIAL: a SERIAL
-- column's DEFAULT is emitted by pg_dump as a separate, later
-- `ALTER TABLE ONLY public.widgets ALTER COLUMN id SET DEFAULT
-- nextval(...)` statement (TOC entry "Type: DEFAULT"), which is
-- another real, previously-unknown bug -- pull only captures a
-- column's default when it is inline in the CREATE TABLE statement
-- itself, so the sequence-owned default is silently dropped and the
-- round-trip loses it.
CREATE TABLE public.widgets (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
