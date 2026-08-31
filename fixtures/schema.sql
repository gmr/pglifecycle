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

-- Range-partitioned table with children, exercising the ATTACH
-- PARTITION round-trip. The children are authored inline with
-- `PARTITION OF ... FOR VALUES`, but `pg_dump` always re-emits them as
-- a plain `CREATE TABLE` plus a separate `ALTER TABLE ONLY parent
-- ATTACH PARTITION child FOR VALUES ...` (TOC entry "Type: TABLE
-- ATTACH"); pull now recognizes that form and folds the child back
-- into the parent's partitions (incl. a DEFAULT partition).
CREATE TABLE events (
    id         BIGINT NOT NULL,
    created_at DATE   NOT NULL,
    payload    TEXT
) PARTITION BY RANGE (created_at);

CREATE TABLE events_2024 PARTITION OF events
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE events_2025 PARTITION OF events
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE events_default PARTITION OF events DEFAULT;

-- Inheritance: a child whose PRIMARY KEY makes columns it inherits
-- rather than declares NOT NULL. PostgreSQL 18 dumps that as a
-- table-level `NOT NULL <column>` constraint, since the child has no
-- column entry to carry it; before it was recognized the whole CREATE
-- TABLE failed to parse and pull dropped the table. The DDL below is
-- portable — older servers simply dump no such constraint. The named
-- and NO INHERIT forms need PostgreSQL 18 syntax to write, so they
-- are covered by unit tests rather than here.
--
-- The child also sorts alphabetically BEFORE its parent
-- ("calibrated_readings" < "sensor_readings"), the same trap the
-- `active_users` view below covers for queries: tables share one
-- libpgdump priority tier, so without a recorded dependency edge the
-- child restores first and pg_restore fails with "relation ... does
-- not exist". pull now derives an edge from INHERITS.
CREATE TABLE sensor_readings (
    taken_at TIMESTAMPTZ,
    sensor   TEXT,
    reading  NUMERIC
);

CREATE TABLE calibrated_readings (
    PRIMARY KEY (taken_at, sensor)
) INHERITS (sensor_readings);

-- Typed table: composite type + CREATE TABLE OF, with an inline
-- column constraint. `pg_dump` keeps a typed table's constraint inline
-- in the `CREATE TABLE ... OF type (...)` statement; pull now walks the
-- typed element list (a distinct grammar production) so the constraint
-- survives the round-trip.
CREATE TYPE point_2d AS (
    x DOUBLE PRECISION,
    y DOUBLE PRECISION
);

CREATE TABLE locations OF point_2d (
    CONSTRAINT locations_x_check CHECK (x IS NOT NULL)
);

-- Table-level CHECK constraints
CREATE TABLE products (
    id       UUID    NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
    price    NUMERIC NOT NULL,
    quantity INTEGER NOT NULL,
    CONSTRAINT products_price_positive CHECK (price >= 0),
    CONSTRAINT products_quantity_nonneg CHECK (quantity >= 0)
);

-- View that sorts alphabetically BEFORE its underlying table
-- ("active_users" < "users"). Table, Sequence, View and ForeignTable
-- share libpgdump's priority tier, so without a recorded dependency
-- edge this view would restore before `users` and pg_restore would
-- fail with "relation ... does not exist". pull now derives view
-- dependency edges by re-parsing the stored query, so build orders it
-- after the tables it references.
CREATE VIEW active_users AS
    SELECT id, name, surname FROM users WHERE state = 'verified';

-- View with security_barrier and check_option.
CREATE VIEW verified_users
    WITH (security_barrier = true, check_option = 'local') AS
    SELECT id, name, surname FROM users WHERE state = 'verified';

-- Zero-argument trigger function + trigger + trigger comment. Trigger
-- functions are always zero-arg, exercising build's `CREATE FUNCTION
-- name() RETURNS trigger` rendering (a missing `()` here is invalid
-- SQL); the `COMMENT ON TRIGGER` exercises pull's trigger-comment
-- match arm.
-- The body is authored in libpgfmt's normalized (two-space) form so the
-- round-trip gate's exact schema diff stays clean; pull reformats
-- function bodies through libpgfmt regardless of --style.
CREATE FUNCTION test.touch_last_modified() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
  NEW.last_modified_at := CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$;

CREATE TRIGGER users_touch_last_modified
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION test.touch_last_modified();

COMMENT ON TRIGGER users_touch_last_modified ON users IS
    'Maintains last_modified_at on update';

-- Per-object COMMENT: column
COMMENT ON COLUMN users.display_name IS
    'Optional user-facing display name';

-- Bare `public` schema reference, exercising case-folding of an
-- unquoted `public` identifier. Uses a SERIAL primary key: pg_dump
-- emits the column default as a separate, later `ALTER TABLE ONLY
-- public.widgets ALTER COLUMN id SET DEFAULT nextval(...)` (TOC entry
-- "Type: DEFAULT"), which pull now folds back onto the column.
CREATE TABLE public.widgets (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
