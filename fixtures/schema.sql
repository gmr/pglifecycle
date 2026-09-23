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

-- A trigger that takes arguments. PostgreSQL stores every argument as
-- text and pg_dump emits it as a string literal inside the function's
-- own parentheses, so the build has to do the same (build deviation
-- 16). The second argument looks numeric to show it stays quoted.
CREATE TRIGGER users_touch_last_modified_args
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION
        test.touch_last_modified('audit', '42');

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

-- Overloaded functions. pull writes each overload to its own file
-- (`overloaded.yaml`, `overloaded_1.yaml`); the loader keys objects by
-- identity, not by bare name, or the second file loads as a duplicate
-- and the whole project fails to load.
-- The bodies are authored in libpgfmt's normalized form, as the
-- trigger function above is, so the schema diff stays clean.
CREATE FUNCTION test.overloaded(value INTEGER) RETURNS TEXT
    LANGUAGE sql IMMUTABLE AS $$
 SELECT value::text;
$$;

CREATE FUNCTION test.overloaded(value TEXT) RETURNS TEXT
    LANGUAGE sql IMMUTABLE AS $$
 SELECT value;
$$;

-- HASH partitioning: `for_values_with` is the only bound a hash
-- partition has, and the partition schema's oneOf used to gate on a
-- property name that did not exist, so every hash partition failed
-- validation.
CREATE TABLE hashed_events (
    id      BIGINT NOT NULL,
    payload TEXT
) PARTITION BY HASH (id);

CREATE TABLE hashed_events_0 PARTITION OF hashed_events
    FOR VALUES WITH (MODULUS 2, REMAINDER 0);

CREATE TABLE hashed_events_1 PARTITION OF hashed_events
    FOR VALUES WITH (MODULUS 2, REMAINDER 1);

-- `toast.`-prefixed storage parameters, which the storage_parameters
-- property-name pattern used to reject for the dot.
CREATE TABLE audited_documents (
    id   BIGINT NOT NULL,
    body TEXT
) WITH (
    fillfactor = 90,
    toast.autovacuum_vacuum_threshold = 10000,
    toast.autovacuum_vacuum_scale_factor = 0.1
);

-- A default on an inherited column. An inheritance child has no column
-- entry of its own for `recorded_at`, so pg_dump writes the default as
-- a standalone `ALTER TABLE ONLY ... SET DEFAULT` (TOC entry
-- "Type: DEFAULT"); it is kept in the child's `column_defaults`, since
-- there is no local column to fold it onto.
CREATE TABLE audit_events (
    recorded_at TIMESTAMPTZ,
    detail      TEXT
);

CREATE TABLE audit_events_archive (
    CONSTRAINT audit_events_archive_detail_check CHECK (detail IS NOT NULL)
) INHERITS (audit_events);

ALTER TABLE audit_events_archive
    ALTER COLUMN recorded_at SET DEFAULT CURRENT_TIMESTAMP;

-- Two tables that reference each other. No creation order satisfies
-- both, so a foreign key rendered inline in CREATE TABLE cannot
-- restore; the build emits every foreign key as its own entry after
-- the tables (build deviation 14).
CREATE TABLE warehouses (
    id           INT NOT NULL PRIMARY KEY,
    lead_staff_id INT NOT NULL
);

CREATE TABLE warehouse_staff (
    id           INT NOT NULL PRIMARY KEY,
    warehouse_id INT NOT NULL,
    CONSTRAINT warehouse_staff_warehouse
        FOREIGN KEY (warehouse_id) REFERENCES warehouses (id)
);

ALTER TABLE warehouses
    ADD CONSTRAINT warehouses_lead_staff
    FOREIGN KEY (lead_staff_id) REFERENCES warehouse_staff (id);

-- Generated columns. PostgreSQL 18 makes VIRTUAL the default and
-- pg_dump omits the keyword for one, so a project that records no kind
-- would rebuild a virtual column as stored (build deviation 15).
CREATE TABLE measurement_samples (
    reading    NUMERIC(6,2) NOT NULL,
    multiplier NUMERIC(6,2) NOT NULL,
    scaled_stored  NUMERIC(12,4) GENERATED ALWAYS AS (reading * multiplier) STORED,
    scaled_virtual NUMERIC(12,4) GENERATED ALWAYS AS (reading * multiplier) VIRTUAL
);

-- A trigger whose function takes arguments. They belong inside the
-- function's own parentheses, as string literals (build deviation 16).
CREATE TABLE searchable_documents (
    title    TEXT,
    body     TEXT,
    fulltext TSVECTOR
);

CREATE TRIGGER searchable_documents_fulltext
    BEFORE INSERT OR UPDATE ON searchable_documents
    FOR EACH ROW EXECUTE FUNCTION
        tsvector_update_trigger(fulltext, 'pg_catalog.english', title, body);

-- btree_gist supplies the GiST operator class a temporal key needs for
-- its non-range columns; without it PostgreSQL rejects `room INT` in
-- the WITHOUT OVERLAPS key below.
CREATE EXTENSION btree_gist;

-- Constraint modifiers PostgreSQL 15 and 18 added. Each changes what
-- the constraint means, and each was silently dropped on pull before,
-- so the table round-tripped as a different object.
CREATE TABLE booking_slots (
    room     INT NOT NULL,
    during   DATERANGE NOT NULL,
    -- WITHOUT OVERLAPS makes the key temporal: two rows may share a
    -- room as long as their ranges do not overlap
    CONSTRAINT booking_slots_pkey PRIMARY KEY (room, during WITHOUT OVERLAPS)
);

CREATE TABLE booking_holds (
    room     INT,
    during   DATERANGE,
    tag_a    INT,
    tag_b    INT,
    -- one null equals another here, so at most one (null, null) row
    CONSTRAINT booking_holds_tags UNIQUE NULLS NOT DISTINCT (tag_a, tag_b),
    CONSTRAINT booking_holds_span UNIQUE (room, during WITHOUT OVERLAPS),
    -- a temporal foreign key names its range column on both sides
    CONSTRAINT booking_holds_slot FOREIGN KEY (room, PERIOD during)
        REFERENCES booking_slots (room, PERIOD during)
);

CREATE TABLE ledger_entries (
    id     INT PRIMARY KEY,
    amount NUMERIC(12,2),
    -- NOT ENFORCED records the rule without checking it. PostgreSQL
    -- marks such a constraint not validated too, and pg_dump writes
    -- only this clause for it.
    CONSTRAINT ledger_entries_positive CHECK (amount > 0) NOT ENFORCED
);

CREATE UNIQUE INDEX booking_holds_room_tag
    ON booking_holds (room, tag_a) NULLS NOT DISTINCT;

-- Identity columns. pg_dump writes each as its own SEQUENCE entry, an
-- `ALTER TABLE ... ADD GENERATED ... AS IDENTITY (...)` with every
-- sequence option spelled out, defaults included. pull used to skip
-- that statement, so every identity column rebuilt as a plain one.
CREATE TABLE identity_default (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
);

-- non-default options, including BY DEFAULT and CYCLE
CREATE TABLE identity_tuned (
    id   INT GENERATED BY DEFAULT AS IDENTITY
             (START WITH 100 INCREMENT BY 5 MAXVALUE 900 CACHE 20 CYCLE),
    note TEXT
);

-- a sequence name other than the <table>_<column>_seq PostgreSQL picks
CREATE TABLE identity_named (
    id SMALLINT GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME identity_named_ids)
);

-- a descending sequence, whose default start is its maximum rather than 1
CREATE TABLE identity_descending (
    id INT GENERATED ALWAYS AS IDENTITY (INCREMENT BY -1)
);

-- NOT VALID constraints: rows already in the table were never checked.
-- The state holds only when the constraint is added with ALTER TABLE;
-- in CREATE TABLE, PostgreSQL checks the new, empty table and records
-- it valid. So these are ALTERs, as pg_dump writes them, and the table
-- needs no rows for the state to stick.
CREATE TABLE legacy_imports (
    id        INT PRIMARY KEY,
    amount    NUMERIC(12,2),
    source_id INT,
    reference TEXT
);

ALTER TABLE legacy_imports
    ADD CONSTRAINT legacy_imports_positive CHECK (amount > 0) NOT VALID;
ALTER TABLE legacy_imports
    ADD CONSTRAINT legacy_imports_source
    FOREIGN KEY (source_id) REFERENCES legacy_imports (id) NOT VALID;
ALTER TABLE legacy_imports
    ADD CONSTRAINT legacy_imports_reference_nn NOT NULL reference NOT VALID;

-- Row-level security. pg_dump writes FORCE inside the TABLE entry, and
-- ENABLE, each policy and each policy comment as entries of their own.
-- CURRENT_USER keeps the fixture free of a cluster-wide role; pg_dump
-- writes the role it resolves to.
CREATE TABLE tenant_notes (
    id     INT PRIMARY KEY,
    tenant TEXT NOT NULL,
    body   TEXT
);
ALTER TABLE tenant_notes ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_notes FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_notes_own ON tenant_notes
    USING (tenant = CURRENT_USER) WITH CHECK (tenant = CURRENT_USER);
CREATE POLICY tenant_notes_read ON tenant_notes FOR SELECT
    TO CURRENT_USER USING (true);
CREATE POLICY tenant_notes_no_blank ON tenant_notes AS RESTRICTIVE
    FOR INSERT WITH CHECK (body <> '');
COMMENT ON POLICY tenant_notes_own ON tenant_notes IS 'tenant isolation';

-- enabled with no policies: every row is hidden from all but the owner
CREATE TABLE sealed_notes (id INT);
ALTER TABLE sealed_notes ENABLE ROW LEVEL SECURITY;

-- Aggregates: a plain one with an initial condition, an ordered-set
-- one, and a comment. The body is in libpgfmt's normalized form (see
-- touch_last_modified above).
CREATE FUNCTION test.add_ints(total INTEGER, value INTEGER) RETURNS INTEGER
    LANGUAGE sql IMMUTABLE AS $$
 SELECT total + value;
$$;

CREATE AGGREGATE test.sum_ints(INTEGER) (
    SFUNC = test.add_ints, STYPE = INTEGER, INITCOND = '0', PARALLEL = SAFE);
COMMENT ON AGGREGATE test.sum_ints(INTEGER) IS 'Adds integers';

CREATE AGGREGATE test.sum_sorted(INTEGER ORDER BY INTEGER) (
    SFUNC = test.add_ints, STYPE = INTEGER);

-- Casts: through a function in this schema, and an I/O conversion. A
-- cast has no schema of its own.
CREATE TYPE test.point_pair AS (x INTEGER, y INTEGER);

CREATE FUNCTION test.point_pair_x(pair test.point_pair) RETURNS INTEGER
    LANGUAGE sql IMMUTABLE AS $$
 SELECT (pair).x;
$$;

CREATE CAST (test.point_pair AS INTEGER)
    WITH FUNCTION test.point_pair_x(test.point_pair) AS ASSIGNMENT;
CREATE CAST (test.point_pair AS TEXT) WITH INOUT;
COMMENT ON CAST (test.point_pair AS TEXT) IS 'Text form of a pair';

-- Collations: a non-deterministic ICU one, one with ICU rules, and a
-- libc one
CREATE COLLATION test.case_insensitive
    (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
COMMENT ON COLLATION test.case_insensitive IS 'Ignores case';
CREATE COLLATION test.b_before_a (provider = icu, locale = 'und', rules = '&b < a');
CREATE COLLATION test.plain_c (provider = libc, locale = 'C');

CREATE DEFAULT CONVERSION test.latin1_to_utf8 FOR 'LATIN1' TO 'UTF8'
    FROM iso8859_1_to_utf8;

-- Text search: a dictionary, and a configuration with one mapping
-- changed from the copy it starts as
CREATE TEXT SEARCH DICTIONARY test.english_simple
    (TEMPLATE = pg_catalog.simple, stopwords = english);
COMMENT ON TEXT SEARCH DICTIONARY test.english_simple IS 'Simple, no stopwords';
CREATE TEXT SEARCH CONFIGURATION test.english_urls (COPY = pg_catalog.english);
ALTER TEXT SEARCH CONFIGURATION test.english_urls
    ALTER MAPPING FOR url WITH test.english_simple;

-- Publications: tables with a column list and a row filter, a schema,
-- and every table. pg_dump writes each table as its own entry.
CREATE TABLE test.replicated (id INTEGER PRIMARY KEY, amount INTEGER, note TEXT);
CREATE PUBLICATION pglifecycle_some
    FOR TABLE test.replicated (id, amount) WHERE (amount > 0)
    WITH (publish = 'insert, update', publish_via_partition_root = true);
COMMENT ON PUBLICATION pglifecycle_some IS 'Positive amounts only';
CREATE PUBLICATION pglifecycle_schema FOR TABLES IN SCHEMA test;
CREATE PUBLICATION pglifecycle_all FOR ALL TABLES;

-- Event triggers: filtered, disabled, and replica-only. The function
-- does nothing, so they are harmless to the DDL the gates run.
CREATE FUNCTION test.note_ddl() RETURNS event_trigger
    LANGUAGE plpgsql AS $$
BEGIN
  NULL;
END;
$$;

CREATE EVENT TRIGGER pglifecycle_ddl_start ON ddl_command_start
    WHEN TAG IN ('CREATE TABLE', 'DROP TABLE')
    EXECUTE FUNCTION test.note_ddl();
COMMENT ON EVENT TRIGGER pglifecycle_ddl_start IS 'Notes table DDL';
CREATE EVENT TRIGGER pglifecycle_drops ON sql_drop
    EXECUTE FUNCTION test.note_ddl();
ALTER EVENT TRIGGER pglifecycle_drops DISABLE;
CREATE EVENT TRIGGER pglifecycle_replica ON ddl_command_end
    EXECUTE FUNCTION test.note_ddl();
ALTER EVENT TRIGGER pglifecycle_replica ENABLE REPLICA;

-- Exclusion constraints: gist over a range, with a predicate and a
-- comment, and btree over an expression with its operator class and
-- order, INCLUDE and deferral. btree_gist, created above, provides =
-- for integers in gist.

CREATE TABLE test.room_bookings (
    room   INTEGER,
    during TSRANGE,
    status TEXT,
    CONSTRAINT room_bookings_no_overlap
        EXCLUDE USING gist (room WITH =, during WITH &&)
        WHERE (status <> 'cancelled')
);
COMMENT ON CONSTRAINT room_bookings_no_overlap ON test.room_bookings IS
    'One booking per room at a time';

CREATE TABLE test.handles (
    handle TEXT,
    owner  INTEGER,
    CONSTRAINT handles_unique_lower
        EXCLUDE USING btree (lower(handle) text_pattern_ops DESC NULLS LAST WITH =)
        INCLUDE (owner) DEFERRABLE INITIALLY DEFERRED
);

-- Replica identity: the whole row, none, and a unique index. pg_dump
-- writes the index form in the index's own entry.
CREATE TABLE test.replica_full (id INTEGER);
ALTER TABLE test.replica_full REPLICA IDENTITY FULL;
CREATE TABLE test.replica_nothing (id INTEGER);
ALTER TABLE test.replica_nothing REPLICA IDENTITY NOTHING;
CREATE TABLE test.replica_index (id INTEGER NOT NULL);
CREATE UNIQUE INDEX replica_index_id ON test.replica_index (id);
ALTER TABLE test.replica_index REPLICA IDENTITY USING INDEX replica_index_id;

-- Default privileges, global and per schema. PUBLIC as the grantee
-- keeps the fixture free of a cluster-wide role. These come last, so
-- the objects above are created under the built-in defaults.
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT SELECT ON TABLES TO PUBLIC;
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT USAGE ON SEQUENCES TO PUBLIC;
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

-- Column attributes pg_dump writes as ALTER COLUMN after CREATE TABLE:
-- compression, storage, the statistics target and attribute options
CREATE TABLE test.documents (
    id   INTEGER PRIMARY KEY,
    body TEXT COMPRESSION lz4,
    blob BYTEA,
    size NUMERIC
);
ALTER TABLE test.documents ALTER COLUMN body SET STORAGE EXTERNAL;
ALTER TABLE test.documents ALTER COLUMN blob SET STORAGE MAIN;
ALTER TABLE test.documents ALTER COLUMN id SET STATISTICS 500;
ALTER TABLE test.documents ALTER COLUMN size SET (n_distinct = 100);

-- Comments on each kind of constraint: primary key, unique, check,
-- foreign key, NOT NULL, and a NOT VALID check, whose comment follows
-- its own entry
CREATE TABLE test.invoices (
    id       INTEGER CONSTRAINT invoices_pkey PRIMARY KEY,
    number   TEXT NOT NULL UNIQUE,
    total    NUMERIC CONSTRAINT invoices_total_positive CHECK (total >= 0),
    document INTEGER CONSTRAINT invoices_document REFERENCES test.documents (id)
);
ALTER TABLE test.invoices
    ADD CONSTRAINT invoices_number_short CHECK (length(number) < 20) NOT VALID;
COMMENT ON CONSTRAINT invoices_pkey ON test.invoices IS 'The invoice id';
COMMENT ON CONSTRAINT invoices_number_key ON test.invoices IS 'One per number';
COMMENT ON CONSTRAINT invoices_total_positive ON test.invoices IS 'No credits';
COMMENT ON CONSTRAINT invoices_document ON test.invoices IS 'Its source';
COMMENT ON CONSTRAINT invoices_number_not_null ON test.invoices IS 'Required';
COMMENT ON CONSTRAINT invoices_number_short ON test.invoices IS 'Legacy rows';

-- Extended statistics: chosen kinds, every kind with a target and a
-- comment, expressions, and one on a materialized view
CREATE TABLE test.measurements (a INTEGER, b INTEGER, label TEXT);
CREATE STATISTICS test.measurements_ab (ndistinct, dependencies)
    ON a, b FROM test.measurements;
CREATE STATISTICS test.measurements_all ON a, b, label FROM test.measurements;
ALTER STATISTICS test.measurements_all SET STATISTICS 500;
COMMENT ON STATISTICS test.measurements_all IS 'Every kind';
CREATE STATISTICS test.measurements_expr (mcv)
    ON (a + b), lower(label) FROM test.measurements;
CREATE STATISTICS test.user_states_stats ON state, total FROM test.user_states;

-- Rules: DO INSTEAD NOTHING with a comment, a conditional DO ALSO with
-- two commands, a disabled one, and one on a view
CREATE TABLE test.ledger (id INTEGER, amount NUMERIC);
CREATE TABLE test.ledger_audit (id INTEGER);
CREATE RULE ledger_no_delete AS ON DELETE TO test.ledger DO INSTEAD NOTHING;
COMMENT ON RULE ledger_no_delete ON test.ledger IS 'Append only';
CREATE RULE ledger_audit_insert AS ON INSERT TO test.ledger
    WHERE new.amount > 0
    DO ALSO (INSERT INTO test.ledger_audit VALUES (new.id);
             INSERT INTO test.ledger_audit VALUES (- new.id));
CREATE RULE ledger_redirect AS ON UPDATE TO test.ledger
    DO INSTEAD UPDATE test.ledger_audit SET id = new.id WHERE ledger_audit.id = old.id;
ALTER TABLE test.ledger DISABLE RULE ledger_redirect;
CREATE VIEW test.ledger_view AS SELECT id, amount FROM test.ledger;
CREATE RULE ledger_view_insert AS ON INSERT TO test.ledger_view
    DO INSTEAD INSERT INTO test.ledger (id, amount) VALUES (new.id, new.amount);

-- Procedures: a PL/pgSQL one with an INOUT parameter, a default and a
-- setting, and a SQL-standard body. A function with a default and one
-- with a SQL-standard body, whose owner statements the round-trip gate
-- runs.
CREATE PROCEDURE test.archive_before(IN days INTEGER, INOUT archived INTEGER DEFAULT 0)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = test AS $$
BEGIN
  archived := days;
END;
$$;
COMMENT ON PROCEDURE test.archive_before(INTEGER, INTEGER) IS 'Archives old rows';

CREATE PROCEDURE test.touch_nothing() LANGUAGE sql
BEGIN ATOMIC
  SELECT 1;
END;

CREATE FUNCTION test.scaled(value INTEGER, factor INTEGER DEFAULT 2)
    RETURNS INTEGER LANGUAGE sql IMMUTABLE AS $$
 SELECT value * factor;
$$;

CREATE FUNCTION test.answer() RETURNS INTEGER LANGUAGE sql
BEGIN ATOMIC
  SELECT 42;
END;

-- Operators: binary with the planner's options and a comment, and
-- prefix
CREATE FUNCTION test.same_parity(a INTEGER, b INTEGER) RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE AS $$
 SELECT (a % 2) = (b % 2);
$$;

CREATE OPERATOR test.=~= (
    FUNCTION = test.same_parity, LEFTARG = INTEGER, RIGHTARG = INTEGER,
    COMMUTATOR = OPERATOR(test.=~=), RESTRICT = eqsel, JOIN = eqjoinsel);
COMMENT ON OPERATOR test.=~= (INTEGER, INTEGER) IS 'Same parity';
CREATE OPERATOR test.!!! (FUNCTION = int4um, RIGHTARG = INTEGER);
