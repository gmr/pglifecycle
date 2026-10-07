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

-- Domain NOT NULL constraints: an unnamed one, a named one, and an
-- unnamed one on a long name, where PostgreSQL cuts the generated name
-- and pg_dump writes it
CREATE DOMAIN test.positive_count AS integer NOT NULL
        CHECK ( value > 0 );

CREATE DOMAIN test.required_label AS text CONSTRAINT label_required NOT NULL;

CREATE DOMAIN test.a_domain_name_that_is_long_enough_to_cut_the_not_null_name
        AS integer NOT NULL;

-- Domain CHECK names: a name that must be quoted, and a CHECK with the
-- name that PostgreSQL makes for a NOT NULL, so that the NOT NULL gets
-- the name `not_null_taken_not_null1`
CREATE DOMAIN test.quoted_check AS integer
        CONSTRAINT "Quoted Check" CHECK ( value > 0 );

CREATE DOMAIN test.not_null_taken AS integer
        CONSTRAINT not_null_taken_not_null CHECK ( value > 0 ) NOT NULL;

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
-- INCLUDE columns are not key columns of the index
CREATE INDEX users_email_names ON users (email) INCLUDE (name, surname);

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

-- Sequence that sorts alphabetically BEFORE the table that it is
-- OWNED BY ("a_counter_seq" < "counters"). pg_dump writes OWNED BY as
-- a separate SEQUENCE OWNED BY entry after the table; build writes it
-- in CREATE SEQUENCE, so the sequence must restore after the table.
CREATE SEQUENCE a_counter_seq;
CREATE TABLE counters (
    id   INTEGER NOT NULL DEFAULT nextval('a_counter_seq'),
    name TEXT    NOT NULL
);
ALTER SEQUENCE a_counter_seq OWNED BY counters.id;

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

-- An INSTEAD OF trigger and its comment on a view. Pull keeps them on
-- the view, as it keeps a trigger on a table.
CREATE FUNCTION test.active_users_insert() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
  RETURN NEW;
END;
$$;

CREATE TRIGGER active_users_insert
    INSTEAD OF INSERT ON active_users
    FOR EACH ROW EXECUTE FUNCTION test.active_users_insert();

COMMENT ON TRIGGER active_users_insert ON active_users IS
    'Redirects inserts';

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

-- The statistics target and the storage of inherited columns. pg_dump
-- writes them in the child's TABLE entry as `ALTER TABLE ONLY ...
-- SET STATISTICS` and `SET STORAGE`; they are kept in the child's
-- `column_settings`, since there is no local column to hold them.
ALTER TABLE ONLY audit_events_archive
    ALTER COLUMN detail SET STATISTICS 500;
ALTER TABLE ONLY audit_events_archive
    ALTER COLUMN detail SET STORAGE EXTERNAL;
ALTER TABLE ONLY audit_events_archive
    ALTER COLUMN recorded_at SET STATISTICS 200;

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

-- A tenant-scoped composite foreign key whose ON DELETE clears only
-- the reference: plain SET NULL would also clear the tenant, which is
-- not null. The other key sets its column list with SET DEFAULT.
CREATE TABLE tenant_folders (
    tenant INT NOT NULL,
    id     INT NOT NULL,
    PRIMARY KEY (tenant, id)
);

CREATE TABLE tenant_files (
    tenant    INT NOT NULL,
    id        INT NOT NULL,
    folder    INT,
    archive   INT DEFAULT 0,
    PRIMARY KEY (tenant, id),
    CONSTRAINT tenant_files_folder FOREIGN KEY (tenant, folder)
        REFERENCES tenant_folders (tenant, id) ON DELETE SET NULL (folder),
    CONSTRAINT tenant_files_archive FOREIGN KEY (tenant, archive)
        REFERENCES tenant_folders (tenant, id)
        ON DELETE SET DEFAULT (archive) ON UPDATE CASCADE
);

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
-- an overload of the same aggregate, with its own comment
CREATE AGGREGATE test.sum_ints(BIGINT) (SFUNC = int8pl, STYPE = BIGINT);
COMMENT ON AGGREGATE test.sum_ints(BIGINT) IS 'Adds bigints';

CREATE AGGREGATE test.sum_sorted(INTEGER ORDER BY INTEGER) (
    SFUNC = test.add_ints, STYPE = INTEGER);
-- an argument name that needs quotes
CREATE AGGREGATE test.sum_named("Weird Arg" INTEGER) (
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
-- a parser and a template, built from the functions of the built-in
-- ones, with comments
CREATE TEXT SEARCH PARSER test.default_copy (
    START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end,
    LEXTYPES = prsd_lextype, HEADLINE = prsd_headline);
COMMENT ON TEXT SEARCH PARSER test.default_copy IS 'Copy of default';
CREATE TEXT SEARCH TEMPLATE test.simple_copy (
    INIT = dsimple_init, LEXIZE = dsimple_lexize);
COMMENT ON TEXT SEARCH TEMPLATE test.simple_copy IS 'Copy of simple';

-- Foreign objects: a wrapper with no handler needs no remote server.
-- The deploy convergence gate changes their options, which deploy has
-- to reconcile in place.
CREATE FOREIGN DATA WRAPPER gate_fdw OPTIONS (debug 'true');
CREATE SERVER gate_srv FOREIGN DATA WRAPPER gate_fdw
    OPTIONS (host 'h', dbname 'w');
CREATE USER MAPPING FOR postgres SERVER gate_srv OPTIONS (usr 'u');
COMMENT ON FOREIGN DATA WRAPPER gate_fdw IS 'A wrapper with no handler';
COMMENT ON SERVER gate_srv IS 'A server with no remote';
-- a user mapping on a second server is a second archive entry of the
-- same item, which deploy also makes
CREATE SERVER gate_srv_b FOREIGN DATA WRAPPER gate_fdw;
CREATE USER MAPPING FOR postgres SERVER gate_srv_b OPTIONS (usr 'b');
CREATE FOREIGN TABLE test.gate_ft (id integer)
    SERVER gate_srv OPTIONS (schema_name 'public', table_name 't');
-- an inheriting foreign table: its columns come from the parent, so it
-- declares none of its own, and it must restore after the parent
CREATE TABLE test.gate_ft_parent (id integer, note text);
CREATE FOREIGN TABLE test.gate_ft_child (CHECK (id > 0))
    INHERITS (test.gate_ft_parent)
    SERVER gate_srv OPTIONS (schema_name 'public', table_name 'c');

-- A procedural language that is not an extension, with a function
-- written in it. Its handlers are plpgsql's, declared in a dumped
-- schema; pg_dump omits a handler that is in pg_catalog.
CREATE FUNCTION test.plcopy_handler() RETURNS language_handler
    LANGUAGE c AS '$libdir/plpgsql', 'plpgsql_call_handler';
CREATE FUNCTION test.plcopy_inline(internal) RETURNS void
    LANGUAGE c AS '$libdir/plpgsql', 'plpgsql_inline_handler';
CREATE FUNCTION test.plcopy_validator(oid) RETURNS void
    LANGUAGE c AS '$libdir/plpgsql', 'plpgsql_validator';
CREATE TRUSTED LANGUAGE plcopy HANDLER test.plcopy_handler
    INLINE test.plcopy_inline VALIDATOR test.plcopy_validator;
COMMENT ON LANGUAGE plcopy IS 'A copy of plpgsql';
CREATE FUNCTION test.in_plcopy() RETURNS INTEGER LANGUAGE plcopy AS $$
BEGIN
  RETURN 1;
END;
$$;
-- An overload of the validator that is written in the language. The
-- language uses only `plcopy_validator(oid)`, and pg_dump orders it
-- after that overload only. An edge to each overload of the bare
-- validator name makes a dependency loop with the other overload
-- (build deviation 41).
CREATE FUNCTION test.plcopy_validator(n INTEGER) RETURNS INTEGER
    LANGUAGE plcopy AS $$
BEGIN
  RETURN n;
END;
$$;

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

-- Column names that need quoting, in each place a column name can be:
-- PostgreSQL folds a name that is not quoted to lowercase, so build
-- and deploy must quote each one
CREATE TABLE test.quoted_cols (
    "Id"        INTEGER GENERATED BY DEFAULT AS IDENTITY,
    "Name"      TEXT NOT NULL DEFAULT 'x',
    "select"    INTEGER CHECK ("select" > 0),
    "Has Space" TEXT COLLATE "C",
    "Total"     INTEGER GENERATED ALWAYS AS ("select" * 2) STORED,
    -- quoted_cols_touch sets this column
    last_modified_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY ("Id"),
    UNIQUE ("Name") INCLUDE ("Has Space")
);
ALTER TABLE test.quoted_cols ALTER COLUMN "Has Space" SET STATISTICS 200;
ALTER TABLE test.quoted_cols ALTER COLUMN "Has Space" SET STORAGE EXTERNAL;
COMMENT ON COLUMN test.quoted_cols."Name" IS 'A quoted column';
CREATE INDEX quoted_cols_name ON test.quoted_cols ("Name" DESC, "select");
CREATE INDEX quoted_cols_select ON test.quoted_cols ("select")
    INCLUDE ("Has Space", "Name");
GRANT SELECT ("Has Space") ON test.quoted_cols TO PUBLIC;
CREATE TRIGGER quoted_cols_touch BEFORE UPDATE OF "Name" ON test.quoted_cols
    FOR EACH ROW EXECUTE FUNCTION test.touch_last_modified();
CREATE TABLE test.quoted_refs (
    "Tenant" INTEGER,
    "Ref"    INTEGER,
    CONSTRAINT quoted_refs_ref FOREIGN KEY ("Ref")
        REFERENCES test.quoted_cols ("Id") ON DELETE SET NULL ("Ref")
);
CREATE TABLE test.quoted_parts ("Key" INTEGER, v TEXT)
    PARTITION BY RANGE ("Key");
CREATE TABLE test.quoted_parts_1 PARTITION OF test.quoted_parts
    FOR VALUES FROM (0) TO (10);
CREATE VIEW test.quoted_view ("Out Col") AS
    SELECT "Name" FROM test.quoted_cols;
CREATE TYPE test."Quoted Type" AS ("Field One" INTEGER);
COMMENT ON COLUMN test."Quoted Type"."Field One" IS 'A quoted attribute';
CREATE STATISTICS test.quoted_stats ON "select", "Id" FROM test.quoted_cols;
CREATE PUBLICATION quoted_pub FOR TABLE test.quoted_cols ("Id", "Name");

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

-- Expression indexes: a cast with an operator class, which needs its
-- own parentheses (the form of an HNSW index on a halfvec cast), a
-- function call, which does not, and a sum with an order
CREATE INDEX measurements_label_cast
    ON test.measurements ((label::varchar(20)) varchar_pattern_ops);
CREATE INDEX measurements_lower
    ON test.measurements (lower(label) text_pattern_ops);
CREATE INDEX measurements_sum ON test.measurements ((a + b) DESC NULLS LAST);

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

-- SQL-standard bodies that use other objects. PostgreSQL examines
-- such a body when it makes the routine, also with
-- check_function_bodies off, so the routine has to come after each
-- object that its body uses. Name order puts `a_calls_later` and
-- `a_proc_calls_later` before the function that they call, and type
-- order puts a function before an aggregate, a table and a view.
-- pg_dump records these dependencies, and pull writes them to
-- `dependencies`. The calls to `overloaded` and `sum_ints` each use one
-- of two overloads.
CREATE FUNCTION test.z_called_later(n INTEGER) RETURNS INTEGER
    LANGUAGE sql IMMUTABLE RETURN n + 1;

CREATE FUNCTION test.a_calls_later() RETURNS TEXT LANGUAGE sql
BEGIN ATOMIC
  SELECT test.overloaded(test.z_called_later(1));
END;

CREATE PROCEDURE test.a_proc_calls_later() LANGUAGE sql
BEGIN ATOMIC
  SELECT test.z_called_later(2);
END;

CREATE FUNCTION test.a_reads_users() RETURNS BIGINT LANGUAGE sql
BEGIN ATOMIC
  SELECT count(*) AS count FROM test.users;
END;

CREATE FUNCTION test.a_reads_view() RETURNS INTEGER LANGUAGE sql
    RETURN (SELECT test.sum_ints(1) AS sum_ints FROM test.active_users);

-- A default and a check that call a function that reads the same
-- table. The table needs the function and the function needs the
-- table, so pg_dump makes the table without them, then the function,
-- then the default (TOC entry "DEFAULT") and the check (TOC entry
-- "CHECK CONSTRAINT") as their own entries. The build does the same
-- (build deviation 40).
CREATE TABLE test.tickets (id INTEGER NOT NULL, n INTEGER);
CREATE FUNCTION test.next_ticket() RETURNS INTEGER LANGUAGE sql
    RETURN (SELECT COALESCE(max(n), 0) + 1 FROM test.tickets);
ALTER TABLE test.tickets ALTER COLUMN n SET DEFAULT test.next_ticket();
CREATE TABLE test.quotas (id INTEGER, n INTEGER);
CREATE FUNCTION test.quota_ok(v INTEGER) RETURNS BOOLEAN LANGUAGE sql
    RETURN v <= (SELECT count(*) FROM test.quotas);
ALTER TABLE test.quotas
    ADD CONSTRAINT quotas_n_check CHECK (test.quota_ok(n));
COMMENT ON CONSTRAINT quotas_n_check ON test.quotas IS 'Within the quota';

-- A domain CHECK that calls a function, which has to exist before the
-- domain. When the function takes the domain, the domain also has to
-- exist before the function, so pg_dump makes the domain without the
-- check, then the function, then the check (TOC entry "CHECK
-- CONSTRAINT"). The build does the same (build deviation 65).
CREATE FUNCTION test.rating_ok(v INTEGER) RETURNS BOOLEAN LANGUAGE sql
    RETURN v BETWEEN 1 AND 5;
CREATE DOMAIN test.rating AS INTEGER
    CONSTRAINT rating_check CHECK (test.rating_ok(VALUE));
CREATE DOMAIN test.score AS INTEGER;
CREATE FUNCTION test.score_ok(v test.score) RETURNS BOOLEAN LANGUAGE sql
    RETURN v::INTEGER >= 0;
ALTER DOMAIN test.score ADD CONSTRAINT score_check CHECK (test.score_ok(VALUE));

-- A NOT VALID domain CHECK. pg_dump writes it as its own entry (TOC
-- entry "CHECK CONSTRAINT"), and the build does the same (build
-- deviation 92).
CREATE DOMAIN test.percent AS INTEGER CONSTRAINT percent_low CHECK (VALUE >= 0);
ALTER DOMAIN test.percent
    ADD CONSTRAINT percent_high CHECK (VALUE <= 100) NOT VALID;

-- A column whose type is the row type of another table. Name order
-- puts `a_segments` before `z_points`, and tables share one priority,
-- so the table of the row type has to be ordered first (build
-- deviation 41).
CREATE TABLE test.z_points (x INTEGER, y INTEGER);
CREATE TABLE test.a_segments (
    id       INTEGER,
    start_at test.z_points,
    stops    test.z_points[]
);

-- A composite type, a domain and a range of a type that is a row type,
-- an enum or a domain of the project. Name order puts each one before
-- the type that it uses, and types and domains share one priority, so
-- the type that it uses has to be ordered first.
CREATE TYPE test.z_mood AS ENUM ('calm', 'busy');
CREATE DOMAIN test.z_level AS INTEGER;
CREATE TYPE test.a_reading AS (
    at    test.z_points,
    level test.z_level,
    mood  test.z_mood
);
CREATE DOMAIN test.a_point_domain AS test.z_points;
CREATE DOMAIN test.a_mood_domain AS test.z_mood;
CREATE TYPE test.a_mood_range AS RANGE (SUBTYPE = test.z_mood);

-- Routines that set a list setting. PostgreSQL searches pg_temp first
-- when search_path does not name it, so a SECURITY DEFINER function
-- names pg_temp last. pg_dump writes each element as a string
-- constant. The last function has elements that need quotes.
CREATE FUNCTION test.definer_now() RETURNS TIMESTAMPTZ
    LANGUAGE sql STABLE SECURITY DEFINER
    SET search_path = pg_catalog, pg_temp AS $$
 SELECT now();
$$;

CREATE PROCEDURE test.definer_touch() LANGUAGE sql
    SET search_path = pg_catalog, pg_temp AS $$
 SELECT 1;
$$;

CREATE FUNCTION test.quoted_path() RETURNS INTEGER LANGUAGE sql
    SET search_path = "Quoted Schema", "$user", '' AS $$
 SELECT 1;
$$;

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
-- an overload of the same operator, with its own comment
CREATE OPERATOR test.!!! (FUNCTION = int8um, RIGHTARG = BIGINT);
COMMENT ON OPERATOR test.!!! (NONE, BIGINT) IS 'Negates a bigint';

-- A SQL-standard body that uses an operator of this schema. Type order
-- puts a function before an operator, so pg_dump's edge to the
-- operator has to go in `dependencies`, and the operator has to come
-- after its own function.
CREATE FUNCTION test.a_uses_operator(a INTEGER, b INTEGER) RETURNS BOOLEAN
    LANGUAGE sql IMMUTABLE RETURN a OPERATOR(test.=~=) b;

-- Access methods: a table method and an index method, with the
-- handlers of the built-in ones. A table and a materialized view use
-- the table method, which pg_dump keeps in the entry, not in the DDL.
CREATE ACCESS METHOD heap_copy TYPE TABLE HANDLER heap_tableam_handler;
COMMENT ON ACCESS METHOD heap_copy IS 'A copy of heap';
CREATE ACCESS METHOD btree_copy TYPE INDEX HANDLER bthandler;
CREATE TABLE test.heap_copied (id INTEGER) USING heap_copy;
CREATE MATERIALIZED VIEW test.heap_copied_ids USING heap_copy AS
SELECT id
  FROM test.heap_copied;

-- Operator families and classes: a btree family with a member that no
-- class has, and a class in it; a default class for the copied index
-- method, which makes its own family; and a gist class with a storage
-- type and an ordering operator, which pg_dump moves to the family
CREATE FUNCTION test.compare_ints(a INTEGER, b INTEGER) RETURNS INTEGER
    LANGUAGE sql IMMUTABLE AS $$
 SELECT a - b;
$$;
CREATE OPERATOR FAMILY test.int_family USING btree;
COMMENT ON OPERATOR FAMILY test.int_family USING btree IS 'Integers';
ALTER OPERATOR FAMILY test.int_family USING btree ADD
    OPERATOR 1 < (INTEGER, BIGINT),
    FUNCTION 1 (INTEGER, BIGINT) btint48cmp(INTEGER, BIGINT);
CREATE OPERATOR CLASS test.int_class FOR TYPE INTEGER USING btree
    FAMILY test.int_family AS
    OPERATOR 1 <, OPERATOR 3 =, FUNCTION 1 test.compare_ints(INTEGER, INTEGER);
COMMENT ON OPERATOR CLASS test.int_class USING btree IS 'By difference';
CREATE OPERATOR CLASS test.int_copy_ops DEFAULT FOR TYPE INTEGER
    USING btree_copy AS
    OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >,
    FUNCTION 1 btint4cmp(INTEGER, INTEGER);
CREATE OPERATOR CLASS test.point_distance FOR TYPE point USING gist AS
    OPERATOR 15 <-> (point, point) FOR ORDER BY float_ops,
    FUNCTION 1 gist_point_consistent(internal, point, smallint, oid, internal),
    FUNCTION 2 gist_box_union(internal, internal),
    FUNCTION 3 gist_point_compress(internal),
    FUNCTION 5 gist_box_penalty(internal, internal, internal),
    FUNCTION 6 gist_box_picksplit(internal, internal),
    FUNCTION 7 gist_box_same(box, box, internal),
    FUNCTION 8 gist_point_distance(internal, point, smallint, oid, internal),
    STORAGE box;
-- an index that uses the default class of the copied method
CREATE INDEX heap_copied_id ON test.heap_copied USING btree_copy (id);

-- Indexes on a partitioned table. pg_dump makes the parent's index ON
-- ONLY, each partition's index on its own, and an INDEX ATTACH for
-- each. One index has partition indexes with names of their own, and
-- a unique constraint's indexes attach with their partitions. One
-- index and the primary key have INCLUDE columns.
CREATE TABLE test.readings (id INTEGER, taken DATE, label TEXT)
    PARTITION BY RANGE (taken);
CREATE TABLE test.readings_2020 PARTITION OF test.readings
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE test.readings_2021 PARTITION OF test.readings
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE INDEX readings_id ON test.readings (id);
CREATE INDEX readings_taken ON ONLY test.readings (taken) INCLUDE (id);
CREATE INDEX readings_2020_by_day ON test.readings_2020 (taken) INCLUDE (id);
ALTER INDEX test.readings_taken ATTACH PARTITION test.readings_2020_by_day;
CREATE INDEX readings_2021_by_day ON test.readings_2021 (taken) INCLUDE (id);
ALTER INDEX test.readings_taken ATTACH PARTITION test.readings_2021_by_day;
ALTER TABLE test.readings ADD CONSTRAINT readings_unique UNIQUE (id, taken);
ALTER TABLE test.readings ADD CONSTRAINT readings_pkey
    PRIMARY KEY (taken, id) INCLUDE (label);
COMMENT ON INDEX test.readings_2020_by_day IS 'Readings by day';

-- A base type: pg_dump writes a SHELL TYPE entry before the functions
-- that the type's input and output need. Its I/O functions are the
-- integer ones, so the type needs no C code.
CREATE TYPE test.base_int;
CREATE FUNCTION test.base_int_in(cstring) RETURNS test.base_int
    AS 'int4in' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION test.base_int_out(test.base_int) RETURNS cstring
    AS 'int4out' LANGUAGE internal IMMUTABLE STRICT;
CREATE TYPE test.base_int (INPUT = test.base_int_in,
    OUTPUT = test.base_int_out, LIKE = integer);
COMMENT ON TYPE test.base_int IS 'An integer with its own I/O';

-- A schema whose quoted name has a period, with a type, its comment
-- and a cast: the period does not separate the name's parts
CREATE SCHEMA "gate.dotted";
CREATE TYPE "gate.dotted"."pair.t" AS (x INTEGER);
COMMENT ON TYPE "gate.dotted"."pair.t" IS 'A type with a period';
CREATE CAST ("gate.dotted"."pair.t" AS TEXT) WITH INOUT;

-- Columns whose expressions hold the keywords of a NOT NULL
-- constraint: a generated IS NOT NULL, nullable and not, and a check
-- on IS NOT NULL. Each keeps its generated expression or its check,
-- and only the NOT NULL column is not null.
CREATE TABLE test.revocations (
    revoked_at   TIMESTAMPTZ,
    revoked      BOOLEAN GENERATED ALWAYS AS (revoked_at IS NOT NULL) STORED,
    revoked_nn   BOOLEAN GENERATED ALWAYS AS (revoked_at IS NOT NULL) STORED
        NOT NULL,
    revoked_v    BOOLEAN GENERATED ALWAYS AS (revoked_at IS NOT NULL) VIRTUAL,
    reason       TEXT CHECK (reason IS NOT NULL OR revoked_at IS NULL)
);

-- Grants on objects that a table makes, which are not tables or
-- sequences of their own in the project: an identity column's
-- sequence, and a partition modeled by its bounds. Each also gets the
-- default privileges of the test schema. deploy grants them when it
-- makes the table.
CREATE TABLE test.granted_ids (id INTEGER GENERATED ALWAYS AS IDENTITY);
GRANT SELECT ON SEQUENCE test.granted_ids_id_seq TO PUBLIC;
CREATE TABLE test.granted_parts (k INTEGER) PARTITION BY RANGE (k);
CREATE TABLE test.granted_parts_1 PARTITION OF test.granted_parts
    FOR VALUES FROM (0) TO (10);
GRANT INSERT ON test.granted_parts_1 TO PUBLIC;

-- Grants on a schema and on objects whose names need quoting: the ACL
-- keys hold each name as it is, and build quotes it
CREATE SCHEMA "Quoted Schema";
GRANT USAGE ON SCHEMA "Quoted Schema" TO PUBLIC;
CREATE TABLE "Quoted Schema"."Quoted Table" (
    "Id" INTEGER GENERATED BY DEFAULT AS IDENTITY,
    "Label" TEXT
);
GRANT SELECT ON "Quoted Schema"."Quoted Table" TO PUBLIC;
GRANT UPDATE ("Label") ON "Quoted Schema"."Quoted Table" TO PUBLIC;
GRANT USAGE ON SEQUENCE "Quoted Schema"."Quoted Table_Id_seq" TO PUBLIC;
CREATE TABLE "Quoted Schema"."Named Seq Table" (
    "Id" INTEGER GENERATED BY DEFAULT AS IDENTITY
        (SEQUENCE NAME "Quoted Schema"."Named Seq")
);
GRANT USAGE ON SEQUENCE "Quoted Schema"."Named Seq" TO PUBLIC;
CREATE VIEW "Quoted Schema"."Quoted View" AS
    SELECT "Label" FROM "Quoted Schema"."Quoted Table";
GRANT SELECT ON "Quoted Schema"."Quoted View" TO PUBLIC;
CREATE TYPE "Quoted Schema"."Quoted Enum" AS ENUM ('a');
REVOKE USAGE ON TYPE "Quoted Schema"."Quoted Enum" FROM PUBLIC;
CREATE FUNCTION "Quoted Schema".quoted_fn(n INTEGER) RETURNS INTEGER
    LANGUAGE sql IMMUTABLE AS $$
 SELECT n;
$$;
REVOKE EXECUTE ON FUNCTION "Quoted Schema".quoted_fn(INTEGER) FROM PUBLIC;
-- routines whose names and parameter names need quoting
CREATE FUNCTION "Quoted Schema"."Quoted Fn"("Count" INTEGER) RETURNS INTEGER
    LANGUAGE sql IMMUTABLE AS $$
 SELECT "Count";
$$;
COMMENT ON FUNCTION "Quoted Schema"."Quoted Fn"(INTEGER) IS 'A quoted function';
REVOKE EXECUTE ON FUNCTION "Quoted Schema"."Quoted Fn"(INTEGER) FROM PUBLIC;
CREATE PROCEDURE "Quoted Schema"."Quoted Proc"() LANGUAGE sql AS $$
 SELECT 1;
$$;
COMMENT ON PROCEDURE "Quoted Schema"."Quoted Proc"() IS 'A quoted procedure';
-- routines whose names have "(" or '"': the argument list is the
-- parenthesized text at the end of a tag, not the text after the
-- first "("
CREATE FUNCTION "Quoted Schema"."f(x)"(n INTEGER) RETURNS INTEGER
    LANGUAGE sql IMMUTABLE AS $$
 SELECT n;
$$;
COMMENT ON FUNCTION "Quoted Schema"."f(x)"(INTEGER) IS 'A name with parentheses';
REVOKE EXECUTE ON FUNCTION "Quoted Schema"."f(x)"(INTEGER) FROM PUBLIC;
CREATE FUNCTION "Quoted Schema"."g""(y"(t TEXT) RETURNS TEXT
    LANGUAGE sql IMMUTABLE AS $$
 SELECT t;
$$;
COMMENT ON FUNCTION "Quoted Schema"."g""(y"(TEXT) IS 'A name with a quote';
REVOKE EXECUTE ON FUNCTION "Quoted Schema"."g""(y"(TEXT) FROM PUBLIC;
-- no arguments: pull writes the name z(x)(), so that the load does
-- not read (x) as the argument list
CREATE FUNCTION "Quoted Schema"."z(x)"() RETURNS INTEGER
    LANGUAGE sql IMMUTABLE AS $$
 SELECT 1;
$$;
COMMENT ON FUNCTION "Quoted Schema"."z(x)"() IS 'No arguments';
REVOKE EXECUTE ON FUNCTION "Quoted Schema"."z(x)"() FROM PUBLIC;
CREATE PROCEDURE "Quoted Schema"."p(y)"() LANGUAGE sql AS $$
 SELECT 1;
$$;
COMMENT ON PROCEDURE "Quoted Schema"."p(y)"() IS 'No arguments';

-- Transforms for the base type: one with both functions and a
-- comment, and one with only its TO SQL function
CREATE FUNCTION test.base_int_from_sql(internal) RETURNS internal
    AS 'int4send' LANGUAGE internal IMMUTABLE;
CREATE FUNCTION test.base_int_to_sql(internal) RETURNS test.base_int
    AS 'int4recv' LANGUAGE internal IMMUTABLE;
CREATE TRANSFORM FOR test.base_int LANGUAGE sql (
    FROM SQL WITH FUNCTION test.base_int_from_sql(internal),
    TO SQL WITH FUNCTION test.base_int_to_sql(internal));
COMMENT ON TRANSFORM FOR test.base_int LANGUAGE sql
    IS 'Converts base_int for SQL functions';
CREATE TRANSFORM FOR test.base_int LANGUAGE plpgsql (
    TO SQL WITH FUNCTION test.base_int_to_sql(internal));

-- A subscription that does not connect, so it needs no publisher.
-- pg_dump writes each option that is not at its default. PostgreSQL
-- does not drop a database that has a subscription, so the gates drop
-- their databases with bin/drop-database.
CREATE SUBSCRIPTION gate_sub
    CONNECTION 'dbname=pglifecycle_nowhere' PUBLICATION gate_pub, "Gate Pub"
    WITH (connect = false, binary = true, streaming = off, origin = none);
COMMENT ON SUBSCRIPTION gate_sub IS 'A subscription with no publisher';

-- Settings of the database, and of a role in the database. A statement
-- must name the database, and the gates give the fixture other
-- database names, so the names come from current_database(). The
-- settings do not change what a session of the gates does: the
-- search_path has the default schemas first, DateStyle is the default
-- value, and the other names are custom settings or the default
-- tablespace. The connection limit does not apply to the superuser of
-- the gates. pg_dump writes it in the same entry as the settings.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I CONNECTION LIMIT 50',
                   current_database());
    EXECUTE format('ALTER DATABASE %I SET work_mem TO %L',
                   current_database(), '64MB');
    EXECUTE format('ALTER DATABASE %I SET search_path TO %L, %L, %L',
                   current_database(), '$user', 'public', 'Gate Path');
    EXECUTE format('ALTER DATABASE %I SET "DateStyle" TO %L',
                   current_database(), 'ISO, MDY');
    EXECUTE format('ALTER DATABASE %I SET gate.note TO %L',
                   current_database(), 'it''s');
    EXECUTE format('ALTER ROLE postgres IN DATABASE %I SET gate.role_note TO %L',
                   current_database(), 'r');
    EXECUTE format('ALTER ROLE postgres IN DATABASE %I SET temp_tablespaces TO %L',
                   current_database(), 'pg_default');
END
$$;

-- The comment of the database. pg_dump writes it in a COMMENT entry
-- that names the database, as for the settings above. The comment has
-- a quote and a line break.
DO $$
BEGIN
    EXECUTE format('COMMENT ON DATABASE %I IS %L', current_database(),
                   E'The gate''s database\nfor the fixtures');
END
$$;
