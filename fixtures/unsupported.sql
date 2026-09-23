-- Fixture schema of constructs `pull` cannot model yet
--
-- Every object here is one `pull` drops into remaining.yaml rather
-- than into the project. bin/coverage-gate pulls this schema and
-- compares the descriptions that land in remaining.yaml against
-- fixtures/unsupported-descs.txt, so a new gap fails the gate and a
-- fix makes the expected list shrink. See PLAN-coverage.md.
--
-- Constructs that parse into the *wrong* model (NOT ENFORCED, NOT
-- VALID, NULLS NOT DISTINCT, WITHOUT OVERLAPS, PERIOD, virtual
-- generated columns) do not belong here: they never reach
-- remaining.yaml. They go into fixtures/schema.sql with their fix,
-- where the round-trip gate's schema diff is the assertion.
--
-- SECURITY LABEL is also absent: it needs a preloaded label provider,
-- and no provider ships with the standard server, so the statement
-- fails on the gate's cluster.

CREATE EXTENSION btree_gist;

CREATE SCHEMA unsupported;
SET search_path = unsupported, public, pg_catalog;

CREATE ROLE pglifecycle_coverage_reader;

-- Exclusion constraint
CREATE TABLE reservations (
    room   INT,
    during DATERANGE,
    CONSTRAINT reservations_no_overlap
        EXCLUDE USING gist (room WITH =, during WITH &&)
);

-- Column storage and compression, and the table's replica identity
CREATE TABLE payloads (
    id   INT PRIMARY KEY,
    body TEXT COMPRESSION lz4
);
ALTER TABLE payloads ALTER COLUMN body SET STORAGE EXTERNAL;
ALTER TABLE payloads REPLICA IDENTITY FULL;

-- Extended statistics
CREATE TABLE measurements (a INT, b INT);
CREATE STATISTICS measurements_stats (ndistinct, dependencies)
    ON a, b FROM measurements;

-- Rule
CREATE TABLE append_only (id INT);
CREATE RULE append_only_no_delete AS
    ON DELETE TO append_only DO INSTEAD NOTHING;

-- Default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA unsupported
    GRANT SELECT ON TABLES TO pglifecycle_coverage_reader;
