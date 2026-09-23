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

CREATE SCHEMA unsupported;
SET search_path = unsupported, public, pg_catalog;

CREATE ROLE pglifecycle_coverage_reader;

-- Column storage and compression
CREATE TABLE payloads (
    id   INT PRIMARY KEY,
    body TEXT COMPRESSION lz4
);
ALTER TABLE payloads ALTER COLUMN body SET STORAGE EXTERNAL;

-- A comment on a primary key, unique, check or foreign key constraint:
-- only an exclusion constraint carries one in the model
COMMENT ON CONSTRAINT payloads_pkey ON payloads IS 'The payload id';

-- Extended statistics
CREATE TABLE measurements (a INT, b INT);
CREATE STATISTICS measurements_stats (ndistinct, dependencies)
    ON a, b FROM measurements;

-- Rule
CREATE TABLE append_only (id INT);
CREATE RULE append_only_no_delete AS
    ON DELETE TO append_only DO INSTEAD NOTHING;
