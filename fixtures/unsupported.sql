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

-- A base type: pg_dump writes a SHELL TYPE entry before the functions
-- that the type's input and output need. Its I/O functions are the
-- integer ones, so the type needs no C code.
CREATE TYPE base_int;
CREATE FUNCTION base_int_in(cstring) RETURNS base_int
    AS 'int4in' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION base_int_out(base_int) RETURNS cstring
    AS 'int4out' LANGUAGE internal IMMUTABLE STRICT;
CREATE TYPE base_int (INPUT = base_int_in, OUTPUT = base_int_out,
    LIKE = integer);

-- A transform for the base type
CREATE FUNCTION base_int_from_sql(internal) RETURNS internal
    AS 'int4send' LANGUAGE internal IMMUTABLE;
CREATE TRANSFORM FOR base_int LANGUAGE sql
    (FROM SQL WITH FUNCTION base_int_from_sql(internal));

-- A subscription that does not connect, so it needs no publisher. Its
-- name must match bin/coverage-gate, which drops it before it drops
-- the database.
CREATE SUBSCRIPTION pglifecycle_coverage_sub
    CONNECTION 'dbname=pglifecycle_nowhere' PUBLICATION nowhere
    WITH (connect = false);
