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

-- Procedures and operators: build and models support them, but pull
-- cannot parse them yet
CREATE PROCEDURE archive(days INT) LANGUAGE sql AS 'SELECT 1';
CREATE OPERATOR === (LEFTARG = INT, RIGHTARG = INT, FUNCTION = int4eq);

-- Operator classes and families, and access methods: no model yet
CREATE FUNCTION compare_ints(INT, INT) RETURNS INT LANGUAGE sql IMMUTABLE
    AS 'SELECT $1 - $2';
CREATE OPERATOR FAMILY int_family USING btree;
CREATE OPERATOR CLASS int_class FOR TYPE INT USING btree FAMILY int_family
    AS OPERATOR 1 <, FUNCTION 1 compare_ints(INT, INT);
CREATE ACCESS METHOD pglifecycle_heap TYPE TABLE HANDLER heap_tableam_handler;
