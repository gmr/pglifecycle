# Sourced by bin/deploy-gates (gate 2). Aggregates compare by
# definition.
#
# 1. Aggregates written by hand in a short form: type aliases and
#    uppercase types, a qualified pg_catalog function, a sort operator
#    without OPERATOR(), and options at their defaults. The plan is
#    empty.
# 2. Drift in the options and the comment converges with CREATE OR
#    REPLACE AGGREGATE, and owner drift with ALTER AGGREGATE ... OWNER
#    TO, without --allow-drop.
# 3. A changed state type changes the result type, so the aggregate is
#    dropped and made again, only with --allow-drop.
# 4. Aggregates that only the database has are dropped, only with
#    --allow-drop.

# $1 is a query that must return t, $2 says what the step checks
expect_aggregate() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# $1 is the number of statements that deploy must withhold without
# --allow-drop, $2 says what the step checks
expect_aggregate_withheld() {
    ./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if grep -q '^DROP AGGREGATE' "${WORKDIR}/withheld.sql" \
        || ! grep -q "^-- destructive statements: $1 excluded" \
            "${WORKDIR}/withheld.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/withheld.sql" >&2
        exit 1
    fi
}

# a role is a cluster object, so an earlier run can have made it
psql -d postgres -q -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'Gate Owner') THEN
        CREATE ROLE "Gate Owner";
    END IF;
END
$$;
SQL

cat > "${WORKDIR}/project/aggregates/test/gate_max.yaml" <<'YAML'
---
name: gate_max
schema: test
owner: postgres
arguments:
- data_type: INT4
  mode: IN
sfunc: pg_catalog.int4larger
state_data_type: INT
state_data_size: 0
ffunc: PG_CATALOG.INT4ABS
finalfunc_extra: false
finalfunc_modify: READ_ONLY
sort_operator: '>'
parallel: UNSAFE
hypothetical: false
YAML
cat > "${WORKDIR}/project/aggregates/test/gate_count.yaml" <<'YAML'
---
name: gate_count
schema: test
owner: postgres
arguments: []
sfunc: int8inc
state_data_type: int8
initial_condition: '0'
parallel: SAFE
YAML
cat > "${WORKDIR}/project/aggregates/test/gate_pick.yaml" <<'YAML'
---
name: gate_pick
schema: test
owner: postgres
arguments:
- data_type: integer
order_by:
- data_type: INTEGER
sfunc: TEST.ADD_INTS
state_data_type: integer
ffunc: test.add_ints
finalfunc_modify: READ_WRITE
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_aggregate "SELECT count(*) = 3 FROM pg_aggregate
    WHERE aggfnoid::regprocedure::text IN ('test.gate_max(integer)',
        'test.gate_count()', 'test.gate_pick(integer,integer)')" \
    "the new aggregates were not made"
expect_empty_plan "aggregate short forms are unchanged"

# drift that CREATE OR REPLACE AGGREGATE reconciles: the options, and
# the comment. Owner drift on an overload is set back in place
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE OR REPLACE AGGREGATE test.sum_ints(integer) (
    SFUNC = test.add_ints, STYPE = integer, INITCOND = '1');
COMMENT ON AGGREGATE test.sum_ints(integer) IS 'drift';
COMMENT ON AGGREGATE test.sum_sorted(integer ORDER BY integer) IS 'drift';
ALTER AGGREGATE test.sum_ints(bigint) OWNER TO "Gate Owner";
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_aggregate "SELECT a.agginitval = '0' AND p.proparallel = 's'
        AND obj_description(p.oid, 'pg_proc') = 'Adds integers'
    FROM pg_aggregate a JOIN pg_proc p ON p.oid = a.aggfnoid
    WHERE p.oid = 'test.sum_ints(integer)'::regprocedure" \
    "the changed aggregate was not replaced"
expect_aggregate "SELECT obj_description(
        'test.sum_sorted(integer, integer)'::regprocedure, 'pg_proc') IS NULL
    AND pg_get_userbyid(proowner) = 'postgres'
    FROM pg_proc WHERE oid = 'test.sum_ints(bigint)'::regprocedure" \
    "the aggregate comment or owner was not set back"
expect_empty_plan "changed aggregates converge in place"

# a changed state type: CREATE OR REPLACE AGGREGATE cannot change the
# result type, so the aggregate is dropped and made again
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP AGGREGATE test.gate_max(integer);
CREATE AGGREGATE test.gate_max(integer) (SFUNC = int84pl, STYPE = bigint,
    INITCOND = '0');
SQL
expect_aggregate_withheld 1 "the aggregate rebuild was not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_aggregate "SELECT aggtranstype = 'integer'::regtype
    FROM pg_aggregate WHERE aggfnoid = 'test.gate_max(integer)'::regprocedure" \
    "the aggregate with a changed state type was not made again"
expect_empty_plan "an aggregate with a changed state type converges"

# aggregates that only the database has: one with no arguments, and an
# ordered-set one, whose archive tag has no ORDER BY
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE AGGREGATE test.stray_count(*) (SFUNC = int8inc, STYPE = int8,
    INITCOND = '0');
CREATE AGGREGATE test.stray_sorted(integer ORDER BY integer) (
    SFUNC = test.add_ints, STYPE = integer);
SQL
expect_aggregate_withheld 2 "the aggregate drops were not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_aggregate "SELECT NOT EXISTS (SELECT FROM pg_proc
    WHERE pronamespace = 'test'::regnamespace
      AND proname IN ('stray_count', 'stray_sorted'))" \
    "the database-only aggregates were not dropped"
expect_empty_plan "database-only aggregates are dropped"
