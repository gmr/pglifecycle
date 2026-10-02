# Sourced by bin/deploy-gates (gate 2). A routine name can have "(" or
# '"'. Deploy names such a routine with its full name when it changes
# its comment and when it drops it.

# $1 is a query that must return t, $2 says what the step checks
expect_routine_name() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# drift in the comments of the fixture routines
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
COMMENT ON FUNCTION "Quoted Schema"."f(x)"(integer) IS 'drift';
COMMENT ON FUNCTION "Quoted Schema"."g""(y"(text) IS NULL;
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_routine_name "SELECT
        obj_description('\"Quoted Schema\".\"f(x)\"(integer)'::regprocedure,
            'pg_proc') = 'A name with parentheses'
        AND obj_description('\"Quoted Schema\".\"g\"\"(y\"(text)'::regprocedure,
            'pg_proc') = 'A name with a quote'" \
    "the comments of routines with ( or \" in the name were not set"
expect_empty_plan "routine comments converge"

# routines that only the database has, with "(" and '"' in the name
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE FUNCTION "Quoted Schema"."h(z)"(integer) RETURNS integer
    LANGUAGE sql AS 'SELECT 1';
CREATE FUNCTION "Quoted Schema"."k""(w"(text) RETURNS integer
    LANGUAGE sql AS 'SELECT 1';
SQL
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_routine_name "SELECT
        to_regprocedure('\"Quoted Schema\".\"h(z)\"(integer)') IS NULL
        AND to_regprocedure('\"Quoted Schema\".\"k\"\"(w\"(text)') IS NULL" \
    "the database-only routines with ( or \" in the name were not dropped"
expect_empty_plan "database-only routines with ( or \" are dropped"
