# Sourced by bin/deploy-gates (gate 2). A routine setting that
# PostgreSQL keeps as a list, such as search_path, is a list in the
# project. Each element is one name. A SECURITY DEFINER function with
# a wrong search_path searches pg_temp first, so the gate compares
# proconfig exactly.
#
# 1. A function and a procedure with a list are made with one name for
#    each element, and the plan is then empty.
# 2. A changed list in the database is a change, and CREATE OR
#    REPLACE sets the list again.
# 3. A list setting written as one string with a comma is refused,
#    because PostgreSQL reads it as one name.

# $1 is a query that must return t, $2 says what the step checks
expect_setting() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        psql -d "${TARGET_DB}" -tAc "SELECT oid::regprocedure, proconfig
            FROM pg_proc WHERE proname IN ('list_path', 'list_path_proc')" \
            >&2
        exit 1
    fi
}

cat > "${WORKDIR}/project/functions/test/list_path.yaml" <<'YAML'
---
name: list_path
schema: test
owner: postgres
returns: text
language: sql
security: DEFINER
configuration:
  search_path:
  - pg_catalog
  - Quoted Schema
  - $user
  - pg_temp
definition: ' SELECT current_user::text;'
YAML
cat > "${WORKDIR}/project/procedures/test/list_path_proc.yaml" <<'YAML'
---
name: list_path_proc
schema: test
owner: postgres
language: sql
configuration:
  search_path: [pg_catalog, pg_temp]
definition: ' SELECT 1;'
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_setting "SELECT proconfig
        = ARRAY['search_path=pg_catalog, \"Quoted Schema\", \"\$user\", pg_temp']
    FROM pg_proc WHERE oid = 'test.list_path()'::regprocedure" \
    "the function list setting is not one name for each element"
expect_setting "SELECT proconfig = ARRAY['search_path=pg_catalog, pg_temp']
    FROM pg_proc WHERE oid = 'test.list_path_proc()'::regprocedure" \
    "the procedure list setting is not one name for each element"
expect_empty_plan "list settings are unchanged"

# the same elements in another order search pg_temp first
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER FUNCTION test.list_path() SET search_path = pg_temp, pg_catalog;
ALTER PROCEDURE test.list_path_proc() SET search_path = pg_temp, pg_catalog;
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_setting "SELECT proconfig
        = ARRAY['search_path=pg_catalog, \"Quoted Schema\", \"\$user\", pg_temp']
    FROM pg_proc WHERE oid = 'test.list_path()'::regprocedure" \
    "the changed function list setting was not set again"
expect_setting "SELECT proconfig = ARRAY['search_path=pg_catalog, pg_temp']
    FROM pg_proc WHERE oid = 'test.list_path_proc()'::regprocedure" \
    "the changed procedure list setting was not set again"
expect_empty_plan "changed list settings converge"

# one string with a comma is one name to PostgreSQL
sed -i.bak 's/^  search_path: .*/  search_path: pg_catalog, pg_temp/' \
    "${WORKDIR}/project/procedures/test/list_path_proc.yaml"
if ./target/debug/pglifecycle deploy -o "${WORKDIR}/refused.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project" \
        > /dev/null 2> "${WORKDIR}/refused.err"; then
    echo "Convergence gate FAILED: a list setting written as one" \
        "string was not refused" >&2
    exit 1
fi
if ! grep -q 'search_path is a list: write each name as a list item' \
        "${WORKDIR}/refused.err"; then
    echo "Convergence gate FAILED: a list setting written as one" \
        "string failed for an unexpected reason" >&2
    cat "${WORKDIR}/refused.err" >&2
    exit 1
fi
echo "Convergence gate passed: a list setting as one string is refused"
mv "${WORKDIR}/project/procedures/test/list_path_proc.yaml.bak" \
    "${WORKDIR}/project/procedures/test/list_path_proc.yaml"
expect_empty_plan "the list setting step leaves the database unchanged"
