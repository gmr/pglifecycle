# Sourced by bin/deploy-gates (gate 2). Procedures compare by
# definition. The project writes them in the short forms that a person
# writes, and the plan stays empty. A changed body, security, setting
# or comment is set with CREATE OR REPLACE PROCEDURE, without
# --allow-drop. A renamed parameter needs a drop, and a procedure that
# only the database has is dropped, both only with --allow-drop.

# $1 is a query that must return t, $2 says what the step checks
expect_procedure() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# the short forms: type aliases and uppercase types, a typmod that
# PostgreSQL does not keep, a default and a setting written as numbers,
# the language in uppercase, a setting name in mixed case, an empty
# parameter list, and the default security written out. A body is
# compared as text, so it has the form that pull writes
cat > "${WORKDIR}/project/procedures/test/archive_before.yaml" <<'YAML'
---
name: archive_before
schema: test
owner: postgres
parameters:
- mode: IN
  data_type: INT4
  name: days
- mode: INOUT
  data_type: INTEGER
  name: archived
  default: 0
language: PLPGSQL
security: DEFINER
configuration:
  Search_Path: test
definition: |-
  BEGIN
    archived := days;
  END;
comment: Archives old rows
YAML
cat > "${WORKDIR}/project/procedures/test/touch_nothing.yaml" <<'YAML'
---
name: touch_nothing
schema: test
owner: postgres
parameters: []
language: sql
security: INVOKER
sql_body: |-
  BEGIN ATOMIC
   SELECT 1;
  END
YAML
cat > "${WORKDIR}/project/procedures/test/gate_proc.yaml" <<'YAML'
---
name: gate_proc
schema: test
owner: postgres
parameters:
- mode: IN
  name: label
  data_type: VARCHAR(20)
- mode: IN
  name: n
  data_type: int4
  default: 1
language: SQL
configuration:
  statement_timeout: 1000
definition: ' SELECT 1;'
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_procedure "SELECT to_regprocedure('test.gate_proc(varchar, integer)')
    IS NOT NULL" "the new procedure was not made"
expect_empty_plan "procedure short forms are unchanged"

# drift that CREATE OR REPLACE PROCEDURE reconciles: a body, the
# security, a setting and comments, also in a schema whose name needs
# quoting
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE OR REPLACE PROCEDURE test.archive_before(
    IN days integer, INOUT archived integer DEFAULT 0)
    LANGUAGE plpgsql SECURITY INVOKER AS $$ BEGIN archived := 0; END; $$;
COMMENT ON PROCEDURE test.archive_before(integer, integer) IS 'drift';
CREATE OR REPLACE PROCEDURE "Quoted Schema"."Quoted Proc"()
    LANGUAGE sql AS $$ SELECT 2; $$;
COMMENT ON PROCEDURE "Quoted Schema"."Quoted Proc"() IS NULL;
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_procedure "SELECT prosrc LIKE '%archived := days%' AND prosecdef
        AND proconfig = '{search_path=test}'
        AND obj_description(oid, 'pg_proc') = 'Archives old rows'
    FROM pg_proc
    WHERE oid = 'test.archive_before(integer, integer)'::regprocedure" \
    "the changed procedure was not replaced"
expect_procedure "SELECT prosrc LIKE '%SELECT 1%'
        AND obj_description(oid, 'pg_proc') = 'A quoted procedure'
    FROM pg_proc
    WHERE oid = '\"Quoted Schema\".\"Quoted Proc\"()'::regprocedure" \
    "the changed quoted procedure was not replaced"
expect_empty_plan "changed procedures converge in place"

# a renamed parameter: CREATE OR REPLACE PROCEDURE cannot rename it, so
# deploy drops the procedure and makes it again, only with --allow-drop
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP PROCEDURE test.gate_proc(varchar, integer);
CREATE PROCEDURE test.gate_proc(IN tag varchar, IN n integer DEFAULT 1)
    LANGUAGE sql SET statement_timeout = 1000 AS $$SELECT 1;$$;
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the procedure rebuild was not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_procedure "SELECT pg_get_function_identity_arguments(
        'test.gate_proc(varchar, integer)'::regprocedure)
    = 'IN label character varying, IN n integer'" \
    "the procedure with a renamed parameter was not made again"
expect_empty_plan "a renamed procedure parameter converges"

# procedures that only the database has, one in a schema whose name
# needs quoting and one with an OUT parameter
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE PROCEDURE "Quoted Schema"."Stray Proc"("Arg" INTEGER)
    LANGUAGE sql AS 'SELECT 1';
CREATE PROCEDURE test.stray_proc(IN a integer, OUT b integer)
    LANGUAGE sql AS 'SELECT a';
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP PROCEDURE' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 2 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the procedure drops were not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_procedure "SELECT
        to_regprocedure('\"Quoted Schema\".\"Stray Proc\"(integer)') IS NULL
        AND to_regprocedure('test.stray_proc(integer)') IS NULL" \
    "the database-only procedures were not dropped"
expect_empty_plan "database-only procedures are dropped"
