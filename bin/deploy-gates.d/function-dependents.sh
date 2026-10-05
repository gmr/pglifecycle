# Sourced by bin/deploy-gates (gate 2). When deploy drops a function
# and makes it again (a change that CREATE OR REPLACE FUNCTION cannot
# do), the objects that depend on the function stop the DROP. Deploy
# drops them before the function and makes them again after it: a view,
# a view on that view, and a column DEFAULT. Without --allow-drop all
# of these statements are withheld together.

mkdir -p "${WORKDIR}/project/views/test"
cat > "${WORKDIR}/project/functions/test/gate_dep.yaml" <<'YAML'
---
name: gate_dep
schema: test
owner: postgres
parameters:
- mode: IN
  name: a
  data_type: integer
- mode: IN
  name: b
  data_type: integer
returns: integer
language: sql
definition: SELECT a + b;
YAML
cat > "${WORKDIR}/project/views/test/gate_dep_view.yaml" <<'YAML'
---
name: gate_dep_view
schema: test
owner: postgres
query: ' SELECT test.gate_dep(1, 1) AS n'
comment: calls test.gate_dep
YAML
cat > "${WORKDIR}/project/views/test/gate_dep_outer.yaml" <<'YAML'
---
name: gate_dep_outer
schema: test
owner: postgres
query: " SELECT n\n   FROM test.gate_dep_view"
dependencies:
  views:
  - test.gate_dep_view
YAML
cat > "${WORKDIR}/project/tables/test/gate_dep_table.yaml" <<'YAML'
---
name: gate_dep_table
schema: test
owner: postgres
columns:
- name: n
  data_type: integer
  default: test.gate_dep(2, 0)
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a function with dependents is made"

# $1 is the CREATE FUNCTION of the database: the database function and
# its dependents are made again from it
mutate_dependents() {
    psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<SQL
ALTER TABLE test.gate_dep_table ALTER COLUMN n DROP DEFAULT;
DROP VIEW test.gate_dep_outer;
DROP VIEW test.gate_dep_view;
DROP FUNCTION test.gate_dep(integer, integer);
$1;
CREATE VIEW test.gate_dep_view AS SELECT test.gate_dep(1, 1) AS n;
COMMENT ON VIEW test.gate_dep_view IS 'calls test.gate_dep';
CREATE VIEW test.gate_dep_outer AS SELECT n FROM test.gate_dep_view;
REVOKE SELECT ON test.gate_dep_view, test.gate_dep_outer FROM PUBLIC;
ALTER TABLE test.gate_dep_table ALTER COLUMN n
    SET DEFAULT test.gate_dep(2, 0);
SQL
}

# $1 says what the step checks: the function and its dependents are
# made again
expect_dependents() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT
            pg_get_function_arguments(
                'test.gate_dep(integer, integer)'::regprocedure)
                = 'a integer, b integer'
            AND (SELECT n FROM test.gate_dep_outer) = 2
            AND obj_description('test.gate_dep_view'::regclass,
                'pg_class') = 'calls test.gate_dep'
            AND pg_get_expr(adbin, adrelid) = 'test.gate_dep(2, 0)'
            FROM pg_attrdef
            WHERE adrelid = 'test.gate_dep_table'::regclass")" != t ]
    then
        echo "Convergence gate FAILED: the function or its dependents" \
            "were not made again ($1)" >&2
        exit 1
    fi
    expect_empty_plan "$1"
}

# a default that the project does not have: CREATE OR REPLACE cannot
# remove it, thus deploy drops the function and makes it again
mutate_dependents 'CREATE FUNCTION test.gate_dep(a integer,
    b integer DEFAULT 0) RETURNS integer LANGUAGE sql
    AS $$SELECT a + b;$$'

# without --allow-drop: the function and each dependent are withheld,
# and the script has no statement for them
./target/debug/pglifecycle deploy -o "${WORKDIR}/dependents.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q '^-- destructive statements: [0-9]* excluded' \
        "${WORKDIR}/dependents.sql" \
    || ! grep -q '^-- dependents rebuilt with a replaced function: 3 ' \
        "${WORKDIR}/dependents.sql" \
    || grep -v '^--' "${WORKDIR}/dependents.sql" | grep -q gate_dep
then
    echo "Convergence gate FAILED: the function and its dependents" \
        "were not withheld together" >&2
    cat "${WORKDIR}/dependents.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_dependents "a replaced function with dependents converges"

# an input parameter with another name: deploy drops the database
# function, which the project does not have, and makes the project's
mutate_dependents 'CREATE FUNCTION test.gate_dep(x integer, b integer)
    RETURNS integer LANGUAGE sql AS $$SELECT x + b;$$'
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_dependents "a removed function with dependents converges"

# $1 says what the step checks; the deploy must fail, because a
# dependent cannot be dropped and made again, and its error must name
# $2
expect_refused() {
    if ./target/debug/pglifecycle deploy --apply --allow-drop "${@:3}" \
            -d "${TARGET_DB}" "${WORKDIR}/project" \
            2> "${WORKDIR}/refused.txt"
    then
        echo "Convergence gate FAILED: deploy did not refuse $1" >&2
        exit 1
    fi
    if ! grep -q "$2" "${WORKDIR}/refused.txt"; then
        echo "Convergence gate FAILED: the error does not name $2" >&2
        cat "${WORKDIR}/refused.txt" >&2
        exit 1
    fi
    echo "Convergence gate passed: deploy refuses $1"
}

# a disabled trigger whose WHEN calls the function. The project does
# not keep the enabled state of a trigger, thus a DROP TRIGGER and a
# CREATE TRIGGER would enable it again: deploy refuses
cat > "${WORKDIR}/project/functions/test/gate_dep_tf.yaml" <<'YAML'
---
name: gate_dep_tf
schema: test
owner: postgres
returns: trigger
language: plpgsql
definition: |-
  BEGIN
    RETURN NEW;
  END;
YAML
cp "${WORKDIR}/project/tables/test/gate_dep_table.yaml" \
    "${WORKDIR}/gate_dep_table.yaml"
cat >> "${WORKDIR}/project/tables/test/gate_dep_table.yaml" <<'YAML'
triggers:
- name: gate_dep_tg
  when: BEFORE
  events:
  - UPDATE
  for_each: ROW
  condition: (test.gate_dep(new.n, 0) > 0)
  function: test.gate_dep_tf()
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a trigger that calls a function with dependents is made"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c 'DROP TRIGGER gate_dep_tg ON test.gate_dep_table'
mutate_dependents 'CREATE FUNCTION test.gate_dep(a integer,
    b integer DEFAULT 0) RETURNS integer LANGUAGE sql
    AS $$SELECT a + b;$$'
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE TRIGGER gate_dep_tg BEFORE UPDATE ON test.gate_dep_table
    FOR EACH ROW WHEN (test.gate_dep(new.n, 0) > 0)
    EXECUTE FUNCTION test.gate_dep_tf();
ALTER TABLE test.gate_dep_table DISABLE TRIGGER gate_dep_tg;
SQL
expect_refused "a disabled trigger" gate_dep_tg
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT tgenabled FROM pg_trigger
        WHERE tgname = 'gate_dep_tg'")" != D ]; then
    echo "Convergence gate FAILED: the trigger is not disabled" >&2
    exit 1
fi
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c 'DROP TRIGGER gate_dep_tg ON test.gate_dep_table'
cp "${WORKDIR}/gate_dep_table.yaml" \
    "${WORKDIR}/project/tables/test/gate_dep_table.yaml"
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_dependents "a function with no disabled trigger converges"
