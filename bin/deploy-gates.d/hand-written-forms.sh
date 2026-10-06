# Sourced by bin/deploy-gates (gate 2). A project can write a value in
# a form that pull does not write. deploy compares the two forms as
# equal, so the plan stays empty:
#
# 1. A dollar-quoted constant in a cast. PostgreSQL writes `$$a$$::text`
#    as `'a'::text`.
# 2. A materialized view query in another layout and with a `;` at the
#    end. deploy formats the query as pull does. The build writes one
#    `;` after the query.
# 3. A SQL function body in another layout. deploy formats the body as
#    pull does.
#
# A real change to each of them is still a change.

table="${WORKDIR}/project/tables/test/hand_written.yaml"
view="${WORKDIR}/project/materialized_views/test/hand_written_view.yaml"
function="${WORKDIR}/project/functions/test/hand_written_fn.yaml"
mkdir -p "${view%/*}" "${function%/*}"

# $1 is a pattern that the plan with --allow-drop must have, $2 says
# what the step checks
expect_hand_written_change() {
    ./target/debug/pglifecycle deploy --allow-drop \
        -o "${WORKDIR}/hand-written.sql" -d "${TARGET_DB}" \
        "${WORKDIR}/project"
    if ! grep -Eq "$1" "${WORKDIR}/hand-written.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/hand-written.sql" >&2
        exit 1
    fi
    echo "Convergence gate passed: $2"
}

cat > "${table}" <<'YAML'
---
name: hand_written
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
- name: label
  data_type: text
  default: $$it's$$::text
primary_key:
- id
check_constraints:
- name: hand_written_label
  expression: (label <> $q$a$q$::text)
YAML
cat > "${view}" <<'YAML'
---
name: hand_written_view
schema: test
owner: postgres
query: |
  SELECT id, label FROM test.hand_written WHERE (label <> 'b'::text);
dependencies:
  tables:
  - test.hand_written
YAML
cat > "${function}" <<'YAML'
---
name: hand_written_fn
schema: test
owner: postgres
returns: integer
language: sql
definition: SELECT a FROM (SELECT 1 AS a, 2 AS b) s;
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "hand-written forms are unchanged"

# real changes: another constant, query and body
perl -pi -e 's/^  default: \$\$it.s\$\$::text$/  default: \$\$is\$\$::text/' \
    "${table}"
expect_hand_written_change \
    "^ALTER TABLE ONLY test\.hand_written ALTER COLUMN label SET DEFAULT" \
    "a changed dollar-quoted default is a change"
perl -pi -e 's/^  default: \$\$is\$\$::text$/  default: \$\$it'"'"'s\$\$::text/' \
    "${table}"
perl -pi -e "s/'b'::text/'c'::text/" "${view}"
expect_hand_written_change \
    "^CREATE MATERIALIZED VIEW test\.hand_written_view .*'c'::text\);$" \
    "a changed materialized view query is a change"
perl -pi -e "s/'c'::text/'b'::text/" "${view}"
perl -pi -e 's/2 AS b/3 AS b/' "${function}"
expect_hand_written_change \
    '^CREATE OR REPLACE FUNCTION test\.hand_written_fn\(\)' \
    "a changed SQL function body is a change"
perl -pi -e 's/3 AS b/2 AS b/' "${function}"
expect_empty_plan "the reverted project is unchanged"

rm "${table}" "${view}" "${function}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP MATERIALIZED VIEW test.hand_written_view;" \
    -c "DROP TABLE test.hand_written;" \
    -c "DROP FUNCTION test.hand_written_fn;"
expect_empty_plan "the hand-written step leaves the database unchanged"
