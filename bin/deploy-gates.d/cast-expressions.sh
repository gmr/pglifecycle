# Sourced by bin/deploy-gates (gate 2). A project can write the type
# of a cast in a generated column expression, in a policy USING or
# WITH CHECK expression, or in a trigger WHEN condition in a form that
# PostgreSQL keeps in another form (`(i)::INT8` is `(i)::bigint`).
# deploy compares the two forms as equal, so the plan stays empty. A
# real change of one of these expressions is still a change.

table="${WORKDIR}/project/tables/test/cast_expressions.yaml"
function="${WORKDIR}/project/functions/test/cast_expressions_fn.yaml"

# $1 is a pattern that the plan with --allow-drop must have, $2 says
# what the step checks
expect_cast_change() {
    ./target/debug/pglifecycle deploy --allow-drop \
        -o "${WORKDIR}/cast-expressions.sql" -d "${TARGET_DB}" \
        "${WORKDIR}/project"
    if ! grep -Eq "$1" "${WORKDIR}/cast-expressions.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/cast-expressions.sql" >&2
        exit 1
    fi
    echo "Convergence gate passed: $2"
}

cat > "${function}" <<'YAML'
---
name: cast_expressions_fn
schema: test
owner: postgres
returns: trigger
language: plpgsql
definition: |-
  BEGIN
    RETURN NEW;
  END;
YAML
cat > "${table}" <<'YAML'
---
name: cast_expressions
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
- name: i
  data_type: smallint
- name: g
  data_type: bigint
  generated:
    expression: ((i)::INT8 * 2)
    kind: stored
primary_key:
- id
triggers:
- name: cast_expressions_trigger
  when: BEFORE
  events:
  - UPDATE
  for_each: ROW
  condition: ((new.i)::INT4 > 0)
  function: test.cast_expressions_fn()
row_level_security:
  enabled: true
policies:
- name: cast_expressions_policy
  using: ((i)::INT4 > 0)
  with_check: ((i)::INT8 > 0)
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "hand-written cast types in expressions are unchanged"

# real changes: another cast type in each expression
perl -pi -e 's/^    expression: \(\(i\)::INT8 \* 2\)$/    expression: ((i)::INT8 * 3)/' \
    "${table}"
expect_cast_change \
    '^ALTER TABLE test\.cast_expressions ALTER COLUMN g SET EXPRESSION AS \(\(i\)::bigint \* 3\)' \
    "a changed generated column expression is a change"
perl -pi -e 's/^    expression: \(\(i\)::INT8 \* 3\)$/    expression: ((i)::INT8 * 2)/' \
    "${table}"
perl -pi -e 's/^  using: \(\(i\)::INT4 > 0\)$/  using: ((i)::INT4 > 1)/' "${table}"
expect_cast_change '^CREATE POLICY cast_expressions_policy .*USING \(+\(i\)::integer > 1\)' \
    "a changed policy USING expression is a change"
perl -pi -e 's/^  using: \(\(i\)::INT4 > 1\)$/  using: ((i)::INT4 > 0)/' "${table}"
perl -pi -e 's/^  with_check: \(\(i\)::INT8 > 0\)$/  with_check: ((i)::INT2 > 0)/' \
    "${table}"
expect_cast_change '^CREATE POLICY cast_expressions_policy .*WITH CHECK \(+\(i\)::smallint > 0\)' \
    "a changed policy WITH CHECK expression is a change"
perl -pi -e 's/^  with_check: \(\(i\)::INT2 > 0\)$/  with_check: ((i)::INT8 > 0)/' \
    "${table}"
perl -pi -e 's/^  condition: \(\(new\.i\)::INT4 > 0\)$/  condition: ((new.i)::INT8 > 0)/' \
    "${table}"
expect_cast_change \
    '^CREATE TRIGGER cast_expressions_trigger .*WHEN \(+\(new\.i\)::bigint > 0\)' \
    "a changed trigger WHEN condition is a change"
perl -pi -e 's/^  condition: \(\(new\.i\)::INT8 > 0\)$/  condition: ((new.i)::INT4 > 0)/' \
    "${table}"
expect_empty_plan "the reverted project is unchanged"

rm "${table}" "${function}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.cast_expressions;" \
    -c "DROP FUNCTION test.cast_expressions_fn;"
expect_empty_plan "the cast-expressions step leaves the database unchanged"
