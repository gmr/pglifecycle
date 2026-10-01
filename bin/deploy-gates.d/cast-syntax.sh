# Sourced by bin/deploy-gates (gate 2). A project can write a cast as
# `x::type` or as `CAST(x AS type)`. PostgreSQL keeps the two forms as
# one cast, and writes it as `(x)::type`: the operand is in
# parentheses, but not a string literal or NULL (`'a'::text`,
# `NULL::integer`). deploy compares the forms as equal, so the plan
# stays empty. This is true for a CHECK constraint, a default, a
# generated column, an index expression and WHERE clause, a policy
# USING and WITH CHECK expression, a trigger WHEN condition and a
# domain default and CHECK constraint. A real change is still a change.

table="${WORKDIR}/project/tables/test/cast_syntax.yaml"
function="${WORKDIR}/project/functions/test/cast_syntax_fn.yaml"
domain="${WORKDIR}/project/domains/test/cast_syntax_domain.yaml"

# $1 is a pattern that the plan with --allow-drop must have, $2 says
# what the step checks
expect_syntax_change() {
    ./target/debug/pglifecycle deploy --allow-drop \
        -o "${WORKDIR}/cast-syntax.sql" -d "${TARGET_DB}" \
        "${WORKDIR}/project"
    if ! grep -Eq "$1" "${WORKDIR}/cast-syntax.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/cast-syntax.sql" >&2
        exit 1
    fi
    echo "Convergence gate passed: $2"
}

cat > "${function}" <<'YAML'
---
name: cast_syntax_fn
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
name: cast_syntax
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
- name: i
  data_type: smallint
- name: t
  data_type: text
- name: d
  data_type: bigint
  default: 1::bigint
- name: e
  data_type: bigint
  default: CAST(2 AS bigint)
- name: n
  data_type: text
  default: CAST('x' AS varchar)
- name: g
  data_type: bigint
  generated:
    expression: (i::int8 * 2)
    kind: stored
indexes:
- name: cast_syntax_expression
  method: btree
  columns:
  - expression: (i::bigint)
- name: cast_syntax_where
  method: btree
  columns:
  - name: id
  where: (CAST(i AS int) > 0)
primary_key:
- id
check_constraints:
- name: cast_syntax_check
  expression: (i::int > 0)
- name: cast_syntax_nested
  expression: (abs(i)::int::bigint > CAST(1 AS bigint))
- name: cast_syntax_literal
  expression: (t <> CAST('a' AS text))
triggers:
- name: cast_syntax_trigger
  when: BEFORE
  events:
  - UPDATE
  for_each: ROW
  condition: (new.i::int > 0)
  function: test.cast_syntax_fn()
row_level_security:
  enabled: true
policies:
- name: cast_syntax_policy
  using: (CAST(i AS integer) > 0)
  with_check: (i::int8 > 0)
YAML
cat > "${domain}" <<'YAML'
---
name: cast_syntax_domain
schema: test
owner: postgres
data_type: integer
default: 1::int2
check_constraints:
- name: cast_syntax_domain_check
  expression: (VALUE::int8 > 0)
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "hand-written cast syntax is unchanged"

# real changes: another operand or constant in a cast
perl -pi -e 's/^  expression: \(i::int > 0\)$/  expression: (i::int > 1)/' \
    "${table}"
expect_syntax_change \
    '^ALTER TABLE test\.cast_syntax ADD CONSTRAINT cast_syntax_check CHECK \(\(\(i\)::integer > 1\)\)' \
    "a changed CHECK constraint is a change"
perl -pi -e 's/^  expression: \(i::int > 1\)$/  expression: (i::int > 0)/' \
    "${table}"
perl -pi -e 's/^  default: 1::bigint$/  default: 3::bigint/' "${table}"
expect_syntax_change \
    '^ALTER TABLE test\.cast_syntax ALTER COLUMN d SET DEFAULT \(3\)::bigint' \
    "a changed default is a change"
perl -pi -e 's/^  default: 3::bigint$/  default: 1::bigint/' "${table}"
perl -pi -e 's/^  condition: \(new\.i::int > 0\)$/  condition: (new.id::bigint > 0)/' \
    "${table}"
expect_syntax_change \
    '^CREATE TRIGGER cast_syntax_trigger .*WHEN \(+\(new\.id\)::bigint > 0\)' \
    "a changed trigger WHEN condition is a change"
perl -pi -e 's/^  condition: \(new\.id::bigint > 0\)$/  condition: (new.i::int > 0)/' \
    "${table}"
expect_empty_plan "the reverted project is unchanged"

rm "${table}" "${function}" "${domain}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.cast_syntax;" \
    -c "DROP FUNCTION test.cast_syntax_fn;" \
    -c "DROP DOMAIN test.cast_syntax_domain;"
expect_empty_plan "the cast-syntax step leaves the database unchanged"
