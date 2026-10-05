# Sourced by bin/deploy-gates (gate 2). A project can leave out a
# part of a CHECK constraint or an index that PostgreSQL adds when it
# keeps the object, and pg_dump then writes:
#
# 1. A CHECK expression with no outer parentheses (`ee > 0`).
#    PostgreSQL keeps it as `(ee > 0)`. This is true for a table CHECK
#    constraint, a NOT VALID one and a domain CHECK constraint.
# 2. An index with no method. PostgreSQL makes a btree index, and
#    pg_dump writes `USING btree`.
# 3. An index column with the default order (`ASC`) and the default
#    NULL placement (`NULLS LAST` with `ASC`, `NULLS FIRST` with
#    `DESC`), which pg_dump does not write.
# 4. An index with `unique`, `recurse` and `nulls_not_distinct` at
#    their defaults, which pg_dump does not write.
#
# deploy compares the two forms as equal, so the plan stays empty. A
# real change is still a change.

table="${WORKDIR}/project/tables/test/default_forms.yaml"
domain="${WORKDIR}/project/domains/test/default_forms_domain.yaml"

# $1 is a pattern that the plan must have, $2 says what the step checks
expect_default_change() {
    ./target/debug/pglifecycle deploy --allow-drop \
        -o "${WORKDIR}/default-forms.sql" -d "${TARGET_DB}" \
        "${WORKDIR}/project"
    if ! grep -Eq "$1" "${WORKDIR}/default-forms.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/default-forms.sql" >&2
        exit 1
    fi
    echo "Convergence gate passed: $2"
}

cat > "${table}" <<'YAML'
---
name: default_forms
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
- name: ee
  data_type: integer
- name: label
  data_type: text
indexes:
- name: default_forms_ee
  columns:
  - name: ee
- name: default_forms_order
  columns:
  - name: ee
    direction: ASC
    null_placement: LAST
  - name: label
    direction: DESC
    null_placement: FIRST
- name: default_forms_expression
  columns:
  - expression: lower(label)
- name: default_forms_flags
  unique: false
  recurse: true
  nulls_not_distinct: false
  columns:
  - name: id
primary_key:
- id
check_constraints:
- name: default_forms_check
  expression: ee > 0
- name: default_forms_not_valid
  expression: ee < 100
  not_valid: true
YAML
cat > "${domain}" <<'YAML'
---
name: default_forms_domain
schema: test
owner: postgres
data_type: integer
check_constraints:
- name: default_forms_domain_check
  expression: VALUE > 0
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "hand-written default forms are unchanged"

# real changes: another CHECK constant, another index method and
# another order
perl -pi -e 's/^  expression: ee > 0$/  expression: ee > 1/' "${table}"
expect_default_change \
    '^ALTER TABLE test\.default_forms ADD CONSTRAINT default_forms_check CHECK \(\(?ee > 1\)?\)' \
    "a changed CHECK constraint is a change"
perl -pi -e 's/^  expression: ee > 1$/  expression: ee > 0/' "${table}"
perl -pi -e 's/^  expression: VALUE > 0$/  expression: VALUE > 1/' \
    "${domain}"
expect_default_change \
    'default_forms_domain_check CHECK \(\(?VALUE > 1\)?\)' \
    "a changed domain CHECK constraint is a change"
perl -pi -e 's/^  expression: VALUE > 1$/  expression: VALUE > 0/' \
    "${domain}"
perl -0pi -e 's/(- name: default_forms_ee\n)/$1  method: hash\n/' \
    "${table}"
expect_default_change \
    '^CREATE INDEX default_forms_ee ON test\.default_forms USING hash' \
    "a changed index method is a change"
perl -0pi -e 's/  method: hash\n//' "${table}"
perl -0pi -e 's/ASC\n    null_placement: LAST/ASC\n    null_placement: FIRST/' \
    "${table}"
expect_default_change \
    '^CREATE INDEX default_forms_order .*NULLS FIRST' \
    "a changed NULL placement is a change"
perl -0pi -e 's/ASC\n    null_placement: FIRST/ASC\n    null_placement: LAST/' \
    "${table}"
expect_empty_plan "the reverted project is unchanged"

rm "${table}" "${domain}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.default_forms;" \
    -c "DROP DOMAIN test.default_forms_domain;"
expect_empty_plan "the default-forms step leaves the database unchanged"
