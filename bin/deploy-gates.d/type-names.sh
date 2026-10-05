# Sourced by bin/deploy-gates (gate 2). A project can write a type
# name in a form that PostgreSQL keeps in another form: an alias
# (`int4`), a keyword form (`float(10)`, `char`, `timestamp`), a
# modifier with spaces (`decimal(10, 2)`) or an array form
# (`int[3]`), a quoted built-in name (`"int4"`) or the array type name
# of a built-in type (`_text`). pg_dump writes the form that
# format_type gives. deploy compares the two forms as equal, so the
# plan stays empty. This is true for a column type, for the type of a
# cast in an index or an exclusion constraint expression, in a WHERE
# clause, a CHECK constraint or a default, for the parameter and
# return types of a function, and for the column types of RETURNS
# TABLE. A real change of type is still a change.

table="${WORKDIR}/project/tables/test/type_names.yaml"
function="${WORKDIR}/project/functions/test/type_names_fn.yaml"
typmods="${WORKDIR}/project/functions/test/type_names_typmods.yaml"
returns_table="${WORKDIR}/project/functions/test/type_names_table.yaml"
domain="${WORKDIR}/project/domains/test/type_names_domain.yaml"

# $1 is a pattern that the plan with --allow-drop must have, $2 says
# what the step checks
expect_type_change() {
    ./target/debug/pglifecycle deploy --allow-drop \
        -o "${WORKDIR}/type-names.sql" -d "${TARGET_DB}" \
        "${WORKDIR}/project"
    if ! grep -Eq "$1" "${WORKDIR}/type-names.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/type-names.sql" >&2
        exit 1
    fi
    echo "Convergence gate passed: $2"
}

cat > "${table}" <<'YAML'
---
name: type_names
schema: test
owner: postgres
columns:
- name: id
  data_type: int4
  nullable: false
- name: f
  data_type: float
- name: f10
  data_type: float(10)
- name: f30
  data_type: FLOAT(30)
- name: c
  data_type: char
- name: nc
  data_type: national char(4)
- name: nv
  data_type: national character varying(7)
- name: v
  data_type: varchar
- name: b
  data_type: bit
- name: vb
  data_type: varbit(4)
- name: ts
  data_type: timestamp
- name: ts3
  data_type: timestamptz(3)
- name: tstz
  data_type: TIMESTAMP WITH TIME ZONE
- name: t2
  data_type: time(2)
- name: ttz
  data_type: timetz(0)
- name: iv
  data_type: interval day to second(3)
- name: ihm
  data_type: INTERVAL HOUR TO MINUTE
- name: d
  data_type: decimal(10, 2)
- name: d10
  data_type: decimal(10)
- name: dn
  data_type: dec
- name: i2
  data_type: int2
- name: i8
  data_type: int8
- name: bo
  data_type: bool
- name: a1
  data_type: int[3]
- name: a2
  data_type: integer ARRAY
- name: a3
  data_type: _int4
- name: qc
  data_type: '"char"'
- name: q4
  data_type: '"int4"'
- name: qv
  data_type: '"varchar"(10)'
- name: qa
  data_type: '"_int4"'
- name: qb
  data_type: '"bpchar"(3)'
- name: qt
  data_type: '"timestamptz"(3)'
- name: qq
  data_type: pg_catalog."int8"
- name: qbit
  data_type: '"bit"'
- name: qtext
  data_type: '"text"'
- name: at
  data_type: _text
- name: au
  data_type: _uuid
- name: av
  data_type: _varchar(4)
- name: atz
  data_type: _timestamptz
- name: ar
  data_type: _int4range
- name: aj
  data_type: _JSONB
- name: dfv
  data_type: text
  default: '''x''::VARCHAR'
- name: dfn
  data_type: bigint
  default: (3)::INT8
indexes:
- name: type_names_cast
  method: btree
  columns:
  - expression: ((nv)::VARCHAR(20))
  - expression: ((i2)::FLOAT(10))
  - expression: ((d)::DECIMAL(12, 2))
- name: type_names_where
  method: btree
  columns:
  - name: id
  where: ((i2)::INT8 > 0)
primary_key:
- id
check_constraints:
- name: type_names_check
  expression: ((i2)::INT4 > 0)
exclude_constraints:
- name: type_names_exclude
  method: btree
  elements:
  - expression: (c)::NATIONAL CHARACTER VARYING(9)
    operator: =
  - expression: (i8)::DOUBLE PRECISION
    operator: =
  where: ((i8)::FLOAT8 > (0)::FLOAT8)
row_level_security:
  enabled: false
YAML
cat > "${function}" <<'YAML'
---
name: type_names_fn
schema: test
owner: postgres
parameters:
- mode: IN
  name: a
  data_type: float
- mode: IN
  name: b
  data_type: char
- mode: IN
  name: c
  data_type: TIMESTAMP WITH TIME ZONE
- mode: IN
  name: d
  data_type: int[3]
returns: SETOF INT4
language: sql
definition: ' SELECT 1;'
YAML
# PostgreSQL keeps no typmod in a parameter or a return type (the
# fields of an interval are its typmod), and writes bpchar there as
# character
cat > "${typmods}" <<'YAML'
---
name: type_names_typmods
schema: test
owner: postgres
parameters:
- mode: IN
  name: a
  data_type: varchar(10)
- mode: IN
  name: b
  data_type: bpchar
- mode: IN
  name: c
  data_type: interval day to second(3)
returns: numeric(10, 2)
language: sql
definition: ' SELECT 1;'
YAML
# PostgreSQL keeps no typmod in a RETURNS TABLE column either
cat > "${returns_table}" <<'YAML'
---
name: type_names_table
schema: test
owner: postgres
returns: TABLE(a int4, b VARCHAR(3), "C" _text, d "numeric"(10, 2))
language: sql
definition: ' SELECT 1;'
YAML
cat > "${domain}" <<'YAML'
---
name: type_names_domain
schema: test
owner: postgres
data_type: int4
default: (1)::INT2
check_constraints:
- name: type_names_domain_check
  expression: ((VALUE)::INT8 > 0)
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "hand-written type names are unchanged"

# real changes: another column type, cast type and parameter type
perl -pi -e 's/^  data_type: float\(10\)$/  data_type: float(30)/' "${table}"
expect_type_change '^ALTER TABLE test\.type_names ALTER COLUMN f10 TYPE ' \
    "a changed column type is a change"
perl -pi -e 's/^  data_type: float\(30\)$/  data_type: float(10)/' "${table}"
perl -pi -e 's/::FLOAT\(10\)\)$/::FLOAT(30))/' "${table}"
expect_type_change \
    '^CREATE INDEX type_names_cast .*\(i2\)::double precision' \
    "a changed cast type in an index is a change"
perl -pi -e 's/::FLOAT\(30\)\)$/::FLOAT(10))/' "${table}"
perl -pi -e 's/::DOUBLE PRECISION$/::REAL/' "${table}"
expect_type_change \
    '^ALTER TABLE test\.type_names ADD CONSTRAINT type_names_exclude .*\(i8\)::real' \
    "a changed cast type in an exclusion constraint is a change"
perl -pi -e 's/::REAL$/::DOUBLE PRECISION/' "${table}"
perl -pi -e 's/^  data_type: varchar\(10\)$/  data_type: text/' "${typmods}"
expect_type_change '^CREATE FUNCTION test\.type_names_typmods\(IN a text' \
    "a changed parameter type is a change"
perl -pi -e 's/^  data_type: text$/  data_type: varchar(10)/' "${typmods}"
perl -pi -e 's/^  data_type: \x27"int4"\x27$/  data_type: \x27"int8"\x27/' \
    "${table}"
expect_type_change '^ALTER TABLE test\.type_names ALTER COLUMN q4 TYPE "int8"' \
    "a changed quoted type name is a change"
perl -pi -e 's/^  data_type: \x27"int8"\x27$/  data_type: \x27"int4"\x27/' \
    "${table}"
perl -pi -e 's/^  data_type: _uuid$/  data_type: _inet/' "${table}"
expect_type_change '^ALTER TABLE test\.type_names ALTER COLUMN au TYPE _inet' \
    "a changed array type name is a change"
perl -pi -e 's/^  data_type: _inet$/  data_type: _uuid/' "${table}"
perl -pi -e 's/^  expression: \(\(i2\)::INT4 > 0\)$/  expression: ((i2)::INT8 > 0)/' \
    "${table}"
expect_type_change '^ALTER TABLE test\.type_names ADD CONSTRAINT type_names_check CHECK \(\(\(i2\)::bigint > 0\)\)' \
    "a changed cast type in a check constraint is a change"
perl -pi -e 's/^  expression: \(\(i2\)::INT8 > 0\)$/  expression: ((i2)::INT4 > 0)/' \
    "${table}"
perl -pi -e 's/ \(3\)::INT8$/ (3)::INT4/' "${table}"
expect_type_change '^ALTER TABLE ONLY test\.type_names ALTER COLUMN dfn SET DEFAULT \(3\)::integer' \
    "a changed cast type in a default is a change"
perl -pi -e 's/ \(3\)::INT4$/ (3)::INT8/' "${table}"
perl -pi -e 's/^  where: \(\(i2\)::INT8 > 0\)$/  where: ((i2)::INT4 > 0)/' "${table}"
expect_type_change '^CREATE INDEX type_names_where .*\(i2\)::integer > 0' \
    "a changed cast type in an index WHERE clause is a change"
perl -pi -e 's/^  where: \(\(i2\)::INT4 > 0\)$/  where: ((i2)::INT8 > 0)/' "${table}"
perl -pi -e 's/ VARCHAR\(3\), / text, /' "${returns_table}"
expect_type_change '^CREATE FUNCTION test\.type_names_table\(\) RETURNS TABLE\(a int4, b text' \
    "a changed RETURNS TABLE column type is a change"
perl -pi -e 's/ text, "C"/ VARCHAR(3), "C"/' "${returns_table}"
perl -pi -e 's/::INT8 > 0\)$/::INT2 > 0)/' "${domain}"
expect_type_change '^CREATE DOMAIN test\.type_names_domain ' \
    "a changed cast type in a domain check constraint is a change"
perl -pi -e 's/::INT2 > 0\)$/::INT8 > 0)/' "${domain}"
expect_empty_plan "the reverted project is unchanged"

rm "${table}" "${function}" "${typmods}" "${returns_table}" "${domain}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.type_names;" \
    -c "DROP FUNCTION test.type_names_fn;" \
    -c "DROP FUNCTION test.type_names_typmods;" \
    -c "DROP FUNCTION test.type_names_table;" \
    -c "DROP DOMAIN test.type_names_domain;"
expect_empty_plan "the type-names step leaves the database unchanged"
