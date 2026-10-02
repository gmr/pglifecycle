# Sourced by bin/deploy-gates (gate 2). PostgreSQL stores no default
# when the default is a NULL of the type of the column (or of the base
# type of a domain): `NULL`, `NULL::integer` and `CAST(NULL AS int)`
# on an integer column. ALTER COLUMN SET DEFAULT NULL removes the
# default. deploy compares such a default as no default, so the plan
# stays empty. A NULL of another type is stored, and stays a default.

table="${WORKDIR}/project/tables/test/null_defaults.yaml"
domain="${WORKDIR}/project/domains/test/null_defaults_domain.yaml"
cast_domain="${WORKDIR}/project/domains/test/null_defaults_cast.yaml"

cat > "${domain}" <<'YAML'
---
name: null_defaults_domain
schema: test
owner: postgres
data_type: integer
default: 'NULL'
YAML
cat > "${cast_domain}" <<'YAML'
---
name: null_defaults_cast
schema: test
owner: postgres
data_type: integer
default: CAST(NULL AS int)
YAML
cat > "${table}" <<'YAML'
---
name: null_defaults
schema: test
owner: postgres
columns:
- name: a
  data_type: integer
  default: 'NULL'
- name: b
  data_type: integer
  default: NULL::int
- name: c
  data_type: integer
  default: null::integer
- name: d
  data_type: int4
  default: CAST(NULL AS int)
- name: e
  data_type: text
  default: (NULL)::text
- name: f
  data_type: timestamp with time zone
  default: NULL::timestamptz
- name: g
  data_type: bigint
  default: NULL::bigint::int8
- name: h
  data_type: integer
  default: NULL::bigint
- name: i
  data_type: test.null_defaults_domain
  default: NULL::integer
- name: j
  data_type: character varying(10)
  default: NULL::character varying
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a NULL default of the column type is no default"

# a database default and a NULL default in the project: deploy
# removes the database default
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "ALTER TABLE test.null_defaults ALTER COLUMN b SET DEFAULT 5;" \
    -c "ALTER DOMAIN test.null_defaults_domain SET DEFAULT 5;"
./target/debug/pglifecycle deploy -o "${WORKDIR}/null-defaults.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
for pattern in \
    '^ALTER TABLE test\.null_defaults ALTER COLUMN b DROP DEFAULT;' \
    '^ALTER DOMAIN test\.null_defaults_domain DROP DEFAULT;'; do
    if ! grep -Eq "${pattern}" "${WORKDIR}/null-defaults.sql"; then
        echo "Convergence gate FAILED: no ${pattern}" >&2
        cat "${WORKDIR}/null-defaults.sql" >&2
        exit 1
    fi
done
echo "Convergence gate passed: a database default becomes a NULL default"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the NULL defaults converge"

# A NULL default on a column of a type with a modifier, of a domain,
# or of an enum, a composite type or an array of a domain: PostgreSQL
# stores a NULL of the base type (`NULL::character varying` on a
# varchar(10) column, `NULL::integer` on a domain over integer) or no
# default. deploy compares the project's NULL in that form. The same
# applies to the default of a domain, and to a default on an
# inherited column, which has the type of the parent's column.
typmods="${WORKDIR}/project/tables/test/null_typmods.yaml"
parent="${WORKDIR}/project/tables/test/null_typmods_parent.yaml"
child="${WORKDIR}/project/tables/test/null_typmods_child.yaml"
varchar_domain="${WORKDIR}/project/domains/test/null_typmods_varchar.yaml"
nested_domain="${WORKDIR}/project/domains/test/null_typmods_nested.yaml"
enum_domain="${WORKDIR}/project/domains/test/null_typmods_enum.yaml"
cat > "${varchar_domain}" <<'YAML'
---
name: null_typmods_varchar
schema: test
owner: postgres
data_type: character varying(10)
default: 'NULL'
YAML
cat > "${nested_domain}" <<'YAML'
---
name: null_typmods_nested
schema: test
owner: postgres
data_type: test.null_defaults_domain
default: 'NULL'
YAML
cat > "${enum_domain}" <<'YAML'
---
name: null_typmods_enum
schema: test
owner: postgres
data_type: test.user_state
default: 'NULL'
YAML
cat > "${typmods}" <<'YAML'
---
name: null_typmods
schema: test
owner: postgres
columns:
- name: a
  data_type: varchar(10)
  default: 'NULL'
- name: b
  data_type: numeric(5,2)
  default: NULL::numeric
- name: c
  data_type: char
  default: 'NULL'
- name: d
  data_type: bit
  default: 'NULL'
- name: e
  data_type: timestamp(3) with time zone
  default: (NULL)
- name: f
  data_type: character varying(10)[]
  default: NULL::varchar[]
- name: g
  data_type: interval(2)
  default: 'NULL'
- name: h
  data_type: test.null_defaults_domain
  default: 'NULL'
- name: i
  data_type: test.null_defaults_domain
  default: NULL::test.null_defaults_domain
- name: j
  data_type: test.null_typmods_varchar
  default: CAST(NULL AS varchar)
- name: k
  data_type: test.user_state
  default: 'NULL'
- name: l
  data_type: test.user_state
  default: NULL::test.user_state
- name: m
  data_type: test.user_state[]
  default: 'NULL'
- name: "n"
  data_type: test.null_defaults_domain[]
  default: 'NULL'
- name: o
  data_type: test.point_pair
  default: 'NULL'
YAML
cat > "${parent}" <<'YAML'
---
name: null_typmods_parent
schema: test
owner: postgres
columns:
- name: v
  data_type: character varying(10)
- name: i
  data_type: integer
- name: d
  data_type: test.null_defaults_domain
YAML
cat > "${child}" <<'YAML'
---
name: null_typmods_child
schema: test
owner: postgres
parents:
- test.null_typmods_parent
column_defaults:
- column: v
  default: 'NULL'
- column: i
  default: 'NULL'
- column: d
  default: 'NULL'
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a NULL default is compared in the form PostgreSQL stores"

# a database default and a NULL default in the project: deploy sets
# the NULL of the base type, or removes the default
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "ALTER TABLE test.null_typmods ALTER COLUMN a SET DEFAULT 'x', \
        ALTER COLUMN h SET DEFAULT 5, \
        ALTER COLUMN k SET DEFAULT 'verified';" \
    -c "ALTER TABLE ONLY test.null_typmods_child \
        ALTER COLUMN v SET DEFAULT 'x', ALTER COLUMN i SET DEFAULT 1;" \
    -c "ALTER DOMAIN test.null_typmods_varchar SET DEFAULT 'x';" \
    -c "ALTER DOMAIN test.null_typmods_enum SET DEFAULT 'verified';"
./target/debug/pglifecycle deploy -o "${WORKDIR}/null-typmods.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
for pattern in \
    '^ALTER TABLE test\.null_typmods ALTER COLUMN a SET DEFAULT NULL::character varying;' \
    '^ALTER TABLE test\.null_typmods ALTER COLUMN h SET DEFAULT NULL::integer;' \
    '^ALTER TABLE test\.null_typmods ALTER COLUMN k DROP DEFAULT;' \
    '^ALTER TABLE ONLY test\.null_typmods_child ALTER COLUMN v SET DEFAULT NULL::character varying;' \
    '^ALTER TABLE ONLY test\.null_typmods_child ALTER COLUMN i DROP DEFAULT;' \
    '^ALTER DOMAIN test\.null_typmods_varchar SET DEFAULT NULL::character varying;' \
    '^ALTER DOMAIN test\.null_typmods_enum DROP DEFAULT;'; do
    if ! grep -Eq "${pattern}" "${WORKDIR}/null-typmods.sql"; then
        echo "Convergence gate FAILED: no ${pattern}" >&2
        cat "${WORKDIR}/null-typmods.sql" >&2
        exit 1
    fi
done
echo "Convergence gate passed: a database default becomes the stored NULL"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the stored NULL defaults converge"

rm "${typmods}" "${parent}" "${child}" "${varchar_domain}" \
    "${nested_domain}" "${enum_domain}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.null_typmods, test.null_typmods_child, \
        test.null_typmods_parent;" \
    -c "DROP DOMAIN test.null_typmods_varchar, test.null_typmods_nested, \
        test.null_typmods_enum;"
expect_empty_plan "the null-typmods step leaves the database unchanged"

rm "${table}" "${domain}" "${cast_domain}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.null_defaults;" \
    -c "DROP DOMAIN test.null_defaults_domain;" \
    -c "DROP DOMAIN test.null_defaults_cast;"
expect_empty_plan "the null-defaults step leaves the database unchanged"
