# Sourced by bin/deploy-gates (gate 2). PostgreSQL stores no default
# when the default is a NULL of the type of the column (or of the base
# type of a domain): `NULL`, `NULL::integer` and `CAST(NULL AS int)`
# on an integer column. ALTER COLUMN SET DEFAULT NULL removes the
# default. deploy compares such a default as no default, so the plan
# stays empty. A NULL of another type, a NULL on a column of a domain
# or of a type with a modifier is stored, and stays a default.

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

rm "${table}" "${domain}" "${cast_domain}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.null_defaults;" \
    -c "DROP DOMAIN test.null_defaults_domain;" \
    -c "DROP DOMAIN test.null_defaults_cast;"
expect_empty_plan "the null-defaults step leaves the database unchanged"
