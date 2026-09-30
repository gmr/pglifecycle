# Sourced by bin/deploy-gates (gate 2). Conversions compare by
# definition. The project writes them in the short forms that a person
# writes, and the plan stays empty. A changed comment or owner is set
# in place, without --allow-drop. PostgreSQL has no ALTER for the other
# settings, so a changed one drops and makes the conversion again, only
# with --allow-drop. A conversion that only the database has is
# dropped, only with --allow-drop.

# $1 is a query that must return t, $2 says what the step checks
expect_conversion() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# a role is a cluster object, so an earlier run can have made it
psql -d postgres -q -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'Gate Owner') THEN
        CREATE ROLE "Gate Owner";
    END IF;
END
$$;
SQL

# the short forms: encoding aliases in other cases, a function in
# pg_catalog written qualified and in uppercase, and default written at
# its default
cat > "${WORKDIR}/project/conversions/test.yaml" <<'YAML'
---
schema: test
conversions:
- name: latin1_to_utf8
  schema: test
  owner: postgres
  default: true
  encoding_from: iso-8859-1
  encoding_to: utf-8
  function: PG_CATALOG.ISO8859_1_TO_UTF8
- name: gate_conv
  schema: test
  owner: postgres
  default: false
  encoding_from: Latin2
  encoding_to: Unicode
  function: iso8859_to_utf8
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
conversion_is() {
    echo "SELECT $2 FROM pg_conversion
        WHERE connamespace = 'test'::regnamespace AND conname = '$1'"
}
expect_conversion "$(conversion_is gate_conv "NOT condefault
    AND pg_encoding_to_char(conforencoding) = 'LATIN2'
    AND pg_encoding_to_char(contoencoding) = 'UTF8'")" \
    "the new conversion was not made"
expect_empty_plan "conversion short forms are unchanged"

# drift that deploy sets in place: an owner and comments
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER CONVERSION test.latin1_to_utf8 OWNER TO "Gate Owner";
COMMENT ON CONVERSION test.gate_conv IS 'drift';
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_conversion "$(conversion_is latin1_to_utf8 \
    "pg_get_userbyid(conowner) = 'postgres'")" \
    "the owner of a conversion was not set back"
expect_conversion "$(conversion_is gate_conv \
    "obj_description(oid, 'pg_conversion') IS NULL")" \
    "the comment of a conversion was not set back"
expect_empty_plan "changed conversions converge in place"

# a conversion made the default: no ALTER form, so deploy drops the
# conversion and makes it again, only with --allow-drop
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP CONVERSION test.gate_conv;
CREATE DEFAULT CONVERSION test.gate_conv FOR 'LATIN2' TO 'UTF8'
    FROM iso8859_to_utf8;
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP CONVERSION' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the conversion rebuild was not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_conversion "$(conversion_is gate_conv "NOT condefault")" \
    "the changed conversion was not made again"
expect_empty_plan "a changed conversion converges with --allow-drop"

# a conversion that only the database has, with a name that needs
# quoting
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c \
    "CREATE CONVERSION test.\"Stray Conv\" FOR 'LATIN3' TO 'UTF8'
        FROM iso8859_to_utf8;"
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP CONVERSION' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the conversion drop was not withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_conversion "SELECT NOT EXISTS (SELECT FROM pg_conversion
    WHERE conname = 'Stray Conv')" \
    "the database-only conversion was not dropped"
expect_empty_plan "a database-only conversion is dropped"
