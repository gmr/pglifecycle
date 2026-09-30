# Sourced by bin/deploy-gates (gate 2). Collations compare by
# definition. The project writes them in the short forms that a person
# writes, and the plan stays empty. A changed comment or owner is set
# in place, without --allow-drop. PostgreSQL has no ALTER for the other
# settings, so a changed one drops and makes the collation again, only
# with --allow-drop. When a column uses the collation, the drop fails
# and deploy rolls back all of its changes. A collation that only the
# database has is dropped, only with --allow-drop.

# $1 is a query that must return t, $2 says what the step checks
expect_collation() {
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

# the short forms: lc_collate and lc_ctype in place of locale, no
# provider (libc is the default), deterministic written at its default,
# and an ICU locale that PostgreSQL writes in its standard form (en-US)
cat > "${WORKDIR}/project/collations/test/plain_c.yaml" <<'YAML'
---
name: plain_c
schema: test
owner: postgres
lc_collate: C
lc_ctype: C
deterministic: true
YAML
cat > "${WORKDIR}/project/collations/test/gate_icu.yaml" <<'YAML'
---
name: gate_icu
schema: test
owner: postgres
provider: icu
locale: EN_us
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_collation "SELECT colllocale = 'en-US' FROM pg_collation
    WHERE oid = 'test.gate_icu'::regcollation" \
    "the new collation was not made"
expect_empty_plan "collation short forms are unchanged"

# drift that deploy sets in place: an owner and comments
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER COLLATION test.plain_c OWNER TO "Gate Owner";
COMMENT ON COLLATION test.plain_c IS 'drift';
COMMENT ON COLLATION test.case_insensitive IS NULL;
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_collation "SELECT pg_get_userbyid(collowner) = 'postgres'
        AND obj_description(oid, 'pg_collation') IS NULL
    FROM pg_collation WHERE oid = 'test.plain_c'::regcollation" \
    "the owner or comment of a collation was not set back"
expect_collation "SELECT obj_description(oid, 'pg_collation') = 'Ignores case'
    FROM pg_collation WHERE oid = 'test.case_insensitive'::regcollation" \
    "the comment of a collation was not set back"
expect_empty_plan "changed collations converge in place"

# changed rules: no ALTER form, so deploy drops the collation and makes
# it again, only with --allow-drop
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP COLLATION test.b_before_a;
CREATE COLLATION test.b_before_a (provider = icu, locale = 'und',
    rules = '&c < a');
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP COLLATION' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the collation rebuild was not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_collation "SELECT collicurules = '&b < a' FROM pg_collation
    WHERE oid = 'test.b_before_a'::regcollation" \
    "the changed collation was not made again"
expect_empty_plan "a changed collation converges with --allow-drop"

# a column that uses the collation: the drop of the rebuild fails, and
# deploy rolls back the transaction, so no change stays, also not the
# comment that the same deploy sets
cat > "${WORKDIR}/project/tables/test/gate_collated.yaml" <<'YAML'
---
name: gate_collated
schema: test
owner: postgres
columns:
- name: v
  data_type: text
  collation: test.plain_c
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the table that uses a collation was made"
perl -pi -e 's/^(lc_collate|lc_ctype): C$/$1: POSIX/' \
    "${WORKDIR}/project/collations/test/plain_c.yaml"
grep -q '^lc_ctype: POSIX$' "${WORKDIR}/project/collations/test/plain_c.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "COMMENT ON COLLATION test.case_insensitive IS 'drift';"
COLLATION_ERR="${WORKDIR}/collation.err"
if ./target/debug/pglifecycle deploy --apply --allow-drop \
        -d "${TARGET_DB}" "${WORKDIR}/project" \
        > /dev/null 2>"${COLLATION_ERR}"; then
    echo "Convergence gate FAILED: the drop of a collation that a column" \
        "uses did not fail" >&2
    exit 1
fi
if ! grep -q 'the transaction was rolled back' "${COLLATION_ERR}" \
    || ! grep -q 'cannot drop collation test.plain_c because other objects' \
        "${COLLATION_ERR}"; then
    echo "Convergence gate FAILED: the failed collation drop does not" \
        "give a clear error" >&2
    cat "${COLLATION_ERR}" >&2
    exit 1
fi
expect_collation "SELECT collcollate = 'C'
        AND obj_description(
            'test.case_insensitive'::regcollation, 'pg_collation') = 'drift'
        AND to_regclass('test.gate_collated') IS NOT NULL
    FROM pg_collation WHERE oid = 'test.plain_c'::regcollation" \
    "the failed deploy did not roll back all of its changes"
grep -m 1 "cannot drop collation" "${COLLATION_ERR}"
echo "Convergence gate passed: a failed collation drop rolls back"
perl -pi -e 's/^(lc_collate|lc_ctype): POSIX$/$1: C/' \
    "${WORKDIR}/project/collations/test/plain_c.yaml"
rm "${WORKDIR}/project/tables/test/gate_collated.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.gate_collated;"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the collation is as the project has it"

# a collation that only the database has, in a schema whose name needs
# quoting
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c \
    "CREATE COLLATION \"Quoted Schema\".\"Stray Coll\" (locale = 'C');"
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP COLLATION' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the collation drop was not withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_collation "SELECT to_regcollation('\"Quoted Schema\".\"Stray Coll\"')
    IS NULL" "the database-only collation was not dropped"
expect_empty_plan "a database-only collation is dropped"
