# Sourced by bin/deploy-gates (gate 2). Access methods compare by
# definition. The project writes them in the short forms that a person
# writes, and the plan stays empty. A changed comment is set in place,
# without --allow-drop. PostgreSQL has no ALTER ACCESS METHOD, so a
# changed type or handler drops and makes the access method again,
# only with --allow-drop. An access method that only the database has
# is dropped, only with --allow-drop.

# $1 is a query that must return t, $2 says what the step checks
expect_access_method() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# the short forms: a handler in pg_catalog written qualified and in
# uppercase, and a new access method whose handler is in mixed case
perl -0pi -e 's/(name: btree_copy\n  type: INDEX\n  handler: )bthandler$/$1pg_catalog.BTHANDLER/m' \
    "${WORKDIR}/project/project.yaml"
perl -0pi -e 's/^access_methods:\n/$&- name: gate_am\n  type: INDEX\n  handler: HashHandler\n/m' \
    "${WORKDIR}/project/project.yaml"
grep -q 'handler: pg_catalog.BTHANDLER$' "${WORKDIR}/project/project.yaml"
grep -q '^- name: gate_am$' "${WORKDIR}/project/project.yaml"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_access_method "SELECT amhandler = 'hashhandler'::regproc
    FROM pg_am WHERE amname = 'gate_am'" \
    "the new access method was not made"
expect_empty_plan "access method short forms are unchanged"

# drift that deploy sets in place: comments
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
COMMENT ON ACCESS METHOD heap_copy IS 'drift';
COMMENT ON ACCESS METHOD gate_am IS 'drift';
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_access_method "SELECT count(*) = 2 AND bool_and(
        obj_description(oid, 'pg_am') IS NOT DISTINCT FROM
        CASE amname WHEN 'heap_copy' THEN 'A copy of heap' END)
    FROM pg_am WHERE amname IN ('heap_copy', 'gate_am')" \
    "the comment of an access method was not set back"
expect_empty_plan "changed access methods converge in place"

# a changed handler: no ALTER form, so deploy drops the access method
# and makes it again, only with --allow-drop
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP ACCESS METHOD gate_am;
CREATE ACCESS METHOD gate_am TYPE INDEX HANDLER bthandler;
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP ACCESS METHOD' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the access method rebuild was not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_access_method "SELECT amhandler = 'hashhandler'::regproc
    FROM pg_am WHERE amname = 'gate_am'" \
    "the changed access method was not made again"
expect_empty_plan "a changed access method converges with --allow-drop"

# an access method that only the database has, with a name that needs
# quoting
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c \
    "CREATE ACCESS METHOD \"Stray AM\" TYPE TABLE
        HANDLER heap_tableam_handler;"
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP ACCESS METHOD' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the access method drop was not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_access_method "SELECT NOT EXISTS (SELECT FROM pg_am
    WHERE amname = 'Stray AM')" \
    "the database-only access method was not dropped"
expect_empty_plan "a database-only access method is dropped"
