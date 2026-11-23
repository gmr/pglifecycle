# Sourced by bin/deploy-gates (gate 2). Privileges on the functions of
# extensions. pg_dump writes them relative to the initial privileges of
# the extension (pg_init_privs) when it has them, else relative to the
# built-in privileges. A grant to PUBLIC in the database that the
# project does not have is revoked, only with --allow-drop.
#
# - gate_buf.pg_buffercache_pages(): the extension script grants
#   EXECUTE only to pg_monitor, and the project has no ACL for it.
# - gate_ext.crosstab(text): no initial privileges. The project
#   revokes EXECUTE from PUBLIC, and the database then has no ACL entry.

# $1 is a query that must return t, $2 says what the step checks
expect_extension_acl() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT EXECUTE ON FUNCTION gate_buf.pg_buffercache_pages() TO PUBLIC;
GRANT EXECUTE ON FUNCTION gate_ext.crosstab(text) TO PUBLIC;
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q "^-- destructive statements: 2 excluded" \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the extension revokes were not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_extension_acl "SELECT NOT bool_or(has_function_privilege('public',
        f::regprocedure, 'EXECUTE'))
    FROM unnest(ARRAY['gate_buf.pg_buffercache_pages()',
                      'gate_ext.crosstab(text)']) AS f" \
    "PUBLIC can still run the functions of the extensions"
expect_extension_acl "SELECT has_function_privilege('pg_monitor',
        'gate_buf.pg_buffercache_pages()', 'EXECUTE')" \
    "pg_monitor lost its initial privilege"
expect_empty_plan "extension function privileges converge"
