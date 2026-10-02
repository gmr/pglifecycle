# Sourced by bin/deploy-gates (gate 2). Privileges on objects that the
# database has: deploy compares the ACL of each object with the ACL
# that the project gives, and emits GRANT and REVOKE for the
# difference. A REVOKE takes access away, thus it is withheld without
# --allow-drop, and the script warns that the database grants more
# than the project. pg_monitor is a role that each cluster has.
public_file="${WORKDIR}/project/roles/PUBLIC.yaml"
cp "${public_file}" "${WORKDIR}/privileges.orig.yaml"

# the ACLs of the objects that the steps change, one line each, with
# the built-in ACL for none and the items in a fixed order
object_acls() {
    psql -d "$1" -qtAc "
        CREATE FUNCTION pg_temp.items(acl aclitem[]) RETURNS text
            LANGUAGE sql AS
            'SELECT string_agg(a::text, '','' ORDER BY a::text)
               FROM unnest(acl) AS a';
        SELECT 'r ' || oid::regclass, pg_temp.items(coalesce(relacl,
                   acldefault((CASE relkind WHEN 'S' THEN 's'
                               ELSE 'r' END)::\"char\", relowner)))
          FROM pg_class
         WHERE oid IN ('test.users'::regclass, 'test.tickets'::regclass,
                       'test.granted_ids_id_seq'::regclass,
                       '\"Quoted Schema\".\"Quoted Table\"'::regclass)
        UNION ALL
        SELECT 'c ' || attrelid::regclass || '.' || attname,
               pg_temp.items(attacl)
          FROM pg_attribute
         WHERE attrelid = '\"Quoted Schema\".\"Quoted Table\"'::regclass
           AND attnum > 0
        UNION ALL
        SELECT 'f ' || oid::regprocedure,
               pg_temp.items(coalesce(proacl, acldefault('f', proowner)))
          FROM pg_proc
         WHERE oid = '\"Quoted Schema\".quoted_fn(integer)'::regprocedure
        UNION ALL
        SELECT 'n ' || nspname,
               pg_temp.items(coalesce(nspacl, acldefault('n', nspowner)))
          FROM pg_namespace WHERE nspname = 'test'
        ORDER BY 1"
}

# $1 is a file that must have the extended regular expression $2;
# $3 says what the step checks
expect_line() {
    if ! grep -Eq "$2" "$1"; then
        echo "Convergence gate FAILED: $3" >&2
        cat "$1" >&2
        exit 1
    fi
}

# drift on objects that the database has: grants that the project does
# not have, on a table, a function and a schema, and grants of the
# project taken away from a table, a column and an identity sequence
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
GRANT SELECT, INSERT ON test.users TO pg_monitor;
GRANT EXECUTE ON FUNCTION "Quoted Schema".quoted_fn(integer) TO PUBLIC;
GRANT USAGE ON SCHEMA test TO pg_monitor WITH GRANT OPTION;
REVOKE SELECT ON test.tickets FROM PUBLIC;
REVOKE UPDATE ("Label") ON "Quoted Schema"."Quoted Table" FROM PUBLIC;
REVOKE USAGE ON SEQUENCE test.granted_ids_id_seq FROM PUBLIC;
SQL

# --no-privileges does not compare privileges
./target/debug/pglifecycle deploy -x -o "${WORKDIR}/privileges-x.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -Eq '^(GRANT|REVOKE)' "${WORKDIR}/privileges-x.sql"; then
    echo "Convergence gate FAILED: --no-privileges changed privileges" >&2
    cat "${WORKDIR}/privileges-x.sql" >&2
    exit 1
fi

# without --allow-drop: the GRANTs are in the script, the REVOKEs are
# withheld with a warning, and --apply refuses
./target/debug/pglifecycle deploy -o "${WORKDIR}/privileges.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
plan="${WORKDIR}/privileges.sql"
expect_line "${plan}" '^GRANT SELECT ON TABLE test\.tickets TO PUBLIC;' \
    "a grant of the project taken away is not given back"
expect_line "${plan}" \
    '^GRANT UPDATE\("Label"\) ON TABLE "Quoted Schema"\."Quoted Table" TO PUBLIC;' \
    "a column grant of the project taken away is not given back"
expect_line "${plan}" \
    '^GRANT USAGE ON SEQUENCE test\.granted_ids_id_seq TO PUBLIC;' \
    "a grant on an identity sequence taken away is not given back"
expect_line "${plan}" '^-- WARNING: TABLE test\.users withheld' \
    "a grant on a table that the project does not have is not withheld"
expect_line "${plan}" '^-- WARNING: FUNCTION .*quoted_fn.* withheld' \
    "a grant on a function that the project does not have is not withheld"
expect_line "${plan}" '^-- WARNING: SCHEMA test withheld' \
    "a grant on a schema that the project does not have is not withheld"
if grep -q '^REVOKE' "${plan}"; then
    echo "Convergence gate FAILED: a REVOKE is in the script without" \
        "--allow-drop" >&2
    cat "${plan}" >&2
    exit 1
fi
if ./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
        "${WORKDIR}/project" 2>/dev/null; then
    echo "Convergence gate FAILED: --apply ran with REVOKEs pending" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(object_acls "${TARGET_DB}")" != "$(object_acls "${SOURCE_DB}")" ]
then
    echo "Convergence gate FAILED: privilege drift remains" >&2
    diff <(object_acls "${SOURCE_DB}") <(object_acls "${TARGET_DB}") >&2
    exit 1
fi
expect_empty_plan "privilege drift on existing objects converged"

# a grant of the project on an existing object is deployed without
# --allow-drop; removed from the project, it is revoked
perl -0pi -e 's/^  tables:\n/$&    test.users:\n    - SELECT\n    - UPDATE\n/m' \
    "${public_file}"
grep -q '^    test.users:$' "${public_file}"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
users_grants="SELECT has_table_privilege('public', 'test.users', 'SELECT')
    AND has_table_privilege('public', 'test.users', 'UPDATE')"
if [ "$(psql -d "${TARGET_DB}" -tAc "${users_grants}")" != t ]; then
    echo "Convergence gate FAILED: a grant of the project on an existing" \
        "object was not deployed" >&2
    exit 1
fi
expect_empty_plan "a new grant on an existing object is deployed"
cp "${WORKDIR}/privileges.orig.yaml" "${public_file}"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(object_acls "${TARGET_DB}")" != "$(object_acls "${SOURCE_DB}")" ]
then
    echo "Convergence gate FAILED: a removed grant was not revoked" >&2
    diff <(object_acls "${SOURCE_DB}") <(object_acls "${TARGET_DB}") >&2
    exit 1
fi
expect_empty_plan "a grant removed from the project is revoked"

# a hand-written form of the same privileges: ALL for the whole list,
# the privileges in another order, the owner's own privileges, the
# built-in privileges of PUBLIC (on a domain, which pg_dump writes as
# a type), and a revoke of a privilege that the database does not grant
perl -0pi -e '
    s/(\n    test\.granted_ids_id_seq:\n)    - SELECT\n    - USAGE\n/$1    - USAGE\n    - SELECT\n/;
    s/(\n    test\.answer\(\):\n    - )ALL/$1EXECUTE/;
    s/^revocations:\n/$&  tables:\n    test.users:\n    - DELETE\n/m;
    s/^  schemata:\n/  domains:\n    test.bcp47_locale:\n    - USAGE\n$&/m;
' "${public_file}"
grep -q '^    test.answer():$' "${public_file}"
grep -q '^    test.bcp47_locale:$' "${public_file}"
cat > "${WORKDIR}/project/roles/postgres.yaml" <<'YAML'
---
name: postgres
create: false
grants:
  tables:
    test.users:
    - ALL
  functions:
    test.answer():
    - EXECUTE
YAML
expect_empty_plan "hand-written privileges are unchanged"
rm "${WORKDIR}/project/roles/postgres.yaml"
cp "${WORKDIR}/privileges.orig.yaml" "${public_file}"

# a new object gets the ACL of the project, and not the default
# privileges of the role that runs the script: postgres takes EXECUTE
# on new functions away from PUBLIC and gives SELECT on new tables in
# test to PUBLIC; "Gate Owner" has no default privileges
psql -d postgres -q -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'Gate Owner') THEN
        CREATE ROLE "Gate Owner";
    END IF;
END
$$;
SQL
cat > "${WORKDIR}/project/tables/test/gate_priv.yaml" <<'YAML'
---
name: gate_priv
schema: test
owner: Gate Owner
columns:
- name: id
  data_type: integer
YAML
mkdir -p "${WORKDIR}/project/functions/test"
cat > "${WORKDIR}/project/functions/test/gate_priv.yaml" <<'YAML'
---
name: gate_priv
schema: test
owner: Gate Owner
returns: integer
language: sql
definition: ' SELECT 1;'
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
new_acls="WITH owner(id) AS (
    SELECT oid FROM pg_roles WHERE rolname = 'Gate Owner'),
acls(acl, expected) AS (
    SELECT coalesce(relacl, acldefault('r', relowner)), acldefault('r', id)
      FROM pg_class, owner WHERE oid = 'test.gate_priv'::regclass
    UNION ALL
    SELECT coalesce(proacl, acldefault('f', proowner)), acldefault('f', id)
      FROM pg_proc, owner WHERE oid = 'test.gate_priv()'::regprocedure
)
SELECT count(*) = 2 AND bool_and(
    ARRAY(SELECT a::text FROM unnest(acl) AS a ORDER BY 1)
    = ARRAY(SELECT a::text FROM unnest(expected) AS a ORDER BY 1))
FROM acls"
if [ "$(psql -d "${TARGET_DB}" -tAc "${new_acls}")" != t ]; then
    echo "Convergence gate FAILED: a new object got the default" \
        "privileges of the connecting role" >&2
    psql -d "${TARGET_DB}" -tAc "SELECT relacl FROM pg_class
        WHERE oid = 'test.gate_priv'::regclass" >&2
    psql -d "${TARGET_DB}" -tAc "SELECT proacl FROM pg_proc
        WHERE oid = 'test.gate_priv()'::regprocedure" >&2
    exit 1
fi
expect_empty_plan "a new object has the ACL of the project"
rm "${WORKDIR}/project/tables/test/gate_priv.yaml" \
    "${WORKDIR}/project/functions/test/gate_priv.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP TABLE test.gate_priv;
DROP FUNCTION test.gate_priv();
SQL
expect_empty_plan "the new objects are removed"
