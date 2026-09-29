# Sourced by bin/deploy-gates (gate 2). deploy gives each object the
# owner that the project names. A new object gets ALTER ... OWNER TO
# after its CREATE. An object that the database has with another owner
# gets ALTER ... OWNER TO in place, without --allow-drop. With
# --no-owner, deploy does not change owners. The role name needs
# quoting.

# $1 is a query that must return t, $2 says what the step checks
expect_owner() {
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

# new objects that the connecting role does not own: a schema, a table
# in it, and a function, whose owner statement needs its signature
cat > "${WORKDIR}/project/schemata/gate_own.yaml" <<'YAML'
---
name: gate_own
owner: Gate Owner
YAML
mkdir -p "${WORKDIR}/project/tables/gate_own" \
    "${WORKDIR}/project/functions/gate_own"
cat > "${WORKDIR}/project/tables/gate_own/t.yaml" <<'YAML'
---
name: t
schema: gate_own
owner: Gate Owner
columns:
- name: id
  data_type: integer
YAML
cat > "${WORKDIR}/project/functions/gate_own/f.yaml" <<'YAML'
---
name: f
schema: gate_own
owner: Gate Owner
parameters:
- mode: IN
  name: n
  data_type: integer
returns: integer
language: sql
definition: ' SELECT n;'
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_owner "SELECT
    pg_get_userbyid(nspowner) = 'Gate Owner'
    FROM pg_namespace WHERE nspname = 'gate_own'" \
    "the new schema does not have the owner of the project"
expect_owner "SELECT
    pg_get_userbyid(relowner) = 'Gate Owner'
    FROM pg_class WHERE oid = 'gate_own.t'::regclass" \
    "the new table does not have the owner of the project"
expect_owner "SELECT
    pg_get_userbyid(proowner) = 'Gate Owner'
    FROM pg_proc WHERE oid = 'gate_own.f(integer)'::regprocedure" \
    "the new function does not have the owner of the project"
expect_empty_plan "new objects have the owner of the project"

# owner drift on objects that the database has: deploy sets the owner
# in place, so it needs no --allow-drop. The owner of a table also
# moves the sequence of its serial column, and PostgreSQL refuses to
# change the owner of a linked sequence alone
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER SCHEMA test OWNER TO "Gate Owner";
ALTER TABLE test.users OWNER TO "Gate Owner";
ALTER TABLE public.widgets OWNER TO "Gate Owner";
ALTER VIEW test.active_users OWNER TO "Gate Owner";
ALTER FUNCTION test.overloaded(integer) OWNER TO "Gate Owner";
SQL
./target/debug/pglifecycle deploy --no-owner -o "${WORKDIR}/owner.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q '^-- no changes' "${WORKDIR}/owner.sql"; then
    echo "Convergence gate FAILED: --no-owner changes owners" >&2
    cat "${WORKDIR}/owner.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_owner "SELECT bool_and(owner = 'postgres') FROM (
    SELECT pg_get_userbyid(nspowner) FROM pg_namespace
        WHERE nspname = 'test'
    UNION ALL SELECT pg_get_userbyid(relowner) FROM pg_class
        WHERE oid IN ('test.users'::regclass,
                      'test.active_users'::regclass,
                      'public.widgets'::regclass,
                      'public.widgets_id_seq'::regclass)
    UNION ALL SELECT pg_get_userbyid(proowner) FROM pg_proc
        WHERE oid = 'test.overloaded(integer)'::regprocedure
) AS owners(owner)" "a changed owner was not set back"
expect_empty_plan "changed owners are set back in place"
