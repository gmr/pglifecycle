# Sourced by bin/deploy-gates (gate 2). deploy --apply --role runs the
# script as that role, as pg_restore --role does: the script sets the
# role after the encoding settings, because psql has no --role option.
# With --no-owner, deploy does not set owners, thus the role that runs
# the script owns the new objects. The role name needs quoting.

# $1 is a query that must return t, $2 says what the step checks
expect_creator() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# a role is a cluster object, so an earlier run can have made it.
# deploy also reads the database as the role (pg_dump --role), and
# only a superuser can read all of it: a subscription and the options
# of a user mapping, for example
psql -d postgres -q -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'Gate Applier')
    THEN
        CREATE ROLE "Gate Applier";
    END IF;
END
$$;
ALTER ROLE "Gate Applier" SUPERUSER;
SQL

cat > "${WORKDIR}/project/schemata/gate_apply.yaml" <<'YAML'
---
name: gate_apply
owner: Gate Applier
YAML
mkdir -p "${WORKDIR}/project/tables/gate_apply"
cat > "${WORKDIR}/project/tables/gate_apply/t.yaml" <<'YAML'
---
name: t
schema: gate_apply
owner: Gate Applier
columns:
- name: id
  data_type: integer
YAML

# the written script sets the role, thus psql gives the same result
./target/debug/pglifecycle deploy --no-owner --role 'Gate Applier' \
    -o "${WORKDIR}/apply-role.sql" -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q '^SET ROLE "Gate Applier";$' "${WORKDIR}/apply-role.sql"; then
    echo "Convergence gate FAILED: the script does not set the role" >&2
    cat "${WORKDIR}/apply-role.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --no-owner --role 'Gate Applier' \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_creator "SELECT bool_and(owner = 'Gate Applier') FROM (
    SELECT pg_get_userbyid(nspowner) FROM pg_namespace
        WHERE nspname = 'gate_apply'
    UNION ALL SELECT pg_get_userbyid(relowner) FROM pg_class
        WHERE oid = 'gate_apply.t'::regclass
) AS owners(owner)" "the role of --role does not make the new objects"
expect_empty_plan "deploy --apply --role makes the objects as the role"

psql -d postgres -q -v ON_ERROR_STOP=1 \
    -c 'ALTER ROLE "Gate Applier" NOSUPERUSER'
