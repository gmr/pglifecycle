# Sourced by bin/deploy-gates (gate 2). deploy does not make roles. It
# warns about each role that the script needs and the database does
# not have, and about each grant on an object that the project does
# not have, which the plan does not include. The warnings do not
# change the script. The grant to the missing role is on a new table,
# because the plan includes the grants of a new object.
psql -d postgres -q -c 'DROP ROLE IF EXISTS gate_missing'
cat > "${WORKDIR}/project/tables/test/gate_roles.yaml" <<'YAML'
---
name: gate_roles
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
YAML
cat > "${WORKDIR}/project/roles/gate_missing.yaml" <<'YAML'
---
name: gate_missing
grants:
  tables:
    test.gate_roles:
    - SELECT
  functions:
    pg_catalog.pg_reload_conf():
    - EXECUTE
YAML
./target/debug/pglifecycle deploy -o "${WORKDIR}/roles.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project" 2> "${WORKDIR}/roles.log"
if ! grep -q 'GRANT SELECT ON TABLE test.gate_roles TO gate_missing' \
        "${WORKDIR}/roles.sql" \
    || grep -q 'pg_reload_conf' "${WORKDIR}/roles.sql"; then
    echo "Convergence gate FAILED: the plan of the grants changed" >&2
    cat "${WORKDIR}/roles.sql" >&2
    exit 1
fi
if [ "$(grep -c 'Role .* is not in the database' "${WORKDIR}/roles.log")" \
        != 1 ] \
    || ! grep -q 'Role gate_missing is not in the database.*gate_roles' \
        "${WORKDIR}/roles.log" \
    || ! grep -qF 'ACL pg_catalog.FUNCTION pg_reload_conf(): the project' \
        "${WORKDIR}/roles.log"; then
    echo "Convergence gate FAILED: deploy does not warn about the role" \
        "and the grant" >&2
    cat "${WORKDIR}/roles.log" >&2
    exit 1
fi
echo "Convergence gate passed: a missing role and an unowned grant warn"
rm "${WORKDIR}/project/roles/gate_missing.yaml" \
    "${WORKDIR}/project/tables/test/gate_roles.yaml"
expect_empty_plan "the grants of a missing role are removed"
