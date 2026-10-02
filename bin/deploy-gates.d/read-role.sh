# Sourced by bin/deploy-gates (gate 2). deploy reads the database as
# the role of --role, as pg_dump --role does. A role that is not a
# superuser cannot read the subscriptions, nor the options of a user
# mapping that is not its own, thus the plan makes them again. deploy
# does not change the plan, but it warns: the warning names the role
# and the objects. The script is the same as without the warning. The
# step does not apply the script, thus the target stays converged.

# a role is a cluster object, so an earlier run can have made it. The
# cleanup of bin/deploy-gates removes it. pg_dump locks each table,
# thus the role must read all of the tables: pg_read_all_data gives
# that, but not the subscriptions or the options of the user mappings
psql -d postgres -q -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'Gate Reader')
    THEN
        CREATE ROLE "Gate Reader" NOSUPERUSER;
        GRANT pg_read_all_data TO "Gate Reader";
    END IF;
END
$$;
SQL

./target/debug/pglifecycle deploy --role 'Gate Reader' \
    -d "${TARGET_DB}" "${WORKDIR}/project" \
    >"${WORKDIR}/read-role.sql" 2>"${WORKDIR}/read-role.err"
grep 'WARN' "${WORKDIR}/read-role.err" >"${WORKDIR}/read-role.warn" || true
for text in 'Gate Reader' 'SUBSCRIPTION gate_sub' \
        'USER MAPPING postgres SERVER gate_srv'; do
    if ! grep -qF "${text}" "${WORKDIR}/read-role.warn"; then
        echo "Convergence gate FAILED: no warning names ${text} when" \
            "--role cannot read all of the database" >&2
        cat "${WORKDIR}/read-role.err" >&2
        exit 1
    fi
done
# the plan is not changed: it makes the objects that the role cannot
# read, and the warning is not in the script
if ! grep -q '^CREATE SUBSCRIPTION gate_sub ' "${WORKDIR}/read-role.sql" \
    || ! grep -q '^ALTER USER MAPPING FOR postgres SERVER gate_srv ' \
        "${WORKDIR}/read-role.sql" \
    || grep -q 'WARN' "${WORKDIR}/read-role.sql"; then
    echo "Convergence gate FAILED: the warning changed the script" >&2
    cat "${WORKDIR}/read-role.sql" >&2
    exit 1
fi

# a superuser reads all of the database, thus there is no warning
./target/debug/pglifecycle deploy -o "${WORKDIR}/read-role.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project" 2>"${WORKDIR}/read-role.err"
if grep -q 'cannot read' "${WORKDIR}/read-role.err"; then
    echo "Convergence gate FAILED: a warning for a superuser" >&2
    cat "${WORKDIR}/read-role.err" >&2
    exit 1
fi
echo "Convergence gate passed: deploy warns when --role cannot read" \
    "all of the database"
