# Sourced by bin/deploy-gates (gate 2). The settings of the database,
# and of a role in the database (fixtures/schema.sql), are in the
# project.yaml of the project. Gate 1 made them in the target.
#
# 1. Drift in the database: a changed value, a removed setting and a
#    setting that the project does not have, for the database and for
#    the role. deploy sets the values again and resets the setting that
#    the project does not have. A RESET is not destructive, thus the
#    plan without --allow-drop has it, but the header of the script and
#    the log name each setting that it resets.
# 2. A setting that the project adds as a YAML boolean is set, and
#    converges; when the project removes it, deploy resets it.
#
# Each time, the settings of the target must be the settings of the
# source.

# the settings of the database $1, one line for each
database_settings() {
    psql -d postgres -tA -v ON_ERROR_STOP=1 -c "
        SELECT setrole::regrole::text, s
          FROM pg_db_role_setting, unnest(setconfig) AS s
         WHERE setdatabase = (SELECT oid FROM pg_database
                               WHERE datname = '$1')
         ORDER BY 1, 2"
}

# the target has the settings of the source, and the plan is empty; $1
# says what the step checks
expect_source_settings() {
    database_settings "${SOURCE_DB}" > "${WORKDIR}/settings-source.txt"
    database_settings "${TARGET_DB}" > "${WORKDIR}/settings-target.txt"
    if ! grep -q 'work_mem=64MB' "${WORKDIR}/settings-source.txt" \
        || ! diff -u "${WORKDIR}/settings-source.txt" \
            "${WORKDIR}/settings-target.txt"; then
        echo "Convergence gate FAILED: $1" >&2
        exit 1
    fi
    expect_empty_plan "$1"
}

expect_source_settings "deploy made the database settings"

psql -d postgres -q -v ON_ERROR_STOP=1 <<SQL
ALTER DATABASE ${TARGET_DB} SET work_mem TO '32MB';
ALTER DATABASE ${TARGET_DB} RESET gate.note;
ALTER DATABASE ${TARGET_DB} SET gate.stray TO 'x';
ALTER ROLE postgres IN DATABASE ${TARGET_DB} SET gate.role_note TO 'drift';
ALTER ROLE postgres IN DATABASE ${TARGET_DB} SET gate.role_stray TO 'y';
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/settings.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project" 2> "${WORKDIR}/settings.log"
for statement in \
    "ALTER DATABASE ${TARGET_DB} SET work_mem TO '64MB';" \
    "ALTER DATABASE ${TARGET_DB} SET \"gate.note\" TO \$\$it's\$\$;" \
    "ALTER DATABASE ${TARGET_DB} RESET \"gate.stray\";" \
    "ALTER ROLE postgres IN DATABASE ${TARGET_DB} SET \"gate.role_note\" TO 'r';" \
    "ALTER ROLE postgres IN DATABASE ${TARGET_DB} RESET \"gate.role_stray\";"
do
    if ! grep -qxF "${statement}" "${WORKDIR}/settings.sql"; then
        echo "Convergence gate FAILED: the plan does not have" \
            "${statement}" >&2
        cat "${WORKDIR}/settings.sql" >&2
        exit 1
    fi
done
if ! grep -q '^-- destructive statements: none$' "${WORKDIR}/settings.sql"
then
    echo "Convergence gate FAILED: a setting change is destructive" >&2
    cat "${WORKDIR}/settings.sql" >&2
    exit 1
fi
# a RESET is not destructive, but the header and the log name each
# setting that the plan resets
for reset in "gate.stray of DATABASE ${TARGET_DB}" \
    "gate.role_stray of ROLE postgres IN DATABASE ${TARGET_DB}"
do
    if ! grep -qF "Setting ${reset}: the project does not have it" \
            "${WORKDIR}/settings.log" \
        || ! grep -q "^-- settings reset: 2 (.*${reset}" \
            "${WORKDIR}/settings.sql"; then
        echo "Convergence gate FAILED: deploy does not name the reset" \
            "of ${reset}" >&2
        cat "${WORKDIR}/settings.log" "${WORKDIR}/settings.sql" >&2
        exit 1
    fi
done
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_source_settings "database settings converge"

cp "${WORKDIR}/project/project.yaml" "${WORKDIR}/project.yaml.bak"
perl -0pi -e 's/^settings:\n/settings:\n- gate.flag: true\n/m' \
    "${WORKDIR}/project/project.yaml"
grep -q '^- gate.flag: true$' "${WORKDIR}/project/project.yaml"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(psql -d "${TARGET_DB}" -tAc "SHOW gate.flag")" != true ]; then
    echo "Convergence gate FAILED: the added setting is not set" >&2
    exit 1
fi
expect_empty_plan "an added boolean setting converges"
mv "${WORKDIR}/project.yaml.bak" "${WORKDIR}/project/project.yaml"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_source_settings "a removed setting is reset"
