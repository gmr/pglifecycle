# Sourced by bin/deploy-gates (gate 2). The deploy script sets the
# session settings of pg_restore that can change the result of DDL, so
# the script runs as a restore of the build does, whatever the settings
# of the session are.
#
# psql runs the script with other settings: client_encoding LATIN1,
# standard_conforming_strings off, xmloption document and
# check_function_bodies on. The step does not use deploy --apply,
# because these settings also change the pg_dump that deploy plans with.
# The script makes a LANGUAGE sql function before the table that its
# body reads, a column with an xml default that is not a document, a
# column with a default that has a backslash, and a comment that has a
# character that is not ASCII. It must not fail, and the plan after it
# must be empty, so the default and the comment are unchanged.

mkdir -p "${WORKDIR}/project/functions/test"
cat > "${WORKDIR}/project/tables/test/session_settings.yaml" <<'YAML'
---
name: session_settings
schema: test
owner: postgres
comment: café
columns:
- name: body
  data_type: xml
  default: '''text''::xml'
- name: path
  data_type: text
  default: '''C:\new''::text'
YAML
cat > "${WORKDIR}/project/functions/test/session_settings_rows.yaml" <<'YAML'
---
name: session_settings_rows
schema: test
owner: postgres
returns: bigint
language: sql
definition: |2-
   SELECT count(*)
     FROM test.session_settings;
YAML
./target/debug/pglifecycle deploy -o "${WORKDIR}/session-settings.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
settings='-c standard_conforming_strings=off -c xmloption=document'
settings+=' -c check_function_bodies=on'
if ! PGCLIENTENCODING=LATIN1 PGOPTIONS="${settings}" \
    psql -X -q -d "${TARGET_DB}" --single-transaction -v ON_ERROR_STOP=1 \
        -f "${WORKDIR}/session-settings.sql" > /dev/null \
        2> "${WORKDIR}/session-settings.err"; then
    echo "Convergence gate FAILED: the script failed with other session" \
        "settings" >&2
    cat "${WORKDIR}/session-settings.err" >&2
    exit 1
fi
expect_empty_plan "the script runs with the session settings of pg_restore"
rm "${WORKDIR}/project/tables/test/session_settings.yaml" \
    "${WORKDIR}/project/functions/test/session_settings_rows.yaml"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the session settings step leaves the database unchanged"
