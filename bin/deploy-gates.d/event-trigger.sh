# Sourced by bin/deploy-gates (gate 2). Event triggers compare by
# definition. The project writes them in the short forms that a person
# writes, and the plan stays empty. A changed state (ENABLE, DISABLE)
# or comment is set in place with ALTER EVENT TRIGGER, without
# --allow-drop. A changed event, tag list or function drops and makes
# the trigger again, only with --allow-drop. A trigger that only the
# database has is dropped, only with --allow-drop.

# $1 is a query that must return t, $2 says what the step checks
expect_event_trigger() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# the short forms: tags in lowercase, mixed case and another order, a
# function name in uppercase, and the default state written out
cat > "${WORKDIR}/project/event_triggers/pglifecycle_ddl_start.yaml" <<'YAML'
---
name: pglifecycle_ddl_start
event: ddl_command_start
filter:
  tags:
  - drop table
  - Create Table
function: TEST.Note_DDL()
enabled: ORIGIN
comment: Notes table DDL
YAML
expect_empty_plan "event trigger short forms are unchanged"

# drift that deploy sets in place: each state and a comment
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER EVENT TRIGGER pglifecycle_ddl_start DISABLE;
ALTER EVENT TRIGGER pglifecycle_drops ENABLE ALWAYS;
ALTER EVENT TRIGGER pglifecycle_replica ENABLE;
COMMENT ON EVENT TRIGGER pglifecycle_drops IS 'drift';
COMMENT ON EVENT TRIGGER pglifecycle_ddl_start IS NULL;
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_event_trigger "SELECT string_agg(evtname || '=' || evtenabled::text
        || '/' || coalesce(obj_description(oid, 'pg_event_trigger'), ''),
        ',' ORDER BY evtname)
    = 'pglifecycle_ddl_start=O/Notes table DDL,pglifecycle_drops=D/,'
      'pglifecycle_replica=R/'
    FROM pg_event_trigger" \
    "the state or comment of an event trigger was not set back"
expect_empty_plan "changed event triggers converge in place"

# changed tags: no ALTER form, so deploy drops the trigger and makes it
# again, with its comment, only with --allow-drop
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP EVENT TRIGGER pglifecycle_ddl_start;
CREATE EVENT TRIGGER pglifecycle_ddl_start ON ddl_command_start
    WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION test.note_ddl();
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP EVENT TRIGGER' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 2 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the event trigger rebuild was not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_event_trigger "SELECT evttags = '{DROP TABLE,CREATE TABLE}'
        AND obj_description(oid, 'pg_event_trigger') = 'Notes table DDL'
    FROM pg_event_trigger WHERE evtname = 'pglifecycle_ddl_start'" \
    "the changed event trigger was not made again"
expect_empty_plan "a changed event trigger converges with --allow-drop"

# an event trigger that only the database has, with a name that needs
# quoting
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c \
    "CREATE EVENT TRIGGER \"Stray Trigger\" ON sql_drop
        EXECUTE FUNCTION test.note_ddl();"
./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if grep -q '^DROP EVENT TRIGGER' "${WORKDIR}/withheld.sql" \
    || ! grep -q '^-- destructive statements: 1 excluded' \
        "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the event trigger drop was not" \
        "withheld" >&2
    cat "${WORKDIR}/withheld.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_event_trigger "SELECT NOT EXISTS (SELECT FROM pg_event_trigger
    WHERE evtname = 'Stray Trigger')" \
    "the database-only event trigger was not dropped"
expect_empty_plan "a database-only event trigger is dropped"

# owner drift: an event trigger that names its owner gets it back in
# place with ALTER EVENT TRIGGER ... OWNER TO, without --allow-drop. A
# trigger whose file names no owner keeps the owner that it has. The
# owner of an event trigger must be a superuser
psql -d postgres -q -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles
                   WHERE rolname = 'Gate Event Owner') THEN
        CREATE ROLE "Gate Event Owner" SUPERUSER;
    END IF;
END
$$;
SQL
grep -q '^owner: postgres$' \
    "${WORKDIR}/project/event_triggers/pglifecycle_drops.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER EVENT TRIGGER pglifecycle_drops OWNER TO "Gate Event Owner";
ALTER EVENT TRIGGER pglifecycle_ddl_start OWNER TO "Gate Event Owner";
SQL
./target/debug/pglifecycle deploy --no-owner -o "${WORKDIR}/owner.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q '^-- no changes' "${WORKDIR}/owner.sql"; then
    echo "Convergence gate FAILED: --no-owner changes the owner of an" \
        "event trigger" >&2
    cat "${WORKDIR}/owner.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_event_trigger "SELECT string_agg(evtname || '='
        || pg_get_userbyid(evtowner), ',' ORDER BY evtname)
    = 'pglifecycle_ddl_start=Gate Event Owner,pglifecycle_drops=postgres,'
      'pglifecycle_replica=postgres'
    FROM pg_event_trigger" \
    "the owner of an event trigger was not set back"
expect_empty_plan "a changed event trigger owner is set back in place"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c 'ALTER EVENT TRIGGER pglifecycle_ddl_start OWNER TO postgres' \
    -c 'DROP ROLE "Gate Event Owner"'
