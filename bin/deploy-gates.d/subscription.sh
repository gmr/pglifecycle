# Sourced by bin/deploy-gates (gate 2). Subscriptions: deploy compares
# their definitions. The gate subscriptions have no publisher, so no
# statement that deploy runs can connect to one.
#
# 1. The subscription written by hand in a short form: the
#    publications in another order, a boolean for streaming, no slot
#    name, options at their defaults, and the options that only
#    CREATE SUBSCRIPTION reads. The plan is empty.
# 2. A password in the database that the project does not have is not
#    a change: pull removes it.
# 3. Drift in the connection, the publications, the options and the
#    comment converges in place, without --allow-drop. The report
#    tells the operator to refresh the publications.
# 4. A changed option that deploy does not change (slot_name) is
#    reported and not applied.
# 5. A subscription that only the database has is withheld without
#    --allow-drop and dropped with it, with a slot name and without.
cat > "${WORKDIR}/project/subscriptions/gate_sub.yaml" <<'YAML'
---
name: gate_sub
connection: dbname=pglifecycle_nowhere
publications:
- Gate Pub
- gate_pub
parameters:
  connect: false
  enabled: false
  create_slot: false
  copy_data: false
  binary: true
  streaming: false
  origin: none
  synchronous_commit: 'off'
  two_phase: false
  disable_on_error: false
  password_required: true
  run_as_owner: false
  failover: false
comment: A subscription with no publisher
YAML
expect_empty_plan "a subscription written in a short form is unchanged"

# the catalog state of each subscription in the target, one line
# each; the publications are a set
subscription_state="SELECT string_agg(format('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s',
        s.subname, s.subconninfo,
        (SELECT array_agg(p ORDER BY p) FROM unnest(s.subpublications) p),
        s.subslotname,
        s.subbinary, s.substream, s.subtwophasestate, s.subdisableonerr,
        s.suborigin, obj_description(s.oid, 'pg_subscription')),
        E'\\n' ORDER BY s.subname)
    FROM pg_subscription s JOIN pg_database d ON d.oid = s.subdbid
    WHERE d.datname = current_database()"
expected_subscriptions="$(psql -d "${TARGET_DB}" -tAc "${subscription_state}")"

psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c \
    "ALTER SUBSCRIPTION gate_sub
        CONNECTION 'dbname=pglifecycle_nowhere password=secret'"
expect_empty_plan "a password that the project does not have is kept"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 -c \
    "ALTER SUBSCRIPTION gate_sub CONNECTION 'dbname=pglifecycle_nowhere'"

psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER SUBSCRIPTION gate_sub CONNECTION 'dbname=pglifecycle_elsewhere';
ALTER SUBSCRIPTION gate_sub SET PUBLICATION gate_pub WITH (refresh = false);
ALTER SUBSCRIPTION gate_sub
    SET (binary = false, streaming = parallel, origin = any,
         disable_on_error = true);
COMMENT ON SUBSCRIPTION gate_sub IS 'drift';
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project" 2>"${WORKDIR}/subscription.err"
if ! grep -q 'REFRESH PUBLICATION' "${WORKDIR}/subscription.err"; then
    echo "Convergence gate FAILED: no REFRESH PUBLICATION note for" \
        "changed subscription publications" >&2
    cat "${WORKDIR}/subscription.err" >&2
    exit 1
fi
if [ "$(psql -d "${TARGET_DB}" -tAc "${subscription_state}")" \
        != "${expected_subscriptions}" ]; then
    echo "Convergence gate FAILED: subscription drift did not converge" >&2
    psql -d "${TARGET_DB}" -tAc "${subscription_state}" >&2
    exit 1
fi
expect_empty_plan "subscription drift converges in place"

# a new slot name needs the slot on the publisher, so deploy reports
# it and does not change it. The gate subscription is disabled, so
# psql can change the slot name without a publisher.
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "ALTER SUBSCRIPTION gate_sub SET (slot_name = 'gate_sub_drift')"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project" 2>"${WORKDIR}/subscription.err"
./target/debug/pglifecycle deploy -o "${WORKDIR}/subscription.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q 'slot_name' "${WORKDIR}/subscription.err" \
    || ! grep -q '^-- not applied: SUBSCRIPTION gate_sub: slot_name' \
        "${WORKDIR}/subscription.sql" \
    || grep -q '^ALTER SUBSCRIPTION .*slot_name' \
        "${WORKDIR}/subscription.sql" \
    || [ "$(psql -d "${TARGET_DB}" -tAc "SELECT subslotname
            FROM pg_subscription
           WHERE subname = 'gate_sub' AND subdbid = (SELECT oid
                 FROM pg_database WHERE datname = current_database())")" \
        != gate_sub_drift ]; then
    echo "Convergence gate FAILED: a slot name change was not only" \
        "reported" >&2
    cat "${WORKDIR}/subscription.err" "${WORKDIR}/subscription.sql" >&2
    exit 1
fi
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "ALTER SUBSCRIPTION gate_sub SET (slot_name = 'gate_sub')"
expect_empty_plan "a slot name change is reported and not applied"

psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
SET client_min_messages = error;
CREATE SUBSCRIPTION gate_sub_stray
    CONNECTION 'dbname=pglifecycle_nowhere' PUBLICATION gate_pub
    WITH (connect = false);
CREATE SUBSCRIPTION gate_sub_slotless
    CONNECTION 'dbname=pglifecycle_nowhere' PUBLICATION gate_pub
    WITH (connect = false, slot_name = NONE);
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/subscription.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q '^-- destructive statements: 2 excluded' \
        "${WORKDIR}/subscription.sql" \
    || grep -q '^DROP SUBSCRIPTION' "${WORKDIR}/subscription.sql"; then
    echo "Convergence gate FAILED: subscription drops were not withheld" >&2
    cat "${WORKDIR}/subscription.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project" 2>"${WORKDIR}/subscription.err"
if ! grep -q 'pg_drop_replication_slot' "${WORKDIR}/subscription.err"; then
    echo "Convergence gate FAILED: no note about the replication slot" \
        "that the publisher keeps" >&2
    cat "${WORKDIR}/subscription.err" >&2
    exit 1
fi
if [ "$(psql -d "${TARGET_DB}" -tAc "${subscription_state}")" \
        != "${expected_subscriptions}" ]; then
    echo "Convergence gate FAILED: database-only subscriptions were not" \
        "dropped" >&2
    psql -d "${TARGET_DB}" -tAc "${subscription_state}" >&2
    exit 1
fi
expect_empty_plan "database-only subscriptions dropped with --allow-drop"
