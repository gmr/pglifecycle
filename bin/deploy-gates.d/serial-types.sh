# Sourced by bin/deploy-gates (gate 2). PostgreSQL keeps no serial
# type: a `serial` column is an `integer` column, NOT NULL, with the
# default nextval() of a sequence that the column owns. deploy finds
# that sequence by its OWNED BY, thus a name that PostgreSQL changed
# for a collision is also found. A new serial table and
# a new serial column keep the serial form.

table="${WORKDIR}/project/tables/test/gate_serial.yaml"
collides="${WORKDIR}/project/tables/test/gate_serial_c.yaml"
taken="${WORKDIR}/project/sequences/test/gate_serial_c_id_seq.yaml"
mkdir -p "${WORKDIR}/project/sequences/test"

serial_table() {
    cat > "${table}" <<YAML
---
name: gate_serial
schema: test
owner: postgres
columns:
- name: id
  data_type: $1
- name: b
  data_type: bigserial
- name: s
  data_type: smallserial
- name: s4
  data_type: serial4
- name: s8
  data_type: serial8
- name: s2
  data_type: serial2
- name: u
  data_type: SERIAL
- name: q
  data_type: '"serial"'
$2
primary_key:
- id
YAML
}

serial_table serial ""
cat > "${taken}" <<'YAML'
---
name: gate_serial_c_id_seq
schema: test
owner: postgres
data_type: integer
increment_by: 1
start_with: 1
cache: 1
YAML
cat > "${collides}" <<'YAML'
---
name: gate_serial_c
schema: test
owner: postgres
dependencies:
  sequences:
  - test.gate_serial_c_id_seq
columns:
- name: id
  data_type: serial
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT count(*) FROM pg_class
        WHERE relname IN ('gate_serial_c_id_seq1', 'gate_serial_id_seq')
          AND relkind = 'S'")" != 2 ]; then
    echo "Convergence gate FAILED: the serial sequences are not made" >&2
    exit 1
fi
expect_empty_plan "a serial column compares with its sequence"

# a new serial column on a table that the database has
serial_table serial "- name: added
  data_type: serial"
./target/debug/pglifecycle deploy -o "${WORKDIR}/serial-add.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -Eq '^ALTER TABLE test\.gate_serial ADD COLUMN added serial;' \
    "${WORKDIR}/serial-add.sql"; then
    echo "Convergence gate FAILED: no ADD COLUMN added serial" >&2
    cat "${WORKDIR}/serial-add.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a new serial column converges"

# drift: the database lost a default and a NOT NULL
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "ALTER TABLE test.gate_serial ALTER COLUMN s DROP DEFAULT, \
        ALTER COLUMN u DROP NOT NULL;"
./target/debug/pglifecycle deploy -o "${WORKDIR}/serial-drift.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
for pattern in \
    "^ALTER TABLE test\.gate_serial ALTER COLUMN s SET DEFAULT nextval\('test\.gate_serial_s_seq'::regclass\);" \
    '^ALTER TABLE test\.gate_serial ALTER COLUMN u SET NOT NULL;'; do
    if ! grep -Eq "${pattern}" "${WORKDIR}/serial-drift.sql"; then
        echo "Convergence gate FAILED: no ${pattern}" >&2
        cat "${WORKDIR}/serial-drift.sql" >&2
        exit 1
    fi
done
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a serial column converges after drift"

# serial to bigserial changes the column and its sequence. A type
# change is destructive
serial_table bigserial "- name: added
  data_type: serial"
./target/debug/pglifecycle deploy --allow-drop \
    -o "${WORKDIR}/serial-type.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
for pattern in \
    '^ALTER SEQUENCE test\.gate_serial_id_seq AS bigint;' \
    '^ALTER TABLE test\.gate_serial ALTER COLUMN id TYPE bigint;'; do
    if ! grep -Eq "${pattern}" "${WORKDIR}/serial-type.sql"; then
        echo "Convergence gate FAILED: no ${pattern}" >&2
        cat "${WORKDIR}/serial-type.sql" >&2
        exit 1
    fi
done
./target/debug/pglifecycle deploy --apply --allow-drop \
    -d "${TARGET_DB}" "${WORKDIR}/project"
expect_empty_plan "serial to bigserial converges"

rm "${table}" "${collides}" "${taken}"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.gate_serial, test.gate_serial_c;" \
    -c "DROP SEQUENCE test.gate_serial_c_id_seq;"
expect_empty_plan "the serial-types step leaves the database unchanged"
