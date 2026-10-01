# Sourced by bin/deploy-gates (gate 2). A table that the database has
# gets a default and two new columns with a default, each of which
# calls nextval on a sequence that this deploy makes. The sequences
# sort after the table. One sequence is owned by a column that the
# table has, and one by a new NOT NULL column, as ADD COLUMN ... serial
# makes it. The statements that call nextval must come after the
# CREATE SEQUENCE, and the OWNED BY of the new column after its ADD
# COLUMN, which gives a value to each row that the table has.

cat > "${WORKDIR}/project/tables/test/gate_seq.yaml" <<'YAML'
---
name: gate_seq
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the table for the sequence order step is made"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "INSERT INTO test.gate_seq (id) VALUES (1), (2);"

mkdir -p "${WORKDIR}/project/sequences/test"
cat > "${WORKDIR}/project/sequences/test/gate_seq_free.yaml" <<'YAML'
---
schema: test
name: gate_seq_free
owner: postgres
YAML
cat > "${WORKDIR}/project/sequences/test/gate_seq_id.yaml" <<'YAML'
---
schema: test
name: gate_seq_id
owner: postgres
data_type: integer
owned_by: test.gate_seq.id
YAML
cat > "${WORKDIR}/project/sequences/test/gate_seq_serial.yaml" <<'YAML'
---
schema: test
name: gate_seq_serial
owner: postgres
data_type: integer
owned_by: test.gate_seq.serial
YAML
cat > "${WORKDIR}/project/tables/test/gate_seq.yaml" <<'YAML'
---
name: gate_seq
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  default: nextval('test.gate_seq_id'::regclass)
- name: n
  data_type: bigint
  default: nextval('test.gate_seq_free'::regclass)
- name: serial
  data_type: integer
  nullable: false
  default: nextval('test.gate_seq_serial'::regclass)
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a changed table comes after the new sequences it calls"
