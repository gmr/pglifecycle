# Sourced by bin/deploy-gates (gate 2). pg_dump writes no AS for a
# bigint sequence. A project sequence with `data_type: bigint` has the
# type of the database, so the plan does not alter it each run.

mkdir -p "${WORKDIR}/project/sequences/test"
cat > "${WORKDIR}/project/sequences/test/gate_seq_bigint.yaml" <<'YAML'
---
schema: test
name: gate_seq_bigint
owner: postgres
data_type: bigint
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a bigint sequence has the type of the database"
rm "${WORKDIR}/project/sequences/test/gate_seq_bigint.yaml"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "the bigint sequence is dropped"
