# Sourced by bin/deploy-gates (gate 2). PostgreSQL makes each column
# of a primary key NOT NULL, and each identity column too, with a NOT
# NULL constraint of the name `<table>_<column>_not_null`. This is true
# for a primary key that CREATE TABLE gives, for one that ALTER TABLE
# adds, and for the primary key of a partitioned table. A project can
# leave out `nullable: false` on such a column. deploy compares the
# column as NOT NULL, so the plan stays empty and does not have the
# DROP NOT NULL that PostgreSQL refuses on a primary key column.

tables="${WORKDIR}/project/tables/test"

cat > "${tables}/pk_not_null.yaml" <<'YAML'
---
name: pk_not_null
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: v
  data_type: text
primary_key:
- id
YAML
cat > "${tables}/pk_not_null_pair.yaml" <<'YAML'
---
name: pk_not_null_pair
schema: test
owner: postgres
columns:
- name: a
  data_type: integer
- name: b
  data_type: text
- name: v
  data_type: text
primary_key:
  name: pk_not_null_pair_key
  columns:
  - a
  - b
YAML
cat > "${tables}/pk_not_null_identity.yaml" <<'YAML'
---
name: pk_not_null_identity
schema: test
owner: postgres
columns:
- name: id
  data_type: bigint
  generated:
    sequence_behavior: ALWAYS
- name: v
  data_type: text
YAML
cat > "${tables}/pk_not_null_parted.yaml" <<'YAML'
---
name: pk_not_null_parted
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
- name: k
  data_type: integer
primary_key:
- id
- k
partition:
  type: RANGE
  columns:
  - k
partitions:
- name: pk_not_null_parted_1
  schema: test
  for_values_from: 0
  for_values_to: 10
  attached: true
YAML
# the partition takes the NOT NULL names of its parent
cat > "${tables}/pk_not_null_parted_1.yaml" <<'YAML'
---
name: pk_not_null_parted_1
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
  nullable: false
  not_null_constraint:
    name: pk_not_null_parted_id_not_null
- name: k
  data_type: integer
  nullable: false
  not_null_constraint:
    name: pk_not_null_parted_k_not_null
primary_key:
- id
- k
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
# PostgreSQL gives each of these columns a NOT NULL constraint of the
# name that deploy expects when it has no name in the project
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT count(DISTINCT conname)
        FROM pg_constraint
        WHERE contype = 'n' AND conname IN (
            'pk_not_null_id_not_null',
            'pk_not_null_pair_a_not_null',
            'pk_not_null_pair_b_not_null',
            'pk_not_null_identity_id_not_null',
            'pk_not_null_parted_id_not_null',
            'pk_not_null_parted_k_not_null'
        )")" != 6 ]; then
    echo "Convergence gate FAILED: a primary key or identity column" \
        "has no NOT NULL constraint of the generated name" >&2
    psql -d "${TARGET_DB}" -tAc "SELECT conname FROM pg_constraint
        WHERE conname LIKE 'pk_not_null%'" >&2
    exit 1
fi
expect_empty_plan "a primary key or identity column is NOT NULL"

# a primary key that ALTER TABLE adds to a table that has rows
perl -0pi -e 's/\nprimary_key:\n- id\n/\n/' "${tables}/pk_not_null.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "ALTER TABLE test.pk_not_null DROP CONSTRAINT pk_not_null_pkey;" \
    -c "ALTER TABLE test.pk_not_null ALTER COLUMN id DROP NOT NULL;" \
    -c "INSERT INTO test.pk_not_null VALUES (1, 'a');"
expect_empty_plan "a table without a primary key is unchanged"
printf 'primary_key:\n- id\n' >> "${tables}/pk_not_null.yaml"
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a primary key that ALTER TABLE adds is NOT NULL"

# without the primary key the column can be null again: deploy
# rebuilds the table, which needs --allow-drop
perl -0pi -e 's/\nprimary_key:\n- id\n/\n/' "${tables}/pk_not_null.yaml"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "a column of a dropped primary key can be null"

rm "${tables}/pk_not_null.yaml" "${tables}/pk_not_null_pair.yaml" \
    "${tables}/pk_not_null_identity.yaml" \
    "${tables}/pk_not_null_parted.yaml" \
    "${tables}/pk_not_null_parted_1.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP TABLE test.pk_not_null, test.pk_not_null_pair,
        test.pk_not_null_identity, test.pk_not_null_parted;"
expect_empty_plan "the primary-key step leaves the database unchanged"
