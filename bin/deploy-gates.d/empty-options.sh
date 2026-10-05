# Sourced by bin/deploy-gates (gate 2). An option list that is empty in
# the project: storage parameters of a table, an index and a
# materialized view, the options of a server and a user mapping, and
# the parameters of a publication. PostgreSQL does not accept an empty
# list, `WITH ()` or `OPTIONS ()`. deploy writes no list for an empty
# one, the objects are made, and the plan stays empty.

project="${WORKDIR}/project"
mkdir -p "${project}/materialized_views/test" "${project}/servers" \
    "${project}/user_mappings" "${project}/publications"
cat > "${project}/tables/test/gate_empty_options.yaml" <<'YAML'
---
name: gate_empty_options
schema: test
owner: postgres
columns:
- name: id
  data_type: integer
storage_parameters: {}
indexes:
- name: gate_empty_options_id_idx
  columns:
  - name: id
  storage_parameters: {}
YAML
cat > "${project}/materialized_views/test/gate_empty_options_mv.yaml" \
    <<'YAML'
---
name: gate_empty_options_mv
schema: test
owner: postgres
storage_parameters: {}
query: ' SELECT 1 AS n'
YAML
cat > "${project}/servers/gate_empty_srv.yaml" <<'YAML'
---
name: gate_empty_srv
foreign_data_wrapper: gate_fdw
options: {}
YAML
cat > "${project}/user_mappings/PUBLIC.yaml" <<'YAML'
---
name: PUBLIC
servers:
- name: gate_empty_srv
  options: {}
YAML
cat > "${project}/publications/gate_empty_pub.yaml" <<'YAML'
---
name: gate_empty_pub
parameters: {}
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" "${project}"
if [ "$(psql -d "${TARGET_DB}" -tAc "SELECT
        to_regclass('test.gate_empty_options') IS NOT NULL
        AND to_regclass('test.gate_empty_options_id_idx') IS NOT NULL
        AND to_regclass('test.gate_empty_options_mv') IS NOT NULL
        AND EXISTS (SELECT FROM pg_user_mappings
                    WHERE srvname = 'gate_empty_srv' AND umuser = 0)
        AND EXISTS (SELECT FROM pg_publication
                    WHERE pubname = 'gate_empty_pub')")" != t ]; then
    echo "Convergence gate FAILED: an empty option list" >&2
    exit 1
fi
expect_empty_plan "an empty option list is no list"

rm "${project}/tables/test/gate_empty_options.yaml" \
    "${project}/materialized_views/test/gate_empty_options_mv.yaml" \
    "${project}/servers/gate_empty_srv.yaml" \
    "${project}/user_mappings/PUBLIC.yaml" \
    "${project}/publications/gate_empty_pub.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
DROP TABLE test.gate_empty_options;
DROP MATERIALIZED VIEW test.gate_empty_options_mv;
DROP PUBLICATION gate_empty_pub;
DROP SERVER gate_empty_srv CASCADE;
SQL
expect_empty_plan "the empty-options step leaves the database unchanged"
