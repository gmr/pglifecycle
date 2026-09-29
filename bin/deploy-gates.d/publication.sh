# Sourced by bin/deploy-gates (gate 2). Publications: deploy compares
# their definitions.
#
# 1. The publications written by hand in a short form: other list
#    orders, a quoted table name, a row filter without its outer
#    parentheses, and parameters at their defaults. The plan is empty.
# 2. Drift in the members, the column list, the row filter, the
#    parameters and the comment converges in place, without
#    --allow-drop. A change to publish_via_partition_root is applied,
#    and the report warns about it.
# 3. A change from FOR ALL TABLES needs a drop and a create, so it is
#    withheld without --allow-drop and applied with it.
# 4. A publication that only the database has is withheld without
#    --allow-drop and dropped with it.
publications="${WORKDIR}/project/publications"
cat > "${publications}/pglifecycle_some.yaml" <<'YAML'
---
name: pglifecycle_some
parameters:
  publish_via_partition_root: true
  publish:
  - update
  - insert
tables:
- name: '"test"."replicated"'
  columns:
  - amount
  - id
  where: amount > 0
comment: Positive amounts only
YAML
cat > "${publications}/pglifecycle_all.yaml" <<'YAML'
---
name: pglifecycle_all
all_tables: true
parameters:
  publish:
  - insert
  - update
  - delete
  - truncate
  publish_via_partition_root: false
  publish_generated_columns: none
YAML
cat > "${publications}/quoted_pub.yaml" <<'YAML'
---
name: quoted_pub
tables:
- name: test.quoted_cols
  columns:
  - Name
  - Id
YAML
# a publication with no members, which deploy makes
cat > "${publications}/gate_pub_empty.yaml" <<'YAML'
---
name: gate_pub_empty
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_empty_plan "publications written in a short form are unchanged"

# the members, column lists, row filters and parameters of each
# publication, as one line of text
publication_state="SELECT string_agg(format('%s|%s|%s|%s|%s|%s|%s|%s',
        p.pubname, p.puballtables, p.pubinsert || ',' || p.pubupdate
        || ',' || p.pubdelete || ',' || p.pubtruncate, p.pubviaroot,
        p.pubgencols,
        (SELECT string_agg(format('%s(%s)%s', r.prrelid::regclass,
                 r.prattrs, pg_get_expr(r.prqual, r.prrelid)), ';'
                 ORDER BY r.prrelid::regclass::text)
           FROM pg_publication_rel r WHERE r.prpubid = p.oid),
        (SELECT string_agg(n.pnnspid::regnamespace::text, ';')
           FROM pg_publication_namespace n WHERE n.pnpubid = p.oid),
        obj_description(p.oid, 'pg_publication')),
        E'\\n' ORDER BY p.pubname)
    FROM pg_publication p"
expected_publications="$(psql -d "${TARGET_DB}" -tAc "${publication_state}")"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER PUBLICATION pglifecycle_some
    SET TABLE ONLY test.replicated (id, amount, note) WHERE (amount > 10);
ALTER PUBLICATION pglifecycle_some
    SET (publish = 'insert', publish_via_partition_root = false);
COMMENT ON PUBLICATION pglifecycle_some IS 'drift';
ALTER PUBLICATION pglifecycle_schema DROP TABLES IN SCHEMA test;
ALTER PUBLICATION quoted_pub SET TABLE test.quoted_cols ("Id");
ALTER PUBLICATION gate_pub_empty ADD TABLE test.replicated;
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project" 2>"${WORKDIR}/publication.err"
if ! grep -q 'publish_via_partition_root' "${WORKDIR}/publication.err"; then
    echo "Convergence gate FAILED: no warning for a changed" \
        "publish_via_partition_root" >&2
    cat "${WORKDIR}/publication.err" >&2
    exit 1
fi
if [ "$(psql -d "${TARGET_DB}" -tAc "${publication_state}")" \
        != "${expected_publications}" ]; then
    echo "Convergence gate FAILED: publication drift did not converge" >&2
    psql -d "${TARGET_DB}" -tAc "${publication_state}" >&2
    exit 1
fi
expect_empty_plan "publication drift converges in place"

psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
SET client_min_messages = error;
DROP PUBLICATION pglifecycle_all;
CREATE PUBLICATION pglifecycle_all FOR TABLE test.replicated;
CREATE PUBLICATION gate_pub_stray FOR TABLE test.replicated;
COMMENT ON PUBLICATION gate_pub_stray IS 'not in the project';
SQL
./target/debug/pglifecycle deploy -o "${WORKDIR}/publication.sql" \
    -d "${TARGET_DB}" "${WORKDIR}/project"
if ! grep -q '^-- destructive statements: 2 excluded' \
        "${WORKDIR}/publication.sql" \
    || grep -q '^DROP PUBLICATION' "${WORKDIR}/publication.sql"; then
    echo "Convergence gate FAILED: publication drops were not withheld" >&2
    cat "${WORKDIR}/publication.sql" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
if [ "$(psql -d "${TARGET_DB}" -tAc "${publication_state}")" \
        != "${expected_publications}" ]; then
    echo "Convergence gate FAILED: FOR ALL TABLES or the database-only" \
        "publication did not converge" >&2
    psql -d "${TARGET_DB}" -tAc "${publication_state}" >&2
    exit 1
fi
expect_empty_plan "publications rebuilt and dropped with --allow-drop"
