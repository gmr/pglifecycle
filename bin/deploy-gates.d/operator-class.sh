# Sourced by bin/deploy-gates (gate 2). Operator classes and operator
# families compare by definition.
#
# 1. Classes and families written by hand in a short form: type
#    aliases, uppercase names, qualified pg_catalog names, members in
#    another order and without the types that PostgreSQL supplies, a
#    class with no family, and optional members in the class. pg_dump
#    writes an optional member in the family, and a class with no
#    family gets one of its own name. The plan is empty.
# 2. Drift in the family members, the comments and the owners
#    converges with ALTER OPERATOR FAMILY and COMMENT ON, without
#    --allow-drop. A member that a class lacks is added to its family.
# 3. A member that only the database family has, and a class and a
#    family that only the database has, are dropped only with
#    --allow-drop.
# 4. A class change that needs a drop fails while an index uses the
#    class. The transaction rolls back, so no other change of the same
#    deploy is made. Without the index, the change is made.

# $1 is a query that must return t, $2 says what the step checks
expect_opclass() {
    if [ "$(psql -d "${TARGET_DB}" -tAc "$1")" != t ]; then
        echo "Convergence gate FAILED: $2" >&2
        exit 1
    fi
}

# $1 is the number of statements that deploy must withhold without
# --allow-drop, $2 says what the step checks
expect_opclass_withheld() {
    ./target/debug/pglifecycle deploy -o "${WORKDIR}/withheld.sql" \
        -d "${TARGET_DB}" "${WORKDIR}/project"
    if grep -Eq '^(DROP OPERATOR|ALTER OPERATOR FAMILY .* DROP)' \
            "${WORKDIR}/withheld.sql" \
        || ! grep -q "^-- destructive statements: $1 excluded" \
            "${WORKDIR}/withheld.sql"; then
        echo "Convergence gate FAILED: $2" >&2
        cat "${WORKDIR}/withheld.sql" >&2
        exit 1
    fi
}

psql -d postgres -q -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'Gate Owner') THEN
        CREATE ROLE "Gate Owner";
    END IF;
END
$$;
SQL

# the families of int_copy_ops, point_distance and gate_int_ops are
# not in the project: each class has no family, so PostgreSQL makes
# one with its name
cat > "${WORKDIR}/project/operator_families/test.yaml" <<'YAML'
---
schema: test
owner: postgres
operator_families:
- name: int_family
  schema: test
  method: BTREE
  functions:
  - support: 1
    types:
    - INT4
    - INT8
    function: pg_catalog.btint48cmp(int, bigint)
  operators:
  - strategy: 1
    name: OPERATOR(pg_catalog.<)
    arguments:
    - int4
    - int8
  comment: Integers
YAML
cat > "${WORKDIR}/project/operator_classes/test.yaml" <<'YAML'
---
schema: test
owner: postgres
operator_classes:
- name: int_class
  schema: test
  method: btree
  data_type: INTEGER
  default: false
  family: TEST.INT_FAMILY
  operators:
  - strategy: 3
    name: '='
  - strategy: 1
    name: <
  functions:
  - support: 1
    function: test.compare_ints(INT4, INT4)
  comment: By difference
- name: int_copy_ops
  schema: test
  method: btree_copy
  data_type: int4
  default: true
  operators:
  - {strategy: 1, name: <}
  - {strategy: 2, name: <=}
  - {strategy: 3, name: '='}
  - {strategy: 4, name: '>='}
  - {strategy: 5, name: '>'}
  functions:
  - support: 1
    function: btint4cmp(integer, integer)
- name: point_distance
  schema: test
  method: gist
  data_type: point
  storage: BOX
  operators:
  - strategy: 15
    name: <->
    arguments:
    - point
    - point
    order_by: float_ops
  functions:
  - {support: 1, function: 'gist_point_consistent(internal, point, int2, oid, internal)'}
  - {support: 2, function: 'gist_box_union(internal, internal)'}
  - {support: 3, function: gist_point_compress(internal)}
  - {support: 5, function: 'gist_box_penalty(internal, internal, internal)'}
  - {support: 6, function: 'gist_box_picksplit(internal, internal)'}
  - {support: 7, function: 'gist_box_same(box, box, internal)'}
  - {support: 8, function: 'gist_point_distance(internal, point, smallint, oid, internal)'}
- name: gate_int_ops
  schema: test
  method: btree
  data_type: integer
  storage: integer
  operators:
  - {strategy: 1, name: <}
  - {strategy: 2, name: <=}
  - {strategy: 3, name: '='}
  - {strategy: 4, name: '>='}
  - {strategy: 5, name: '>'}
  functions:
  - support: 2
    function: pg_catalog.btint4sortsupport(INTERNAL)
  - support: 1
    function: BTINT4CMP(INT4, INT4)
YAML
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT EXISTS (SELECT FROM pg_opclass
    WHERE opcname = 'gate_int_ops'
      AND opcnamespace = 'test'::regnamespace)" \
    "the new operator class was not made"
expect_empty_plan "operator class and family short forms are unchanged"

# drift that ALTER OPERATOR FAMILY and COMMENT ON reconcile: a removed
# family member, a removed class member, comments and owners
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER OPERATOR FAMILY test.int_family USING btree
    DROP FUNCTION 1 (integer, bigint);
ALTER OPERATOR FAMILY test.gate_int_ops USING btree
    DROP FUNCTION 2 (integer, integer);
COMMENT ON OPERATOR FAMILY test.int_family USING btree IS 'drift';
COMMENT ON OPERATOR CLASS test.int_class USING btree IS NULL;
ALTER OPERATOR FAMILY test.int_family USING btree OWNER TO "Gate Owner";
ALTER OPERATOR CLASS test.int_class USING btree OWNER TO "Gate Owner";
SQL
./target/debug/pglifecycle deploy --apply -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT
    (SELECT count(*) = 1 FROM pg_amproc
        WHERE amprocfamily = f.oid AND amproc = 'btint48cmp'::regproc)
    AND obj_description(f.oid, 'pg_opfamily') = 'Integers'
    AND pg_get_userbyid(f.opfowner) = 'postgres'
    FROM pg_opfamily f
    WHERE f.opfname = 'int_family' AND f.opfnamespace = 'test'::regnamespace" \
    "the operator family was not set back"
expect_opclass "SELECT obj_description(c.oid, 'pg_opclass') = 'By difference'
    AND pg_get_userbyid(c.opcowner) = 'postgres'
    FROM pg_opclass c
    WHERE c.opcname = 'int_class' AND c.opcnamespace = 'test'::regnamespace" \
    "the operator class was not set back"
expect_opclass "SELECT count(*) = 1 FROM pg_amproc p
    JOIN pg_opfamily f ON f.oid = p.amprocfamily
    WHERE f.opfname = 'gate_int_ops' AND p.amprocnum = 2" \
    "the member of the operator class was not added"
expect_empty_plan "changed operator classes and families converge in place"

# a member that only the database family has
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "ALTER OPERATOR FAMILY test.int_family USING btree
            ADD OPERATOR 3 = (integer, bigint);"
expect_opclass_withheld 1 "the family member drop was not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT NOT EXISTS (SELECT FROM pg_amop a
    JOIN pg_opfamily f ON f.oid = a.amopfamily
    WHERE f.opfname = 'int_family' AND a.amopstrategy = 3
      AND a.amoprighttype = 'bigint'::regtype)" \
    "the database-only family member was not dropped"
expect_empty_plan "a database-only family member is dropped"

# another operator in the slot of a family member: the ADD needs the
# DROP, so it is withheld with it, and the script without --allow-drop
# runs
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER OPERATOR FAMILY test.int_family USING btree
    DROP OPERATOR 1 (integer, bigint);
ALTER OPERATOR FAMILY test.int_family USING btree
    ADD OPERATOR 1 <= (integer, bigint);
SQL
expect_opclass_withheld 2 "the family member change was not withheld"
if ! psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
        -f "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the script without --allow-drop" \
        "does not run" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT EXISTS (SELECT FROM pg_amop a
    JOIN pg_opfamily f ON f.oid = a.amopfamily
    WHERE f.opfname = 'int_family' AND a.amopstrategy = 1
      AND a.amoprighttype = 'bigint'::regtype
      AND a.amopopr = '<(integer, bigint)'::regoperator)" \
    "the changed family member was not set back"
expect_empty_plan "a changed family member converges"

# a class and a family that only the database has
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE OPERATOR FAMILY test.stray_family USING hash;
CREATE OPERATOR CLASS test.stray_class FOR TYPE integer USING hash
    FAMILY test.stray_family AS OPERATOR 1 =, FUNCTION 1 hashint4(integer);
SQL
expect_opclass_withheld 2 "the class and family drops were not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT NOT EXISTS (SELECT FROM pg_opfamily
    WHERE opfname IN ('stray_family', 'stray_class'))" \
    "the database-only class and family were not dropped"
expect_empty_plan "a database-only class and family are dropped"

# a removed hard member needs a drop and a create of the class. An
# index that uses the class stops the drop: deploy fails, and the
# transaction rolls back, so the changed family comment of the same
# deploy is not made
perl -0pi -e 's/  - strategy: 3\n    name: .=.\n  - strategy: 1\n/  - strategy: 1\n/' \
    "${WORKDIR}/project/operator_classes/test.yaml"
! grep -q 'strategy: 3$' "${WORKDIR}/project/operator_classes/test.yaml"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE INDEX gate_int_class_idx ON test.warehouses (id test.int_class);
COMMENT ON OPERATOR FAMILY test.int_family USING btree IS 'drift';
SQL
# the rebuild makes the comment of the class again, so it is withheld
# with it
expect_opclass_withheld 2 "the operator class rebuild was not withheld"
if ./target/debug/pglifecycle deploy --apply --allow-drop \
        -d "${TARGET_DB}" "${WORKDIR}/project" \
        > /dev/null 2> "${WORKDIR}/opclass.err"; then
    echo "Convergence gate FAILED: the operator class was dropped while" \
        "an index uses it" >&2
    exit 1
fi
if ! grep -q 'rolled back' "${WORKDIR}/opclass.err" \
    || ! grep -q 'cannot drop operator class test.int_class' \
        "${WORKDIR}/opclass.err"; then
    echo "Convergence gate FAILED: the failed drop has no clear error" >&2
    cat "${WORKDIR}/opclass.err" >&2
    exit 1
fi
expect_opclass "SELECT
    (SELECT count(*) = 2 FROM pg_amop a JOIN pg_opclass c
        ON c.opcfamily = a.amopfamily
        WHERE c.opcname = 'int_class' AND a.amoplefttype = a.amoprighttype)
    AND obj_description(f.oid, 'pg_opfamily') = 'drift'
    AND to_regclass('test.gate_int_class_idx') IS NOT NULL
    FROM pg_opfamily f WHERE f.opfname = 'int_family'" \
    "the failed deploy made a part of its changes"
echo "Convergence gate passed: a class drop that an index stops rolls back"
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "DROP INDEX test.gate_int_class_idx;"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT count(*) = 1 FROM pg_amop a
    JOIN pg_opclass c ON c.opcfamily = a.amopfamily
    WHERE c.opcname = 'int_class' AND a.amoplefttype = a.amoprighttype" \
    "the operator class was not made again"
expect_empty_plan "a changed operator class converges"
