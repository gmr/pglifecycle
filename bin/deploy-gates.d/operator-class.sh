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
# 5. A member that only the family of a class with no family has is
#    dropped from the family only with --allow-drop. An ADD in the slot
#    of such a member is withheld with the drop.
# 6. A class change that needs a drop, for a class with a member in its
#    family, drops that member from the family first.
# 7. A class moves to a new family while its old family is only in the
#    database: the drop of the old family drops the class too, and the
#    rebuild of the class does not fail.
# 8. An ADD of a class member in the slot of a member that the project
#    family of the class drops is withheld with the drop.
# 9. A new class whose implied family the database has, with a member
#    and no class: the family is dropped first, and the class is made,
#    only with --allow-drop.

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

# a member that only the family of a class with no family has: the
# family is not dropped, as that drops the class too, but the member is
# dropped from it, only with --allow-drop
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
    -c "ALTER OPERATOR FAMILY test.gate_int_ops USING btree
            ADD OPERATOR 3 = (integer, bigint);"
expect_opclass_withheld 1 "the implied family member drop was not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT NOT EXISTS (SELECT FROM pg_amop a
    JOIN pg_opfamily f ON f.oid = a.amopfamily
    WHERE f.opfname = 'gate_int_ops' AND a.amoprighttype = 'bigint'::regtype)
    AND EXISTS (SELECT FROM pg_opclass WHERE opcname = 'gate_int_ops')" \
    "the implied family member was not dropped"
expect_empty_plan "a member of an implied family is dropped"

# another function in the slot of a member that the class gives, in its
# implied family: the ADD needs the DROP, so it is withheld with it
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER OPERATOR FAMILY test.gate_int_ops USING btree
    DROP FUNCTION 2 (integer, integer);
ALTER OPERATOR FAMILY test.gate_int_ops USING btree
    ADD FUNCTION 2 (integer, integer) btint8sortsupport(internal);
SQL
expect_opclass_withheld 2 "the implied family member change was not withheld"
if ! psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
        -f "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the script without --allow-drop" \
        "does not run" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT EXISTS (SELECT FROM pg_amproc p
    JOIN pg_opfamily f ON f.oid = p.amprocfamily
    WHERE f.opfname = 'gate_int_ops' AND p.amprocnum = 2
      AND p.amproc = 'btint4sortsupport'::regproc)" \
    "the changed implied family member was not set back"
expect_empty_plan "a changed member of an implied family converges"

# a class change that needs a drop, for a class whose sort support
# function PostgreSQL keeps in its family: the rebuild drops that
# member from the family first, as the create gives it again
perl -0pi -e "s/  - \{strategy: 4, name: '>='\}\n(  - \{strategy: 5, name: '>'\}\n  functions:\n  - support: 2\n)/\$1/" \
    "${WORKDIR}/project/operator_classes/test.yaml"
[ "$(grep -c "strategy: 4, name: '>='" \
    "${WORKDIR}/project/operator_classes/test.yaml")" = 1 ]
expect_opclass_withheld 2 "the rebuild of the class was not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT
    (SELECT count(*) = 4 FROM pg_amop a
        JOIN pg_opfamily f ON f.oid = a.amopfamily
        WHERE f.opfname = 'gate_int_ops')
    AND (SELECT count(*) = 2 FROM pg_amproc p
        JOIN pg_opfamily f ON f.oid = p.amprocfamily
        WHERE f.opfname = 'gate_int_ops')" \
    "the class with a member in its family was not made again"
expect_empty_plan "a class with a member in its family is made again"

# the class moves to a new family, and its old family is then only in
# the database. Dropping the old family drops the class too, so the
# drop of the class in the rebuild must not fail
cat >> "${WORKDIR}/project/operator_families/test.yaml" <<'YAML'
- name: gate_family
  schema: test
  method: btree
YAML
perl -0pi -e 's/(- name: gate_int_ops\n  schema: test\n  method: btree\n)/$1  family: test.gate_family\n/' \
    "${WORKDIR}/project/operator_classes/test.yaml"
grep -q 'family: test.gate_family' \
    "${WORKDIR}/project/operator_classes/test.yaml"
expect_opclass_withheld 2 "the move of the class was not withheld"
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT
    NOT EXISTS (SELECT FROM pg_opfamily WHERE opfname = 'gate_int_ops')
    AND (SELECT f.opfname = 'gate_family' FROM pg_opclass c
        JOIN pg_opfamily f ON f.oid = c.opcfamily
        WHERE c.opcname = 'gate_int_ops')" \
    "the class did not move to its new family"
expect_empty_plan "a class moves to a new family"

# another function in the slot of a member that the class gives, in the
# family of the project that the class names: the family drops the
# other function, and the ADD of the class needs that DROP, so it is
# withheld with it
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
ALTER OPERATOR FAMILY test.gate_family USING btree
    DROP FUNCTION 2 (integer, integer);
ALTER OPERATOR FAMILY test.gate_family USING btree
    ADD FUNCTION 2 (integer, integer) btint8sortsupport(internal);
SQL
expect_opclass_withheld 2 "the family member change was not withheld \
with the class member that needs it"
if ! psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
        -f "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the script without --allow-drop" \
        "does not run" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT EXISTS (SELECT FROM pg_amproc p
    JOIN pg_opfamily f ON f.oid = p.amprocfamily
    WHERE f.opfname = 'gate_family' AND p.amprocnum = 2
      AND p.amproc = 'btint4sortsupport'::regproc)" \
    "the class member in the slot of a dropped family member was not added"
expect_empty_plan "a class member in the slot of a dropped family member \
converges"

# a new class with no family, where the database has its implied family
# with a member and no class: the create of the class fails on the
# member, so the plan drops the family first, and makes the class, only
# with --allow-drop
psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE OPERATOR FAMILY test.gate_new_ops USING hash;
ALTER OPERATOR FAMILY test.gate_new_ops USING hash
    ADD FUNCTION 1 hashint4(integer);
SQL
cat >> "${WORKDIR}/project/operator_classes/test.yaml" <<'YAML'
- name: gate_new_ops
  schema: test
  method: hash
  data_type: integer
  operators:
  - {strategy: 1, name: '='}
  functions:
  - {support: 1, function: hashint4(integer)}
YAML
expect_opclass_withheld 2 "the drop of the implied family was not withheld \
with the create of the class"
if ! psql -d "${TARGET_DB}" -q -v ON_ERROR_STOP=1 \
        -f "${WORKDIR}/withheld.sql"; then
    echo "Convergence gate FAILED: the script without --allow-drop" \
        "does not run" >&2
    exit 1
fi
./target/debug/pglifecycle deploy --apply --allow-drop -d "${TARGET_DB}" \
    "${WORKDIR}/project"
expect_opclass "SELECT EXISTS (SELECT FROM pg_opclass
    WHERE opcname = 'gate_new_ops'
      AND opcnamespace = 'test'::regnamespace)" \
    "the class was not made in place of its implied family"
expect_empty_plan "a new class replaces its implied family with members"
