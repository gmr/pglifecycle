# Project Format

A pglifecycle project is a directory of YAML files, one file per
database object, validated against the JSON-Schema definitions in
[`schemata/`](https://github.com/gmr/pglifecycle/tree/main/schemata).

```text
my-project/
├── project.yaml          # name, superuser, extensions, languages,
│                         # access methods, database comment,
│                         # security labels and settings
├── schemata/             # one file per schema
│   └── test.yaml
├── tables/               # <schema>/<table>.yaml
│   └── test/
│       └── users.yaml
├── views/                # <schema>/<view>.yaml
├── materialized_views/
├── functions/            # <schema>/<function>.yaml
├── sequences/
├── domains/
├── types/                # one container file per schema
├── roles/                # <role>.yaml
├── users/
├── groups/
└── ...                   # aggregates, casts, collations, conversions,
                          # event_triggers, operators,
                          # operator_classes, operator_families,
                          # publications,
                          # servers, subscriptions, tablespaces,
                          # text_search, transforms, user_mappings,
                          # dml
```

Objects are structured data, not SQL. A table file, for example:

```yaml
---
name: users
schema: test
owner: postgres
columns:
  - name: id
    data_type: uuid
    nullable: false
    default: uuid_generate_v4()
  - name: email
    data_type: test.email_address
    nullable: false
indexes:
  - name: users_unique_email
    unique: true
    method: btree
    columns:
      - name: email
primary_key:
  - id
```

## Conventions

- The file location implies `schema` and `name`; both may be omitted
  from the file body and are injected on load.
- A `dependencies` key (e.g. `dependencies: {tables: [test.users]}`)
  makes the build create the object after the objects that it names.
  Use it for a relationship that the definition does not state in a
  field, such as the relations of a view query or the objects of a
  SQL-standard function body (`BEGIN ATOMIC` or `RETURN`). PostgreSQL
  checks such a body when it creates the function, so the function
  must come after each function, aggregate, operator, table, view or
  sequence that the body uses. pull writes these entries for views,
  materialized views, functions and procedures. A restore and a deploy
  do not check a string body (`AS $$ ... $$`), and PostgreSQL does not
  record what one uses, so pull writes no entries for it.
- An entry names an object as `schema.name`. A function or aggregate
  entry also gives the argument types of one overload, as
  pull writes them: `test.f(integer, text)`, or `test.agg(*)` for an
  aggregate with no arguments. An operator entry gives its left and
  right argument types: `test.=~=(integer, integer)`, or
  `test.!!!(NONE, integer)` for a prefix operator. The types compare
  as PostgreSQL resolves them, so `int4`, `INTEGER` and
  `pg_catalog.int4` are the same type. An entry with no argument list
  is correct only when one overload has the name. If more than one
  overload has the name, or no overload has the argument types, the
  load fails. An entry for an object that the project does not have
  gives a warning and orders nothing.
- A foreign key needs no entry: the build adds each foreign key after
  all tables. The load ignores a table-on-table entry unless the table
  inherits from, or is `LIKE`, the other table, or has a column of the
  other table's row type, and gives a warning. The definition already
  states those three relationships, so a table needs no entries for
  them.
- A function field of an aggregate, a cast, a conversion, a type, a
  language, a transform, an access method or an event trigger needs no
  entry either. The load orders the object after the overload that
  PostgreSQL calls: for example, the state function of an aggregate
  with the state type and the arguments of the aggregate. A field that
  gives the argument types (`test.f(integer)`) names that overload.
  When no overload has the argument types, the load orders the object
  after each overload of the name.
- ACL grants and revocations live on the grantee's role, user, or
  group file under `grants:`/`revocations:`, keyed by object:

```yaml
---
name: PUBLIC
create: false
grants:
  schemata:
    test:
      - USAGE
```

- Role memberships live in the same place, as `roles:`/`groups:`
  arrays naming the roles the grantee is a member of. `build` emits
  them as `GRANT role TO grantee` (or `REVOKE ... FROM` under
  `revocations:`). The granted role does not have to be a project
  file — reserved roles like `pg_read_all_data` can only ever be
  referenced, never created, and this is how to express membership in
  them:

```yaml
---
name: alice
grants:
  roles:
    - developers
    - pg_read_all_data
```
- A membership that does not behave the way a plain `GRANT role TO
  member` grants it is written as a mapping instead of a bare name.
  `admin: true` emits `WITH ADMIN OPTION`; `inherit` and `set` emit the
  per-membership `WITH INHERIT` / `WITH SET` options, which need
  PostgreSQL 16 or later. Omit `inherit` to defer to the member role's
  own `inherit` option, which is what PostgreSQL does — so `pull` keeps
  an explicit `inherit: true` only for a `NOINHERIT` member, where it
  is the only thing making the membership inherit:

```yaml
---
name: alice
grants:
  roles:
    - developers
    - role: partition_writer
      inherit: false
    - role: analytics
      admin: true
```

  The grantor (`GRANTED BY`) is not carried: it records who granted a
  membership in one cluster, not what the schema is, and PostgreSQL 16
  and later writes one for every membership.

- A column's default belongs on the column. A table that inherits a
  column has no column entry to carry one, so a default on an
  inherited column lives at the table level under `column_defaults:`,
  which `build` emits as `ALTER TABLE ONLY <table> ALTER COLUMN
  <column> SET DEFAULT ...` the way `pg_dump` writes it:

```yaml
---
name: audit_events_archive
schema: test
parents:
  - test.audit_events
column_defaults:
  - column: recorded_at
    default: CURRENT_TIMESTAMP
```

- Row-level security lives on the table. `row_level_security` states
  whether it is enabled and forced, and `policies` lists the policies.
  Each policy keeps only what differs from `CREATE POLICY`'s defaults:
  `restrictive: true` for `AS RESTRICTIVE`, a `command` other than
  `ALL`, `roles` other than `PUBLIC`. `using` and `with_check` hold the
  expression inside the `USING (...)` / `WITH CHECK (...)` clause,
  written the way PostgreSQL reports it: an operator expression keeps
  its own parentheses, as in `(tenant = CURRENT_USER)`. Write it that
  way, or deploy sees a change on every run.

```yaml
---
name: tenant_notes
schema: test
row_level_security:
  enabled: true
  forced: true
policies:
  - name: tenant_notes_own
    using: (tenant = CURRENT_USER)
    with_check: (tenant = CURRENT_USER)
    comment: tenant isolation
  - name: tenant_notes_no_blank
    restrictive: true
    command: INSERT
    with_check: (body <> ''::text)
```

  `pull` writes `row_level_security` for every table, and a table with
  that key has exactly the policies listed. A table without either key,
  such as one pulled before pglifecycle modeled row security, is not
  managed: `deploy` leaves its row security and policies as the
  database has them. Pull the project again to record them.

- An exclusion constraint lists each element as an index column (a
  `name` or an `expression`, with its `collation`, `opclass`,
  `direction` and `null_placement`) and the `operator` two rows are
  compared with. `replica_identity` is `FULL`, `NOTHING`, or
  `{index: name}`; absent is `DEFAULT`, the primary key.

```yaml
exclude_constraints:
  - name: room_bookings_no_overlap
    method: gist
    elements:
      - name: room
        operator: =
      - name: during
        operator: '&&'
    where: (status <> 'cancelled'::text)
replica_identity: FULL
```

- Columns may carry `storage`, `compression`, `statistics` (the
  statistics target) and `options` (such as `n_distinct`), which
  `build` writes as `ALTER COLUMN ... SET` after `CREATE TABLE`, as
  `pg_dump` does. Comments on a table's primary key, unique, check,
  foreign key and NOT NULL constraints live in `constraint_comments`,
  keyed by constraint name.

- Tables and views carry `rules`. A rule's `commands` are absent for
  `DO INSTEAD NOTHING`. A view's internal `_RETURN` rule is its query,
  never a rule.

```yaml
rules:
  - name: ledger_audit_insert
    event: INSERT
    condition: (new.amount > (0)::numeric)
    commands:
      - |-
        INSERT INTO test.ledger_audit (id)
          VALUES (new.id)
  - name: ledger_no_delete
    event: DELETE
    instead: true
    comment: Append only
```

  Conditions and commands keep the form `pg_dump` writes them in.

- Extended statistics live in `statistics/<schema>/<name>.yaml`, not
  on the table: the name is schema-qualified, and the owner need not
  own the table.

```yaml
---
name: measurements_ab
schema: test
owner: postgres
table: test.measurements
kinds: [ndistinct, dependencies]
elements: [a, b]
```

- Default privileges live in `default_privileges/<role>.yaml`, one file
  for each role whose new objects they apply to (`ALTER DEFAULT
  PRIVILEGES FOR ROLE`). Global and per-schema declarations compose in
  PostgreSQL, so each is kept as written; `build` emits the
  revocations first, then the grants, as `pg_dump` does.

```yaml
---
name: app_owner
grants:
  - schema: reporting
    object_type: TABLES
    grantee: analyst
    privileges: [SELECT]
revocations:
  - object_type: FUNCTIONS
    grantee: PUBLIC
    privileges: [EXECUTE]
```

- A partition is listed in its parent's `partitions` by its bounds.
  A partition with properties of its own (indexes, constraints,
  defaults, triggers, a replica identity, …) is also a table file of
  its own, and its entry in the parent's list has `attached: true`:
  `build` creates it as an ordinary table and attaches it with `ALTER
  TABLE ... ATTACH PARTITION`, as `pg_dump` writes it.

  `CREATE TABLE ... PARTITION OF` makes in a partition what its
  parent has: the column defaults, the CHECK and NOT NULL
  constraints, and a primary key, unique and exclusion constraint and
  index for each one of the parent. `pull` writes these for the
  partition, thus a partition of a table with a primary key is a
  table file of its own. `deploy` compares a partition that the
  project gives by its bounds only without these, so the plan stays
  empty. It does not compare the names that PostgreSQL gives to them
  in the partition. A partition that `PARTITION OF` made has the NOT
  NULL names of its parent, and a partition that `deploy` makes and
  attaches has names of its own; PostgreSQL cannot rename either. A
  partition table file that gives no NOT NULL name accepts both.

- An index with no `method` is a btree index, as `pull` writes it
  (`method: btree`). `deploy` compares an index without the values
  that `pg_dump` does not write: `unique: false`, `recurse: true`,
  `nulls_not_distinct: false`, `direction: ASC`, and the
  `null_placement` of the order (`LAST` with `ASC`, `FIRST` with
  `DESC`). An operator class that is the default for the
  type of the column, and a tablespace that is the default of the
  database, are a change: `pg_dump` does not write them, and `deploy`
  does not know these defaults. Leave them out.

- An index column that is an expression, such as a cast for an HNSW
  index, is written without parentheses around all of it:
  `expression: (embedding)::public.halfvec(1536)`. `build` adds them,
  because `CREATE INDEX` needs them for an expression that is not a
  function call. Parentheses around all of it are also accepted, and
  `deploy` compares the expression without them.

  The type of a cast can be in any form of a type name (see the next
  item), and this is also true in an exclusion constraint expression,
  in the `where` of an index or an exclusion constraint, in a `CHECK`
  constraint of a table or a domain, and in a default of a column or a
  domain: `(label)::VARCHAR(20)` compares equal to
  `(label)::character varying(20)`, which PostgreSQL writes. In these
  expressions, and in a generated column, a policy and a trigger
  `condition`, a cast can also be `label::text` or `CAST(label AS
  text)`: `deploy` compares it as `(label)::text`, which PostgreSQL
  writes. A string literal or a NULL keeps no parentheses
  (`'a'::text`), and an operand in parentheses stays as it is.
  `deploy` compares the remaining text of the expression as it is, so
  write it as `pull` writes it. Give a type that is not a built-in
  type its schema, as `pull` writes it:
  `(embedding)::public.halfvec(1536)`.
  The deploy script runs with the empty `search_path` of `pg_restore`,
  so a type with no schema is not found and the script fails.

- A type name (a column `data_type`, a parameter type, a `returns`
  type and the type of each column of a `TABLE(...)` return type, the
  source or target type of a cast or the type of a transform, or the
  type of a cast in an expression of the item before) can be in these
  forms. `deploy` compares it in the form that PostgreSQL writes, so
  these forms are not a change: an alias (`int4`, `varchar`,
  `timestamptz`, `decimal`, `bool`), a name in uppercase, `float` and
  `float(p)` (`double precision`, or `real` for a precision of 1 to
  24), `char` and `bit` with no length (`character(1)`, `bit(1)`),
  `timestamp(3)` (`timestamp(3) without time zone`), spaces in a
  modifier (`decimal(10, 2)`), `numeric(10)` (`numeric(10,0)`), an
  array bound, `ARRAY` or the array type name of a built-in type
  (`int[3]`, `integer ARRAY` and `_int4` are `integer[]`, `_text` is
  `text[]`, and `_varchar(4)` is `character varying(4)[]`), and the
  quoted name of a built-in type (`"int4"` is `integer`, and
  `"varchar"(10)` is `character varying(10)`). PostgreSQL keeps no
  typmod in a parameter, a return, a `TABLE(...)` column, a cast or a
  transform type, so there `varchar(10)` is `character varying` and
  `bpchar` is `character`. `"char"` and `"bit"` keep their quotes, as
  PostgreSQL writes them. A quoted name that is not the name of a
  type in `pg_catalog` (`"integer"` is not a type name) and a type
  that is not a built-in type keep their names: `_mood` and
  `public._text` are not changed.

  The restore and the deploy script run with an empty `search_path`,
  so give each type that is not a built-in type its schema, as `pull`
  writes it: `public.citext`, not `citext`. The load accepts a name
  with no schema when it is a built-in type, or a type or a domain of
  the project in the schema of the object that uses it. For any other
  name with no schema, the load fails with an error that names the
  file, the field and the type. When the project has extensions, the
  type can be a type of an extension, which the project does not
  have, so the load gives a warning and continues.

  A column type can be `serial`, `bigserial` or `smallserial` (or
  `serial4`, `serial8`, `serial2`). PostgreSQL keeps no serial type:
  it makes an `integer` (`bigint`, `smallint`) column with
  `nullable: false`, the default
  `nextval('<schema>.<sequence>'::regclass)` and a sequence that the
  column owns. `pull` writes that form. `deploy` compares a serial
  column in that form, with the sequence that the column owns in the
  database, so a serial column is not a change. `deploy` finds the
  sequence by its `OWNED BY`, thus a name that PostgreSQL changed
  (`<table>_<column>_seq1` when the name is in use) is also found. A
  new table or a new column is made with the serial type. A change
  from `serial` to `bigserial` changes the column type and the type of
  the sequence. A change to a smaller type (`bigserial` to `serial`)
  is destructive: `deploy` changes the column and the sequence only
  with `--allow-drop`. `deploy` compares the privileges of the
  sequence only when the project grants privileges on it. A serial
  column has a sequence with the default options: when the database
  sequence has other options (for example `increment_by: 10`),
  `deploy` changes them back to the defaults. To keep other options,
  write the column and the sequence as `pull` writes them. Do not
  write the sequence of a serial column as its own file: write the
  column in one of the two forms.

- PostgreSQL makes each column of the primary key and each identity
  column NOT NULL, with a NOT NULL constraint of the name
  `<table>_<column>_not_null`. `pull` writes `nullable: false` for
  these columns. A project can leave it out: `deploy` compares the
  column as NOT NULL. `nullable: true` on one of these columns is a
  load error, because the database cannot have it.

- A storage parameter (`storage_parameters` of a table, a
  materialized view or an index) can be a YAML number or boolean:
  `fillfactor: 90` and `autovacuum_enabled: false`. PostgreSQL keeps
  each value as text, and `pull` writes the text (`'90'`). `deploy`
  compares the value that PostgreSQL reads. A boolean and the words
  `true`, `false`, `on`, `off`, `yes` and `no` (in any case) are
  booleans, so `false` and `'off'` are equal. A number is its value,
  so `0.1` and `'0.10'` are equal. `1` and `0` compare as numbers, not
  as booleans. An empty map (`storage_parameters: {}`) is the same as
  no storage parameters.

- A `check_constraint` on a column is a CHECK of the table with the
  name that PostgreSQL gives it: `<table>_<column>_check` when the
  expression uses one column, else `<table>_check` (with a number
  when a CHECK of the table has the name). `build` and `deploy` write
  it with that name,
  and `pull` writes it in
  `check_constraints` of the table with that name. `deploy` compares
  the column CHECK with that table CHECK, so `check_constraint: ee > 0`
  on the column `ee` of `t` is the same as the CHECK `t_ee_check` with
  the expression `(ee > 0)`.

- A `collation` (of a column, a domain, a type, an index column, an
  exclusion constraint or a partition key) can leave out the
  `pg_catalog` schema: `'"C"'` and `pg_catalog."C"` are one
  collation, because PostgreSQL always searches `pg_catalog`. A
  name that is not quoted is in lowercase, as in SQL: `C` is the
  collation `c`, which does not exist, so write `'"C"'`.

- A generated column's `expression` can leave out the parentheses
  around all of it, and a generated column with no `kind` is stored.
  `deploy` compares the expression as text otherwise, so write it as
  `pull` writes it, with the casts PostgreSQL adds: `lower('X' ||
  a::text)` is `lower(('X'::text || (a)::text))`. A different text is
  set again with `SET EXPRESSION` on each deploy.

- A function or procedure with no `parameters` can give its argument
  types at the end of its `name`: `name: f(integer)` is `f` with one
  `integer` argument. Thus a name that has `(` needs its argument list
  when the routine has no arguments: `name: z(x)()` is the routine
  `"z(x)"` with no arguments, and `name: z(x)` is `z` with an argument
  of type `x`. `pull` writes `z(x)()`.

- A function's or procedure's `sql_body` (a `RETURN` expression or a
  `BEGIN ATOMIC` block) is not kept as it is written. PostgreSQL
  parses it and keeps the parsed form, and `pull` writes that form.
  `deploy` compares the body as text, so a `sql_body` written in any
  other form gets a `CREATE OR REPLACE` on each deploy. This does no
  harm, but the plan is never empty. Write the body as `pull` writes
  it:

    | Written | As PostgreSQL keeps it and `pull` writes it |
    | --- | --- |
    | `RETURN 'a'` | `RETURN 'a'::text` |
    | `RETURN pg_catalog.lower(x)` | `RETURN lower(x)` |
    | <code>RETURN lower(x) &#124;&#124; s.g() &#124;&#124; 'y'</code> | <code>RETURN ((lower(x) &#124;&#124; s.g()) &#124;&#124; 'y'::text)</code> |
    | `SELECT pg_catalog.abs(x) + pg_catalog.int4(1);` | `SELECT (abs(x) + 1);` |

  PostgreSQL removes the `pg_catalog.` schema, adds the casts and
  parentheses it infers, and evaluates a cast of a constant.

  An output column of a constant with no type, such as `SELECT 'a'`,
  is `SELECT 'a'::text`. When `deploy` makes the routine from that
  body, PostgreSQL writes `SELECT 'a'::text AS text`, because the name
  of the column is now the name of the type. `deploy` compares the two
  bodies as equal: it removes an `AS` name that is the name that
  PostgreSQL gives to the column, for a constant with a cast (also in
  parentheses, with `COLLATE`, or in a subquery of one value). A name in
  any other schema keeps its schema (`s.g()`). A body in `definition`
  (`AS $$ ... $$`) is kept as it is written, so this does not apply to
  it. To get the form for a body, create the function in a scratch
  database and `pull` it.

- A function's or procedure's `configuration` sets each setting when
  the routine starts. A value is a string, a number or a boolean. A
  setting that PostgreSQL keeps as a list of names is a list, with one
  name for each item. These settings are `search_path`,
  `temp_tablespaces`, `local_preload_libraries`,
  `session_preload_libraries`, `shared_preload_libraries`,
  `oauth_validator_libraries`, `output_plugin_libraries` and
  `unix_socket_directories`. Write each name as it is, with no quotes
  in it (`$user`, `My Schema`). `build` writes each item as its own
  string constant, `SET search_path = 'pg_catalog', 'pg_temp'`, as
  `pg_dump` does, and `pull` writes a value with more than one name as
  a list:

```yaml
---
name: whoami
returns: text
language: sql
security: DEFINER
configuration:
  search_path: [pg_catalog, pg_temp]
definition: SELECT current_user::text;
```

  For one of these settings, one string with a comma is an error. As
  one string, `pg_catalog, pg_temp` names one schema, so PostgreSQL
  searches `pg_temp` first, which is not safe for a `SECURITY DEFINER`
  function. One name can be a string (`search_path: pg_catalog`). A
  list for any other setting is also an error: `pg_dump` writes the
  value of such a setting as one string, so write it as one string
  (`DateStyle: iso, mdy`). An empty list is an error, because SET
  needs a value. For an empty value, write an empty string
  (`search_path: ''`).

- A role's or user's `settings` sets each setting when the role starts
  a session (`ALTER ROLE ... SET`). It has one object for each
  setting, and each value has the forms of a routine's
  `configuration`. `build` writes each item of a list as its own
  string constant, `ALTER ROLE app SET search_path TO '$user',
  'public'`, as `pg_dumpall` does:

```yaml
---
name: app
settings:
- search_path: [$user, public]
- work_mem: 64MB
```

- The settings of the database are in `project.yaml`. `settings` sets
  each setting when a session connects to the database (`ALTER
  DATABASE ... SET`), and `role_settings` sets them when one role
  connects to it (`ALTER ROLE ... IN DATABASE ... SET`), keyed by the
  role name. Each list has the form of a role's `settings`. The role
  does not have to be in the project. `pull` reads both from `pg_dump`,
  thus also with `--no-roles` and `--dump`. `build` writes them as
  `pg_dump` does (see [build](commands.md#build)), and `deploy` makes
  them the settings of the database. `comment` is the comment of the
  database (`COMMENT ON DATABASE`), and `security_labels` its security
  labels. `connection_limit` is the number of connections that the
  database allows (`ALTER DATABASE ... CONNECTION LIMIT`; -1, the
  default, is no limit), and `is_template: true` makes the database a
  template (`ALTER DATABASE ... IS_TEMPLATE`). `pull`,
  `build` and `deploy` use them as they use the settings. `pg_dump`
  cannot connect to a database with `ALLOW_CONNECTIONS false`, thus a
  project has no field for it:

```yaml
---
name: app
comment: The application database
security_labels:
  sepgsql: system_u:object_r:sepgsql_db_t:s0
connection_limit: 50
settings:
- search_path: [$user, public]
- work_mem: 64MB
role_settings:
  app_user:
  - statement_timeout: 5s
```

- An object that can have a security label has `security_labels`,
  next to its `comment`: a schema, a table and each of its columns
  and partitions, a view, a materialized view, a sequence, a domain,
  a type, a function, a procedure, an aggregate, a language, a
  publication, a subscription, a role, a user, a group, a tablespace
  and the database. The key is the name of the label provider, and
  the value is the label (`SECURITY LABEL FOR provider ON ... IS
  'label'`). `pull` reads the labels, `build` writes a `SECURITY
  LABEL` entry after the object, and `deploy` changes them in place.
  An object with no `security_labels` does not manage its labels:
  `deploy` leaves the labels of the database as they are, as it does
  for `row_level_security`. An object with the field has exactly the
  labels that it lists: a label that only the database has gets `IS
  NULL`, and `security_labels: {}` removes all of them. A label
  applies only when its provider is loaded in the server, for example
  with `shared_preload_libraries`. `pull` writes the field only for an
  object that has labels, thus `pull --no-security-labels` gives a
  project that does not manage labels.

```yaml
---
name: customers
schema: app
owner: postgres
columns:
  - name: email
    data_type: text
    security_labels:
      anon: MASKED WITH FUNCTION anon.fake_email()
security_labels:
  sepgsql: system_u:object_r:sepgsql_table_t:s0
```

- A foreign key can leave out its `name`. The project then uses the
  name PostgreSQL generates, `<table>_<columns>_fkey`, cut to 63
  bytes. When that name is already in use, PostgreSQL adds a number,
  so give such a key its name.

- A primary key or unique constraint on one column can be the column
  name alone (`primary_key: id`), and an integer default can be a
  number (`default: 0`). Each is the same as the form `pull` writes.
  For a NULL default, write `default: 'NULL'` with quotes: YAML reads
  an unquoted `NULL` as no value, and the load fails.

- An index of a partitioned table has `recurse: false` when it is made
  `ON ONLY` the partitioned table, as `pg_dump` writes it. Each
  partition's index is then in the partition's table file, with the
  index it belongs to as `parent`, and `build` and `deploy` attach it
  with `ALTER INDEX parent ATTACH PARTITION`. The `parent` index must
  have `recurse: false`, or `build` stops with an error. The index of a
  unique or primary key constraint needs no `parent`: it attaches with
  its partition.

- A cast has no schema of its own. `pull` files it in the
  `casts/<schema>.yaml` of the first schema its function or types name,
  or `public` when they are all built-in.

- An event trigger has no schema. Its `owner` is optional, and it
  must be a superuser, as PostgreSQL requires. `pull` writes it. When
  the file has no `owner`, `build` gives the entry the superuser of
  the project, and `deploy` does not set or compare the owner.

- A transform has no schema and no owner. `pull` files it in the
  `transforms/<schema>.yaml` of its type, or of the first schema its
  functions name when the type is built-in, or `public`.

- A publication lists each table as its qualified name, or as a
  mapping that also limits the columns or rows published, and names
  whole schemas under `schemas:`. Each table is exactly the table
  named: `pull` lists an inheritance child on its own, as `pg_dump`
  does, and `build` adds every table with `ONLY`.

```yaml
---
name: replicated_positive
tables:
  - name: test.replicated
    columns: [id, amount]
    where: (amount > 0)
  - test.audit_log
schemas:
  - reporting
parameters:
  publish: [insert, update]
```

- Grants on views may be written under either `tables:` or `views:`.
  PostgreSQL grants on views with `TABLE` syntax, so both emit
  `GRANT ... ON TABLE` and coalesce into a single ACL entry; `pull`
  writes view grants under `tables:`.

- `create: false` defines a role without creating it — used for
  built-in pseudo-roles like `PUBLIC`.
