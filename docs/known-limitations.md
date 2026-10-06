# Known Limitations

pglifecycle 2.0 is in beta. This page lists the limits that we know
of in this release.

The main workflow is solid: `pull` a database, `build` the project,
and `deploy` the project to a database. A project that `pull` writes
gives an empty deploy plan for the database that it came from, and
the gates test this on each change.

Most limits are in two areas:

- A form that you write by hand, which PostgreSQL keeps in another
  form. `deploy` then finds a change on each run. Write the form that
  `pull` writes.
- Rare shapes of objects, for example a partitioned table that gets a
  new partition, or an object type that `pull` does not model.

Some limits are already described with the command that they apply
to. This page links to those descriptions and does not repeat them.

## Pull

`pull` writes each dump entry that it cannot model to
`remaining.yaml`, and then fails. Use `--allow-unsupported` to accept
the project without these entries. See
[Unsupported dump entries](commands.md#unsupported-dump-entries).

- **Security labels.** `pull` does not model `SECURITY LABEL` entries.
  Use `--no-security-labels` to leave them out of the dump.
- **Database properties other than settings.** `CONNECTION LIMIT`,
  `IS_TEMPLATE` and `ALLOW_CONNECTIONS` go to `remaining.yaml`. `pull`
  keeps the settings of the database. Set the other properties by
  hand.
- **Comments on objects that pull does not model.** Such a comment
  goes to `remaining.yaml` with its object.
- **Column settings on an inheritance child.** `ALTER TABLE ONLY child
  ALTER COLUMN c SET STATISTICS` (or `SET STORAGE`) on a column that
  the child inherits puts the table entry in `remaining.yaml`. Set the
  value by hand after the restore or the deploy.
- **A NOT VALID domain CHECK.** `pg_dump` writes a `NOT VALID`
  domain CHECK as an `ALTER DOMAIN` entry of its own. The model has no
  place for `NOT VALID`, so `pull` stops with `1 dump entry could not
  be modeled (CHECK CONSTRAINT)`. Use `--allow-unsupported`: the entry
  goes to `remaining.yaml`. Then add the CHECK by hand after the
  restore.

## Build

- **Text search mappings that differ only in case.** A text search
  configuration with two token types that differ only in case (for
  example `word` and `Word`) gets two `ADD MAPPING` statements, and
  the restore fails. Write each token type one time, in lowercase.
- **A table CHECK with the name of a column CHECK.** A column
  `check_constraint` on column `c` of table `t` gets the name
  `t_c_check`. When the table also has a CHECK with that name, the
  load accepts the project, but `CREATE TABLE` fails. Give the table
  CHECK another name.
- **Required fields that validation does not check.** The schemata of
  arguments, conversions, domains, materialized views, tablespaces
  and views do not make their required fields mandatory. Validation
  does not report a missing field in these files, for example the
  `location` of a tablespace. Give each field that the object needs.
- **Settings and the database comment.** The archive restores the
  settings and the comment of the database only with `pg_restore
  --create`. See [build](commands.md#build).

## Deploy: false changes for hand-written forms

`deploy` compares some fields as text. PostgreSQL keeps these fields
in its own form, and `pull` writes that form. A field in another form
is a change on each deploy, so the plan is never empty. The change
does no harm, but it hides real changes.

The workaround is the same for each item: write the form that `pull`
writes. To find that form, apply the change one time, then `pull` the
database and copy the field from the file.

These limits are already described:

- Expressions (defaults, CHECK constraints, index and policy
  expressions, generated columns): the casts and parentheses that
  PostgreSQL adds. See
  [Destructive statements and limits](commands.md#destructive-statements-and-limits).
- `sql_body` of a function or procedure, generated columns, the
  default operator class of an index, and a tablespace that is the
  default. See [Conventions](project-format.md#conventions).
- Row filters of publications and expressions of statistics. See
  [Destructive statements and limits](commands.md#destructive-statements-and-limits).
- A project that you pull with another `--style`. See [pull](commands.md#pull).

These limits are new on this page:

- **The layout of a SQL function body.** `pull` formats a function
  body with libpgfmt: `SELECT a, a;` becomes `SELECT a,` and `a;` on
  two lines. A body with another layout gets a `CREATE OR REPLACE` on
  each deploy.
- **Materialized view queries.** `deploy` compares the `query` of a
  materialized view as text. Write the query as `pull` writes it, with
  no `;` at the end: with a `;`, the build writes `;;`.
- **Dollar-quoted constants in expressions.** PostgreSQL writes
  `$$a$$::text` as `'a'::text`. Write a string constant in single
  quotes.
- **A column CHECK on no column or on other columns.** `deploy` gives
  a column `check_constraint` the name `<table>_<column>_check`. When
  the expression uses no column, or more than one column, PostgreSQL
  gives it the name `<table>_check`. Write such a constraint as a
  table CHECK, with the name that PostgreSQL gives.
- **A user mapping file named `public`.** `pull` writes the mapping
  for `PUBLIC` in a file named `PUBLIC`. A file named `public` does
  not match it, so `deploy` drops and makes the mapping on each run.
- **Partitioned tables with mixed partitions.** When some partitions
  of a table are only bounds in the parent and others are files with
  `attached: true`, the parent's `ON ONLY` index is a change on each
  deploy. Write all partitions in the same form.
- **A new unnamed domain CHECK before other unnamed checks.**
  PostgreSQL names unnamed checks in order: `<domain>_check`,
  `<domain>_check1`, and so on. A new unnamed check before existing
  ones changes their names, and `deploy` plans a rebuild of the
  domain. Add a new check after the others, or give it a name.
- **The detailed form of key columns.** A unique constraint written
  as `- columns: [w]`, or a primary key written as `primary_key:
  {columns: [w]}`, does not compare equal to the plain list that
  `pull` writes, so `deploy` changes it on each run. Write the plain
  list form that `pull` writes.

## Deploy: order and rebuild limits

When `deploy` cannot make a change safely, it withholds the change
(without `--allow-drop`) or stops with an error. In some cases below,
it writes a script that fails on apply. The script runs in one
transaction, so the database does not change.

These limits are already described:

- Dependent objects that `deploy` cannot make again, objects that
  `--exclude-table` or `--exclude-schema` hides, and the rebuild of a
  table with attached partitions, inheritance children or owned
  sequences. See
  [Destructive statements and limits](commands.md#destructive-statements-and-limits).
- Changes that fall back to drop and create, for example a column
  type, the column order, partitioning and storage parameters. See
  [Change reconciliation](commands.md#change-reconciliation).
- A subscription refresh and changes to `slot_name`, `two_phase` and
  `failover`. See
  [Destructive statements and limits](commands.md#destructive-statements-and-limits).
- Roles, users and tablespaces: `deploy` does not change them. See
  [Destructive statements and limits](commands.md#destructive-statements-and-limits).

These limits are new on this page:

- **A change to a primary key or a unique constraint.** A change to
  the columns or the `INCLUDE` columns of a primary key or a unique
  constraint drops the table and makes it again. Without
  `--allow-drop`, the change is withheld. When a foreign key of
  another table references the constraint, `deploy` stops with an
  error. See
  [Destructive statements and limits](commands.md#destructive-statements-and-limits).
  To keep the data, make the change by hand, then `pull` the database.
- **A new partition of an existing partitioned table.** `deploy`
  plans a rebuild of the parent table, not `CREATE TABLE ... PARTITION
  OF`. Without `--allow-drop`, the rebuild is withheld. With
  `--allow-drop`, the rebuild loses the data of the parent and of the
  partitions that are only bounds. Make the partition by hand, then
  `pull` the database.
- **A statement that calls a function that the script makes later.**
  Only the in-place statements of a table wait for a new function.
  Other statements do not, for example a `CREATE OR REPLACE` or a
  statement of a rebuild. Also, a new column with a default that calls
  a new function, which uses that column, makes a loop. The script
  then fails on apply. Deploy the function first, then the rest.
- **An attached partition without the parent's NOT NULL.** When a
  hand-written partition file with `attached: true` does not have the
  NOT NULL of a parent column, `ATTACH PARTITION` fails. Give the
  column `nullable: false`, as the parent has it.
- **A change from integer to serial.** For a column that does not own
  a sequence in the database, `deploy` writes `ALTER COLUMN ... TYPE
  serial`, and PostgreSQL refuses it. Make the sequence and the
  default by hand, then `pull` the database.
- **NO INHERIT on a parent's NOT NULL.** After `deploy` makes a NOT
  NULL of a parent `NO INHERIT`, the children keep a local copy. A
  second deploy removes it.
- **The drop of a `PUBLIC` user mapping.** For a mapping for `PUBLIC`
  that only the database has, `deploy` writes `DROP USER MAPPING ...
  FOR "PUBLIC"`. In quotes, `PUBLIC` is the name of a role, so the
  statement does not drop the mapping. Drop it by hand.

## Connection and CLI

These limits are already described:

- A role that is not a superuser cannot read subscriptions and the
  options of most user mappings. See [deploy](commands.md#deploy).
- pglifecycle does not read `pg_service.conf` to show the connection.
  See [pull](commands.md#pull).

These limits are new on this page:

- **pg_dump warnings.** When `pg_dump` or `pg_dumpall` succeeds,
  pglifecycle does not show its warnings. Run `pg_dump --schema-only`
  by hand to see them.
- **A connection string in `PGDATABASE`.** pglifecycle reads
  `PGDATABASE` as the `--dbname` value, so it accepts a connection
  string there. libpq, `psql` and `pg_dump` read `PGDATABASE` only as
  a database name. Give a connection string with `--dbname`, not in
  `PGDATABASE`.
- **Subscriptions that a role cannot read.** A role that is not a
  superuser does not see the subscriptions. `deploy` warns about a
  subscription that the project adds or changes, but not about one
  that only the database has. That subscription is not dropped. Run
  `deploy` as a superuser.
