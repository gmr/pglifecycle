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
- Rare shapes of objects, or an object type that `pull` does not
  model.

Some limits are already described with the command that they apply
to. This page links to those descriptions and does not repeat them.

## Pull

`pull` writes each dump entry that it cannot model to
`remaining.yaml`, and then fails. Use `--allow-unsupported` to accept
the project without these entries. See
[Unsupported dump entries](commands.md#unsupported-dump-entries).

- **Comments and security labels on objects that pull does not
  model.** Such a comment or label goes to `remaining.yaml` with its
  object. A security label on a column of a view or of a materialized
  view also goes there. Use `--no-security-labels` to leave the labels
  out of the dump.
- **Column settings on an inheritance child.** `ALTER TABLE ONLY child
  ALTER COLUMN c SET STATISTICS` (or `SET STORAGE`) on a column that
  the child inherits puts the table entry in `remaining.yaml`. Set the
  value by hand after the restore or the deploy.

## Build

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
- **A new column with a default that calls a new function.** When the
  function has a SQL-standard body (`sql_body`), it can read the new
  column, so `deploy` adds the column with no default, then makes the
  function, then sets the default. Thus the rows that the table has
  get NULL in the new column, not the default. With `nullable: false`,
  the ADD COLUMN fails on a table that has rows. To give the rows a
  value, add the column by hand and fill it, then deploy.
- **A rebuild that calls a new function that reads the object.** The
  drop and create of an object wait for a new function that they
  call. When that function has a SQL-standard body that reads the
  object, the function and the object need each other first, and the
  script fails on apply. Deploy the function first, then the rest.
- **A change from integer to serial.** For a column that does not own
  a sequence in the database, `deploy` writes `ALTER COLUMN ... TYPE
  serial`, and PostgreSQL refuses it. Make the sequence and the
  default by hand, then `pull` the database.
- **NO INHERIT on one of two parents.** When a child has a NOT NULL
  from two parents, and only one parent changes it to `NO INHERIT`,
  the child keeps a NOT NULL of its own. PostgreSQL cannot remove it
  while the other parent gives the NOT NULL. Give the child that NOT
  NULL in the project, as `pull` writes it.

## Connection and CLI

These limits are already described:

- A role that is not a superuser cannot read subscriptions and the
  options of most user mappings. See [deploy](commands.md#deploy).
- pglifecycle does not read `pg_service.conf` to show the connection.
  See [pull](commands.md#pull).
