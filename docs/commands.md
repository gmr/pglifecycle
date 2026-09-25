# Commands

All commands share the logging options `-L/--log-file FILE`,
`-v/--verbose`, and `--debug`.

## create

Create a skeleton project.

```bash
pglifecycle create [OPTIONS] DEST
```

| Option | Description |
| --- | --- |
| `--encoding ENCODING` | Database encoding (default `UTF-8`) |
| `--force` | Write to `DEST` even if it already exists |
| `--name NAME` | Override the default project name |
| `--no-gitkeep` | Do not create `.gitkeep` files in empty directories |
| `--no-stdstrings` | Turn off standard conforming strings |
| `--superuser NAME` | Superuser name (default `postgres`) |
| `--include-mode-headers` | Prefix generated files with editor mode headers |

## build

Generate a `pg_restore`-compatible archive from a project. The project
is loaded and validated against the JSON-Schema contract before the
archive is written; entries are ordered with pg_dump's weighted
topological sort.

```bash
pglifecycle build PROJECT DEST
```

## deploy

Compare a live database (or an existing dump) against the project and
emit the DDL needed to make the database match: `CREATE` for objects
missing from the database, `DROP` for objects missing from the
project, and an in-place reconciliation (or a drop+recreate fallback)
for objects that exist in both but differ. The script goes to stdout
(or `-o FILE`) with a summary on stderr; by default nothing is
executed, so it can be applied as a separate CI step:

```bash
pglifecycle deploy -o deploy.sql PROJECT
psql --single-transaction -v ON_ERROR_STOP=1 -f deploy.sql
```

`--apply` runs the script directly instead, in a single transaction
via `psql` (it rolls back on the first error and refuses while gated
destructive statements are pending).

| Option | Description |
| --- | --- |
| `-D, --dump FILE` | Compare against a `pg_dump -Fc` file instead of connecting |
| `-o, --output FILE` | Write the DDL script to FILE instead of stdout |
| `--apply` | Execute the script in one transaction via psql (conflicts with `--dump`) |
| `--allow-drop` | Include destructive statements in the script |
| `-x, --no-privileges` | Do not include GRANT/REVOKE |
| `--error-file FILE` | Where to record failures and their DDL (default `pglifecycle-errors.log`) |
| `-T, --exclude-table PATTERN` | Exclude tables/views/sequences matching `PATTERN` (repeatable; conflicts with `--dump`) |
| `-N, --exclude-schema PATTERN` | Exclude schemas matching `PATTERN` (repeatable; conflicts with `--dump`) |
| `--exclude-extension PATTERN` | Exclude extensions matching `PATTERN` (repeatable; conflicts with `--dump`) |

The exclude patterns are the same as `pull`'s and are passed through to
`pg_dump`. Excluding the schemas the project does not manage keeps them
out of the snapshot, which both shortens the dump and silences the
"unmanaged" dependency warnings.

The connection options match `pull` (see below). Like `pull`, `deploy`
snapshots and formats the database with libpgfmt, so DDL that fails to
parse or format — and the statement in flight if it is interrupted — is
recorded to the error report (`--error-file`); see
[Diagnosing parse and format failures](#diagnosing-parse-and-format-failures).

### Change reconciliation

Objects that differ between the project and the database are
reconciled in place where PostgreSQL can express it:

- **Tables** — add column, set/drop default, set/drop not-null,
  add/drop check constraints, foreign keys and exclusion constraints,
  primary-key and unique additions, index and trigger create/drop,
  `REPLICA IDENTITY`, column storage, compression, statistics target
  and options, rules (`CREATE OR REPLACE RULE`, their state and
  comment), and comment changes, including comments on constraints. A
  changed comment on a constraint is set alone, without rebuilding the
  constraint; a constraint that is re-added gets its comment again.
  A changed comment on an index is set alone, too. The indexes of a
  partitioned table and of its partitions change as one group:
  PostgreSQL does not drop an index that is attached to another, and
  dropping the partitioned table's index drops its partitions'. So when
  the definition of an index of the group changes, or an index of the
  group is removed, deploy drops the partitioned table's index, then
  makes all of them again and attaches them. A change to the comment
  only does not rebuild the group.
  Identity columns are added, and their `ALWAYS`/`BY DEFAULT` behavior
  and sequence options changed, with `ALTER COLUMN`; renaming an
  identity's sequence falls back. A `NOT VALID` check, foreign key or
  NOT NULL constraint that the project marks valid is validated with
  `VALIDATE CONSTRAINT`, which avoids the full scan under a heavy lock
  that dropping and re-adding it would take. Dropping a column, changing a column
  type, reordering columns, and partitioning/storage changes fall back
  to drop+recreate.
- **Row-level security** — `ENABLE`/`DISABLE` and `[NO] FORCE ROW
  LEVEL SECURITY`, and one statement per policy, labeled with the
  policy's own name in the script. A new policy is created. A changed
  comment is set with `COMMENT ON POLICY`, and a role change that
  narrows access (fewer roles on a permissive policy, more on a
  restrictive one) with `ALTER POLICY ... TO`. Every other policy
  change drops and re-creates the policy. A table file without
  `row_level_security` or `policies`, such as one pulled before
  pglifecycle modeled them, leaves the table's row security as the
  database has it.
- **Functions and views** — `CREATE OR REPLACE` (a function whose
  return type changed must be dropped first, so it falls back). A
  view's rules are reconciled after it.
- **Sequences** — a single `ALTER SEQUENCE` of the changed options.
- **Domains** — set/drop default; a base-type or constraint change
  falls back.
- **Enum types** — `ALTER TYPE ... ADD VALUE` for appended values;
  reordering or removing values falls back.
- **Extensions** — `ALTER EXTENSION ... UPDATE` / `SET SCHEMA`.
- **Foreign data wrappers** — handler, validator, and `OPTIONS`
  (`ADD`/`SET`/`DROP`) changes, plus comments.
- **Foreign servers** — `VERSION` and `OPTIONS` changes, plus comments;
  a wrapper or `TYPE` change (neither is alterable) falls back, as does
  clearing an existing `VERSION` (it cannot be removed in place).
- **User mappings** — per-server `OPTIONS` changes; a mapping the
  project adds or drops is created or dropped. A `password` the project
  does not carry is left untouched, so a redacted pull (the default)
  never strips a live credential.
- **Foreign tables** — `OPTIONS` changes and comments in place; a server
  or column change falls back to drop+recreate.
- Everything else falls back to drop+recreate.

### Destructive statements and limits

Destructive statements — `DROP` for database-only objects, data-losing
column changes, and every drop+recreate fallback — are excluded from
the script unless `--allow-drop` is given; each exclusion is reported
on stderr and counted in the script header, and `--apply` refuses while
any are pending. Index, trigger, and constraint drops issued while
reconciling a table are *not* gated: they lose no data and the project
is authoritative.

`DROP RULE` is gated although a rule holds no data. A `DO INSTEAD
NOTHING` rule can block writes, so dropping one can let through changes
the database refused before. A project pulled with a version of
pglifecycle that did not model rules has none on any table or view,
and the gate stops a deploy of that project from dropping every rule.

`DROP IDENTITY` is gated although it keeps every row: the sequence goes
with it, so adding the identity back restarts the numbering and
collides with existing keys. A project pulled with a version of
pglifecycle that did not model identity columns has none on any
column, so deploying it asks for exactly this on each one; the gate is
what stops that from stripping them. Pull the project again to record
them.

Row-security reconciliation is gated when it can give a role access to
rows it could not see before: `DISABLE` and `NO FORCE ROW LEVEL
SECURITY`, the drop of a restrictive policy, and every policy change
except a comment or a role change that narrows access. Whether an
edited `USING` or `WITH CHECK` expression allows more rows or fewer
cannot be decided in general, so every such edit is gated. Enabling
or forcing row security, dropping a permissive policy and adding a
policy are always included: a new policy is one the project adds
explicitly, and gating it while `ENABLE` runs would hide every row.

A withheld statement that opens access leaves the database stricter
than the project. Any other withheld policy change is different: the
database keeps the old policy, which can allow more than the project
does. The script header names each one with a `-- WARNING:` line, and
the stderr report says the same.

Ownership is not managed (the script behaves like
`pg_restore --no-owner`), and roles, users, groups, and tablespaces are
skipped entirely — they are cluster-level objects a single-database
dump cannot capture. Aggregates, casts, collations, conversions, event
triggers, publications, text search objects, default privileges,
extended statistics, procedures, operators, operator classes and
families, and access methods are created when
missing but otherwise only existence-checked: `pull` models them, but
`deploy` does not compare their definitions yet, so a changed one is
left as the database has it. An aggregate or procedure is matched by
its name and input types, and an operator by its name and argument
types, so each overload is checked on its own. An operator class or
family is matched by its name and its index method. Text search
objects are checked per schema: when a schema has any text search
object in the database, `deploy` creates none of the project's text
search objects in that schema. Object types `pull` does not yet model
(transforms, subscriptions, …) are handled the same way. Privileges on
created objects are emitted (unless `-x`); privilege changes on objects
that already exist are not yet diffed.

## pull

Create a project from a live database or an existing dump. Entry DDL is
parsed into structured YAML (columns, constraints, indexes, and ACLs as
data), view queries and function bodies are formatted, and child
objects are merged into their owners.

```bash
pglifecycle pull [OPTIONS] DEST
```

| Option | Description |
| --- | --- |
| `-D, --dump FILE` | Use an existing `pg_dump -Fc` file instead of connecting |
| `--no-roles` | Skip cluster role/user extraction (role/user extraction is enabled by default for live connections; always skipped with `--dump`) |
| `--include-password-hashes` | Include role password hashes in users (omitted by default via `pg_dumpall --no-role-passwords`) |
| `--include-mode-headers` | Prefix each generated file with editor mode headers (see below) |
| `-i, --ignore FILE` | File listing project paths to skip writing |
| `--force` | Write to `DEST` even if it already exists |
| `--update` | Merge into an existing project, rewriting only changed files |
| `--prune` | With `--update`, delete files whose objects left the database |
| `--gitkeep` | Create `.gitkeep` files in empty directories |
| `--remove-empty-dirs` | Remove empty directories after generation |
| `--allow-unsupported` | Accept a project that does not reproduce the source database (see below) |
| `--error-file FILE` | Where to record failures and their DDL (default `pglifecycle-errors.log`) |
| `--style STYLE` | libpgfmt style for view/materialized view queries and function bodies (default `pg_dump`) |
| `-T, --exclude-table PATTERN` | Exclude tables/views/sequences matching `PATTERN` (repeatable; ignored with `--dump`) |
| `-N, --exclude-schema PATTERN` | Exclude schemas matching `PATTERN` (repeatable; ignored with `--dump`) |
| `--exclude-extension PATTERN` | Exclude extensions matching `PATTERN` (repeatable; ignored with `--dump`) |

## Unsupported dump entries

`pull` parses every schema entry in the archive into the object model.
An entry it cannot model is written verbatim to `remaining.yaml` at the
project root, and `pull` then **fails**, because the project it produced
would not rebuild the database it came from — and nothing downstream
(`build`, `deploy`, or a review of the YAML) could tell that something
went missing.

```console
$ pglifecycle pull ./project -d mydb
...
error: 3 dump entries could not be modeled (TRANSFORM, SUBSCRIPTION), so the
generated project would not reproduce the source database.
The entries were preserved in ./project/remaining.yaml; re-run with
--allow-unsupported to accept the project as it is.
```

A `COMMENT` entry counts as unmodeled when the model has no place for
it, such as a comment on an object type that `pull` does not model.

The project directory is written either way, so `remaining.yaml` is
there to inspect. `--allow-unsupported` downgrades the failure to a
warning for the cases where an incomplete project is what you want.

`deploy` reports the same condition from the other side: objects in the
database it cannot model are named in a warning and left untouched,
since they cannot be represented in the plan.

`--save-remaining` is accepted and ignored; `remaining.yaml` is now
always written when there is anything to put in it. It holds the only
copy of those entries, so the `--ignore` file cannot hold it back.

The exclude patterns are passed through to `pg_dump` (`--exclude-table`,
`--exclude-schema`, `--exclude-extension`) and use the same pattern
syntax. They apply only when connecting to a database; with `--dump` the
archive is already built, so they are rejected as conflicting.

The `--style` value is one of libpgfmt's styles — `river`, `mozilla`,
`aweber`, `dbt`, `gitlab`, `kickstarter`, `mattmc3`, or `pg_dump` — and
controls only how view/materialized view queries and function bodies are
formatted in the generated project. Note that `deploy` always re-formats the database
side with the default (`pg_dump`) to compare it against the project, so
pulling with a non-default style will make `deploy` report
formatting-only differences for every view and function.

With `--include-mode-headers`, each generated file is prefixed with two
comment lines above the `---` document marker — an Emacs modeline and a
`# pglifecycle: <kind>` type comment (`<kind>` is the object-type noun,
e.g. `table`, `materialized_view`, `function`) that editor extensions
can key off to detect pglifecycle files:

```yaml
# -*- mode: pglifecycle -*-
# pglifecycle: materialized_view
---
name: autoresponder_service_package_info
schema: public
```

Without the flag (the default), files begin directly at the `---`
marker. The same flag is available on `create`.

### Diagnosing parse and format failures

DDL that fails to parse, or SQL that fails to format, is logged and the
offending statement is written to the error report (`--error-file`,
default `pglifecycle-errors.log` in the working directory) alongside its
full DDL, so a failure can be correlated with the exact statement that
produced it. The file is created only when there is something to report.

If `pull` is interrupted with Ctrl-C — for example because the formatter
is stuck on a pathological statement — the statement in flight at that
moment is written to the same report before exiting, turning a hang into
a reproducer.

Connection options mirror the PostgreSQL client tools and honor the
standard `PGHOST`, `PGPORT`, `PGUSER`, and `PGDATABASE` environment
variables:

| Option | Description |
| --- | --- |
| `-d, --dbname NAME` | Database name to connect to |
| `-h, --host HOST` | Server host or socket directory (default `localhost`) |
| `-p, --port PORT` | Server port (default `5432`) |
| `-U, --username NAME` | Username to operate as |
| `-w, --no-password` | Never prompt for a password |
| `-W, --password` | Prompt for a password up front |
| `--role NAME` | Role to assume when connecting |

pglifecycle does its own password prompting rather than letting
`pg_dump`, `pg_dumpall`, and `psql` prompt: they write the prompt to
`/dev/tty`, where the progress bar overwrites it, and the command then
appears to hang with no prompt in sight. The client tools always run
with `-w`, and pglifecycle supplies the password in the environment.

A prompt appears when the server asks for a password that `PGPASSWORD`
or a pgpass file did not supply, and the failed attempt is retried with
it; `-W` prompts up front instead. The progress bars are hidden while
the prompt is on screen. `-w` suppresses prompting altogether, as does a
non-interactive stdin, so scripted runs fail with the error rather than
waiting for input.

DDL options:

| Option | Description |
| --- | --- |
| `-x, --no-privileges` | Do not include GRANT/REVOKE |
| `--no-security-labels` | Do not include security label assignments |
| `--no-tablespaces` | Do not include tablespace assignments |

With `--update`, `DEST` must be an existing project (it must contain
`project.yaml`). The pull is rendered as usual but only files whose
content actually changed are written, so `git diff` afterwards shows
exactly what changed in the database. Files for objects that no longer
exist in the database are reported as warnings and left in place;
`--prune` deletes them instead (confined to the directories `pull`
manages — `dml/` and other project content is never touched). Paths
listed in the `--ignore` file are neither rewritten nor pruned, except
`remaining.yaml`, which the file cannot hold back. Note
that overloaded function files are numbered in dump order
(`name.yaml`, `name_1.yaml`, …), so adding or removing an overload can
renumber a sibling's file.

Cluster roles and users are extracted via `pg_dumpall --roles-only`
whenever `pull` connects to a live database (use `--no-roles` to skip,
and note they cannot be extracted from a `--dump` file). They are
classified when written: a role with the `LOGIN` attribute becomes a
file in `users/`; everything else lands in `roles/`. Roles that appear
only as ACL grantees (such as `PUBLIC`) are written with
`create: false` so `build` defines but never creates them. The
reserved `pg_*` roles are cluster-managed (and uncreatable), so they
are excluded. Password hashes are omitted unless
`--include-password-hashes` is given. Reading hashes requires
`pg_authid`, which managed platforms (e.g. RDS) restrict; when it is
denied, `pull` falls back to a passwordless roles dump (warning that
hashes were unavailable) rather than dropping all roles.

### Foreign data wrappers, servers, and foreign tables

Foreign objects round-trip through `pull` and `build`:

- **Foreign data wrappers** are written into `project.yaml` under
  `foreign_data_wrappers` (alongside extensions and languages), with
  their handler, validator, and options. Extension-owned wrappers (e.g.
  `postgres_fdw`'s own wrapper) are created by their extension and are
  not emitted here.
- **Foreign servers** get one file each in `servers/`, carrying the
  wrapper name, optional `type`/`version`, and connection `options`.
- **User mappings** get one file each in `user_mappings/`, grouping all
  of a user's server mappings. A mapping's `password` option is a
  secret and is **redacted by default**; pass `--include-password-hashes`
  to write it (the same flag that controls role password hashes).
- **Foreign tables** live in `tables/` like ordinary tables, with a
  `server` field and an open `options` map (keys depend on the wrapper —
  e.g. `schema_name`/`table_name` for `postgres_fdw`, `filename`/`format`
  for `file_fdw`). `build` renders them as `CREATE FOREIGN TABLE`.
