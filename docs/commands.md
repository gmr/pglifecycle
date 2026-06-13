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

## build

Generate a `pg_restore`-compatible archive from a project. The project
is loaded and validated against the JSON-Schema contract before the
archive is written; entries are ordered with pg_dump's weighted
topological sort.

```bash
pglifecycle build PROJECT DEST
```

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
| `-r, --extract-roles` | Extract roles and users via `pg_dumpall --roles-only` |
| `-i, --ignore FILE` | File listing project paths to skip writing |
| `--force` | Write to `DEST` even if it already exists |
| `--update` | Merge into an existing project, rewriting only changed files |
| `--prune` | With `--update`, delete files whose objects left the database |
| `--gitkeep` | Create `.gitkeep` files in empty directories |
| `--remove-empty-dirs` | Remove empty directories after generation |
| `--save-remaining` | Save unprocessed dump entries to `remaining.yaml` |

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
| `-W, --password` | Force a password prompt |
| `--role NAME` | Role to assume when connecting |

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
listed in the `--ignore` file are neither rewritten nor pruned. Note
that overloaded function files are numbered in dump order
(`name.yaml`, `name_1.yaml`, …), so adding or removing an overload can
renumber a sibling's file.

Roles are classified when written: a role with a password or
`VALID UNTIL` becomes a file in `users/`; everything else lands in
`roles/`. Roles that appear only as ACL grantees (such as `PUBLIC`)
are written with `create: false` so `build` defines but never creates
them.
