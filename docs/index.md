# pglifecycle

A PostgreSQL schema management tool.

pglifecycle keeps your database schema in a version-controlled project
of YAML files — one file per database object — and moves schema between
the repository and live databases:

- **`pull`** reads a live database (or a `pg_dump` archive) and writes
  the project directory.
- **`build`** turns the project into a `pg_restore`-compatible custom
  format archive.
- **`create`** scaffolds an empty project.

The project format is validated against JSON-Schema definitions for
every PostgreSQL object type, so the repository is the contract: tables,
columns, constraints, indexes, functions, ACLs, and roles are structured
data, not opaque SQL.

## Installation

### Homebrew (macOS / Linux)

pglifecycle is published in the shared [gmr/postgres
tap](https://github.com/gmr/homebrew-postgres):

```bash
brew tap gmr/postgres
brew install pglifecycle
```

!!! note
    Homebrew 6.0 added [tap trust](https://docs.brew.sh/Tap-Trust), and
    some versions fail to install third-party taps inside the build
    sandbox (the error mentions `build.rb ... exited with 1`). If you
    hit this, trust the formula first with
    `brew trust --formula gmr/postgres/pglifecycle`, or set
    `HOMEBREW_NO_REQUIRE_TAP_TRUST=1` for the install.

### Cargo

```bash
cargo install pglifecycle
```

### Release binaries

Prebuilt binaries for Linux and macOS (x86_64 and aarch64) are attached
to each [GitHub release](https://github.com/gmr/pglifecycle/releases).

## Quick start

Pull an existing database into a new project:

```bash
pglifecycle pull -h localhost -d mydb my-project/
```

Build the project into a restorable archive:

```bash
pglifecycle build my-project/ mydb.dump
pg_restore -d mydb_copy mydb.dump
```

Or start from scratch:

```bash
pglifecycle create my-project/
```

See [Commands](commands.md) for the full reference and
[Project Format](project-format.md) for the directory layout.
