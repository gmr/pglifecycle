# pglifecycle

A PostgreSQL schema management tool

[![Crates.io](https://img.shields.io/crates/v/pglifecycle.svg)](https://crates.io/crates/pglifecycle)
[![Testing](https://github.com/gmr/pglifecycle/workflows/Testing/badge.svg?)](https://github.com/gmr/pglifecycle/actions?workflow=Testing)
[![License](https://img.shields.io/github/license/gmr/pglifecycle.svg)](https://github.com/gmr/pglifecycle/blob/main/LICENSE)

pglifecycle keeps your database schema in a version-controlled project
of YAML files — one file per database object, validated against a
JSON-Schema contract — and moves schema between the repository and live
databases:

- **`pull`** reads a live database (or a `pg_dump` archive) and writes
  the project directory
- **`build`** turns the project into a `pg_restore`-compatible custom
  format archive
- **`create`** scaffolds an empty project

## Installation

### Homebrew

```bash
brew install gmr/pglifecycle/pglifecycle
```

### Cargo

```bash
cargo install pglifecycle
```

Prebuilt binaries for Linux and macOS (x86_64 and aarch64) are attached
to each [release](https://github.com/gmr/pglifecycle/releases).

## Usage

```bash
# pull an existing database into a new project
pglifecycle pull -h localhost -d mydb my-project/

# build the project into a restorable archive
pglifecycle build my-project/ mydb.dump
pg_restore -d mydb_copy mydb.dump
```

See the [documentation](https://gmr.github.io/pglifecycle/) for the
full command reference and project format.
