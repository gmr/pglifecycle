# pglifecycle

A PostgreSQL schema management tool, being rewritten in Rust. See PLAN.md
for the rewrite plan and phase gates.

## Development

```bash
cargo build          # Build the binary
cargo test           # Run tests
cargo fmt            # Format (max_width 79, see rustfmt.toml)
cargo clippy --all-targets -- -D warnings
```

The Python implementation this replaces is preserved at the
`python-final` tag; reference it via:

```bash
git clone --depth 1 --branch python-final https://github.com/gmr/pglifecycle /tmp/pglifecycle-py
```

It is the parity oracle until the round-trip gates in PLAN.md pass.

## Testing

- Integration tests live in `tests/`
- Round-trip tests require PostgreSQL (via Docker Compose)
- `fixtures/` holds the test database schema

## Key Directories

- `src/` - Rust sources (module map in PLAN.md)
- `schemata/` - JSON-Schema (YAML) definitions for PostgreSQL objects —
  the on-disk project contract, carried over from Python unchanged
- `test-project/` - Example project structure (parity contract)
- `fixtures/` - Test database schema
- `bin/` - Utility scripts (fixture data generation)
