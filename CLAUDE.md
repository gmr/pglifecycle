# pglifecycle

A PostgreSQL schema management tool.

## Development

```bash
./bootstrap          # Full setup: venv, deps, docker, fixtures
ci/test              # Run linting + tests with coverage
```

## Build System

- **pyproject.toml** with hatchling build backend
- **hatch-vcs** for version management from git tags
- **uv** or **pip** for dependency management
- **dependency-groups** for dev/docs extras

## Testing

- **pytest** as the test runner
- **coverage** for code coverage reporting
- Tests live in `tests/`
- Integration tests require PostgreSQL (via Docker Compose)

## Code Style

- **ruff** for linting and formatting
- 79 character line length
- Single quotes
- Pre-commit hooks configured

## Key Directories

- `pglifecycle/` - Main package
- `pglifecycle/schemata/` - YAML schema definitions for PostgreSQL objects
- `tests/` - Test suite
- `fixtures/` - Test database schema
- `bin/` - Utility scripts (fixture data generation)
- `test-project/` - Example project structure
- `ci/` - CI scripts
