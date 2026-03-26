# Contributing

To get setup in the environment and run the tests, take the following steps:

```bash
uv sync
uv run pre-commit install
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Test Coverage

Pull requests that make changes or additions that are not covered by tests
will likely be closed without review.
