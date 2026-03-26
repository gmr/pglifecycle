# Contributing

To get setup in the environment and run the tests, take the following steps:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pre-commit install

pytest
ruff check .
ruff format --check .
```

## Test Coverage

Pull requests that make changes or additions that are not covered by tests
will likely be closed without review.
