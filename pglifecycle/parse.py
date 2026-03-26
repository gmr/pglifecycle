import logging

import pgparse

from pglifecycle import tokenizer

LOGGER = logging.getLogger(__name__)


def sql(value: str) -> list[dict] | dict:
    """Parse a blob with one or more SQL statements.

    Returns a single dict if the input contains one statement,
    or a list of dicts if there are multiple statements.

    """
    results = [
        tokenizer.from_libpg_query(node) for node in pgparse.parse(value)
    ]
    if len(results) == 1:
        return results[0]
    return results
