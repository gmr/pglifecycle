import logging
import typing

import pgparse

from pglifecycle import tokenizer

LOGGER = logging.getLogger(__name__)


def sql(value: str) -> typing.Generator[dict, None, None]:
    """Parse a blob with one or more SQL statements"""
    for node in pgparse.parse(value):
        yield tokenizer.from_libpg_query(node)
