"""
Misc Utilities

"""

import re
import typing

NO_QUOTE = re.compile('^[a-z0-9_]+$')


def quote_ident(value: str) -> str:
    """Quote a PostgreSQL identifier (object name, etc)"""
    if NO_QUOTE.search(value):
        return value
    return '"{}"'.format(value.replace('"', '""'))


def postgres_value(value: typing.Any, nested: bool = False) -> str:
    """Return a Postgres value as a string, quoted if required, etc."""
    if isinstance(value, str):
        if "'" in value:
            return f'$${value}$$'
        return f"'{value}'"
    if isinstance(value, list):
        return ('[{}]' if nested else 'ARRAY[{}]').format(
            ', '.join(postgres_value(v, True) for v in value)
        )
    return str(value)


def split_name(value: str) -> tuple[str | None, str]:
    """Take a postgres ident and return the proper namespace & tag value"""
    parts = value.partition('.')
    if (parts[1], parts[2]) == ('', ''):
        return None, value
    return parts[0], parts[2]
