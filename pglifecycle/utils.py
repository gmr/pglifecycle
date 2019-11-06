"""
Misc Utilities

"""
import typing


def quote_ident(value: str) -> str:
    """Quote a PostgreSQL identifier (object name, etc)"""
    return '"{}"'.format(value.replace('"', '""'))


def postgres_value(value: typing.Any, nested: bool = False) -> str:
    """Return a Postgres value as a string, quoted if required, etc."""
    if isinstance(value, str):
        if "'" in value:
            return '$${}$$'.format(value)
        return "'{}'".format(value)
    if isinstance(value, list):
        return ('[{}]' if nested else 'ARRAY[{}]').format(
            ', '.join(postgres_value(v, True) for v in value))
    return str(value)
