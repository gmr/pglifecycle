"""
Misc Utilities

"""


def quote_ident(value: str) -> str:
    """Quote a PostgreSQL identifier (object name, etc)"""
    return '"{}"'.format(value.replace('"', '""'))
