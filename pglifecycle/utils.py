"""
Misc Utilities

"""


def quote_ident(value: str) -> str:
    """Quote a PostgreSQL identifier"""
    return '"{}"'.format(value.replace('"', '""'))
