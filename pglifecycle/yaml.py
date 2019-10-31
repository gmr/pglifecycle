"""
Common interface for working with YAML files

"""
import logging
import pathlib
import typing

import ruamel.yaml as yaml
from ruamel.yaml import scalarstring, scanner

LOGGER = logging.getLogger(__name__)


def is_yaml(path: pathlib.Path) -> bool:
    """Returns `True` if the file exists and ends with a YAML extension"""
    return (path.is_file()
            and (path.name.endswith('.yml') or path.name.endswith('.yaml')))


def load(path: pathlib.Path) -> dict:
    """Load a YAML file, returning its contents.

    :raises: RuntimeError

    """
    with path.open() as handle:
        try:
            return yaml.safe_load(handle)
        except scanner.ScannerError as error:
            LOGGER.critical('Failed to parse YAML from %s: %s',
                            path, error)
            raise RuntimeError('YAML parse failure')


def save(path: pathlib.Path, data: dict) -> typing.NoReturn:
    """Save a YAML file, overwriting flow styles

    :param path: The relative path to the file
    :param dict data: The data for the file

    """
    with path.open('w') as handle:
        dump(handle, data)


def dump(handle: typing.TextIO, data: dict) -> typing.NoReturn:
    """Save the data in YAML format to the IO handle."""
    handle.write('---\n')
    yml = yaml.YAML()
    yml.default_flow_style = False
    yml.indent(mapping=2, sequence=4, offset=2)
    yml.dump(_yaml_reformat(data), handle)


def _yaml_reformat(data: typing.Any) -> typing.Any:
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str) and '\n' in value:
                data[key] = scalarstring.PreservedScalarString(value)
            if isinstance(value, list):
                data[key] = _yaml_reformat(value)
    elif isinstance(data, list):
        data = [_yaml_reformat(i) for i in data]
    return data
