"""
Common interface for working with YAML files

"""
import logging
import pathlib

import ruamel.yaml as yaml
from ruamel.yaml import scanner

LOGGER = logging.getLogger(__name__)


def is_yaml(path: pathlib.Path) -> bool:
    """Returns `True` if the file exists and ends with a YAML extension"""
    return (path.is_file()
            and any(path.name.endswith(v) for v in ['.yml', '.yaml']))


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
