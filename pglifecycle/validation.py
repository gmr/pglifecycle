"""
Data validation using bundled JSON-Schema files

"""
import functools
import logging
import pathlib

import jsonschema
from jsonschema import exceptions
import pkg_resources

from pglifecycle import yaml

LOGGER = logging.getLogger(__name__)


def validate_object(obj_type: str, name: str, data: dict) -> bool:
    """Validate a data object using JSON-Schema"""
    schema = _load_schemata(obj_type.lower())

    # import json
    # with open('{}.json'.format(obj_type), 'w') as handle:
    #     json.dump(schema, handle, indent=2)

    try:
        jsonschema.validate(data, schema)
    except exceptions.ValidationError as error:
        LOGGER.critical('Validation error for %s %s: %s for %r: %s',
                        obj_type, name, error.message,
                        error.path[0] if error.path
                        else error.absolute_schema_path[0],
                        error.instance)
        return False
    return True


@functools.lru_cache(maxsize=64)
def _load_schemata(obj_type: str) -> dict:
    """Load the schemata from the package, returning merged results of
    other schema files if referenced in the file loaded.

    :raises: FileNotFoundError

    """
    schema_path = pathlib.Path(pkg_resources.resource_filename(
        'pglifecycle', 'schemata/{}.yml'.format(obj_type).replace(' ', '_')))
    if not schema_path.exists():
        raise FileNotFoundError(
            'Schema file not found for object type {!r}'.format(obj_type))
    return _preprocess(yaml.load(schema_path))


def _preprocess(schema: dict) -> dict:
    """Merge in other schemas within the package if the `$package_schema` key
    is found.

    """
    schema_out = {}
    for key, value in [(k, v) for k, v in schema.items()]:
        if key == '$package_schema':
            schema_out.update(_load_schemata(value))
        elif isinstance(value, dict):
            schema_out[key] = _preprocess(value)
        elif isinstance(value, list):
            schema_out[key] = [_preprocess(v) if isinstance(v, dict) else v
                               for v in value]
        else:
            schema_out[key] = value
    return schema_out
