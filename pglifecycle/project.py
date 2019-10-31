"""
Core in-memory management of the project

"""
from __future__ import annotations

import dataclasses
import logging
import os
import pathlib
import typing
import weakref

from pglifecycle import constants, models, validation, yaml

LOGGER = logging.getLogger(__name__)

_READ_ORDER = [
    constants.SCHEMA,
    constants.OPERATOR,
    constants.AGGREGATE,
    constants.COLLATION,
    constants.CONVERSION,
    constants.TYPE,
    constants.DOMAIN,
    constants.TABLESPACE,
    constants.TABLE,
    constants.SEQUENCE,
    constants.FUNCTION,
    constants.PROCEDURE,
    constants.VIEW,
    constants.MATERIALIZED_VIEW,
    constants.CAST,
    constants.TEXT_SEARCH_CONFIGURATION,
    constants.TEXT_SEARCH_DICTIONARY,
    constants.FOREIGN_DATA_WRAPPER,
    constants.SERVER,
    constants.EVENT_TRIGGER,
    constants.PUBLICATION,
    constants.SUBSCRIPTION,
    constants.USER_MAPPING
]

_PER_SCHEMA_FILES = [
    constants.CONVERSION,
    constants.OPERATOR,
    constants.TYPE
]

_SCHEMALESS_OBJECTS = [
    constants.FOREIGN_DATA_WRAPPER,
    constants.SCHEMA,
    constants.SERVER
]


class Project:
    """Represents the complete project including all database objects,
    and is a common way to interact with a project.

    :param str name: The name of the project, defaults to `postgres`
    :param str encoding: The database encoding to use, defaults to 'UTF8'
    :param bool stdstrings: Enable/disable stdstrings, defaults to True
    :param str superuser: The name of the superuser, defaults to `postgres`

    """
    def __init__(self,
                 path: os.PathLike,
                 name: str = 'postgres',
                 encoding: str = 'UTF8',
                 stdstrings: bool = True,
                 superuser: str = 'postgres'):
        self.name: str = name
        self.encoding: str = encoding
        self.extensions: typing.List[models.Extension] = []
        self.languages: typing.List[models.Language] = []
        self.path = pathlib.Path(path).absolute()
        self.stdstrings: bool = stdstrings
        self.superuser: str = superuser
        self._load_errors = 0
        self._inventory: dict = {k: {} for k in constants.PATHS.keys()}
        for key in [constants.EXTENSION, constants.PROCEDURAL_LANGUAGE]:
            self._inventory[key] = {}
        self._dependencies = {k: {} for k in self._inventory.keys()}

    def __repr__(self) -> str:
        return '<Project path="{!s}">'.format(self.path)

    def load(self) -> Project:
        """Load the project from the specified project directory

        :raises: RuntimeError

        """
        self._read_project_file()
        for obj_type in _READ_ORDER:
            if obj_type in _PER_SCHEMA_FILES:
                self._read_objects_files(obj_type, models.MAPPINGS[obj_type])
            else:
                schemaless = obj_type in _SCHEMALESS_OBJECTS
                self._read_object_files(
                    obj_type, schemaless, models.MAPPINGS[obj_type])
        if self._load_errors:
            LOGGER.critical('Project load failed with %i errors',
                            self._load_errors)
            raise RuntimeError('Project load failure')
        return self

    def save(self, path: os.PathLike):
        """Save the project to the specified project directory"""
        pass

    def _iterate_files(self, file_type: str, schemaless: bool = False) \
            -> typing.Generator[dict, None, None]:
        """Generator that will iterate over all of the subdirectories and
        files for the specified `file_type`, parsing the YAML and returning
        a tuple of the schema name, object name, and a dict of values from the
        file.

        """
        path = self.path.joinpath(constants.PATHS[file_type])
        if not path.exists():
            LOGGER.warning('No %s file found in project', file_type)
            return
        for child in sorted(path.iterdir(), key=lambda p: str(p)):
            if child.is_dir():
                for s_child in sorted(child.iterdir(), key=lambda p: str(p)):
                    if yaml.is_yaml(s_child):
                        yield self._preprocess_definition(
                            s_child.parent.name, s_child.name.split('.')[0],
                            yaml.load(s_child), schemaless)
            elif yaml.is_yaml(child):
                yield self._preprocess_definition(
                    child.name.split('.')[0], None, yaml.load(child),
                    schemaless)

    @staticmethod
    def _object_name(definition: dict, schemaless: bool = False):
        if schemaless or 'schema' not in definition:
            return definition['name']
        return '{}.{}'.format(definition['schema'], definition['name'])

    def _preprocess_definition(self, schema: str,
                               name: typing.Optional[str],
                               definition: dict,
                               schemaless: bool) -> dict:
        if schema and 'schema' not in definition and not schemaless:
            definition['schema'] = schema
        if name and 'name' not in definition:
            definition['name'] = name
        if 'owner' not in definition:
            definition['owner'] = self.superuser
        return definition

    def _cache_and_remove_dependencies(self, obj_type: str, definition: dict):
        name = self._object_name(definition, 'schema' in definition)
        self._dependencies[obj_type][name] = definition['dependencies']
        del definition['dependencies']

    def _process_object_dependencies(self, definition: dict) \
            -> typing.NoReturn:
        dependencies = []
        for dot, names in definition['dependencies'].items():
            obj_type = constants.OBJ_KEYS[dot]
            for name in names:
                try:
                    dependencies.append(weakref.ref(
                        self._inventory[obj_type][name]))
                except KeyError:
                    LOGGER.error('Reference error for %s %s',
                                 obj_type, name)
                    self._load_errors += 1
        definition['dependencies'] = dependencies

    def _read_object_files(self, obj_type: str, schemaless: bool,
                           model: dataclasses.dataclass) -> typing.NoReturn:
        LOGGER.debug('Reading %s objects', obj_type)
        for definition in self._iterate_files(obj_type, schemaless):
            name = self._object_name(definition)
            if not validation.validate_object(obj_type, name, definition):
                self._load_errors += 1
                continue
            if 'dependencies' in definition:
                self._cache_and_remove_dependencies(obj_type, definition)
            self._inventory[obj_type][name] = model(**definition)

    def _read_objects_files(self, obj_type: str, model: dataclasses.dataclass):
        LOGGER.debug('Reading %s objects', obj_type)
        key = [k for k, v in constants.OBJ_KEYS.items() if v == obj_type][0]
        for definition in self._iterate_files(obj_type):
            if not validation.validate_object(
                    key, definition['schema'], definition):
                self._load_errors += 1
                continue
            for entry in definition.get(key):
                if not validation.validate_object(
                        obj_type,
                        '{}.{}'.format(definition['schema'], entry['name']),
                        entry):
                    self._load_errors += 1
                    continue
                if 'dependencies' in entry:
                    self._cache_and_remove_dependencies(obj_type, definition)
                self._inventory[obj_type][entry['name']] = model(**entry)

    def _read_project_file(self) -> typing.NoReturn:
        LOGGER.info('Loading project from %s', self.path)
        project_file = self.path / 'project.yaml'
        if not project_file.exists():
            raise RuntimeError('Missing project file')
        project = yaml.load(project_file)
        validation.validate_object('project', 'project.yaml', project)
        self.name = project.get('name', self.name)
        self.encoding = project.get('encoding', self.encoding)
        for extension in project.get('extensions'):
            name = self._object_name(extension)
            self._inventory[constants.EXTENSION][name] = \
                models.Extension(**extension)
        for language in project.get('languages'):
            name = self._object_name(language)
            self._inventory[constants.PROCEDURAL_LANGUAGE][name] = \
                models.Language(**language)


def load(path: os.PathLike) -> Project:
    """Load the project from the specified project directory, returning a
    :py:class:`~pglifecycle.project.Project` instance.

    """
    return Project(path).load()
