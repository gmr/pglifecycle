"""
Core in-memory management of the project

"""
from __future__ import annotations

import dataclasses
import logging
import os
import pathlib
import typing

import pgdumplib

from pglifecycle import constants as const
from pglifecycle import models, utils, validation, yaml


LOGGER = logging.getLogger(__name__)

_READ_ORDER = [
    const.SCHEMA,
    const.OPERATOR,
    const.AGGREGATE,
    const.COLLATION,
    const.CONVERSION,
    const.TYPE,
    const.DOMAIN,
    const.TABLESPACE,
    const.TABLE,
    const.SEQUENCE,
    const.FUNCTION,
    const.PROCEDURE,
    const.VIEW,
    const.MATERIALIZED_VIEW,
    # const.CAST,
    # const.TEXT_SEARCH_CONFIGURATION,
    # const.TEXT_SEARCH_DICTIONARY,
    # const.FOREIGN_DATA_WRAPPER,
    # const.SERVER,
    # const.EVENT_TRIGGER,
    # const.PUBLICATION,
    # const.SUBSCRIPTION,
    # const.USER_MAPPING
]

_PER_SCHEMA_FILES = [
    const.CONVERSION,
    const.OPERATOR,
    const. TYPE
]

_SCHEMALESS_OBJECTS = [
    const.FOREIGN_DATA_WRAPPER,
    const.SCHEMA,
    const.SERVER
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
        self._dump = None
        self._load_errors = 0
        self._inv: dict = {k: {} for k in const.PATHS.keys()}
        for key in [const.EXTENSION, const.PROCEDURAL_LANGUAGE]:
            self._inv[key] = {}
        self._deps = {k: {} for k in self._inv.keys()}

    def __repr__(self) -> str:
        return '<Project path="{!s}">'.format(self.path)

    def build(self, path: os.PathLike) -> typing.NoReturn:
        """Build the project into a pg_restore -Fc compatible archive"""
        LOGGER.info('Saving build artifact to %s', path)
        self._dump = pgdumplib.new(self.name, self.encoding)
        self._dump_schemas()
        self._dump_extensions()
        self._dump_languages()

        self._dump.save(path)
        LOGGER.info('Build artifact saved with %i entries',
                    len(self._dump.entries))

    def load(self) -> Project:
        """Load the project from the specified project directory

        :raises: RuntimeError

        """
        self._read_project_file()
        for ot in _READ_ORDER:
            if ot in _PER_SCHEMA_FILES:
                self._read_objects_files(ot, models.MAPPINGS[ot])
            else:
                schemaless = ot in _SCHEMALESS_OBJECTS
                self._read_object_files(ot, schemaless, models.MAPPINGS[ot])
        self._validate_dependencies()
        if self._load_errors:
            LOGGER.critical('Project load failed with %i errors',
                            self._load_errors)
            raise RuntimeError('Project load failure')
        LOGGER.info('Project loaded')
        return self

    def save(self, path: os.PathLike) -> typing.NoReturn:
        """Save the project to the specified project directory"""
        pass

    def _cache_and_remove_dependencies(self, obj_type: str, name: str,
                                       defn: dict) -> typing.NoReturn:
        self._deps[obj_type][name] = defn['dependencies']
        del defn['dependencies']

    def _dump_extensions(self) -> typing.NoReturn:
        for name in self._inv[const.EXTENSION]:
            sql = ['CREATE EXTENSION IF NOT EXISTS',
                   utils.quote_ident(name)]
            if any([self._inv[const.EXTENSION][name].schema,
                    self._inv[const.EXTENSION][name].version]):
                sql.append('WITH')
            if self._inv[const.EXTENSION][name].schema:
                sql.append('SCHEMA')
                sql.append(utils.quote_ident(
                    self._inv[const.EXTENSION][name].schema))
            if self._inv[const.EXTENSION][name].version:
                sql.append('VERSION')
                sql.append(utils.quote_ident(
                    self._inv[const.EXTENSION][name].version))
            if self._inv[const.EXTENSION][name].cascade:
                sql.append('CASCADE')
            self._dump.add_entry(
                desc=const.EXTENSION,
                namespace=self._inv[const.EXTENSION][name].schema,
                tag=name, owner=self.superuser,
                defn='{};\n'.format(' '.join(sql)))

    def _dump_languages(self) -> typing.NoReturn:
        for name in self._inv[const.PROCEDURAL_LANGUAGE]:
            sql = ['CREATE']
            if self._inv[const.PROCEDURAL_LANGUAGE][name].replace:
                sql.append('OR REPLACE')
            if self._inv[const.PROCEDURAL_LANGUAGE][name].trusted:
                sql.append('TRUSTED')
            sql.append('LANGUAGE')
            sql.append(name)
            if self._inv[const.PROCEDURAL_LANGUAGE][name].handler:
                sql.append('HANDLER')
                sql.append(self._inv[const.PROCEDURAL_LANGUAGE][name].handler)
            if self._inv[const.PROCEDURAL_LANGUAGE][name].inline_handler:
                sql.append('INLINE')
                sql.append(
                    self._inv[const.PROCEDURAL_LANGUAGE][name].inline_handler)
            if self._inv[const.PROCEDURAL_LANGUAGE][name].validator:
                sql.append('VALIDATOR')
                sql.append(
                    self._inv[const.PROCEDURAL_LANGUAGE][name].validator)
            self._dump.add_entry(
                desc=const.PROCEDURAL_LANGUAGE, tag=name, owner=self.superuser,
                defn='{};\n'.format(' '.join(sql)))

    def _dump_schemas(self) -> typing.NoReturn:
        for name in self._inv[const.SCHEMA]:
            sql = ['CREATE SCHEMA IF NOT EXISTS', utils.quote_ident(name)]
            if self._inv[const.SCHEMA][name].authorization:
                sql.append('AUTHORIZATION')
                sql.append(utils.quote_ident(
                    self._inv[const.SCHEMA][name].authorization))
            self._dump.add_entry(
                desc=const.SCHEMA, tag=name,
                owner=self._inv[const.SCHEMA][name].owner,
                defn='{};\n'.format(' '.join(sql)))

    def _iterate_files(self, file_type: str, schemaless: bool = False) \
            -> typing.Generator[dict, None, None]:
        """Generator that will iterate over all of the subdirectories and
        files for the specified `file_type`, parsing the YAML and returning
        a tuple of the schema name, object name, and a dict of values from the
        file.

        """
        path = self.path.joinpath(const.PATHS[file_type])
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

    def _read_object_files(self, obj_type: str, schemaless: bool,
                           model: dataclasses.dataclass) -> typing.NoReturn:
        LOGGER.debug('Reading %s objects', obj_type)
        for defn in self._iterate_files(obj_type, schemaless):
            name = self._object_name(defn)
            if not validation.validate_object(obj_type, name, defn):
                self._load_errors += 1
                continue
            if 'dependencies' in defn:
                self._cache_and_remove_dependencies(obj_type, name, defn)
            self._inv[obj_type][name] = model(**defn)

    def _read_objects_files(self, obj_type: str, model: dataclasses.dataclass):
        LOGGER.debug('Reading %s objects', obj_type)
        key = [k for k, v in const.OBJ_KEYS.items() if v == obj_type][0]
        for defn in self._iterate_files(obj_type):
            if not validation.validate_object(key, defn['schema'], defn):
                self._load_errors += 1
                continue
            for entry in defn.get(key):
                name = self._object_name(entry)
                if not validation.validate_object(obj_type, name, entry):
                    self._load_errors += 1
                    continue
                if 'dependencies' in entry:
                    self._cache_and_remove_dependencies(obj_type, name, defn)
                self._inv[obj_type][name] = model(**entry)

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
            self._inv[const.EXTENSION][name] = \
                models.Extension(**extension)
        for language in project.get('languages'):
            name = self._object_name(language)
            self._inv[const.PROCEDURAL_LANGUAGE][name] = \
                models.Language(**language)

    def _validate_dependencies(self) -> typing.NoReturn:
        LOGGER.debug('Validating dependencies')
        count = 0
        for ot in self._deps.keys():
            for name in self._deps[ot].keys():
                for obj_type, dnames in self._deps[ot][name].items():
                    dot = const.OBJ_KEYS[obj_type]
                    for dname in sorted(set(dnames)):
                        if dname not in self._inv[dot]:
                            LOGGER.error('Reference error for %s %s',
                                         dot, dname)
                            self._load_errors += 1
                        count += 1
        LOGGER.debug('Validated %i dependencies', count)


def load(path: os.PathLike) -> Project:
    """Load the project from the specified project directory, returning a
    :py:class:`~pglifecycle.project.Project` instance.

    """
    return Project(path).load()
