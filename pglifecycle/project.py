"""
Core in-memory management of the project

"""
from __future__ import annotations

import collections
import dataclasses
import logging
import os
import pathlib
import typing

import pgdumplib

from pglifecycle import constants as const
from pglifecycle import models, utils, validation, yaml

LOGGER = logging.getLogger(__name__)

_PendingDependency = collections.namedtuple(
    'PendingDependency', ['dump_id', 'parent_desc', 'parent_name'])


class Project:
    """Represents the complete project including all database objects,
    and is a common way to interact with a project.

    :param str name: The name of the project, defaults to `postgres`
    :param str encoding: The database encoding to use, defaults to 'UTF8'
    :param bool stdstrings: Enable/disable stdstrings, defaults to True
    :param str superuser: The name of the superuser, defaults to `postgres`

    """
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
        const.CAST,
        const.TEXT_SEARCH,
        # const.FOREIGN_DATA_WRAPPER,
        # const.SERVER,
        # const.EVENT_TRIGGER,
        # const.PUBLICATION,
        # const.SUBSCRIPTION,
        # const.USER_MAPPING
    ]

    _PER_SCHEMA_FILES = [
        const.CAST,
        const.CONVERSION,
        const.OPERATOR,
        const.TEXT_SEARCH,
        const.TYPE
    ]

    _SCHEMALESS_OBJECTS = [
        const.FOREIGN_DATA_WRAPPER,
        const.SCHEMA,
        const.SERVER
    ]

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
        self._pending_deps: typing.List[_PendingDependency] = []

    def __repr__(self) -> str:
        return '<Project path="{!s}">'.format(self.path)

    def build(self, path: os.PathLike) -> typing.NoReturn:
        """Build the project into a pg_restore -Fc compatible archive"""
        LOGGER.info('Saving build artifact to %s', path)
        self._dump = pgdumplib.new(self.name, self.encoding)
        self._dump_schemas()
        self._dump_extensions()
        self._dump_languages()
        self._dump_tables()
        self._dump.save(path)
        LOGGER.info('Build artifact saved with %i entries',
                    len(self._dump.entries))

    def load(self) -> Project:
        """Load the project from the specified project directory

        :raises: RuntimeError

        """
        self._read_project_file()
        for ot in self._READ_ORDER:
            if ot in self._PER_SCHEMA_FILES:
                self._read_objects_files(ot, models.MAPPINGS[ot])
            else:
                schemaless = ot in self._SCHEMALESS_OBJECTS
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

    def _add_comment_to_dump(self, obj_type: str, schema: str, name: str,
                             owner: str, parent: str,
                             comment: str) -> typing.NoReturn:
        sql = ['COMMENT ON', obj_type]
        if obj_type == const.SCHEMA:
            sql.append('{}.{}'.format(schema, name))
        elif obj_type in {const.CONSTRAINT, const.POLICY,
                          const.RULE, const.TRIGGER}:
            sql.append('{}.{}'.format(schema, name))
            sql.append('ON')
            sql.append(parent)
        else:
            sql.append(parent)
        sql.append('IS')
        sql.append('$${}$$;\n'.format(comment))
        entry = self._dump.add_entry(
            const.COMMENT, schema, name, owner, ' '.join(sql))
        self._pending_deps.append(
            _PendingDependency(entry.dump_id, obj_type, parent))

    @staticmethod
    def _build_column_definition(defn: dict) -> models.Column:
        if 'generated' in defn:
            defn['generated'] = models.ColumnGenerated(**defn['generated'])
        return models.Column(**defn)

    def _build_create_table_stmt(self, name: str) -> str:
        sql = ['CREATE']
        if self._inv[const.TABLE][name].unlogged:
            sql.append('UNLOGGED')
        sql.append('TABLE')
        sql.append(name)
        sql.append('(')
        if self._inv[const.TABLE][name].like_table:
            sql.append('LIKE')
            sql.append(self._inv[const.TABLE][name].name)
            for field in [field for field in
                          self._inv[const.TABLE][name].like_table.fields()
                          if field.startswith('include_')
                          and getattr(
                              self._inv[const.TABLE][name].like_table, field)
                          is not None]:
                sql.append('INCLUDING' if getattr(
                    self._inv[const.TABLE][name], field) else 'EXCLUDING')
                sql.append(field)
        else:
            inner_sql = []
            if self._inv[const.TABLE][name].columns:
                for col in self._inv[const.TABLE][name].columns:
                    column = [col.name or col.expression, col.data_type]
                    if col.collation:
                        column.append('COLLATION')
                        column.append(col.collation)
                    if not col.nullable:
                        column.append('NOT NULL')
                    if col.check_constraint:
                        column.append('CHECK')
                        column.append(col.check_constraint)
                    if col.default:
                        column.append('DEFAULT')
                        column.append(utils.postgres_value(col.default))
                    if col.generated and col.expression:
                        column.append('GENERATED ALWAYS AS')
                        column.append(col.expression)
                        column.append('STORED')
                    elif col.generated and col.sequence:
                        column.append('GENERATED')
                        column.append(col.sequence_behavior)
                        column.append('AS IDENTITY')
                        column.append(col.expression)
                    inner_sql.append(' '.join(column))
                    if col.comment:
                        self._add_comment_to_dump(
                            const.TABLE,
                            self._inv[const.TABLE][name].schema,
                            '{}.{}'.format(
                                self._inv[const.TABLE][name].name, col.name),
                            self._inv[const.TABLE][name].owner, name,
                            col.comment)

            for item in self._inv[const.TABLE][name].unique_constraints or []:
                inner_sql.append(self._format_sql_constraint('UNIQUE', item))
            if self._inv[const.TABLE][name].primary_key:
                inner_sql.append(self._format_sql_constraint(
                    'PRIMARY_KEY', self._inv[const.TABLE][name].primary_key))
            for fk in self._inv[const.TABLE][name].foreign_keys or []:
                fk_sql = ['FOREIGN KEY ({})'.format(', '.join(fk.columns)),
                          'REFERENCES',
                          fk.references.name,
                          '({})'.format(', '.join(fk.references.columns))]
                if fk.match_type:
                    fk_sql.append('MATCH')
                    fk_sql.append(fk.match_type)
                if fk.on_delete != 'NO ACTION':
                    fk_sql.append('ON_DELETE')
                    fk_sql.append(fk.on_delete)
                if fk.on_update != 'NO ACTION':
                    fk_sql.append('ON_UPDATE')
                    fk_sql.append(fk.on_update)
                if fk.deferrable:
                    fk_sql.append('DEFERRABLE')
                if fk.initially_deferred:
                    fk_sql.append('INITIALLY DEFERRED')
                inner_sql.append(' '.join(fk_sql))
            sql.append(', '.join(inner_sql))
        sql.append(')')
        if self._inv[const.TABLE][name].parents:
            sql.append(', '.join(self._inv[const.TABLE][name].parents))
        if self._inv[const.TABLE][name].partition:
            sql.append('PARTITION BY')
            sql.append(self._inv[const.TABLE][name].partition.type)
            sql.append('(')
            columns = []
            for col in self._inv[const.TABLE][name].partition.columns:
                column = [col.name or col.expression]
                if col.collation:
                    column.append('COLLATION')
                    column.append(col.collation)
                if col.opclass:
                    column.append(col.opclass)
            sql.append(', '.join(columns))
            sql.append(')')
        if self._inv[const.TABLE][name].access_method:
            sql.append('USING')
            sql.append(self._inv[const.TABLE][name].access_method)
        if self._inv[const.TABLE][name].storage_parameters:
            sql.append('WITH')
            params = []
            for key, value in self._inv[
                    const.TABLE][name].storage_parameters.items():
                params.append('{}={}'.format(key, value))
            sql.append(', '.join(params))
        if self._inv[const.TABLE][name].tablespace:
            sql.append('TABLESPACE')
            sql.append(self._inv[const.TABLE][name].tablespace)
        return ' '.join(sql)

    @staticmethod
    def _build_fk_definition(defn: dict) -> models.ForeignKey:
        defn['references'] = models.ForeignKeyReference(**defn['references'])
        return models.ForeignKey(**defn)

    @staticmethod
    def _build_partition_column(defn: typing.Union[dict, str]) \
            -> models.TablePartitionColumn:
        if isinstance(defn, dict):
            return models.TablePartitionColumn(**defn)
        return models.TablePartitionColumn(name=defn)

    @staticmethod
    def _build_index_definition(defn: dict) -> models.Index:
        defn['columns'] = [models.IndexColumn(**c) for c in defn['columns']]
        return models.Index(**defn)

    def _build_table_definition(self, defn: dict) -> models.Table:
        if 'columns' in defn:
            defn['columns'] = [self._build_column_definition(c)
                               for c in defn['columns']]
        if 'indexes' in defn:
            defn['indexes'] = [self._build_index_definition(i)
                               for i in defn['indexes']]
        if 'like_table' in defn:
            defn['like_table'] = models.LikeTable(**defn['like_table'])
        if 'check_constraints' in defn:
            defn['check_constraints'] = [models.CheckConstraint(**cc)
                                         for cc in defn['check_constraints']]
        for constraint in ['primary_key', 'unique_constraints']:
            if constraint in defn:
                value = []
                for item in defn[constraint]:
                    if isinstance(item, dict):
                        value.append(models.ConstraintColumns(**item))
                    elif isinstance(item, list):
                        value.append(models.ConstraintColumns(columns=item))
                defn[constraint] = value
        if 'triggers' in defn:
            defn['triggers'] = [models.Trigger(**t) for t in defn['triggers']]
        if 'partition' in defn:
            defn['partition']['columns'] = [
                self._build_partition_column(c)
                for c in defn['partition']['columns']]
            defn['partition'] = models.TablePartitionBehavior(
                **defn['partition'])
        if 'partitions' in defn:
            defn['partitions'] = [models.TablePartition(**p)
                                  for p in defn['partitions']]
        if 'foreign_keys' in defn:
            defn['foreign_keys'] = [self._build_fk_definition(fk)
                                    for fk in defn['foreign_keys']]
        return models.Table(**defn)

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
                tag=self._inv[const.EXTENSION][name].name,
                owner=self.superuser,
                defn='{};\n'.format(' '.join(sql)))

    def _dump_index(self, index: models.Index, schema: str, owner: str,
                    parent: str) -> typing.NoReturn:
        sql = ['CREATE UNIQUE INDEX'] if index.unique else ['CREATE INDEX']
        sql.append(index.name)
        sql.append('ON')
        if index.recurse is False:
            sql.append('ONLY')
        sql.append(parent)
        if index.method:
            sql.append('USING')
            sql.append(index.method)
        sql.append('(')
        columns = []
        for col in index.columns:
            column = [col.name or col.expression]
            if col.collation:
                column.append('COLLATION')
                column.append(col.collation)
            if col.opclass:
                column.append(col.opclass)
            if col.direction:
                column.append(col.direction)
            if col.null_placement:
                column.append('NULLS')
                column.append(col.null_placement)
        sql.append(', '.join(columns))
        sql.append(')')
        if index.include:
            sql.append('INCLUDE ({})'.format(', '.join(index.include)))
        if index.storage_parameters:
            sql.append('WITH')
            sp_sql = []
            for key, value in index.storage_parameters.items():
                sp_sql.append('{}={}'.format(key, value))
            sql.append(', '.join(sp_sql))
        if index.tablespace:
            sql.append('TABLESPACE')
            sql.append(index.tablespace)
        if index.where:
            sql.append('WHERE')
            sql.append(index.where)
        entry = self._dump.add_entry(
            desc=const.INDEX,
            namespace=schema,
            tablespace=index.tablespace,
            tag=index.name,
            owner=owner,
            defn='{};\n'.format(' '.join(sql)))
        self._pending_deps.append(
            _PendingDependency(entry.dump_id, const.TABLE, parent))
        if index.comment:
            self._add_comment_to_dump(
                const.INDEX, schema, index.name, owner, parent, index.comment)

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
                desc=const.PROCEDURAL_LANGUAGE,
                tag=self._inv[const.PROCEDURAL_LANGUAGE][name].name,
                owner=self.superuser,
                defn='{};\n'.format(' '.join(sql)))

    def _dump_schemas(self) -> typing.NoReturn:
        for name in self._inv[const.SCHEMA]:
            sql = ['CREATE SCHEMA IF NOT EXISTS', utils.quote_ident(name)]
            if self._inv[const.SCHEMA][name].authorization:
                sql.append('AUTHORIZATION')
                sql.append(utils.quote_ident(
                    self._inv[const.SCHEMA][name].authorization))
            self._dump.add_entry(
                desc=const.SCHEMA,
                tag=self._inv[const.SCHEMA][name].name,
                owner=self._inv[const.SCHEMA][name].owner,
                defn='{};\n'.format(' '.join(sql)))

    def _dump_tables(self) -> typing.NoReturn:
        for name in self._inv[const.TABLE]:
            self._dump_table(name)

    def _dump_table(self, name):
        self._dump.add_entry(
            const.TABLE,
            self._inv[const.TABLE][name].schema,
            self._inv[const.TABLE][name].name,
            self._inv[const.TABLE][name].owner,
            (self._inv[const.TABLE][name].sql
             if self._inv[const.TABLE][name].sql
             else self._build_create_table_stmt(name)),
            None, None,
            [], self._inv[const.TABLE][name].tablespace)
        if self._inv[const.TABLE][name].comment:
            self._add_comment_to_dump(
                const.TABLE, self._inv[const.TABLE][name].schema,
                self._inv[const.TABLE][name].name,
                self._inv[const.TABLE][name].owner,
                name, self._inv[const.TABLE][name].comment)
        for index in self._inv[const.TABLE][name].indexes or []:
            self._dump_index(index, self._inv[const.TABLE][name].schema,
                             self._inv[const.TABLE][name].owner, name)
        for trigger in self._inv[const.TABLE][name].triggers or []:
            self._dump_trigger(trigger, self._inv[const.TABLE][name].schema,
                               self._inv[const.TABLE][name].owner, name)

    def _dump_trigger(self, trigger: models.Trigger, schema: str, owner: str,
                      parent: str) -> typing.NoReturn:
        defn = trigger.sql
        if not trigger.sql:
            sql = ['CREATE TRIGGER', trigger.name, trigger.when]
            sql.append(' OR '.join(trigger.events))
            sql.append('ON')
            sql.append(parent)
            if trigger.for_each:
                sql.append('FOR EACH')
                sql.append(trigger.for_each)
            if trigger.condition:
                sql.append('WHEN')
                sql.append(trigger.condition)
            sql.append('EXECUTE FUNCTION')
            sql.append(trigger.function)
            if trigger.arguments:
                sql.append('({})'.format(
                    ', '.join([str(a) for a in trigger.arguments])))
            defn = '{}\n'.format(' '.join(sql))
        entry = self._dump.add_entry(
            const.TRIGGER, schema, trigger.name, owner, defn)
        self._pending_deps.append(
            _PendingDependency(entry.dump_id, const.TABLE, parent))
        if trigger.comment:
            self._add_comment_to_dump(
                const.TRIGGER, schema, trigger.name, owner,
                parent, trigger.comment)

    @staticmethod
    def _format_sql_constraint(constraint_type: str,
                               constraint: models.ConstraintColumns) -> str:
        sql = ['{} ({})'.format(constraint_type,
                                ', '.join(constraint.columns))]
        if constraint.include:
            sql.append(', '.join(constraint.include))
        return ' '.join(sql)

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
            if obj_type == const.TABLE:
                self._inv[obj_type][name] = self._build_table_definition(defn)
            else:
                self._inv[obj_type][name] = model(**defn)

    def _read_objects_files(self, obj_type: str, model: dataclasses.dataclass):
        LOGGER.debug('Reading %s objects', obj_type)
        key = [k for k, v in const.OBJ_KEYS.items() if v == obj_type][0]
        for defn in self._iterate_files(obj_type):
            if not validation.validate_object(key, defn['schema'], defn):
                self._load_errors += 1
                continue
            if obj_type == const.TEXT_SEARCH:
                self._read_text_search_definition(defn)
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

    @staticmethod
    def _read_text_search_definition(defn: dict) -> typing.NoReturn:
        if defn.get('sql'):
            return models.TextSearch(defn['schema'], defn['sql'])
        configs, dicts, parsers, templates = None, None, None, None
        if defn.get('configurations'):
            configs = [models.TextSearchConfig(**c)
                       for c in defn['configurations']]
        if defn.get('dictionaries'):
            dicts = [models.TextSearchDict(**d)
                     for d in defn['dictionaries']]
        if defn.get('parsers'):
            parsers = [models.TextSearchParser(**p)
                       for p in defn['parsers']]
        if defn.get('templates'):
            templates = [models.TextSearchTemplate(**t)
                         for t in defn['templates']]
        return models.TextSearch(
            defn['schema'],
            configurations=configs,
            dictionaries=dicts,
            parsers=parsers,
            templates=templates)

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
