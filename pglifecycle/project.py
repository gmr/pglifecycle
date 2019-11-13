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
        const.VIEW,
        const.MATERIALIZED_VIEW,
        const.CAST,
        const.TEXT_SEARCH,
        const.SERVER,
        const.EVENT_TRIGGER,
        const.PUBLICATION,
        const.SUBSCRIPTION,
        const.USER_MAPPING
    ]

    _PER_SCHEMA_FILES = [
        const.CAST,
        const.CONVERSION,
        const.OPERATOR,
        const.TEXT_SEARCH,
        const.TYPE
    ]

    _OWNERLESS = [
        const.GROUP,
        const.PUBLICATION,
        const.ROLE,
        const.SERVER,
        const.SUBSCRIPTION,
        const.TEXT_SEARCH,
        const.USER,
        const.USER_MAPPING
    ]

    _SCHEMALESS = [
        const.GROUP,
        const.PUBLICATION,
        const.ROLE,
        const.SCHEMA,
        const.SERVER,
        const.SUBSCRIPTION,
        const.TABLESPACE,
        const.USER,
        const.USER_MAPPING
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
        self.fdws: typing.List[models.ForeignDataWrapper] = []
        self.languages: typing.List[models.Language] = []
        self.path = pathlib.Path(path).absolute()
        self.stdstrings: bool = stdstrings
        self.superuser: str = superuser
        self._dump = None
        self._load_errors = 0
        self._inv: dict = {k: {} for k in const.PATHS.keys()}
        for key in [const.EXTENSION,
                    const.FOREIGN_DATA_WRAPPER,
                    const.PROCEDURAL_LANGUAGE]:
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
        self._dump_foreign_data_wrappers()
        self._dump_languages()
        self._dump_operators()
        self._dump_aggregates()
        self._dump_collations()
        self._dump_conversions()
        self._dump_types()
        self._dump_domains()
        self._dump_tablespaces()
        self._dump_tables()
        self._dump_sequences()
        self._dump_functions()
        self._dump_views()
        self._dump_materialized_views()
        self._dump_casts()

        self._dump.save(path)
        LOGGER.info('Build artifact saved with %i entries',
                    len(self._dump.entries))

    def create(self, force: bool = False,
               gitkeep: bool = True) -> typing.NoReturn:
        """Create an empty, stub project"""
        self._create_directories(force, gitkeep)
        yaml.save(
            self.path / 'project.yaml', {
                'name': self.name,
                'encoding': self.encoding,
                'stdstrings': self.stdstrings,
                'superuser': self.superuser})

    def load(self) -> Project:
        """Load the project from the specified project directory

        :raises: RuntimeError

        """
        self._read_project_file()
        for ot in self._READ_ORDER:
            self._read_object_files(ot, models.MAPPINGS[ot])
        self._read_role_files(const.GROUP, models.Group)
        self._read_role_files(const.ROLE, models.Role)
        self._read_role_files(const.USER, models.User)
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

    def _add_comment_to_dump(self, obj_type: str,
                             schema: typing.Optional[str],
                             name: str,
                             owner: str,
                             parent: typing.Optional[str],
                             comment: str) -> typing.NoReturn:
        sql = ['COMMENT ON', obj_type]
        if obj_type == const.TABLESPACE:
            sql.append(name)
        elif parent:
            sql.append('{}.{}'.format(schema, name))
            sql.append('ON')
            sql.append(parent)
        else:
            sql.append('{}.{}'.format(schema, name))
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

    def _build_agg_definition(self, defn: dict) -> models.Aggregate:
        defn['arguments'] = [models.Argument(**a) for a in defn['arguments']]
        if 'dependencies' in defn:
            self._cache_and_remove_dependencies(
                const.AGGREGATE, self._object_name(defn), defn)
        return models.Aggregate(**defn)

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

    def _create_directories(self, exist_ok: bool = False,
                            gitkeep: bool = True) -> typing.NoReturn:
        LOGGER.debug('Creating %s', self.path)
        self.path.mkdir(exist_ok=exist_ok)
        os.makedirs(self.path, exist_ok=exist_ok)
        for value in const.PATHS.values():
            subdir_path = self.path / value
            subdir_path.mkdir(exist_ok=exist_ok)
            if gitkeep:
                gitkeep_path = subdir_path / '.gitkeep'
                gitkeep_path.touch(exist_ok=exist_ok)

    def _dump_aggregates(self) -> typing.NoReturn:
        for name in self._inv[const.AGGREGATE]:
            if self._inv[const.AGGREGATE][name].sql:
                sql = [self._inv[const.AGGREGATE][name].sql]
            else:
                sql = ['CREATE AGGREGATE', utils.quote_ident(
                    self._inv[const.AGGREGATE][name].name)]
                args = []
                for argument in self._inv[const.AGGREGATE][name].arguments:
                    arg = [argument.mode]
                    if argument.name:
                        arg.append(argument.name)
                    arg.append(argument.data_type)
                    args.append(' '.join(arg))
                sql.append('({})'.format(', '.join(args)))
                options = [
                    'SFUNC = {}'.format(
                        self._inv[const.AGGREGATE][name].sfunc),
                    'STYPE = {}'.format(
                        self._inv[const.AGGREGATE][name].state_data_type)]
                if self._inv[const.AGGREGATE][name].state_data_size:
                    options.append('SSPACE = {}'.format(
                        self._inv[const.AGGREGATE][name].state_data_size))
                if self._inv[const.AGGREGATE][name].ffunc:
                    options.append('FINALFUNC = {}'.format(
                        self._inv[const.AGGREGATE][name].ffunc))
                if self._inv[const.AGGREGATE][name].finalfunc_extra:
                    options.append('FINALFUNC_EXTRA = {}'.format(
                        self._inv[const.AGGREGATE][name].finalfunc_extra))
                if self._inv[const.AGGREGATE][name].finalfunc_modify:
                    options.append('FINALFUNC_MODIFY = {}'.format(
                        self._inv[const.AGGREGATE][name].finalfunc_modify))
                if self._inv[const.AGGREGATE][name].combinefunc:
                    options.append('COMBINEFUNC = {}'.format(
                        self._inv[const.AGGREGATE][name].combinefunc))
                if self._inv[const.AGGREGATE][name].serialfunc:
                    options.append('SERIALFUNC = {}'.format(
                        self._inv[const.AGGREGATE][name].serialfunc))
                if self._inv[const.AGGREGATE][name].deserialfunc:
                    options.append('DESERIALFUNC = {}'.format(
                        self._inv[const.AGGREGATE][name].deserialfunc))
                if self._inv[const.AGGREGATE][name].initial_condition:
                    options.append('INITCOND = {}'.format(
                        self._inv[const.AGGREGATE][name].initial_condition))
                if self._inv[const.AGGREGATE][name].msfunc:
                    options.append('MSFUNC = {}'.format(
                        self._inv[const.AGGREGATE][name].msfunc))
                if self._inv[const.AGGREGATE][name].minvfunc:
                    options.append('MINVFUNC = {}'.format(
                        self._inv[const.AGGREGATE][name].minvfunc))
                if self._inv[const.AGGREGATE][name].mstate_data_type:
                    options.append('MSTYPE = {}'.format(
                        self._inv[const.AGGREGATE][name].mstate_data_type))
                if self._inv[const.AGGREGATE][name].mstate_data_size:
                    options.append('MSSPACE = {}'.format(
                        self._inv[const.AGGREGATE][name].mstate_data_size))
                if self._inv[const.AGGREGATE][name].mffunc:
                    options.append('MFINALFUNC = {}'.format(
                        self._inv[const.AGGREGATE][name].mffunc))
                if self._inv[const.AGGREGATE][name].mfinalfunc_extra:
                    options.append('MFINALFUNC_EXTRA')
                if self._inv[const.AGGREGATE][name].mfinalfunc_modify:
                    options.append('MFINALFUNC_MODIFY = {}'.format(
                        self._inv[const.AGGREGATE][name].mfinalfunc_modify))
                if self._inv[const.AGGREGATE][name].minitial_condition:
                    options.append('MINITCOND = {}'.format(
                        self._inv[const.AGGREGATE][name].minitial_condition))
                if self._inv[const.AGGREGATE][name].sort_operator:
                    options.append('SORTOP = {}'.format(
                        self._inv[const.AGGREGATE][name].sort_operator))
                if self._inv[const.AGGREGATE][name].parallel:
                    options.append('PARALLEL = {}'.format(
                        self._inv[const.AGGREGATE][name].parallel))
                if self._inv[const.AGGREGATE][name].hypothetical:
                    options.append('HYPOTHETICAL')
                sql.append('({})'.format(', '.join(options)))
            self._dump.add_entry(
                desc=const.AGGREGATE,
                namespace=self._inv[const.AGGREGATE][name].schema,
                tag=self._inv[const.AGGREGATE][name].name,
                owner=self._inv[const.AGGREGATE][name].owner,
                defn='{};\n'.format(' '.join(sql)))
            if self._inv[const.AGGREGATE][name].comment:
                self._add_comment_to_dump(
                    const.AGGREGATE,
                    self._inv[const.AGGREGATE][name].schema,
                    self._inv[const.AGGREGATE][name].name,
                    self._inv[const.AGGREGATE][name].owner, None,
                    self._inv[const.AGGREGATE][name].comment)

    def _dump_casts(self) -> typing.NoReturn:
        for name in self._inv[const.CAST]:
            cast = self._inv[const.CAST][name]
            if cast.sql:
                sql = [cast.sql]
            else:
                sql = ['CREATE CAST {}'.format(name)]
                if cast.function:
                    sql.append('WITH FUNCTION')
                    sql.append(cast.function)
                elif cast.inout:
                    sql.append('WITH INOUT')
                else:
                    sql.append('WITHOUT FUNCTION')
                if cast.assignment:
                    sql.append('AS ASSIGNMENT')
                if cast.implicit:
                    sql.append('AS IMPLICIT')
            self._dump.add_entry(
                desc=const.CAST, namespace=cast.schema, tag=name,
                owner=cast.owner, defn='{};\n'.format(' '.join(sql)))
            if cast.comment:
                self._add_comment_to_dump(
                    const.CAST, cast.schema, name, cast.owner, None,
                    cast.comment)

    def _dump_collations(self) -> typing.NoReturn:
        for name in self._inv[const.COLLATION]:
            if self._inv[const.COLLATION][name].sql:
                sql = [self._inv[const.COLLATION][name].sql]
            else:
                sql = ['CREATE COLLATION',
                       utils.quote_ident(
                           self._inv[const.COLLATION][name].name)]
                if self._inv[const.COLLATION][name].copy_from:
                    sql.append('FROM')
                    sql.append(self._inv[const.COLLATION][name].copy_from)
                else:
                    options = []
                    if self._inv[const.COLLATION][name].locale:
                        options.append('LOCALE = {}'.format(
                            self._inv[const.COLLATION][name].locale))
                    if self._inv[const.COLLATION][name].lc_collate:
                        options.append('LC_COLLATE = {}'.format(
                            self._inv[const.COLLATION][name].lc_collate))
                    if self._inv[const.COLLATION][name].lc_ctype:
                        options.append('LC_CTYPE = {}'.format(
                            self._inv[const.COLLATION][name].lc_ctype))
                    if self._inv[const.COLLATION][name].provider:
                        options.append('PROVIDER = {}'.format(
                            self._inv[const.COLLATION][name].provider))
                    if self._inv[const.COLLATION][name].deterministic:
                        options.append('DETERMINISTIC = {}'.format(
                            self._inv[const.COLLATION][name].deterministic))
                    if self._inv[const.COLLATION][name].version:
                        options.append('VERSION = {}'.format(
                            self._inv[const.COLLATION][name].version))
                    sql.append('({})'.format(', '.join(options)))
            self._dump.add_entry(
                desc=const.COLLATION,
                namespace=self._inv[const.COLLATION][name].schema,
                tag=self._inv[const.COLLATION][name].name,
                owner=self._inv[const.COLLATION][name].owner,
                defn='{};\n'.format(' '.join(sql)))
            if self._inv[const.COLLATION][name].comment:
                self._add_comment_to_dump(
                    const.COLLATION,
                    self._inv[const.COLLATION][name].schema,
                    self._inv[const.COLLATION][name].name,
                    self._inv[const.COLLATION][name].owner, None,
                    self._inv[const.COLLATION][name].comment)

    def _dump_conversions(self) -> typing.NoReturn:
        for name in self._inv[const.CONVERSION]:
            if self._inv[const.CONVERSION][name].sql:
                sql = [self._inv[const.CONVERSION][name].sql]
            else:
                sql = ['CREATE DEFAULT'
                       if self._inv[const.CONVERSION][name].default
                       else 'CREATE', 'CONVERSION',
                       utils.quote_ident(
                           self._inv[const.CONVERSION][name].name),
                       'FOR', self._inv[const.CONVERSION][name].encoding_from,
                       'TO', self._inv[const.CONVERSION][name].encoding_to,
                       'FROM', self._inv[const.CONVERSION][name].function]
            self._dump.add_entry(
                desc=const.CONVERSION,
                namespace=self._inv[const.CONVERSION][name].schema,
                tag=self._inv[const.CONVERSION][name].name,
                owner=self._inv[const.CONVERSION][name].owner,
                defn='{};\n'.format(' '.join(sql)))
            if self._inv[const.CONVERSION][name].comment:
                self._add_comment_to_dump(
                    const.CONVERSION,
                    self._inv[const.CONVERSION][name].schema,
                    self._inv[const.CONVERSION][name].name,
                    self._inv[const.CONVERSION][name].owner, None,
                    self._inv[const.CONVERSION][name].comment)

    def _dump_domains(self) -> typing.NoReturn:
        for name in self._inv[const.DOMAIN]:
            if self._inv[const.DOMAIN][name].sql:
                sql = [self._inv[const.DOMAIN][name].sql]
            else:
                sql = ['CREATE DOMAIN',
                       utils.quote_ident(self._inv[const.DOMAIN][name].name),
                       'AS', self._inv[const.DOMAIN][name].data_type]
                if self._inv[const.DOMAIN][name].collation:
                    sql.append('COLLATE')
                    sql.append(self._inv[const.DOMAIN][name].collation)
                if self._inv[const.DOMAIN][name].default:
                    sql.append('DEFAULT')
                    sql.append(utils.postgres_value(
                        self._inv[const.DOMAIN][name].default))
                if self._inv[const.DOMAIN][name].check_constraints:
                    constraints = []
                    for c in self._inv[const.DOMAIN][name].check_constraints:
                        value = ['CONSTRAINT']
                        if c.name:
                            value.append(c.name)
                        if c.nullable is not None:
                            value.append(
                                'NULL' if c.nullable else 'NOT NULL')
                        if c.expression:
                            value.append('CHECK ({})'.format(c.expression))
                        constraints.append(' '.join(value))
                    sql.append(' '.join(constraints))
            self._dump.add_entry(
                desc=const.DOMAIN,
                namespace=self._inv[const.DOMAIN][name].schema,
                tag=self._inv[const.DOMAIN][name].name,
                owner=self._inv[const.DOMAIN][name].owner,
                defn='{};\n'.format(' '.join(sql)))
            if self._inv[const.DOMAIN][name].comment:
                self._add_comment_to_dump(
                    const.DOMAIN,
                    self._inv[const.DOMAIN][name].schema,
                    self._inv[const.DOMAIN][name].name,
                    self._inv[const.DOMAIN][name].owner, None,
                    self._inv[const.DOMAIN][name].comment)

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

    def _dump_foreign_data_wrappers(self) -> typing.NoReturn:
        for name in self._inv[const.FOREIGN_DATA_WRAPPER]:
            sql = ['CREATE FOREIGN DATA WAPPER', utils.quote_ident(name)]
            if self._inv[const.FOREIGN_DATA_WRAPPER][name].handler:
                sql.append('HANDLER')
                sql.append(self._inv[const.FOREIGN_DATA_WRAPPER][name].handler)
            else:
                sql.append('NO HANDLER')
            if self._inv[const.FOREIGN_DATA_WRAPPER][name].validator:
                sql.append('VALIDATOR')
                sql.append(
                    self._inv[const.FOREIGN_DATA_WRAPPER][name].validator)
            else:
                sql.append('NO VALIDATOR')
            if self._inv[const.FOREIGN_DATA_WRAPPER][name].options:
                sql.append('OPTIONS ({})'.format(
                    ', '.join([
                        '{} {}'.format(k, utils.postgres_value(v))
                        for k, v in self._inv[
                            const.FOREIGN_DATA_WRAPPER][name].options.items()
                    ])))
            self._dump.add_entry(
                const.FOREIGN_DATA_WRAPPER, None, name, self.superuser,
                '{};\n'.format(' '.join(sql)))

    def _dump_functions(self) -> typing.NoReturn:
        for name in self._inv[const.FUNCTION]:
            function = self._inv[const.FUNCTION][name]
            if function.sql:
                sql = [function.sql]
            else:
                func_name = function.name
                if function.parameters:
                    params = []
                    for param in function.parameters:
                        value = [param.mode]
                        if param.name:
                            value.append(param.name)
                        value.append(param.data_type)
                        if param.default:
                            value.append('=')
                            value.append(param.default)
                        params.append(' '.join(value))
                    func_name = '{}()'.format(
                        function.name.split('(')[0], ', '.join(params))
                sql = ['CREATE FUNCTION', func_name,
                       'RETURNS', function.returns,
                       'LANGUAGE', function.language]
                if function.transform_types:
                    tts = []
                    for tt in function.transform_types:
                        tts.append('FOR TYPE {}'.format(tt))
                    sql.append('TRANSFORM {}'.format(', '.join(tts)))
                if function.window:
                    sql.append('WINDOW')
                if function.immutable:
                    sql.append('IMMUTABLE')
                if function.stable:
                    sql.append('STABLE')
                if function.volatile:
                    sql.append('VOLATILE')
                if function.leak_proof is not None:
                    if not function.leak_proof:
                        sql.append('NOT')
                    sql.append('LEAKPROOF')
                if function.called_on_null_input:
                    sql.append('CALLED ON NULL INPUT')
                elif function.called_on_null_input is False:
                    sql.append('RETURNS NULL ON NULL INPUT')
                if function.strict:
                    sql.append('STRICT')
                if function.security:
                    sql.append('SECURITY')
                    sql.append(function.security)
                if function.parallel:
                    sql.append('PARALLEL')
                    sql.append(function.parallel)
                if function.cost:
                    sql.append('COST')
                    sql.append(str(function.cost))
                if function.rows:
                    sql.append('ROWS')
                    sql.append(str(function.rows))
                if function.support:
                    sql.append('SUPPORT')
                    sql.append(function.support)
                if function.configuration:
                    for k, v in function.configuration.items():
                        sql.append('SET {} = {}'.format(
                            k, utils.postgres_value(v)))
                sql.append('AS')
                if function.definition:
                    sql = ['{} $$\n{}\n$$'.format(
                        ' '.join(sql), function.definition)]
                elif function.object_file and function.link_symbol:
                    sql.append('{}, {}'.format(
                        utils.postgres_value(function.object_file),
                        utils.postgres_value(function.link_symbol)))
            self._dump.add_entry(
                desc=const.FUNCTION, namespace=function.schema,
                tag=function.name, owner=function.owner,
                defn='{};\n'.format(' '.join(sql)))
            if function.comment:
                self._add_comment_to_dump(
                    const.FUNCTION, function.schema, function.name,
                    function.owner, None, function.comment)

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

    def _dump_materialized_views(self) -> typing.NoReturn:
        for name in self._inv[const.MATERIALIZED_VIEW]:
            view = self._inv[const.MATERIALIZED_VIEW][name]
            if view.sql:
                sql = [view.sql]
            else:
                sql = ['CREATE MATERIALIZED VIEW',
                       utils.quote_ident(view.name)]
                if view.columns:
                    columns = []
                    for column in view.columns:
                        columns.append(column.name)
                        if column.comment:
                            self._add_comment_to_dump(
                                const.COLUMN, view.schema,
                                '{}.{}'.format(view.name, column.name),
                                view.owner, None, column.comment)
                    sql.append('({})'.format(', '.join(columns)))
                if view.table_access_method:
                    sql.append('USING')
                    sql.append(view.table_access_method)
                if view.storage_parameters:
                    sql.append('WITH')
                    params = []
                    for key, value in view.storage_parameters.items():
                        params.append('{} = {}'.format(key, value))
                    sql.append(', '.join(params))
                if view.tablespace:
                    sql.append('TABLESPACE')
                    sql.append(view.tablespace)
                sql.append('AS')
                sql.append(view.query)
            self._dump.add_entry(
                desc=const.MATERIALIZED_VIEW, namespace=view.schema,
                tag=view.name, owner=view.owner,
                defn='{};\n'.format(' '.join(sql)))
            if view.comment:
                self._add_comment_to_dump(
                    const.MATERIALIZED_VIEW, view.schema, view.name,
                    view.owner, None, view.comment)

    def _dump_operators(self) -> typing.NoReturn:
        for name in self._inv[const.OPERATOR]:
            if self._inv[const.OPERATOR][name].sql:
                sql = [self._inv[const.OPERATOR][name].sql]
            else:
                sql = ['CREATE OPERATOR',
                       utils.quote_ident(self._inv[const.OPERATOR][name].name)]
                options = ['FUNCTION = {}'.format(
                    self._inv[const.OPERATOR][name].function)]
                if self._inv[const.OPERATOR][name].left_arg:
                    options.append('LEFTARG = {}'.format(
                        self._inv[const.OPERATOR][name].left_arg))
                if self._inv[const.OPERATOR][name].right_arg:
                    options.append('RIGHTARG = {}'.format(
                        self._inv[const.OPERATOR][name].right_arg))
                if self._inv[const.OPERATOR][name].commutator:
                    options.append('COMMUTATOR = {}'.format(
                        self._inv[const.OPERATOR][name].commutator))
                if self._inv[const.OPERATOR][name].negator:
                    options.append('NEGATOR = {}'.format(
                        self._inv[const.OPERATOR][name].negator))
                if self._inv[const.OPERATOR][name].restrict:
                    options.append('RESTRICT = {}'.format(
                        self._inv[const.OPERATOR][name].restrict))
                if self._inv[const.OPERATOR][name].join:
                    options.append('JOIN = {}'.format(
                        self._inv[const.OPERATOR][name].join))
                if self._inv[const.OPERATOR][name].hashes:
                    options.append('HASHES')
                if self._inv[const.OPERATOR][name].merges:
                    options.append('MERGES')
                sql.append('({})'.format(', '.join(options)))
            self._dump.add_entry(
                desc=const.OPERATOR,
                namespace=self._inv[const.OPERATOR][name].schema,
                tag=self._inv[const.OPERATOR][name].name,
                owner=self._inv[const.OPERATOR][name].owner,
                defn='{};\n'.format(' '.join(sql)))
            if self._inv[const.OPERATOR][name].comment:
                self._add_comment_to_dump(
                    const.OPERATOR,
                    self._inv[const.OPERATOR][name].schema,
                    self._inv[const.OPERATOR][name].name,
                    self._inv[const.OPERATOR][name].owner, None,
                    self._inv[const.OPERATOR][name].comment)

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

    def _dump_sequences(self) -> typing.NoReturn:
        for name in self._inv[const.SEQUENCE]:
            if self._inv[const.SEQUENCE][name].sql:
                sql = [self._inv[const.SEQUENCE][name].sql]
            else:
                sql = ['CREATE SEQUENCE',
                       utils.quote_ident(self._inv[const.SEQUENCE][name].name)]
                if self._inv[const.SEQUENCE][name].data_type:
                    sql.append('AS')
                    sql.append(self._inv[const.SEQUENCE][name].data_type)
                if self._inv[const.SEQUENCE][name].increment_by:
                    sql.append('INCREMENT BY')
                    sql.append(str(
                        self._inv[const.SEQUENCE][name].increment_by))
                if self._inv[const.SEQUENCE][name].min_value:
                    sql.append('MINVALUE')
                    sql.append(str(self._inv[const.SEQUENCE][name].min_value))
                if self._inv[const.SEQUENCE][name].max_value:
                    sql.append('MAXVALUE')
                    sql.append(str(self._inv[const.SEQUENCE][name].max_value))
                if self._inv[const.SEQUENCE][name].start_with:
                    sql.append('START WITH')
                    sql.append(str(self._inv[const.SEQUENCE][name].start_with))
                if self._inv[const.SEQUENCE][name].cache:
                    sql.append('CACHE')
                    sql.append(str(self._inv[const.SEQUENCE][name].cache))
                if self._inv[const.SEQUENCE][name].cycle is not None:
                    if not self._inv[const.SEQUENCE][name].cycle:
                        sql.append('NO')
                    sql.append('CYCLE')
                if self._inv[const.SEQUENCE][name].owned_by:
                    sql.append('OWNED BY')
                    sql.append(self._inv[const.SEQUENCE][name].owned_by)
            self._dump.add_entry(
                desc=const.SEQUENCE,
                tag=self._inv[const.SEQUENCE][name].name,
                owner=self._inv[const.SEQUENCE][name].owner,
                defn='{};\n'.format(' '.join(sql)))
            if self._inv[const.SEQUENCE][name].comment:
                self._add_comment_to_dump(
                    const.SEQUENCE, None,
                    self._inv[const.SEQUENCE][name].name,
                    self._inv[const.SEQUENCE][name].owner, None,
                    self._inv[const.SEQUENCE][name].comment)

    def _dump_table(self, name):
        table = self._inv[const.TABLE][name]
        if table.sql:
            sql = [table.sql]
        else:
            sql = ['CREATE']
            if table.unlogged:
                sql.append('UNLOGGED')
            sql.append('TABLE')
            sql.append(name)
            sql.append('(')
            if table.like_table:
                sql.append('LIKE')
                sql.append(table.name)
                for field in [field for field in table.like_table.fields()
                              if field.startswith('include_')
                              and getattr(table.like_table, field)
                              is not None]:
                    sql.append('INCLUDING' if getattr(table, field) else
                               'EXCLUDING')
                    sql.append(field)
            else:
                inner_sql = []
                if table.columns:
                    for col in table.columns:
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
                        if col.generated and col.generated.expression:
                            column.append('GENERATED ALWAYS AS')
                            column.append(col.generated.expression)
                            column.append('STORED')
                        elif col.generated and col.generated.sequence:
                            column.append('GENERATED')
                            column.append(col.generated.sequence_behavior)
                            column.append('AS IDENTITY')
                        inner_sql.append(' '.join(column))
                        if col.comment:
                            self._add_comment_to_dump(
                                const.COLUMN, table.schema,
                                '{}.{}'.format(table.name, col.name),
                                table.owner, None, col.comment)
                for item in table.unique_constraints or []:
                    inner_sql.append(
                        self._format_sql_constraint('UNIQUE', item))
                if table.primary_key:
                    inner_sql.append(self._format_sql_constraint(
                        'PRIMARY_KEY', table.primary_key))
                for fk in table.foreign_keys or []:
                    fk_sql = ['FOREIGN KEY ({})'.format(', '.join(fk.columns)),
                              'REFERENCES', fk.references.name,
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
            if table.parents:
                sql.append(', '.join(table.parents))
            if table.partition:
                sql.append('PARTITION BY')
                sql.append(table.partition.type)
                sql.append('(')
                columns = []
                for col in table.partition.columns:
                    column = [col.name or col.expression]
                    if col.collation:
                        column.append('COLLATION')
                        column.append(col.collation)
                    if col.opclass:
                        column.append(col.opclass)
                sql.append(', '.join(columns))
                sql.append(')')
            if table.access_method:
                sql.append('USING')
                sql.append(table.access_method)
            if table.storage_parameters:
                sql.append('WITH')
                params = []
                for key, value in table.storage_parameters.items():
                    params.append('{}={}'.format(key, value))
                sql.append(', '.join(params))
            if table.tablespace:
                sql.append('TABLESPACE')
                sql.append(table.tablespace)
        self._dump.add_entry(
            const.TABLE, table.schema, table.name, table.owner,
            '{};\n'.format(' '.join(sql)), None, None, [], table.tablespace)
        if table.comment:
            self._add_comment_to_dump(
                const.TABLE, table.schema, table.name, table.owner,
                name, table.comment)
        for index in table.indexes or []:
            self._dump_index(index, table.schema, table.owner, name)
        for trigger in table.triggers or []:
            self._dump_trigger(trigger, table.schema, table.owner, name)

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

    def _dump_tables(self) -> typing.NoReturn:
        for name in self._inv[const.TABLE]:
            self._dump_table(name)

    def _dump_tablespaces(self) -> typing.NoReturn:
        for name in self._inv[const.TABLESPACE]:
            sql = ['CREATE TABLESPACE',
                   utils.quote_ident(self._inv[const.TABLESPACE][name].name),
                   'OWNER',
                   self._inv[const.TABLESPACE][name].owner,
                   'LOCATION',
                   self._inv[const.TABLESPACE][name].location]
            if self._inv[const.TABLESPACE][name].options:
                options = []
                for k, v in self._inv[const.TABLESPACE][name].options.items():
                    options.append('{}={}'.format(k, utils.postgres_value(v)))
                sql.append('WITH ({})'.format(','.join(options)))
            self._dump.add_entry(
                desc=const.TABLESPACE,
                tag=self._inv[const.TABLESPACE][name].name,
                owner=self._inv[const.TABLESPACE][name].owner,
                defn='{};\n'.format(' '.join(sql)))
            if self._inv[const.TABLESPACE][name].comment:
                self._add_comment_to_dump(
                    const.TABLESPACE, None,
                    self._inv[const.TABLESPACE][name].name,
                    self._inv[const.TABLESPACE][name].owner, None,
                    self._inv[const.TABLESPACE][name].comment)

    def _dump_types(self) -> typing.NoReturn:
        for name in self._inv[const.TYPE]:
            if self._inv[const.TYPE][name].sql:
                sql = [self._inv[const.TYPE][name].sql]
            else:
                sql = ['CREATE TYPE',
                       utils.quote_ident(self._inv[const.TYPE][name].name),
                       'AS']
                if self._inv[const.TYPE][name].type == 'base':
                    options = [
                        'INPUT = {}'.format(self._inv[const.TYPE][name].input),
                        'OUTPUT = {}'.format(
                            self._inv[const.TYPE][name].output)]
                    if self._inv[const.TYPE][name].receive:
                        options.append('RECEIVE = {}'.format(
                            self._inv[const.TYPE][name].receive))
                    if self._inv[const.TYPE][name].send:
                        options.append('SEND = {}'.format(
                            self._inv[const.TYPE][name].receive))
                    if self._inv[const.TYPE][name].typmod_in:
                        options.append('TYPMOD_IN = {}'.format(
                            self._inv[const.TYPE][name].typmod_in))
                    if self._inv[const.TYPE][name].typmod_out:
                        options.append('TYPMOD_OUT = {}'.format(
                            self._inv[const.TYPE][name].typmod_out))
                    if self._inv[const.TYPE][name].analyze:
                        options.append('ANALYZE = {}'.format(
                            self._inv[const.TYPE][name].analyze))
                    if self._inv[const.TYPE][name].internal_length:
                        options.append('INTERNALLENGTH = {}'.format(
                            self._inv[const.TYPE][name].internal_length))
                    if self._inv[const.TYPE][name].passed_by_value:
                        options.append('PASSEDBYVALUE')
                    if self._inv[const.TYPE][name].alignment:
                        options.append('ALIGNMENT = {}'.format(
                            self._inv[const.TYPE][name].alignment))
                    if self._inv[const.TYPE][name].storage:
                        options.append('STORAGE = {}'.format(
                            self._inv[const.TYPE][name].storage))
                    if self._inv[const.TYPE][name].like_type:
                        options.append('LIKE = {}'.format(
                            self._inv[const.TYPE][name].like_type))
                    if self._inv[const.TYPE][name].category:
                        options.append('CATEGORY = {}'.format(
                            utils.postgres_value(
                                self._inv[const.TYPE][name].category)))
                    if self._inv[const.TYPE][name].preferred:
                        options.append('PREFERRED = {}'.format(
                            self._inv[const.TYPE][name].preferred))
                    if self._inv[const.TYPE][name].default:
                        options.append('DEFAULT = {}'.format(
                            utils.postgres_value(
                                self._inv[const.TYPE][name].default)))
                    if self._inv[const.TYPE][name].element:
                        options.append('ELEMENT = {}'.format(
                            self._inv[const.TYPE][name].element))
                    if self._inv[const.TYPE][name].delimiter:
                        options.append('DELIMITER = {}'.format(
                            utils.postgres_value(
                                self._inv[const.TYPE][name].demiter)))
                    if self._inv[const.TYPE][name].collatable:
                        options.append('COLLATABLE = {}'.format(
                            self._inv[const.TYPE][name].collatable))
                    sql.append('({})'.format(', '.join(options)))
                elif self._inv[const.TYPE][name].type == 'composite':
                    columns = []
                    for column in self._inv[const.TYPE][name].columns:
                        col = [column.name, column.data_type]
                        if column.collation:
                            col.append('COLLATE')
                            col.append(column.collation)
                        columns.append(' '.join(col))
                    sql.append('({})'.format(', '.join(columns)))
                elif self._inv[const.TYPE][name].type == 'enum':
                    sql.append('ENUM')
                    sql.append('({})'.format(', '.join(
                        utils.postgres_value(e) for e in
                        self._inv[const.TYPE][name].enum)))
                elif self._inv[const.TYPE][name].type == 'range':
                    sql.append('RANGE')
                    options = ['SUBTYPE = {}'.format(
                        self._inv[const.TYPE][name].subtype)]
                    if self._inv[const.TYPE][name].subtype_opclass:
                        options.append('SUBTYPE_OPCLASS = {}'.format(
                            self._inv[const.TYPE][name].subtype))
                    if self._inv[const.TYPE][name].collation:
                        options.append('COLLATION = {}'.format(
                            self._inv[const.TYPE][name].collation))
                    if self._inv[const.TYPE][name].collation:
                        options.append('CANONICAL = {}'.format(
                            self._inv[const.TYPE][name].canonical))
                    if self._inv[const.TYPE][name].subtype_diff:
                        options.append('SUBTYPE_DIFF = {}'.format(
                            self._inv[const.TYPE][name].subtype_diff))
                    sql.append('({})'.format(', '.join(options)))
            self._dump.add_entry(
                desc=const.TYPE,
                namespace=self._inv[const.TYPE][name].schema,
                tag=self._inv[const.TYPE][name].name,
                owner=self._inv[const.TYPE][name].owner,
                defn='{};\n'.format(' '.join(sql)))
            if self._inv[const.TYPE][name].comment:
                self._add_comment_to_dump(
                    const.TYPE,
                    self._inv[const.TYPE][name].schema,
                    self._inv[const.TYPE][name].name,
                    self._inv[const.TYPE][name].owner, None,
                    self._inv[const.TYPE][name].comment)

    def _dump_views(self) -> typing.NoReturn:
        for name in self._inv[const.VIEW]:
            view = self._inv[const.VIEW][name]
            if view.sql:
                sql = [view.sql]
            else:
                sql = ['CREATE RECURSIVE VIEW' if view.recursive else
                       'CREATE VIEW', utils.quote_ident(view.name)]
                if view.columns:
                    columns = []
                    for column in view.columns:
                        columns.append(column.name)
                        if column.comment:
                            self._add_comment_to_dump(
                                const.COLUMN, view.schema,
                                '{}.{}'.format(view.name, column.name),
                                view.owner, None, column.comment)
                    sql.append('({})'.format(', '.join(columns)))
                if view.check_option:
                    sql.append('WITH check_option = ')
                    sql.append(view.check_option)
                if view.security_barrier:
                    sql.append('WITH security_barrier = ')
                    sql.append(view.security_barrier)
                sql.append('AS')
                sql.append(view.query)
                if view.check_option:
                    sql.append('WITH')
                    sql.append(view.check_option)
                    sql.append('CHECK OPTION')
            self._dump.add_entry(
                desc=const.VIEW, namespace=view.schema, tag=view.name,
                owner=view.owner, defn='{};\n'.format(' '.join(sql)))
            if view.comment:
                self._add_comment_to_dump(
                    const.VIEW, view.schema, view.name, view.owner, None,
                    view.comment)

    @staticmethod
    def _format_sql_constraint(constraint_type: str,
                               constraint: models.ConstraintColumns) -> str:
        sql = ['{} ({})'.format(constraint_type,
                                ', '.join(constraint.columns))]
        if constraint.include:
            sql.append(', '.join(constraint.include))
        return ' '.join(sql)

    def _iterate_files(self, ot: str) -> typing.Generator[dict, None, None]:
        """Generator that will iterate over all of the subdirectories and
        files for the specified object type, parsing the YAML and returning
        a tuple of the schema name, object name, and a dict of values from the
        file.

        """
        path = self.path.joinpath(const.PATHS[ot])
        if not path.exists():
            LOGGER.warning('No %s file found in project', ot)
            return
        for child in sorted(path.iterdir(), key=lambda p: str(p)):
            if child.is_dir():
                for s_child in sorted(child.iterdir(), key=lambda p: str(p)):
                    if yaml.is_yaml(s_child):
                        yield self._preprocess_definition(
                            ot, s_child.parent.name,
                            s_child.name.split('.')[0], yaml.load(s_child))
            elif yaml.is_yaml(child):
                yield self._preprocess_definition(
                    ot, child.name.split('.')[0], None, yaml.load(child))

    @staticmethod
    def _object_name(definition: dict, schemaless: bool = False):
        if 'name' not in definition:
            LOGGER.critical('name missing from definition: %r', definition)
            raise RuntimeError('Missing object name')
        if schemaless or 'schema' not in definition:
            return definition['name']
        return '{}.{}'.format(definition['schema'], definition['name'])

    def _preprocess_definition(self, obj_type: str, schema: str,
                               name: typing.Optional[str],
                               definition: dict) -> dict:
        if obj_type not in self._SCHEMALESS and 'schema' not in definition:
            definition['schema'] = schema
        if name and 'name' not in definition:
            definition['name'] = name
        if obj_type not in self._OWNERLESS and 'owner' not in definition:
            definition['owner'] = self.superuser
        return definition

    def _read_object_files(self, obj_type: str,
                           model: dataclasses.dataclass) -> typing.NoReturn:
        if obj_type in self._PER_SCHEMA_FILES:
            return self._read_objects_files(obj_type, model)
        LOGGER.debug('Reading %s objects', obj_type)
        for defn in self._iterate_files(obj_type):
            name = self._object_name(defn)
            if not validation.validate_object(obj_type, name, defn):
                self._load_errors += 1
                continue
            if 'dependencies' in defn:
                self._cache_and_remove_dependencies(obj_type, name, defn)
            if obj_type == const.AGGREGATE:
                self._inv[obj_type][name] = self._build_agg_definition(defn)
            elif obj_type == const.DOMAIN and defn.get('check_constraints'):
                defn['check_constraints'] = [
                    models.DomainConstraint(**c)
                    for c in defn['check_constraints']]
                self._inv[obj_type][name] = model(**defn)
            elif obj_type == const.FUNCTION and defn.get('parameters'):
                defn['parameters'] = [
                    models.FunctionParameter(**p) for p in defn['parameters']]
                self._inv[obj_type][name] = model(**defn)
            elif obj_type in {const.MATERIALIZED_VIEW,
                              const.VIEW} and defn.get('columns'):
                defn['columns'] = [
                    models.ViewColumn(c) if isinstance(c, str) else
                    models.ViewColumn(**c)
                    for c in defn['columns']]
                self._inv[obj_type][name] = model(**defn)
            elif obj_type == const.TABLE:
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
                self._inv[obj_type][defn['schema']] = \
                    self._read_text_search_definition(defn)
                continue
            for entry in defn.get(key):
                if 'owner' not in entry:
                    entry['owner'] = defn['owner']
                if 'schema' not in entry:
                    entry['schema'] = defn['schema']
                if obj_type == const.CAST:
                    name = '({} AS {})'.format(
                        entry.get('source_type'), entry.get('target_type'))
                else:
                    name = self._object_name(entry)
                if not validation.validate_object(obj_type, name, entry):
                    self._load_errors += 1
                    continue
                if obj_type == const.TYPE and entry.get('columns'):
                    entry['columns'] = [models.TypeColumn(**c)
                                        for c in entry['columns']]
                if 'dependencies' in entry:
                    self._cache_and_remove_dependencies(
                        obj_type, name, defn)
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
        for fdw in project.get('foreign_data_wrappers'):
            if 'owner' not in fdw:
                fdw['owner'] = self.superuser
            self._inv[const.FOREIGN_DATA_WRAPPER][fdw['name']] = \
                models.ForeignDataWrapper(**fdw)
        for language in project.get('languages'):
            name = self._object_name(language)
            self._inv[const.PROCEDURAL_LANGUAGE][name] = \
                models.Language(**language)

    def _read_role_files(self, obj_type: str,
                         model: dataclasses.dataclass) -> typing.NoReturn:
        LOGGER.debug('Reading %s', const.PATHS[obj_type].name.upper())
        for defn in self._iterate_files(obj_type):
            validation.validate_object(obj_type, defn['name'], defn)
            if 'grants' in defn:
                defn['grants'] = models.ACLs(**defn['grants'])
            if 'revocations' in defn:
                defn['revocations'] = models.ACLs(**defn['revocations'])
            self._inv[obj_type][defn['name']] = model(**defn)

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
