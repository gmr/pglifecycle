"""
Used by pgdumplib.project.Project to build a Dump artifact

"""
import dataclasses
import logging
import os
import typing

import pgdumplib
import pgdumplib.dump

from pglifecycle import constants, models, utils

LOGGER = logging.getLogger(__name__)


class Dump:

    """Used to construct a dump from a project"""
    def __init__(self, project):
        self.project = project
        self._dump = pgdumplib.new(project.name, project.encoding)
        self._dump_id_map = {}

    def save(self, path: os.PathLike) -> typing.NoReturn:
        for item in self.project.sorted_inventory:
            getattr(self, '_dump_{}'.format(
                item.desc.replace(' ', '_').lower()))(item)
        self._dump.save(path)
        LOGGER.debug('Saved pg_dump -Fc compatible dump to %s with %i entries',
                     path, len(self._dump.entries))

    def _add_comment(self,
                     desc: str,
                     namespace: str,
                     tag: str,
                     owner: str,
                     parent_dump_id: int,
                     parent_tag: typing.Optional[str],
                     comment: str):
        create_sql = [constants.COMMENT, constants.ON, desc]
        if parent_tag:
            sql = ['{}.{}'.format(namespace, tag), constants.ON, parent_tag]
            create_sql += sql
        elif namespace:
            create_sql.append('{}.{}'.format(namespace, tag))
        else:
            create_sql.append(tag)
        create_sql.append(constants.IS)
        create_sql.append('$${}$$;\n'.format(comment))
        self._add_entry(
            constants.COMMENT, namespace, tag, owner, create_sql, [],
            [parent_dump_id])

    def _add_entry(self, desc: str, namespace: str, name: str, owner: str,
                   defn: typing.List[str],
                   drop_stmt: typing.List[str],
                   dependencies: typing.Optional[typing.List[int]] = None,
                   tablespace: typing.Optional[str] = None) \
            -> pgdumplib.dump.Entry:
        LOGGER.debug('Adding %s %s.%s', desc, namespace, name)
        return self._dump.add_entry(
            desc, namespace, name, owner,
            '{};\n'.format(' '.join(defn)),
            '{};\n'.format(' '.join(drop_stmt)),
            dependencies=dependencies or [],
            tablespace=tablespace)

    def _add_item(self, item: models.Item,
                  defn: typing.List[str],
                  drop_stmt: typing.List[str],
                  name: typing.Optional[str] = None,
                  no_owner: bool = False) -> typing.NoReturn:
        entry = self._add_entry(
            item.desc,
            getattr(item.definition, 'schema', ''),
            getattr(item.definition, 'name', name),
            getattr(item.definition, 'owner', ''
                    if no_owner else self.project.superuser),
            defn, drop_stmt,
            None, getattr(item.definition, 'tablespace', None))
        self._dump_id_map[item.id] = entry.dump_id
        if getattr(item.definition, 'comment', None):
            self._add_comment(
                item.desc,
                getattr(item.definition, 'schema', ''),
                getattr(item.definition, 'name', name),
                getattr(item.definition, 'owner',
                        '' if no_owner else self.project.superuser),
                entry.dump_id, None,
                item.definition.comment)

    def _add_text_search_item(self, item: models.Item, desc: str,
                              defn: typing.List[str],
                              drop_stmt: typing.List[str],
                              name: typing.Optional[str] = None,
                              comment: typing.Optional[str] = None) \
            -> typing.NoReturn:
        entry = self._add_entry(
            desc,
            getattr(item.definition, 'schema', ''),
            getattr(item.definition, 'name', name),
            getattr(item.definition, 'owner', self.project.superuser),
            defn, drop_stmt,
            getattr(item.definition, 'tablespace', None))
        if comment:
            self._add_comment(
                desc,
                getattr(item.definition, 'schema', ''),
                getattr(item.definition, 'name', name),
                self.project.superuser, entry.dump_id, None, comment)

    def _dump_aggregate(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            create_sql = [
                constants.CREATE, constants.AGGREGATE, self._item_name(item)]
            args = []
            for argument in item.definition.arguments:
                arg = [argument.mode]
                if argument.name:
                    arg.append(argument.name)
                arg.append(argument.data_type)
                args.append(' '.join(arg))
            create_sql.append('({})'.format(', '.join(args)))
            options = ['SFUNC = {}'.format(item.definition.sfunc),
                       'STYPE = {}'.format(item.definition.state_data_type)]
            if item.definition.state_data_size:
                options.append('SSPACE = {}'.format(
                    item.definition.state_data_size))
            if item.definition.ffunc:
                options.append('FINALFUNC = {}'.format(item.definition.ffunc))
            if item.definition.finalfunc_extra:
                options.append('FINALFUNC_EXTRA = {}'.format(
                    item.definition.finalfunc_extra))
            if item.definition.finalfunc_modify:
                options.append('FINALFUNC_MODIFY = {}'.format(
                    item.definition.finalfunc_modify))
            if item.definition.combinefunc:
                options.append('COMBINEFUNC = {}'.format(
                    item.definition.combinefunc))
            if item.definition.serialfunc:
                options.append('SERIALFUNC = {}'.format(
                    item.definition.serialfunc))
            if item.definition.deserialfunc:
                options.append('DESERIALFUNC = {}'.format(
                    item.definition.deserialfunc))
            if item.definition.initial_condition:
                options.append('INITCOND = {}'.format(
                    item.definition.initial_condition))
            if item.definition.msfunc:
                options.append('MSFUNC = {}'.format(item.definition.msfunc))
            if item.definition.minvfunc:
                options.append('MINVFUNC = {}'.format(
                    item.definition.minvfunc))
            if item.definition.mstate_data_type:
                options.append('MSTYPE = {}'.format(
                    item.definition.mstate_data_type))
            if item.definition.mstate_data_size:
                options.append('MSSPACE = {}'.format(
                    item.definition.mstate_data_size))
            if item.definition.mffunc:
                options.append('MFINALFUNC = {}'.format(
                    item.definition.mffunc))
            if item.definition.mfinalfunc_extra:
                options.append('MFINALFUNC_EXTRA')
            if item.definition.mfinalfunc_modify:
                options.append('MFINALFUNC_MODIFY = {}'.format(
                    item.definition.mfinalfunc_modify))
            if item.definition.minitial_condition:
                options.append('MINITCOND = {}'.format(
                    item.definition.minitial_condition))
            if item.definition.sort_operator:
                options.append('SORTOP = {}'.format(
                    item.definition.sort_operator))
            if item.definition.parallel:
                options.append(' '.join(
                    [constants.PARALLEL, '=', item.definition.parallel]))
            if item.definition.hypothetical:
                options.append('HYPOTHETICAL')
            create_sql.append('({})'.format(', '.join(options)))
            drop_sql = [constants.DROP, constants.AGGREGATE,
                        constants.IF_EXISTS, self._item_name(item),
                        '({})'.format(', '.join(args))]
        self._add_item(item, create_sql, drop_sql)

    def _dump_cast(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql, name = [item.definition.sql], [], None
        else:
            name = '({} AS {})'.format(
                utils.quote_ident(item.definition.source_type),
                utils.quote_ident(item.definition.target_type))
            create_sql = [constants.CREATE, constants.CAST, name]
            if item.definition.function:
                create_sql += [constants.WITH, constants.FUNCTION,
                               item.definition.function]
            elif item.definition.inout:
                create_sql += [constants.WITH, constants.INOUT]
            else:
                create_sql += [constants.WITHOUT, constants.FUNCTION]
            if item.definition.assignment:
                create_sql += [constants.AS, constants.ASSIGNMENT]
            if item.definition.implicit:
                create_sql += [constants.AS, constants.IMPLICIT]
            drop_sql = [constants.DROP, constants.CAST,
                        constants.IF_EXISTS, name]
        self._add_item(item, create_sql, drop_sql, name)

    def _dump_collation(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            create_sql = [constants.CREATE, constants.COLLATION,
                          self._item_name(item)]
            if item.definition.copy_from:
                create_sql.append(constants.FROM)
                create_sql.append(item.definition.copy_from)
            else:
                options = []
                if item.definition.locale:
                    options.append(' '.join(
                        [constants.LOCALE, '=', item.definition.locale]))
                if item.definition.lc_collate:
                    options.append('LC_COLLATE = {}'.format(
                        item.definition.lc_collate))
                if item.definition.lc_ctype:
                    options.append('LC_CTYPE = {}'.format(
                        item.definition.lc_ctype))
                if item.definition.provider:
                    options.append('PROVIDER = {}'.format(
                        item.definition.provider))
                if item.definition.deterministic:
                    options.append('DETERMINISTIC = {}'.format(
                        item.definition.deterministic))
                if item.definition.version:
                    options.append(' '.join(
                        [constants.OPTIONS, '=', item.definition.version]))
                create_sql.append('({})'.format(', '.join(options)))
            drop_sql = [constants.DROP, constants.COLLATION,
                        constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_conversion(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            create_sql = [constants.CREATE]
            if item.definition.default:
                create_sql.append(constants.DEFAULT)
            create_sql += [
                constants.CONVERSION,
                self._item_name(item),
                constants.FOR, item.definition.encoding_from,
                constants.TO, item.definition.encoding_to,
                constants.FROM, item.definition.function]
        drop_sql = [constants.DROP, constants.CONVERSION,
                    constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_domain(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            create_sql = [
                constants.CREATE, constants.DOMAIN, self._item_name(item),
                'AS', item.definition.data_type]
            if item.definition.collation:
                create_sql.append('COLLATE')
                create_sql.append(item.definition.collation)
            if item.definition.default:
                create_sql.append(constants.DEFAULT)
                create_sql.append(utils.postgres_value(
                    item.definition.default))
            if item.definition.check_constraints:
                constraints = []
                for c in item.definition.check_constraints:
                    value = [constants.CONSTRAINT]
                    if c.name:
                        value.append(c.name)
                    if c.nullable is not None:
                        value.append(
                            constants.NULL if c.nullable
                            else constants.NOT_NULL)
                    if c.expression:
                        value.append('CHECK ({})'.format(c.expression))
                    constraints.append(' '.join(value))
                create_sql.append(' '.join(constraints))
            drop_sql = [constants.DROP, constants.DOMAIN,
                        constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_event_trigger(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            create_sql = [constants.CREATE, constants.EVENT_TRIGGER,
                          self._item_name(item),
                          constants.ON, item.definition.event]
            if item.definition.filter:
                create_sql.append('WHEN TAG IN ({})'.format(
                    item.definition.filter.tags))
            create_sql.append(constants.EXECUTE)
            create_sql.append(constants.FUNCTION)
            create_sql.append(item.definition.function)
            drop_sql = [constants.DROP, constants.EVENT_TRIGGER,
                        constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_extension(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.EXTENSION,
                      constants.IF_NOT_EXISTS,
                      utils.quote_ident(item.definition.name)]
        if any([item.definition.schema, item.definition.version]):
            create_sql.append(constants.WITH)
        if item.definition.schema:
            create_sql += [constants.SCHEMA,
                           utils.quote_ident(item.definition.schema)]
        if item.definition.version:
            create_sql += [constants.VERSION,
                           utils.quote_ident(item.definition.version)]
        if item.definition.cascade:
            create_sql.append(constants.CASCADE)
        drop_sql = [constants.DROP, constants.EXTENSION,
                    constants.IF_EXISTS,
                    utils.quote_ident(item.definition.name)]
        self._add_item(item, create_sql, drop_sql, no_owner=True)

    def _dump_foreign_data_wrapper(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.FOREIGN_DATA_WRAPPER,
                      self._item_name(item)]
        if item.definition.handler:
            create_sql.append(constants.HANDLER)
            create_sql.append(item.definition.handler)
        else:
            create_sql += [constants.NO, constants.HANDLER]
        if item.definition.validator:
            create_sql += [constants.VALIDATOR, item.definition.validator]
        else:
            create_sql += [constants.NO, constants.VALIDATOR]
        if item.definition.options:
            create_sql.append('{} ({})'.format(
                constants.OPTIONS,
                ', '.join(['{} {}'.format(k, utils.postgres_value(v))
                           for k, v in item.definition.options.items()])))
        drop_sql = [constants.DROP, constants.FOREIGN_DATA_WRAPPER,
                    constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_function(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            func_name = item.definition.name
            if item.definition.parameters:
                params = []
                for param in item.definition.parameters:
                    value = [param.mode]
                    if param.name:
                        value.append(param.name)
                    value.append(param.data_type)
                    if param.default:
                        value.append('=')
                        value.append(param.default)
                    params.append(' '.join(value))
                func_name = '{}()'.format(
                    item.definition.name.split('(')[0], ', '.join(params))
            create_sql = [constants.CREATE, constants.FUNCTION, func_name,
                          constants.RETURNS, item.definition.returns,
                          constants.LANGUAGE, item.definition.language]
            drop_sql = [constants.DROP, constants.FUNCTION,
                        constants.IF_EXISTS, func_name]
            if item.definition.transform_types:
                tts = []
                for tt in item.definition.transform_types:
                    tts.append(' '.join([constants.FOR, constants.TYPE, tt]))
                create_sql.append('{} {}'.format(
                    constants.TRANSFORM, ', '.join(tts)))
            if item.definition.window:
                create_sql.append(constants.WINDOW)
            if item.definition.immutable:
                create_sql.append(constants.IMMUTABLE)
            if item.definition.stable:
                create_sql.append(constants.STABLE)
            if item.definition.volatile:
                create_sql.append(constants.VOLATILE)
            if item.definition.leak_proof is not None:
                if not item.definition.leak_proof:
                    create_sql.append(constants.NOT)
                create_sql.append(constants.LEAKPROOF)
            if item.definition.called_on_null_input:
                create_sql.append('CALLED ON NULL INPUT')
            elif item.definition.called_on_null_input is False:
                create_sql.append('RETURNS NULL ON NULL INPUT')
            if item.definition.strict:
                create_sql.append(constants.STRICT)
            if item.definition.security:
                create_sql += [constants.SECURITY, item.definition.security]
            if item.definition.parallel:
                create_sql += [constants.PARALLEL, item.definition.parallel]
            if item.definition.cost:
                create_sql += [constants.COST, str(item.definition.cost)]
            if item.definition.rows:
                create_sql += [constants.ROWS, str(item.definition.rows)]
            if item.definition.support:
                create_sql += [constants.SUPPORT, item.definition.support]
            if item.definition.configuration:
                for k, v in item.definition.configuration.items():
                    create_sql.append('{} {} = {}'.format(
                        constants.SET, k, utils.postgres_value(v)))
            create_sql.append(constants.AS)
            if item.definition.definition:
                create_sql = ['{} $$\n{}\n$$'.format(
                    ' '.join(create_sql), item.definition.definition)]
            elif item.definition.object_file and item.definition.link_symbol:
                create_sql.append('{}, {}'.format(
                    utils.postgres_value(item.definition.object_file),
                    utils.postgres_value(item.definition.link_symbol)))
        self._add_item(item, create_sql, drop_sql)

    def _dump_group(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.GROUP,
                      utils.quote_ident(item.definition.name)]
        if item.definition.options.create_db is not None:
            create_sql.append(self._format_bool_option(
                'CREATEDB', item.definition.options.create_db))
        if item.definition.options.create_role is not None:
            create_sql.append(self._format_bool_option(
                'CREATEROLE', item.definition.options.create_role))
        if item.definition.options.inherit is not None:
            create_sql.append(self._format_bool_option(
                'INHERIT', item.definition.options.inherit))
        if item.definition.options.superuser is not None:
            create_sql.append(self._format_bool_option(
                'SUPERUSER', item.definition.options.superuser))
        drop_sql = [constants.DROP, constants.GROUP, constants.IF_EXISTS,
                    utils.quote_ident(item.definition.name)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_index(self, index: models.Index,
                    parent: models.Item) -> typing.NoReturn:
        name = '{}.{}'.format(
            utils.quote_ident(parent.definition.schema),
            utils.quote_ident(index.name))
        create_sql = [constants.CREATE]
        if index.unique:
            create_sql.append(constants.UNIQUE)
        create_sql += [constants.INDEX, name, constants.ON]
        if index.recurse is False:
            create_sql.append(constants.ONLY)
        create_sql.append(self._item_name(parent))
        if index.method:
            create_sql.append(constants.USING)
            create_sql.append(index.method)
        create_sql.append('(')
        columns = [self._dump_index_column(c) for c in index.columns]
        create_sql.append(', '.join(columns))
        create_sql.append(')')
        if index.include:
            create_sql.append('INCLUDE ({})'.format(
                ', '.join(index.include)))
        if index.storage_parameters:
            create_sql.append(constants.WITH)
            sp_sql = []
            for key, value in index.storage_parameters.items():
                sp_sql.append('{}={}'.format(key, value))
            create_sql.append(', '.join(sp_sql))
        if index.tablespace:
            create_sql += [constants.TABLESPACE, index.tablespace]
        if index.where:
            create_sql += [constants.WHERE, index.where]
        drop_sql = [constants.DROP, constants.INDEX, constants.IF_EXISTS, name]
        entry = self._add_entry(
            constants.INDEX, parent.definition.schema, index.name,
            parent.definition.owner, create_sql, drop_sql,
            [self._dump_id_map[parent.id]], index.tablespace)
        if getattr(index, 'comment'):
            self._add_comment(
                constants.INDEX, parent.definition.schema, index.name,
                parent.definition.owner, entry.dump_id, None, index.comment)

    @staticmethod
    def _dump_index_column(column: models.IndexColumn) -> str:
        sql = [column.name or column.expression]
        if column.collation:
            sql += [constants.COLLATION, column.collation]
        if column.opclass:
            sql.append(column.opclass)
        if column.direction:
            sql.append(column.direction)
        if column.null_placement:
            sql += ['NULLS', column.null_placement]
        return ' '.join(sql)

    def _dump_materialized_view(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            create_sql = [constants.CREATE, constants.MATERIALIZED_VIEW,
                          self._item_name(item)]
            if item.definition.columns:
                create_sql.append('({})'.format(
                    ', '.join([c.name for c in item.definition.columns])))
            if item.definition.table_access_method:
                create_sql += [constants.USING,
                               item.definition.table_access_method]
            if item.definition.storage_parameters:
                create_sql.append(constants.WITH)
                params = []
                for key, value in item.definition.storage_parameters.items():
                    params.append('{} = {}'.format(key, value))
                create_sql.append(', '.join(params))
            if item.definition.tablespace:
                create_sql += [constants.TABLESPACE,
                               item.definition.tablespace]
            create_sql += [constants.AS, item.definition.query]
            drop_sql = [constants.DROP, constants.MATERIALIZED_VIEW,
                        constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_operator(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            name = '{}.{}'.format(item.definition.schema, item.definition.name)
            create_sql = [constants.CREATE, constants.OPERATOR, name]
            options = [' '.join(
                [constants.PROCEDURE, '=', item.definition.function])]
            if item.definition.left_arg:
                options.append('LEFTARG = {}'.format(
                    item.definition.left_arg))
            if item.definition.right_arg:
                options.append('RIGHTARG = {}'.format(
                    item.definition.right_arg))
            if item.definition.commutator:
                options.append('COMMUTATOR = {}'.format(
                    item.definition.commutator))
            if item.definition.negator:
                options.append('NEGATOR = {}'.format(
                    item.definition.negator))
            if item.definition.restrict:
                options.append('RESTRICT = {}'.format(
                    item.definition.restrict))
            if item.definition.join:
                options.append(' '.join(
                    [constants.JOIN, '=', item.definition.join]))
            if item.definition.hashes:
                options.append(constants.HASHES)
            if item.definition.merges:
                options.append(constants.MERGES)
            create_sql.append('({})'.format(', '.join(options)))
            drop_sql = [constants.DROP, constants.OPERATOR,
                        constants.IF_EXISTS, name,
                        '({}, {})'.format(item.definition.left_arg or 'NONE',
                                          item.definition.right_arg or 'NONE')]
        self._add_item(item, create_sql, drop_sql)

    def _dump_procedural_language(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE]
        if item.definition.replace:
            create_sql += [constants.OR, constants.REPLACE]
        if item.definition.trusted:
            create_sql.append(constants.TRUSTED)
        create_sql.append(constants.LANGUAGE)
        create_sql.append(self._item_name(item))
        if item.definition.handler:
            create_sql += [constants.HANDLER, item.definition.handler]
        if item.definition.inline_handler:
            create_sql += [constants.INLINE, item.definition.inline_handler]
        if item.definition.validator:
            create_sql += [constants.VALIDATOR, item.definition.validator]
        drop_sql = [constants.DROP, constants.LANGUAGE,
                    constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_publication(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.PUBLICATION,
                      self._item_name(item)]
        if item.definition.all_tables:
            create_sql.append('FOR ALL TABLES')
        else:
            create_sql += [constants.FOR, constants.TABLE,
                           ', '.join(item.definition.tables)]
        if item.definition.parameters:
            create_sql += [constants.WITH, self._format_parameters(item)]
        drop_sql = [constants.DROP, constants.PUBLICATION,
                    constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_role(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.ROLE,
                      utils.quote_ident(item.definition.name)]
        if item.definition.options.bypass_rls is not None:
            create_sql.append(self._format_bool_option(
                'BYPASSRLS', item.definition.options.create_db))
        if item.definition.options.connection_limit is not None:
            create_sql += ['CONNECTION LIMIT',
                           str(item.definition.options.connection_limit)]
        if item.definition.options.create_db is not None:
            create_sql.append(self._format_bool_option(
                'CREATEDB', item.definition.options.create_db))
        if item.definition.options.create_role is not None:
            create_sql.append(self._format_bool_option(
                'CREATEROLE', item.definition.options.create_role))
        if item.definition.options.inherit is not None:
            create_sql.append(self._format_bool_option(
                'INHERIT', item.definition.options.inherit))
        if item.definition.options.login is not None:
            create_sql.append(self._format_bool_option(
                'LOGIN', item.definition.options.login))
        if item.definition.options.superuser is not None:
            create_sql.append(self._format_bool_option(
                'SUPERUSER', item.definition.options.superuser))
        drop_sql = [constants.DROP, constants.ROLE, constants.IF_EXISTS,
                    utils.quote_ident(item.definition.name)]
        self._add_item(
            item, create_sql,
            drop_sql if item.definition.name != self.project.superuser else [])

    def _dump_schema(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.SCHEMA,
                      constants.IF_NOT_EXISTS, self._item_name(item)]
        if item.definition.authorization:
            create_sql.append(constants.AUTHORIZATION)
            create_sql.append(utils.quote_ident(item.definition.authorization))
        drop_sql = [constants.DROP, constants.SCHEMA, constants.IF_EXISTS,
                    self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_sequence(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            create_sql = [constants.CREATE, constants.SEQUENCE,
                          self._item_name(item)]
            if item.definition.data_type:
                create_sql.append(constants.AS)
                create_sql.append(item.definition.data_type)
            if item.definition.increment_by:
                create_sql.append('INCREMENT BY')
                create_sql.append(str(item.definition.increment_by))
            if item.definition.min_value:
                create_sql.append('MINVALUE')
                create_sql.append(str(item.definition.min_value))
            if item.definition.max_value:
                create_sql.append('MAXVALUE')
                create_sql.append(str(item.definition.max_value))
            if item.definition.start_with:
                create_sql.append('START WITH')
                create_sql.append(str(item.definition.start_with))
            if item.definition.cache:
                create_sql.append('CACHE')
                create_sql.append(str(item.definition.cache))
            if item.definition.cycle is not None:
                if not item.definition.cycle:
                    create_sql.append(constants.NO)
                create_sql.append('CYCLE')
            if item.definition.owned_by:
                create_sql.append('OWNED BY')
                create_sql.append(item.definition.owned_by)
            drop_sql = [constants.DROP, constants.SEQUENCE,
                        constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_server(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.SERVER,
                      self._item_name(item)]
        if item.definition.type:
            create_sql += [constants.TYPE,
                           utils.postgres_value(item.definition.type)]
        if item.definition.version:
            create_sql += [constants.VERSION,
                           utils.postgres_value(item.definition.version)]
        create_sql += [constants.FOREIGN_DATA_WRAPPER,
                       item.definition.foreign_data_wrapper]
        if item.definition.options:
            options = []
            for k, v in item.definition.options.items():
                options.append('{} {}'.format(k, utils.postgres_value(v)))
            create_sql.append('{} {}'.format(
                constants.OPTIONS, ', '.join(options)))
        drop_sql = [constants.DROP, constants.SERVER, constants.IF_EXISTS,
                    self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_subscription(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.SUBSCRIPTION,
                      self._item_name(item), 'CONNECTION',
                      item.definition.connection, constants.PUBLICATION,
                      ', '.join(item.definition.publications)]
        if item.definition.parameters:
            create_sql += [constants.WITH, self._format_parameters(item)]
        drop_sql = [constants.DROP, constants.SUBSCRIPTION,
                    constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_table(self, item: models.TableItem) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            create_sql = [constants.CREATE]
            if item.definition.unlogged:
                create_sql.append('UNLOGGED')
            create_sql.append(constants.TABLE)
            create_sql.append(self._item_name(item))
            create_sql.append('(')
            if item.definition.like_table:
                create_sql.append('LIKE')
                create_sql.append(item.definition.like_table.name)
                for field in self._dump_table_like_table_fields(item):
                    create_sql.append(
                        'INCLUDING'
                        if getattr(item.definition.like_table, field)
                        else 'EXCLUDING')
                    create_sql.append(field)
            else:
                inner_sql = []
                if item.definition.columns:
                    for column in item.definition.columns:
                        inner_sql.append(self._dump_table_column(column))
                for item in item.definition.unique_constraints or []:
                    inner_sql.append(
                        self._format_sql_constraint(constants.UNIQUE, item))
                if item.definition.primary_key:
                    inner_sql.append(self._format_sql_constraint(
                        'PRIMARY KEY', item.definition.primary_key))
                for fk in item.definition.foreign_keys or []:
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
                create_sql.append(', '.join(inner_sql))
                create_sql.append(')')
            if item.definition.parents:
                create_sql.append(', '.join(item.definition.parents))
            if item.definition.partition:
                create_sql.append('PARTITION BY')
                create_sql.append(item.definition.partition.type)
                create_sql.append('(')
                columns = []
                for col in item.definition.partition.columns:
                    column = [col.name or col.expression]
                    if col.collation:
                        column += [constants.COLLATION, col.collation]
                    if col.opclass:
                        column.append(col.opclass)
                create_sql.append(', '.join(columns))
                create_sql.append(')')
            if item.definition.access_method:
                create_sql += [constants.USING, item.definition.access_method]
            if item.definition.storage_parameters:
                create_sql.append(constants.WITH)
                params = []
                for key, value in item.definition.storage_parameters.items():
                    params.append('{}={}'.format(key, value))
                create_sql.append(', '.join(params))
            if item.definition.tablespace:
                create_sql.append(constants.TABLESPACE)
                create_sql.append(item.definition.tablespace)
            drop_sql = [constants.DROP, constants.TABLE,
                        constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)
        for index in item.definition.indexes or []:
            self._dump_index(index, item)
        for trigger in item.definition.triggers or []:
            self._dump_trigger(trigger, item)

    @staticmethod
    def _dump_table_column(column: models.Column) -> str:
        sql = [column.name, column.data_type]
        if column.collation:
            column += [constants.COLLATION, column.collation]
        if not column.nullable:
            sql.append(constants.NOT_NULL)
        if column.check_constraint:
            sql += [constants.CHECK, column.check_constraint]
        if column.default:
            sql += [constants.DEFAULT, utils.postgres_value(column.default)]
        if column.generated and column.generated.expression:
            sql.append('GENERATED ALWAYS AS')
            sql.append(column.generated.expression)
            sql.append('STORED')
        elif column.generated and column.generated.sequence:
            sql.append('GENERATED')
            sql.append(column.generated.sequence_behavior)
            sql.append('AS IDENTITY')
        return ' '.join(sql)

    @staticmethod
    def _dump_table_like_table_fields(item: models.Item) -> list:
        return [f for f in dataclasses.fields(item.definition.like_table)
                if f.startswith('include_')
                and getattr(item.definition.like_table, f) is not None]

    def _dump_tablespace(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.TABLESPACE,
                      self._item_name(item),
                      constants.OWNER, item.definition.owner,
                      constants.LOCATION, item.definition.location]
        if item.definition.options:
            options = []
            for k, v in item.definition.options.items():
                options.append('{}={}'.format(k, utils.postgres_value(v)))
            create_sql.append('{} ({})'.format(
                constants.WITH, ','.join(options)))
        drop_sql = [constants.DROP, constants.TABLESPACE,
                    constants.IF_EXISTS, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_text_search(self, item: models.Item) -> typing.NoReturn:
        for config in item.definition.configurations or []:
            if config.sql:
                create_sql, drop_sql = [config.sql], []
            else:
                if config.parser:
                    value = ['PARSER', '=', config.parser]
                elif config.source:
                    value = ['SOURCE', '=', config.source]
                else:
                    raise RuntimeError
                create_sql = [
                    constants.CREATE, constants.TEXT_SEARCH_CONFIGURATION,
                    utils.quote_ident(config.name), '({})'.format(value)]
                drop_sql = [constants.DROP,
                            constants.TEXT_SEARCH_CONFIGURATION,
                            constants.IF_EXISTS,
                            utils.quote_ident(config.name)]
            self._add_text_search_item(
                item, constants.TEXT_SEARCH_CONFIGURATION,
                create_sql, drop_sql, config.comment)

        for dictionary in item.definition.dictionaries or []:
            if dictionary.sql:
                create_sql, drop_sql = [dictionary.sql], []
            else:
                value = [' '.join(['TEMPLATE', '=', dictionary.template])]
                if dictionary.options:
                    for k, v in dictionary.options.items():
                        value.append('{} = {}'.format(
                            k, utils.postgres_value(v)))
                create_sql = [
                    constants.CREATE, constants.TEXT_SEARCH_DICTIONARY,
                    utils.quote_ident(dictionary.name),
                    '({})'.format(', '.join(value))]
                drop_sql = [constants.DROP, constants.TEXT_SEARCH_DICTIONARY,
                            constants.IF_EXISTS,
                            utils.quote_ident(dictionary.name)]
            self._add_text_search_item(
                item, constants.TEXT_SEARCH_DICTIONARY, create_sql, drop_sql,
                dictionary.comment)

        for parser in item.definition.parsers or []:
            if parser.sql:
                create_sql, drop_sql = [parser.sql], []
            else:
                value = [' '.join(['START', '=', parser.start_function]),
                         ' '.join(
                             ['GETTOKEN', '=', parser.gettoken_function]),
                         ' '.join(['END', '=', parser.end_function]),
                         ' '.join(
                             ['LEXTYPES', '=', parser.lextypes_function])]
                if parser.headline_function:
                    value.append(' '.join(
                        ['HEADLINE', '=', parser.headline_function]))
                create_sql = [constants.CREATE, constants.TEXT_SEARCH_PARSER,
                              utils.quote_ident(parser.name),
                              '({})'.format(', '.join(value))]
                drop_sql = [constants.DROP, constants.TEXT_SEARCH_PARSER,
                            constants.IF_EXISTS,
                            utils.quote_ident(parser.name)]
            self._add_text_search_item(
                item, constants.TEXT_SEARCH_PARSER, create_sql, drop_sql,
                parser.comment)

        for template in item.definition.templates or []:
            if template.sql:
                create_sql, drop_sql = [template.sql], []
            else:
                value = []
                if template.init_function:
                    value.append(
                        ' '.join(['INIT', '=', template.init_function]))
                value.append(
                    ' '.join(['LEXIZE', '=', template.lexize_function]))
                create_sql = [constants.CREATE, constants.TEXT_SEARCH_TEMPLATE,
                              utils.quote_ident(template.name),
                              '({})'.format(', '.join(value))]
                drop_sql = [constants.DROP, constants.TEXT_SEARCH_TEMPLATE,
                            constants.IF_EXISTS,
                            utils.quote_ident(template.name)]
            self._add_text_search_item(
                item, constants.TEXT_SEARCH_TEMPLATE, create_sql, drop_sql,
                template.comment)

    def _dump_trigger(self, trigger: models.Trigger,
                      parent: models.TableItem) -> typing.NoReturn:
        if trigger.sql:
            create_sql, drop_sql = [trigger.sql], []
        else:
            create_sql = [
                constants.CREATE, constants.TRIGGER,
                trigger.name, trigger.when,
                ' {} '.format(constants.OR).join(trigger.events),
                constants.ON, self._item_name(parent)]
            if trigger.for_each:
                create_sql += [constants.FOR_EACH, trigger.for_each]
            if trigger.condition:
                create_sql += [constants.WHEN, trigger.condition]
            create_sql += [constants.EXECUTE, constants.FUNCTION,
                           trigger.function]
            if trigger.arguments:
                create_sql.append('({})'.format(
                    ', '.join([str(a) for a in trigger.arguments])))
            drop_sql = [constants.DROP, constants.TRIGGER, constants.IF_EXISTS,
                        trigger.name, constants.ON, self._item_name(parent)]
        entry = self._add_entry(
            constants.TRIGGER, parent.definition.schema, trigger.name,
            parent.definition.owner, create_sql, drop_sql,
            [self._dump_id_map[parent.id]])
        if getattr(trigger, 'comment'):
            self._add_comment(
                constants.TRIGGER, parent.definition.schema, trigger.name,
                parent.definition.owner, entry.dump_id, None, trigger.comment)

    def _dump_type(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            create_sql = [constants.CREATE, constants.TYPE,
                          self._item_name(item), constants.AS]
            if item.definition.type == 'base':
                options = [
                    'INPUT = {}'.format(item.definition.input),
                    'OUTPUT = {}'.format(
                        item.definition.output)]
                if item.definition.receive:
                    options.append('RECEIVE = {}'.format(
                        item.definition.receive))
                if item.definition.send:
                    options.append('SEND = {}'.format(
                        item.definition.receive))
                if item.definition.typmod_in:
                    options.append('TYPMOD_IN = {}'.format(
                        item.definition.typmod_in))
                if item.definition.typmod_out:
                    options.append('TYPMOD_OUT = {}'.format(
                        item.definition.typmod_out))
                if item.definition.analyze:
                    options.append('ANALYZE = {}'.format(
                        item.definition.analyze))
                if item.definition.internal_length:
                    options.append('INTERNALLENGTH = {}'.format(
                        item.definition.internal_length))
                if item.definition.passed_by_value:
                    options.append('PASSEDBYVALUE')
                if item.definition.alignment:
                    options.append('ALIGNMENT = {}'.format(
                        item.definition.alignment))
                if item.definition.storage:
                    options.append('STORAGE = {}'.format(
                        item.definition.storage))
                if item.definition.like_type:
                    options.append('LIKE = {}'.format(
                        item.definition.like_type))
                if item.definition.category:
                    options.append('CATEGORY = {}'.format(
                        utils.postgres_value(
                            item.definition.category)))
                if item.definition.preferred:
                    options.append('PREFERRED = {}'.format(
                        item.definition.preferred))
                if item.definition.default:
                    options.append('DEFAULT = {}'.format(
                        utils.postgres_value(
                            item.definition.default)))
                if item.definition.element:
                    options.append('ELEMENT = {}'.format(
                        item.definition.element))
                if item.definition.delimiter:
                    options.append('DELIMITER = {}'.format(
                        utils.postgres_value(
                            item.definition.delimiter)))
                if item.definition.collatable:
                    options.append('COLLATABLE = {}'.format(
                        item.definition.collatable))
                create_sql.append('({})'.format(', '.join(options)))
            elif item.definition.type == 'composite':
                columns = []
                for column in item.definition.columns:
                    col = [column.name, column.data_type]
                    if column.collation:
                        col.append('COLLATE')
                        col.append(column.collation)
                    columns.append(' '.join(col))
                create_sql.append('({})'.format(', '.join(columns)))
            elif item.definition.type == 'enum':
                create_sql.append('ENUM')
                create_sql.append('({})'.format(', '.join(
                    utils.postgres_value(e) for e in
                    item.definition.enum)))
            elif item.definition.type == 'range':
                create_sql.append('RANGE')
                options = ['SUBTYPE = {}'.format(
                    item.definition.subtype)]
                if item.definition.subtype_opclass:
                    options.append('SUBTYPE_OPCLASS = {}'.format(
                        item.definition.subtype))
                if item.definition.collation:
                    options.append('COLLATION = {}'.format(
                        item.definition.collation))
                if item.definition.collation:
                    options.append('CANONICAL = {}'.format(
                        item.definition.canonical))
                if item.definition.subtype_diff:
                    options.append('SUBTYPE_DIFF = {}'.format(
                        item.definition.subtype_diff))
                create_sql.append('({})'.format(', '.join(options)))
            drop_sql = [constants.DROP, constants.TYPE, constants.IF_EXISTS,
                        self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_user(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.USER,
                      utils.quote_ident(item.definition.name)]
        if item.definition.options.bypass_rls is not None:
            create_sql.append(self._format_bool_option(
                'BYPASSRLS', item.definition.options.create_db))
        if item.definition.options.connection_limit is not None:
            create_sql += ['CONNECTION LIMIT',
                           str(item.definition.options.connection_limit)]
        if item.definition.options.create_db is not None:
            create_sql.append(self._format_bool_option(
                'CREATEDB', item.definition.options.create_db))
        if item.definition.options.create_role is not None:
            create_sql.append(self._format_bool_option(
                'CREATEROLE', item.definition.options.create_role))
        if item.definition.options.inherit is not None:
            create_sql.append(self._format_bool_option(
                'INHERIT', item.definition.options.inherit))
        create_sql.append('LOGIN')
        if item.definition.options.superuser is not None:
            create_sql.append(self._format_bool_option(
                'SUPERUSER', item.definition.options.superuser))
        if item.definition.valid_until:
            create_sql.append('VALID UNTIL')
            create_sql.append(
                utils.postgres_value(item.definition.valid_until))
        if item.definition.password is None:
            create_sql.append('PASSWORD NULL')
        elif item.definition.password:
            if item.definition.password.startswith('md5'):
                create_sql.append('ENCRYPTED')
            create_sql.append('PASSWORD')
            create_sql.append(utils.postgres_value(item.definition.password))
        drop_sql = [constants.DROP, constants.USER, constants.IF_EXISTS,
                    utils.quote_ident(item.definition.name)]
        self._add_item(
            item, create_sql,
            drop_sql if item.definition.name != self.project.superuser else [])

    def _dump_user_mapping(self, item: models.Item) -> typing.NoReturn:
        for server in item.definition.servers:
            create_sql = [
                constants.CREATE, constants.USER_MAPPING, constants.FOR,
                utils.quote_ident(item.definition.name), constants.SERVER,
                utils.quote_ident(server.name)]
            if server.options:
                opts = []
                for k, v in server.options.items():
                    opts.append('{} {}'.format(k, utils.postgres_value(v)))
                create_sql.append('{} ({})'.format(
                    constants.OPTIONS, ','.join(opts)))
            drop_sql = [
                constants.DROP, constants.USER_MAPPING, constants.IF_EXISTS,
                constants.FOR, utils.quote_ident(item.definition.name),
                constants.SERVER, utils.quote_ident(server.name)]
            self._add_item(item, create_sql, drop_sql)

    def _dump_view(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql, drop_sql = [item.definition.sql], []
        else:
            create_sql = [constants.CREATE]
            if item.definition.recursive:
                create_sql.append(constants.RECURSIVE)
            create_sql.append(constants.VIEW)
            create_sql.append(self._item_name(item))
            if item.definition.columns:
                create_sql.append('({})'.format(
                    ', '.join([c.name for c in item.definition.columns])))
            if item.definition.check_option:
                create_sql.append('WITH check_option = ')
                create_sql.append(item.definition.check_option)
            if item.definition.security_barrier:
                create_sql.append('WITH security_barrier = ')
                create_sql.append(item.definition.security_barrier)
            create_sql.append(constants.AS)
            create_sql.append(item.definition.query)
            drop_sql = [constants.DROP, constants.VIEW, constants.IF_EXISTS,
                        self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    @staticmethod
    def _format_bool_option(name: str, value: typing.Optional[bool]) -> str:
        return name if value else 'NO{}'.format(name)

    @staticmethod
    def _format_parameters(item: models.Item) -> str:
        params = []
        for k, v in item.definition.parameters.items():
            params.append('{} = {}'.format(k, utils.postgres_value(v)))
        return ', '.join(params)

    @staticmethod
    def _format_sql_constraint(constraint_type: str,
                               constraint: models.ConstraintColumns) -> str:
        sql = ['{} ({})'.format(constraint_type,
                                ', '.join(constraint.columns))]
        if constraint.include:
            sql.append(', '.join(constraint.include))
        return ' '.join(sql)

    @staticmethod
    def _item_name(item: models.Item) -> str:
        if getattr(item.definition, 'schema', None):
            return '{}.{}'.format(
                utils.quote_ident(item.definition.schema),
                utils.quote_ident(item.definition.name))
        return utils.quote_ident(item.definition.name)
