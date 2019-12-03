"""
Used by pgdumplib.project.Project to build a Dump artifact

"""
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

    def _add_comment(self,
                     desc: str,
                     namespace: str,
                     tag: str,
                     owner: str,
                     parent_dump_id: int,
                     parent_tag: typing.Optional[str],
                     comment: str):
        create_sql = [constants.COMMENT, constants.ON, desc]
        drop_sql = [constants.DROP, constants.COMMENT, constants.ON, desc]
        if parent_tag:
            sql = ['{}.{}'.format(namespace, tag), constants.ON, parent_tag]
            create_sql += sql
            drop_sql += sql
        elif namespace:
            create_sql.append('{}.{}'.format(namespace, tag))
            drop_sql.append('{}.{}'.format(namespace, tag))
        else:
            create_sql.append(tag)
            drop_sql.append(tag)
        create_sql.append(constants.IS)
        create_sql.append('$${}$$;\n'.format(comment))
        self._add_entry(
            constants.COMMENT, namespace, tag, owner, create_sql, drop_sql,
            [parent_dump_id])

    def _add_entry(self, desc: str, namespace: str, name: str, owner: str,
                   defn: typing.List[str],
                   drop_stmt: typing.List[str],
                   dependencies: typing.Optional[typing.List[int]] = None,
                   tablespace: typing.Optional[str] = None) \
            -> pgdumplib.dump.Entry:
        return self._dump.add_entry(
            desc, namespace, name, owner,
            '{};\n'.format(' '.join(defn)),
            '{};\n'.format(' '.join(drop_stmt)),
            dependencies=dependencies or [],
            tablespace=tablespace)

    def _add_item(self, item: models.Item, defn: typing.List[str],
                  drop_stmt: typing.List[str]) -> typing.NoReturn:
        entry = self._add_entry(
            item.desc,
            getattr(item.definition, 'schema', ''),
            item.definition.name,
            getattr(item.definition, 'owner', self.project.superuser),
            defn, drop_stmt,
            getattr(item.definition, 'tablespace', None))
        self._dump_id_map[item.id] = entry.dump_id
        if getattr(item.definition, 'comment'):
            self._add_comment(
                item.desc,
                getattr(item.definition, 'schema', ''),
                item.definition.name,
                item.definition.owner, entry.dump_id, None,
                item.definition.comment)

    def _dump_aggregate(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql = [item.definition.sql]
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
        drop_sql = [constants.DROP, constants.AGGREGATE, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_cast(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql = [item.definition.sql]
        else:
            create_sql = [constants.CREATE, constants.CAST,
                          self._item_name(item)]
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
        drop_sql = [constants.DROP, constants.CAST, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_collation(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql = [item.definition.sql]
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
        drop_sql = [constants.DROP, constants.COLLATION, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_conversion(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql = [item.definition.sql]
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
                    self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_domain(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql = [item.definition.sql]
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
        drop_sql = [constants.DROP, constants.DOMAIN, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_event_trigger(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql = [item.definition.sql]
        else:
            create_sql = [constants.CREATE, constants.EVENT_TRIGGER,
                          constants.ON, item.definition.event]
            if item.definition.filter:
                create_sql.append('WHEN TAG IN ({})'.format(
                    item.definition.filter.tags))
            create_sql.append(constants.EXECUTE)
            create_sql.append(constants.FUNCTION)
            create_sql.append(item.definition.function)
        drop_sql = [constants.DROP, constants.EVENT_TRIGGER,
                    constants.ON, item.definition.event]
        self._add_item(item, create_sql, drop_sql)

    def _dump_extension(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.EXTENSION,
                      constants.IF_NOT_EXISTS,
                      self._item_name(item)]
        drop_sql = [constants.DROP, constants.EXTENSION,
                    self._item_name(item)]
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
        self._add_item(item, create_sql, drop_sql)

    def _dump_foreign_data_wrapper(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.FOREIGN_DATA_WRAPPER,
                      self._item_name(item)]
        drop_sql = [constants.CREATE, constants.FOREIGN_DATA_WRAPPER,
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
        self._add_item(item, create_sql, drop_sql)

    def _dump_function(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            sql = [item.definition.sql]
            drop_sql = ''
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
            sql = [constants.CREATE, constants.FUNCTION, func_name,
                   constants.RETURNS, item.definition.returns,
                   constants.LANGUAGE, item.definition.language]
            drop_sql = [constants.DROP, constants.FUNCTION, func_name]
            if item.definition.transform_types:
                tts = []
                for tt in item.definition.transform_types:
                    tts.append(' '.join([constants.FOR, constants.TYPE, tt]))
                sql.append('{} {}'.format(constants.TRANSFORM, ', '.join(tts)))
            if item.definition.window:
                sql.append(constants.WINDOW)
            if item.definition.immutable:
                sql.append(constants.IMMUTABLE)
            if item.definition.stable:
                sql.append(constants.STABLE)
            if item.definition.volatile:
                sql.append(constants.VOLATILE)
            if item.definition.leak_proof is not None:
                if not item.definition.leak_proof:
                    sql.append(constants.NOT)
                sql.append(constants.LEAKPROOF)
            if item.definition.called_on_null_input:
                sql.append('CALLED ON NULL INPUT')
            elif item.definition.called_on_null_input is False:
                sql.append('RETURNS NULL ON NULL INPUT')
            if item.definition.strict:
                sql.append(constants.STRICT)
            if item.definition.security:
                sql += [constants.SECURITY, item.definition.security]
            if item.definition.parallel:
                sql += [constants.PARALLEL, item.definition.parallel]
            if item.definition.cost:
                sql += [constants.COST, str(item.definition.cost)]
            if item.definition.rows:
                sql += [constants.ROWS, str(item.definition.rows)]
            if item.definition.support:
                sql += [constants.SUPPORT, item.definition.support]
            if item.definition.configuration:
                for k, v in item.definition.configuration.items():
                    sql.append('{} {} = {}'.format(
                        constants.SET, k, utils.postgres_value(v)))
            sql.append(constants.AS)
            if item.definition.definition:
                sql = ['{} $$\n{}\n$$'.format(
                    ' '.join(sql), item.definition.definition)]
            elif item.definition.object_file and item.definition.link_symbol:
                sql.append('{}, {}'.format(
                    utils.postgres_value(item.definition.object_file),
                    utils.postgres_value(item.definition.link_symbol)))
        self._add_item(item, sql, drop_sql)

    def _dump_index(self, item: models.Item, parent: str) -> typing.NoReturn:
        create_sql = [constants.CREATE]
        if item.definition.unique:
            create_sql.append(constants.UNIQUE)
        create_sql += [constants.INDEX, item.definition.name, constants.ON]
        if item.definition.recurse is False:
            create_sql.append(constants.ONLY)
        create_sql.append(parent)
        if item.definition.method:
            create_sql.append(constants.USING)
            create_sql.append(item.definition.method)
        create_sql.append('(')
        columns = []
        for col in item.definition.columns:
            column = [col.name or col.expression]
            if col.collation:
                column += [constants.COLLATION, col.collation]
            if col.opclass:
                column.append(col.opclass)
            if col.direction:
                column.append(col.direction)
            if col.null_placement:
                column += ['NULLS', col.null_placement]
        create_sql.append(', '.join(columns))
        create_sql.append(')')
        if item.definition.include:
            create_sql.append('INCLUDE ({})'.format(
                ', '.join(item.definition.include)))
        if item.definition.storage_parameters:
            create_sql.append(constants.WITH)
            sp_sql = []
            for key, value in item.definition.storage_parameters.items():
                sp_sql.append('{}={}'.format(key, value))
            create_sql.append(', '.join(sp_sql))
        if item.definition.tablespace:
            create_sql += [constants.TABLESPACE, item.definition.tablespace]
        if item.definition.where:
            create_sql += [constants.WHERE, item.definition.where]
        drop_sql = [constants.DROP, constants.INDEX, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_operator(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql = [item.definition.sql.rstrip('; ')]
        else:
            create_sql = [constants.CREATE, constants.OPERATOR,
                          self._item_name(item)]
            options = [' '.join(
                [constants.FUNCTION, '=', item.definition.function])]
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
        drop_sql = [constants.DROP, constants.OPERATOR, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_procedural_language(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE]
        drop_sql = [constants.DROP,
                    constants.LANGUAGE,
                    self._item_name(item)]
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
        self._add_item(item, create_sql, drop_sql)

    def _dump_schema(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.SCHEMA,
                      constants.IF_NOT_EXISTS, self._item_name(item)]
        if item.definition.authorization:
            create_sql.append(constants.AUTHORIZATION)
            create_sql.append(utils.quote_ident(item.definition.authorization))
        drop_sql = [constants.CREATE, constants.SCHEMA, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_tablespace(self, item: models.Item) -> typing.NoReturn:
        create_sql = [constants.CREATE, constants.TABLESPACE,
                      utils.quote_ident(item.definition.name),
                      constants.OWNER, item.definition.owner,
                      constants.LOCATION, item.definition.location]
        if item.definition.options:
            options = []
            for k, v in item.definition.options.items():
                options.append('{}={}'.format(k, utils.postgres_value(v)))
            create_sql.append('{} ({})'.format(
                constants.WITH, ','.join(options)))
        drop_sql = [constants.DROP, constants.TABLESPACE,
                    utils.quote_ident(item.definition.name)]
        self._add_item(item, create_sql, drop_sql)

    def _dump_type(self, item: models.Item) -> typing.NoReturn:
        if item.definition.sql:
            create_sql = [item.definition.sql]
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
        drop_sql = [constants.DROP, constants.TYPE, self._item_name(item)]
        self._add_item(item, create_sql, drop_sql)

    @staticmethod
    def _item_name(item: models.Item) -> str:
        if getattr(item.definition, 'schema', None):
            return '{}.{}'.format(
                utils.quote_ident(item.definition.schema),
                utils.quote_ident(item.definition.name))
        return utils.quote_ident(item.definition.name)
