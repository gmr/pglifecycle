"""
Restructure parsed SQL generated from pgparse/libpg_query

"""
import logging
import typing

import stringcase

from pglifecycle import constants

LOGGER = logging.getLogger(__name__)


def from_libpg_query(node: list) -> list:
    """Return a data structure that is pgpretty formatter compatible"""
    return Reformatter().reformat(node)


class Reformatter:
    """Class used to reformat libpg_query generated data structures"""

    def reformat(self, node: typing.Union[dict, int, list, str, None]) -> \
            typing.Union[dict, int, list, str, None]:
        """Reformat the node to generate a pgpretty data structure"""
        if isinstance(node, dict):
            for key in node.keys():
                name = '_{}'.format(stringcase.snakecase(key))
                LOGGER.debug('%s(%r)', name, node)
                meth = getattr(self, name, None)
                if meth is None:
                    msg = '{} ({}) is an unsupported node type'.format(
                        key, name)
                    raise RuntimeError(msg)
                return meth(node[key])
        elif isinstance(node, list):
            return [self.reformat(n) for n in node]
        elif isinstance(node, (int, str)):
            return node
        elif node is None:
            return None
        else:
            raise ValueError('Unsupported type: {}'.format(type(node)))

    def _a__array_expr(self, node: dict) -> str:
        return 'ARRAY[{}]'.format(
            ', '.join(str(self.reformat(e)) for e in node['elements']))

    def _a__const(self, node: dict) -> typing.Union[int, str, None]:
        node = self.reformat(node['val'])
        if isinstance(node, str):
            return repr(node)
        return node

    def _a__expr(self, node: dict) -> list:
        lexpr = self._a__expr_side(node['lexpr'])
        rexpr = self._a__expr_side(node['rexpr'])
        if node['kind'] == 0:
            return [lexpr, ''.join(self.reformat(node['name'])), rexpr]
        elif node['kind'] in {1, 2}:
            return [
                lexpr,
                self.reformat(node['name']),
                constants.A_EXPR_KIND[node['kind']],
                '({})'.format(rexpr)]
        elif node['kind'] in {3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14}:
            return [lexpr, constants.A_EXPR_KIND[node['kind']], rexpr]
        elif node['kind'] == 5:
            return ['NULLIF', lexpr, rexpr]
        LOGGER.error('Unsupported A_Expr: %r', node)
        raise RuntimeError

    def _a__expr_side(self, node: dict) -> str:
        if 'A_Expr' in node:
            lexpr = 'A_Expr' in node['A_Expr']['lexpr']
            rexpr = 'A_Expr' in node['A_Expr']['rexpr']
            if lexpr and rexpr:
                return '({})'.format(self.reformat(node))
        return self.reformat(node)

    def _a__indices(self, node: dict) -> str:
        if node.get('is_slice'):
            return '[{}:{}]'.format(
                self.reformat(node['lidx']), self.reformat(node['uidx']))
        if 'lidx' not in node and 'uidx' in node:
            return '[{}]'.format(self.reformat(node['uidx']))
        raise RuntimeError

    @staticmethod
    def _a__star(_node: dict) -> str:
        return '*'

    @staticmethod
    def _alias(node: dict) -> str:
        return node['aliasname']

    def _alter_table_cmd(self, node: dict) -> dict:
        operation = constants.AlterTableType(
            node['subtype']).name.replace('_', ' ')

        if node['subtype'] == constants.AlterTableType.ADD_COLUMN:
            command = {
                'operation': operation,
                'column': self.reformat(node['def'])
            }
        elif node['subtype'] == constants.AlterTableType.COLUMN_DEFAULT:
            if 'def' not in node:
                command = {
                    'operation': 'DROP DEFAULT',
                    'column': node['name']
                }
            else:
                command = {
                    'operation': 'SET DEFAULT',
                    'column': node['name'],
                    'value': self.reformat(node['def'])
                }
        elif node['subtype'] == constants.AlterTableType.DROP_COLUMN:
            command = {
                'operation': operation,
                'column': node['name'],
                'cascade': node['behavior'] == 1
            }
        elif node['subtype'] == constants.AlterTableType.ALTER_COLUMN_TYPE:
            column = {'name': node['name']}
            column.update(self.reformat(node['def']))
            command = {
                'operation': operation,
                'column': column
            }
        elif node['subtype'] in [constants.AlterTableType.DROP_NOT_NULL,
                                 constants.AlterTableType.SET_NOT_NULL,
                                 constants.AlterTableType.DROP_CONSTRAINT,
                                 constants.AlterTableType.CLUSTER_ON]:
            command = {
                'operation': operation,
                'column': node['name']
            }
        else:
            command = {
                'operation': operation,
                'definition': self.reformat(node['def'])
            }
        return command

    def _alter_table_stmt(self, node: dict) -> dict:
        return {
            'stmt_type': '{} {}'.format(constants.ALTER, constants.TABLE),
            'relation': self.reformat(node['relation']),
            'commands': self.reformat(node['cmds'])
        }

    def _bool_expr(self, node: dict) -> list:
        LOGGER.debug('BoolExpr: %r, %r',
                     node, constants.BOOL_OP[node['boolop']])
        if len(node['args']) == 1:
            return [
                constants.BOOL_OP[node['boolop']],
                self.reformat(node['args'])
            ]
        output = []
        for value in self._ensure_list(self.reformat(node['args'])):
            output.append(value)
            output.append(constants.BOOL_OP[node['boolop']])
        return output[:-1]

    def _boolean_test(self, node: dict) -> str:
        return '{} IS {}'.format(self.reformat(node['arg']),
                                 constants.BOOL_TEST[node['booltesttype']])

    @staticmethod
    def _capitalize_keywords(node: list) -> list:
        return [v.upper() if v in ['excluded', 'old', 'new'] else v
                for v in node]

    def _column_def(self, node: dict):
        temp = self._ensure_list(self.reformat(node.get('constraints', [])))
        constraints = {}
        for key in {'default', 'nullable', 'primary_key'}:
            constraints[key] = [c.get(key) for c in temp if key in c]
            if constraints[key]:
                constraints[key] = constraints[key][0]
                temp = [c for c in temp if key not in c]
        if len(temp) > 1:
            LOGGER.error('Temp: %r', temp)
            raise ValueError
        nullable = constraints.get('nullable', []) == []
        primary_key = constraints.get('primary_key', []) != []
        column = {
            'name': node.get('colname'),
            'type': self.reformat(node['typeName']),
            'default': constraints.get('default') or None,
            'nullable': nullable if node.get('colname') else None,
            'constraint': temp[0] if temp else None,
            'is_local': node.get('is_local'),
            'primary_key': primary_key if node.get('colname') else None
        }
        if 'raw_default' in node and node['raw_default']:
            column['using'] = self.reformat(node['raw_default'])

        for key, value in list(column.items()):
            if value is None:
                del column[key]
        return column

    def _column_ref(self, node: dict):
        if 'fields' in node:
            fields = self._ensure_list(self.reformat(node['fields']))
            return '.'.join(self._capitalize_keywords(fields))
        LOGGER.error('Unsupported ColumnRef: %r', node)
        raise RuntimeError

    def _comment_stmt(self, node: dict) -> dict:
        return {
            'type': constants.ObjectType(node['objtype']).name,
            'name': '.'.join(self.reformat(node['object'])),
            'comment': node['comment']
        }

    def _common_table_expr(self, node: dict) -> dict:
        if 'aliascolnames' in node:
            name = '{}({})'.format(
                node['ctename'],
                ', '.join(self.reformat(node['aliascolnames'])))
        else:
            name = node['ctename']
        return {
            'name': name,
            'query': self.reformat(node['ctequery'])
        }

    def _composite_type_stmt(self, node: dict) -> dict:
        self.typevar_ = {
            'stmt_type': constants.TYPE,
            'name': self.reformat(node['typevar'])
        }
        output = self.typevar_
        if 'coldeflist' in node:
            output['attributes'] = [
                {'name': e['name'], 'type': e['type']}
                for e in self._ensure_list(
                    self.reformat(node['coldeflist']))]
        return output

    def _constraint(self, node: dict) -> dict:
        if node['contype'] == 0:       # CONSTR_NULL
            return {'constraint': 'NULL', 'nullable': True}
        elif node['contype'] == 1:     # CONSTR_NOTNULL
            return {'constraint': 'NOT NULL', 'nullable': False}
        elif node['contype'] == 2:     # CONSTR_DEFAULT
            return {
                'constraint': 'DEFAULT',
                'default': self.reformat(node['raw_expr'])
            }
        elif node['contype'] == 3:    # CONSTR_IDENTITY
            return {
                'constraint': 'IDENTITY',
                'generated': constants.GENERATED[node['generated_when']],
                'options': self.reformat(node.get('options', {}))
            }
        elif node['contype'] == 4:    # CONSTR_CHECK
            value = {
                'constraint': 'CHECK',
            }
            if 'conname' in node:
                value['name'] = node['conname']
            value['expression'] = self.reformat(node['raw_expr'])
            value['initially_valid'] = node.get('initially_valid', False)
            return value
        elif node['contype'] == 5:    # CONSTR_PRIMARY
            if 'keys' in node:
                return {
                    'constraint': 'PRIMARY KEY',
                    'columns': self.reformat(node['keys'])
                }
            else:
                LOGGER.error('Unsupported constraint: %r', node)
                raise RuntimeError
        elif node['contype'] == 6:    # CONSTR_UNIQUE
            constraint = {
                'constraint': 'UNIQUE'
            }
            if 'conname' in node:
                constraint['name'] = node['conname']
            if 'keys' in node:
                constraint['columns'] = self.reformat(node['keys'])
            else:
                LOGGER.error('Unsupported constraint: %r', node)
                raise RuntimeError
            return constraint
        elif node['contype'] == 7:    # CONSTR_EXCLUSION
            return {
                'constraint': 'EXCLUSION',
                'access_method': node['access_method'],
                'exclusions': self.reformat(node['exclusions'])
            }
        elif node['contype'] == 8:    # CONSTR_FOREIGN
            fk_col = self._ensure_list(self.reformat(node['fk_attrs']))
            ref_col = self._ensure_list(self.reformat(node['pk_attrs']))
            return {
                'constraint': 'FOREIGN KEY',
                'name': node.get('conname'),
                'fk_columns': fk_col,
                'ref_table': self.reformat(node['pktable']),
                'ref_columns': ref_col,
                'match': constants.FK_MATCH.get(node['fk_matchtype']),
                'on_delete': constants.FK_ACTION[node['fk_del_action']],
                'on_update': constants.FK_ACTION[node['fk_upd_action']],
                'deferrable': node.get('deferrable', None),
                'initially_deferred': node.get('initdeferred', None)
            }
        elif node['contype'] in [9, 10, 11, 12]:
            # CONSTR_ATTR_DEFERRABLE
            # CONSTR_ATTR_NOT_DEFERRABLE
            # CONSTR_ATTR_DEFERRED
            # CONSTR_ATTR_IMMEDIATE
            LOGGER.error('Unsupported constraint: %r', node)
            raise RuntimeError

    def _create_enum_stmt(self, node: dict) -> dict:
        return {
            'stmt_type': constants.TYPE,
            'name': self.reformat(node['typeName'])[0],
            'values': self.reformat(node['vals'])}

    def _create_range_stmt(self, node: dict) -> dict:
        return {
            'stmt_type': constants.TYPE,
            'name': self.reformat(node['typeName'])[0],
            'range': {e['defname']: e['arg']
                      for e in self.reformat(node['params'])}}

    def _create_seq_stmt(self, node: dict) -> dict:
        sequence = {
            'schema': node['sequence']['RangeVar']['schemaname'],
            'name': node['sequence']['RangeVar']['relname']
        }
        map_keys = {
            'maxvalue': 'max_value',
            'minvalue': 'min_value',
        }
        for row in self.reformat(node['options']):
            if 'arg' in row:
                if row['name'] == 'cache' and row['arg'] == 1:
                    continue
                sequence[map_keys.get(row['name'], row['name'])] = row['arg']
            elif 'action' in row and row['action'] == 'UNSPECIFIED':
                continue
            else:
                LOGGER.critical('Unsupported _create_seq_stmt option: %r', row)
                raise RuntimeError
        return sequence

    def _create_stmt(self, node: dict) -> dict:
        stmt = {'stmt_type': constants.TABLE}
        stmt.update({k: self.reformat(v) for k, v in node.items()})
        return stmt

    def _create_trig_stmt(self, node: dict) -> dict:
        events = []
        if node['events'] & constants.TRIGGER_INSERT:
            events.append(constants.INSERT)
        if node['events'] & constants.TRIGGER_UPDATE:
            events.append(constants.UPDATE)
        if node['events'] & constants.TRIGGER_DELETE:
            events.append(constants.DELETE)
        if node['events'] & constants.TRIGGER_TRUNCATE:
            events.append(constants.TRUNCATE)
        if node.get('timing', 0) & constants.TRIGGER_BEFORE:
            when = constants.BEFORE
        elif node.get('timing', 0) & constants.TRIGGER_INSTEAD:
            when = constants.INSTEAD
        else:
            when = constants.AFTER
        funcname = self.reformat(node['funcname'])
        if isinstance(funcname, list):
            funcname = '.'.join(funcname)
        transitions = self.reformat(node.get('transitionRels', []))
        if not isinstance(transitions, list):
            transitions = [transitions]
        return {
            'stmt_type': constants.TRIGGER,
            'when': when,
            'events': events,
            'relation': self._relation(node['relation']),
            'name': node['trigname'],
            'row': node.get('row', False),
            'transitions': transitions,
            'condition': self.reformat(node.get('whenClause')),
            'function': '{}()'.format(funcname)
        }

    @staticmethod
    def _current_of_expr(node: dict) -> str:
        return 'CURRENT OF {}'.format(node['cursor_name'])

    def _def_elem(self, node: dict) -> dict:
        if 'arg' in node:
            return {
                'arg': self.reformat(node['arg']),
                'name': node['defname']}
        elif 'defaction' in node:
            return {
                'action': constants.DefElemAction(node['defaction']).name,
                'name': node['defname']}
        else:
            LOGGER.debug('Unsupported _def_elem node: %r', node)
            raise RuntimeError

    def _delete_stmt(self, node: dict) -> dict:
        stmt = {
            'stmt_type': constants.DELETE,
            'from': self._relation(node['relation']),
            'using': None,
            'where': None,
            'returning': None
        }
        if 'usingClause' in node:
            stmt['using'] = self._ensure_list(
                self.reformat(node['usingClause']))
        if 'whereClause' in node:
            stmt['where'] = self._ensure_list(
                self.reformat(node['whereClause']))
        if 'returningList' in node:
            stmt['returning'] = self._ensure_list(
                self.reformat(node['returningList']))
        return stmt

    def _define_stmt(self, node: dict) -> dict:
        return {
            'stmt_type': constants.TYPE,
            'name': self.reformat(node['defnames'])[0],
            'definition': {e['defname']: e['arg']
                           for e in self.reformat(node['definition'])}}

    @staticmethod
    def _ensure_list(node: typing.Any) -> list:
        return node if isinstance(node, list) else [node]

    def _func_call(self, node: dict) -> str:
        return '{}({})'.format(
            '.'.join(self.reformat(node['funcname'])),
            ', '.join([str(a) for a in self.reformat(node.get('args', []))]))

    def _grouping_set(self, node: dict) -> dict:
        return {
            'type': constants.GROUPING_SET[node['kind']],
            'values': self.reformat(node.get('content', []))
        }

    def _index_elem(self, node: dict) -> dict:
        return {
            'name': node.get('name', self.reformat(node.get('expr'))),
            'null_order': constants.NULL_ORDERING[node['nulls_ordering']],
            'order': constants.ORDERING[node['ordering']]
        }

    def _index_stmt(self, node: dict) -> dict:
        options = self.reformat(node.get('options', {}))
        if not isinstance(options, list):
            options = [options] if options else []
        return {
            'stmt_type': constants.INDEX,
            'name': node['idxname'],
            'relation': self._relation(node['relation']),
            'type': node['accessMethod'],
            'columns': self._ensure_list(self.reformat(node['indexParams'])),
            'where': self.reformat(node.get('whereClause')),
            'options': {r['defname']: r['arg'] for r in options},
            'tablespace': node.get('tableSpace'),
            'unique': node.get('unique', False)
        }

    def _infer_clause(self, node: dict) -> dict:
        return {
            'elements': self.reformat(node.get('indexElems')),
            'constraint': node.get('conname'),
            'where': self.reformat(node.get('whereClause'))
        }

    def _insert_stmt(self, node: dict) -> dict:
        return {
            'stmt_type': constants.INSERT,
            'with': self.reformat(node.get('withClause')),
            'target': self._relation(node['relation']),
            'columns': self.reformat(node.get('cols')),
            'default': 'selectStmt' not in node,
            'select': self.reformat(node.get('selectStmt')),
            'returning': self.reformat(node.get('returningList')),
            'on_conflict': self.reformat(node.get('onConflictClause'))
        }

    @staticmethod
    def _integer(node: dict) -> int:
        return node['ival']

    def _join_expr(self, node: dict) -> dict:
        return {
            'left': self.reformat(node['larg']),
            'right': self.reformat(node['rarg']),
            'type': constants.JOIN_TYPE[node['jointype']],
            'natural': node.get('isNatural', False),
            'using': self.reformat(node.get('usingClause')),
            'on': self.reformat(node.get('quals'))
        }

    def _multi_assign_ref(self, node: dict) -> list:
        return self.reformat(node['source'])

    @staticmethod
    def _notify_stmt(node: dict) -> list:
        return ['NOTIFY', node['conditionname']]

    def _null_test(self, node: dict) -> list:
        return [
            self.reformat(node['arg']),
            constants.NULL_TEST[node['nulltesttype']],
            'NULL'
        ]

    def _on_conflict_clause(self, node: dict) -> dict:
        if 'targetList' in node:
            self._set_res_target_mode(node['targetList'], 'update')
        return {
            'action': constants.ON_CONFLICT[node['action']],
            'infer': self.reformat(node.get('infer')),
            'target': self.reformat(node.get('targetList')),
            'where': self.reformat(node.get('whereClause'))
        }

    @staticmethod
    def _partition_elem(node: dict) -> str:
        return node['name']

    def _partition_spec(self, node: dict) -> dict:
        return {
            'partition_by': node['strategy'],
            'columns': self.reformat(node['partParams'])
        }

    def _range_function(self, node: dict) -> dict:
        return {
            'alias': self.reformat(node.get('alias')),
            'functions': [f for f in self.reformat(node['functions'])[0]
                          if f is not None],
            'column_defs': ['{} {}'.format(f['name'], f['type'])
                            for f in self.reformat(
                                node.get('coldeflist', []))],
            'lateral': node.get('lateral', False),
            'ordinality': node.get('ordinality', False)
        }

    def _range_var(self, node: dict) -> str:
        return self._relation(node)

    def _raw_stmt(self, node: dict) -> str:
        return self.reformat(node['stmt'])

    def _relation(self, node: typing.Union[dict, list, str]) \
            -> typing.Union[list, str]:
        LOGGER.debug('_relation: %r', node)
        if isinstance(node, (list, str)):
            return node
        if 'RangeVar' in node:
            return self._relation(self.reformat(node))
        if 'schemaname' in node:
            name = '{}.{}'.format(node['schemaname'], node['relname'])
        elif 'relname' in node:
            name = node['relname']
        else:
            LOGGER.debug('Unsupported _relation node: %r', node)
            raise RuntimeError
        if 'alias' in node:
            return [name, 'AS', self.reformat(node['alias'])]
        return name

    def _rename_stmt(self, node: dict) -> dict:
        if node['renameType'] == constants.ObjectType.COLUMN:
            return {
                'stmt_type': 'ALTER {}'.format(constants.ObjectType(
                    node['relationType']).name),
                'relation': self.reformat(node['relation']),
                'commands': [
                    {
                        'operation': 'RENAME COLUMN',
                        'old_name': node['subname'],
                        'new_name': node['newname']
                    }
                ]
            }
        elif node['renameType'] == constants.ObjectType.TABLE:
            return {
                'stmt_type': 'RENAME {}'.format(constants.ObjectType(
                    node['renameType']).name),
                'old_name': self.reformat(node['relation']),
                'new_name': node['newname'],
                'cascade': node['behavior'] == 1,
                'missing_ok': node.get('missing_ok', False)
            }
        return {
            'stmt_type': 'RENAME {}'.format(constants.ObjectType(
                node['renameType']).name),
            'old_name': node['subname'],
            'new_name': node['newname'],
            'cascade': node['behavior'] == 1,
            'missing_ok': node.get('missing_ok', False)
        }

    def _res_target(self, node: dict) -> typing.Union[list, str]:
        name = node.get('name')
        if 'indirection' in node:
            name = [name, self.reformat(node['indirection'])]
        if node.get('mode') == 'update':
            if 'MultiAssignRef' in node['val']:
                return name
            return [name, '=', self.reformat(node['val'])]
        if name and 'val' in node:
            return [self.reformat(node['val']), 'AS', name]
        return name if name else self.reformat(node['val'])

    @staticmethod
    def _role_spec(node: dict) -> str:
        return node.get('rolename', constants.ACL_ROLE_TYPE[node['roletype']])

    def _row_expr(self, node: dict) -> list:
        return self.reformat(node['args'])

    def _rule_stmt(self, node: dict) -> dict:
        return {
            'stmt_type': constants.RULE,
            'name': self.reformat(node['rulename']),
            'table': self._relation(node['relation']),
            'event': constants.RULE_EVENTS[node['event']],
            'instead': node.get('instead', False),
            'replace': node.get('replace', False),
            'where': self.reformat(node.get('whereClause')),
            'action': self.reformat(node['actions'])}

    @staticmethod
    def _s_q_l_value_function(node) -> str:
        return constants.SQL_VALUE_FUNCTION[node['op']]

    def _select_stmt(self, node: dict) -> dict:
        temp = self.reformat(node.get('distinctClause'))
        set_stmt = None
        if node.get('op', 0) > 0:
            set_stmt = {
                'operation': constants.SELECT_OP[node['op']],
                'left': self.reformat(node.get('larg')),
                'all': node.get('all', False),
                'right': self.reformat(node.get('rarg'))
            }
        return {
            'stmt_type': constants.SELECT,
            'distinct': temp == [None],
            'distinct_on': temp if temp and temp != [None] else None,
            'into': self.reformat(node.get('intoClause')),
            'targets': self.reformat(node.get('targetList')),
            'from': self.reformat(node.get('fromClause')),
            'where': self.reformat(node.get('whereClause')),
            'group_by': self.reformat(node.get('groupClause')),
            'having': self.reformat(node.get('havingClause')),
            'window': self.reformat(node.get('windowClause')),
            'values': self.reformat(node.get('valuesLists')),
            'order_by': self.reformat(node.get('sortClause')),
            'limit': self.reformat(node.get('limitCount')),
            'offset': self.reformat(node.get('limitOffset')),
            'locking': self.reformat(node.get('lockingClause')),
            'with': self.reformat(node.get('withClause')),
            'set': set_stmt
        }

    @staticmethod
    def _set_to_default(_node: dict) -> str:
        return constants.DEFAULT

    @staticmethod
    def _set_res_target_mode(values: dict, mode: str) -> typing.NoReturn:
        for offset in range(0, len(values)):
            values[offset]['ResTarget']['mode'] = mode

    def _sort_by(self, node: dict) -> str:
        return self.reformat(node['node'])

    @staticmethod
    def _string(node: dict) -> str:
        return node['str']

    def _sub_link(self, node: dict) -> list:
        if node['subLinkType'] in {0, 6}:
            return ['{}({})'.format(
                constants.SUBLINK_TYPE[node['subLinkType']],
                self.reformat(node['subselect']))]
        if node['subLinkType'] in {1, 2}:
            sublink_type = 'IN'
            if 'operName' in node:
                sublink_type = constants.SUBLINK_TYPE[node['subLinkType']]
                sublink_type = '{} {}'.format(
                    self.reformat(node['operName']), sublink_type)
            return [
                self.reformat(node['testexpr']),
                sublink_type,
                self.reformat(node['subselect'])
            ]
        elif node['subLinkType'] == 3:
            return [
                self.reformat(node['testexpr']),
                constants.ROW_COMPARE[node['op']],
                self.reformat(node['subselect'])
            ]
        elif node['subLinkType'] in {4, 5}:
            return self.reformat(node['subselect'])
        raise RuntimeError(
            'Unsupported sublink: {}'.format(node['subLinkType']))

    @staticmethod
    def _trigger_transition(node: dict) -> dict:
        return {
            'name': node['name'],
            'is_new': node.get('isNew', False),
            'is_table': node.get('isTable', False)
        }

    def _type_cast(self, node: dict) -> str:
        type_name = self.reformat(node['typeName'])
        node = self.reformat(node['arg'])
        if type_name == 'bool':
            return constants.BOOL_TEST[node]
        return '{}::{}'.format(node, type_name)

    def _type_name(self, node: dict) -> str:
        name = self.reformat(node['names'])
        if name[0] == 'pg_catalog':
            name.remove('pg_catalog')
        if len(name) > 1:
            name = '.'.join(name)
        else:
            name = name[0]
        if name == 'bpchar':
            name = 'char'
        parts = [name]
        if 'typmods' in node:
            precision = self.reformat(node['typmods'])[0]
            LOGGER.debug('Precision: %r', precision)
            if name == 'interval':
                parts.append(' ')
                parts.append(constants.INTERVAL_FIELDS[precision])
            else:
                parts.append('({})'.format(precision))
        if 'arrayBounds' in node:
            for _iter in range(0, len(node['arrayBounds'])):
                parts.append('[]')
        return ''.join(parts)

    def _update_stmt(self, node: dict) -> dict:
        first_target = node['targetList'][0]
        if 'MultiAssignRef' in first_target['ResTarget'].get('val', {}).keys():
            targets = [
                '({})'.format(', '.join(e['ResTarget']['name']
                                        for e in node['targetList'])),
                '=',
                self.reformat(first_target['ResTarget']['val'])]
        else:
            self._set_res_target_mode(node['targetList'], 'update')
            targets = self.reformat(node['targetList'])
        LOGGER.debug('Targets: %r', targets)
        return {
            'stmt_type': constants.UPDATE,
            'target': self._relation(node['relation']),
            'set': targets,
            'with': self.reformat(node.get('withClause')),
            'from': self.reformat(node.get('fromClause')),
            'where': self.reformat(node.get('whereClause')),
            'returning': self.reformat(node.get('returningList')),
        }

    def _view_stmt(self, node: dict) -> dict:
        return {
            'stmt_type': constants.VIEW,
            'check_option':
                constants.VIEW_CHECK_OPTION[node.get('withCheckOption', 0)],
            'name': self.reformat(node['view']),
            'sql': self.reformat(node['query'])
        }

    def _with_clause(self, node: dict) -> dict:
        return {
            'ctes': [self.reformat(c) for c in node.get('ctes', [])],
            'recursive': node.get('recursive', False)
        }
