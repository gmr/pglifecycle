import enum
import logging
import typing

import pglast
import pglast.error
import pglast.printer
import stringcase

from pglifecycle import constants

LOGGER = logging.getLogger(__name__)

ParsedStmt = typing.Union[dict, int, list, str, None]

_A_EXPR_KIND = {
    0: None,  # Normal Operator
    1: 'ANY',
    2: 'ALL',
    3: 'IS DISTINCT FROM',
    4: 'IS NOT DISTINCT FROM',
    5: 'NULLIF',
    6: 'IS {}OF',
    7: 'IN',
    8: 'LIKE',
    9: 'ILIKE',
    10: 'SIMILAR',
    11: 'BETWEEN',
    12: 'NOT BETWEEN',
    13: 'BETWEEN SYMMETRIC',
    14: 'NOT BETWEEN SYMMETRIC'
}
_BOOLOP = {0: ' AND ', 1: ' OR ', 2: ' NOT '}
_BOOL_TEST = {1: 'TRUE', 2: 'FALSE', "'t'": 'TRUE', "'f'": 'FALSE'}
_FK_ACTION = {
    'a': None,  # NO ACTION
    'c': 'CASCADE',
    'n': 'SET NULL',
    'r': 'RESTRICT'
}
_FK_MATCH = {'f': 'FULL', 'p': 'PARTIAL', 's': None}
_GENERATED = {'a': 'ALWAYS', 'd': 'BY DEFAULT'}
_INTERVAL_FIELDS = {
    4: 'YEAR',
    2: 'MONTH',
    8: 'DAY',
    1024: 'HOUR',
    2048: 'MINUTE',
    4096: 'SECOND',
    6: 'YEAR TO MONTH',
    1032: 'DAY TO HOUR',
    3072: 'HOUR TO MINUTE',
    3080: 'DAY TO MINUTE',
    6144: 'MINUTE_TO_SECOND',
    7168: 'HOUR TO SECOND',
    7176: 'DAY TO SECOND'
}
_NULLORDERING = {0: None, 1: 'FIRST', 2: 'LAST'}
_NULLTEST = {0: 'IS', 1: 'IS NOT'}
_ON_CONFLICT = {1: 'DO NOTHING', 2: 'DO UPDATE SET'}
_ORDERING = {0: None, 1: 'ASC', 2: 'DESC'}
_RULE_EVENTS = {
    1: constants.SELECT,
    2: constants.UPDATE,
    3: constants.INSERT,
    4: constants.DELETE,
}
_SELECT_OP = {1: 'UNION'}
_TRIGGER_INSERT = (1 << 2)
_TRIGGER_DELETE = (1 << 3)
_TRIGGER_UPDATE = (1 << 4)
_TRIGGER_TRUNCATE = (1 << 5)
_TRIGGER_BEFORE = (1 << 1)
_TRIGGER_AFTER = 0x00000000
_TRIGGER_INSTEAD = (1 << 6)


class _DefElemAction(enum.IntEnum):
    """Actions for a DefElem"""
    UNSPEC = 0
    SET = 1
    ADD = 2
    DROP = 3


class _GrantObjectType(enum.IntEnum):
    """Actions for a DefElem"""
    COLUMN = 0
    TABLE = 1
    SEQUENCE = 2
    DATABASE = 3
    DOMAIN = 4
    FOREIGN_DATA_WRAPPER = 5
    FOREIGN_SERVER = 6
    FUNCTION = 7
    PROCEDURAL_LANGUAGE = 8
    LARGE_OBJECT = 9
    SCHEMA = 10
    TABLESPACE = 11
    TYPE = 12


class _GrantTargetType(enum.IntEnum):
    """Actions for a DefElem"""
    OBJECT = 0
    ALL_IN_SCHEMA = 1
    DEFAULTS = 2


class _ObjectType(enum.IntEnum):
    """PostgreSQL Object Types"""
    ACCESS_METHOD = 0
    AGGREGATE = 1
    AMOP = 2
    AMPROC = 3
    ATTRIBUTE = 4
    CAST = 5
    COLUMN = 6
    COLLATION = 7
    CONVERSION = 8
    DATABASE = 9
    DEFAULT = 10
    DEFACL = 11
    DOMAIN = 12
    DOMCONSTRAINT = 13
    EVENT_TRIGGER = 14
    EXTENSION = 15
    FDW = 16
    FOREIGN_SERVER = 17
    FOREIGN_TABLE = 18
    FUNCTION = 19
    INDEX = 20
    LANGUAGE = 21
    LARGEOBJECT = 22
    MATVIEW = 23
    OPCLASS = 24
    OPERATOR = 25
    OPFAMILY = 26
    POLICY = 27
    PUBLICATION = 28
    PUBLICATION_REL = 29
    ROLE = 30
    RULE = 31
    SCHEMA = 32
    SEQUENCE = 33
    SUBSCRIPTION = 34
    STATISTIC_EXT = 35
    TABCONSTRAINT = 36
    TABLE = 37
    TABLESPACE = 38
    TRANSFORM = 39
    TRIGGER = 40
    TSCONFIGURATION = 41
    TSDICTIONARY = 42
    TSPARSER = 43
    TSTEMPLATE = 44
    TYPE = 45
    USER_MAPPING = 46
    VIEW = 47


class _RoleSpecType(enum.IntEnum):
    """RoleSpec - a role name or one of a few special values"""
    CSTRING = 0
    CURRENT_USER = 1
    SESSION_USER = 2
    PUBLIC = 3


class _RoleType(enum.IntEnum):
    """PostgreSQL Role Types"""
    ROLE = 0
    USER = 1
    GROUP = 2


class _VariableSetStmt(enum.IntEnum):
    """PostgreSQL Object Types"""
    VALUE = 0
    DEFAULT = 1
    CURRENT = 2
    MULTI = 3
    RESET = 4
    RESET_ALL = 5


def sql(value: str) -> ParsedStmt:
    """Parse SQL returning a more abbreviated data structure

    Generator function that iterates over SQL statements returning the
    parsed data structure for each statement.

    """
    try:
        return _SQLParser().parse(value)
    except pglast.error.Error as error:
        LOGGER.error('Failed to parse statement: %r', error)
        LOGGER.warning('Failed statement: %s', value)


class _SQLParser:
    """SQL Parser"""

    def __init__(self):
        self.sql = None

    def parse(self, value: str) -> dict:
        """Parse the given SQL statement, returning structured data"""
        LOGGER.debug('SQL: %r', value)
        self.sql = value
        return self._parse(pglast.parse_sql(value))

    def _parse(self, value: ParsedStmt) -> ParsedStmt:
        LOGGER.debug('_parse %r', value)
        if value is None:
            return None
        elif (isinstance(value, list) and len(value) == 1
              and isinstance(value[0], dict)):
            value = value[0]
        if isinstance(value, dict):
            parsed = []
            for k, v in value.items():
                if isinstance(v, (int, str)):
                    parsed.append(self._parse(v))
                else:
                    name = '_{}'.format(stringcase.snakecase(k))
                    parser = getattr(self, name, None)
                    if parser is None:
                        msg = '{} ({}) is an unsupported node type'.format(
                            k, name)
                        raise RuntimeError(msg)
                    parsed.append(parser(v))
            if all(isinstance(r, dict) for r in parsed):
                flattened = dict(i for m in parsed for i in m.items())
                return flattened
            return parsed if len(parsed) > 1 else parsed[0]
        elif isinstance(value, list):
            for offset, entry in enumerate(value):
                if entry:
                    value[offset] = self._parse(entry)
            return value
        return value

    def _a__array_expr(self, value: dict) -> str:
        return 'ARRAY[{}]'.format(', '.join(
            str(self._parse(e)) for e in value['elements']))

    def _a__const(self, value: dict) -> typing.Union[int, str, None]:
        value = self._parse(value['val'])
        if isinstance(value, str):
            return repr(value)
        return value

    def _a__expr(self, value: dict) -> str:
        lexpr = self._a__expr_side(value['lexpr'])
        rexpr = self._a__expr_side(value['rexpr'])
        if value['kind'] == 0:
            return '{} {} {}'.format(lexpr, ''.join(
                self._parse(value['name'])), rexpr)
        elif value['kind'] in {1, 2}:
            return '{} {} {} ({})'.format(
                lexpr, ''.join(self._parse(value['name'])),
                _A_EXPR_KIND[value['kind']],
                rexpr)
        elif value['kind'] in {3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14}:
            return '{} {} {}'.format(lexpr, _A_EXPR_KIND[value['kind']], rexpr)
        elif value['kind'] == 5:
            return 'NULLIF({}, {})'.format(lexpr, rexpr)
        LOGGER.error('Unsupported A_Expr: %r', value)
        raise RuntimeError

    def _a__expr_side(self, value: dict) -> str:
        if 'A_Expr' in value:
            lexpr = 'A_Expr' in value['A_Expr']['lexpr']
            rexpr = 'A_Expr' in value['A_Expr']['rexpr']
            if lexpr and rexpr:
                return '({})'.format(self._parse(value))
        return self._parse(value)

    @staticmethod
    def _access_priv(value: dict) -> str:
        return value['priv_name']

    def _alter_role_stmt(self, value: dict) -> dict:
        if value['action'] != 1:
            LOGGER.error('Unsupported _alter_role_stmt: %r', value)
            raise RuntimeError

        def _format_option(option: dict) -> typing.Union[str, dict]:
            if option['action'] is not None:
                LOGGER.error('Option Action is not None: %r', value)
                raise RuntimeError
            if option['arg'] == 0:
                return 'NO{}'.format(option['name'].upper())
            elif option['arg'] == 1:
                return option['name'].upper()
            if isinstance(option['arg'], str) and \
                    option['arg'][0] == "'" and \
                    option['arg'][-1] == "'":
                option['arg'] = option['arg'][1:-1]
            return {option['name']: option['arg']}

        options = [_format_option(o) for o in self._parse(value['options'])]
        stmt = {
            'role': self._parse(value['role']),
            'options': [o for o in options if not isinstance(o, dict)]
        }
        if any(isinstance(o, dict) for o in options):
            for option in [o for o in options if isinstance(o, dict)]:
                stmt.update(option)
        return stmt

    def _alter_role_set_stmt(self, value: dict) -> dict:
        return {
            'role': self._parse(value['role']),
            'settings': self._parse(value['setstmt'])
        }

    def _alter_seq_stmt(self, value: dict) -> dict:
        return {
            'sequence': self._parse(value['sequence']),
            'options': self._parse(value['options'])
        }

    def _alter_table_cmd(self, value: dict) -> dict:
        if value['subtype'] == 3:  # Default
            return {
                'type': 'default',
                'column': self._parse(value['name']),
                'definition': self._parse(value['def'])
            }
        if value['subtype'] == 14:  # Foreign Key
            return {
                'type': 'foreign key',
                'definition': self._parse(value['def'])
            }
        elif value['subtype'] == 9:  # Column Storage
            return {
                'type': 'storage_mode',
                'column': self._parse(value['name']),
                'storage': self._parse(value['def'])
            }
        elif value['subtype'] == 28:  # Primary Key
            return {'type': 'primary key', 'name': value['name']}
        LOGGER.error('Unsupported Alter Table: %r', value)
        raise RuntimeError

    def _alter_table_stmt(self, value: dict) -> dict:
        return {
            'relation': self._parse(value['relation']),
            'commands': self._parse(value['cmds'])
        }

    def _bool_expr(self, value: dict) -> str:
        values = self._parse(value['args'])
        if all(not isinstance(i, list) for i in values):
            return _BOOLOP[value['boolop']].join(values)
        values = [''.join(i) for r in values for i in r]
        return _BOOLOP[value['boolop']].join(values)

    def _boolean_test(self, value: dict) -> str:
        return _BOOLOP[value.get('boolop', 0)].join(
            '{} IS {}'.format(
                self._parse(f), _BOOL_TEST[value['booltesttype']])
            for f in value['arg'])

    @staticmethod
    def _capitalize_keywords(value: list) -> list:
        return [
            v.upper() if v in ['excluded', 'old', 'new'] else v for v in value
        ]

    def _column_ref(self, value: dict) -> str:
        if 'fields' in value:
            fields = self._parse(value['fields'])
            return '.'.join(self._capitalize_keywords(fields))
        LOGGER.error('Unsupported ColumnRef: %r', value)
        raise RuntimeError

    def _comment_stmt(self, value: dict) -> dict:
        return {
            'object': self._parse(value['object']),
            'object_type': _ObjectType(value['objtype']).name,
            'comment': self._parse(value['comment']).strip()
        }

    def _constraint(self, value: dict) -> dict:
        if value['contype'] == 1:
            return {'type': 'null', 'nullable': False}
        elif value['contype'] == 2:
            return {'type': 'default', 'value': self._parse(value['raw_expr'])}
        # elif value['contype'] == 3:
        #     return {
        #         'generated': _GENERATED[value['generated_when']],
        #         'options': self._parse(value.get('options', {}))
        #     }
        # elif value['contype'] == 4:
        #     return {
        #         'type': value.get('conname', 'check')
        #         'expr': self._parse(value['raw_expr']),
        #         'initially_valid': value.get('initially_valid', False)
        #     }
        elif value['contype'] == 5:
            return {
                'type': 'primary key',
                'name': value.get('conname'),
                'keys': self._ensure_list(self._parse(value['keys']))
            }
        elif value['contype'] == 6:
            return {
                'type': 'unique',
                'name': value['conname'],
                'columns': self._ensure_list(self._parse(value['keys']))
            }
        elif value['contype'] == 8:
            return {
                'type': 'foreign key',
                'name': value['conname'],
                'fk_columns': self._parse(value['fk_attrs']),
                'ref_table': self._parse(value['pktable']),
                'ref_columns': self._parse(value['pk_attrs']),
                'match': _FK_MATCH.get(value['fk_matchtype']),
                'on_delete': _FK_ACTION[value['fk_del_action']],
                'on_update': _FK_ACTION[value['fk_upd_action']],
                'deferrable': value.get('deferrable', None),
                'initially_deferred': value.get('initdeferred', None)
            }
        LOGGER.error('Unsupported constraint: %r', value)
        raise RuntimeError

    @staticmethod
    def _create_p_lang_stmt(value: dict) -> dict:
        return {
            'name': value['plname'],
            'handler': value.get('plhandler'),
            'inline_handler': value.get('plinline'),
            'replace': value.get('replace', False),
            'trusted': value.get('pltrusted'),
            'validator': value.get('plvalidator')
        }

    @staticmethod
    def _create_role_stmt(value: dict) -> dict:
        return {
            'type': _RoleType(value['stmt_type']).name,
            'role': value['role'],
            'options': value.get('options')
        }

    def _create_trig_stmt(self, value: dict) -> dict:
        events = []
        if value['events'] & _TRIGGER_INSERT:
            events.append(constants.INSERT)
        if value['events'] & _TRIGGER_UPDATE:
            events.append(constants.UPDATE)
        if value['events'] & _TRIGGER_DELETE:
            events.append(constants.DELETE)
        if value['events'] & _TRIGGER_TRUNCATE:
            events.append(constants.TRUNCATE)
        if value.get('timing', 0) & _TRIGGER_BEFORE:
            when = constants.BEFORE
        elif value.get('timing', 0) & _TRIGGER_INSTEAD:
            when = constants.INSTEAD
        else:
            when = constants.AFTER
        funcname = self._parse(value['funcname'])
        if isinstance(funcname, list):
            funcname = '.'.join(funcname)
        transitions = self._parse(value.get('transitionRels', []))
        if not isinstance(transitions, list):
            transitions = [transitions]
        return {
            'when': when,
            'events': events,
            'relation': self._relation_name(self._parse(value['relation'])),
            'name': value['trigname'],
            'row': value.get('row', False),
            'transitions': transitions,
            'condition': self._parse(value.get('whenClause')),
            'function': '{}()'.format(funcname)
        }

    def _def_elem(self, value: dict) -> dict:
        return {
            'name':
            value['defname'],
            'arg':
            self._parse(value['arg']),
            'action': (_DefElemAction(value['defaction']).name
                       if value.get('defaction', 0) > 0 else None)
        }

    def _define_stmt(self, value: dict) -> dict:
        return {
            'type': _ObjectType(value['kind']).name,
            'name': '.'.join(self._parse(value['defnames'])),
            'options':
            {o['name']: o['arg']
             for o in self._parse(value['definition'])}
        }

    @staticmethod
    def _ensure_list(value: typing.Any) -> list:
        if not isinstance(value, list):
            return [value]
        return value

    def _func_call(self, value: dict) -> str:
        args = [
            str(a)
            for a in self._ensure_list(self._parse(value.get('args', [])))
        ]
        funcname = '.'.join(self._ensure_list(self._parse(value['funcname'])))
        return '{}({})'.format(funcname, ', '.join(args))

    def _grant_role_stmt(self, value: dict) -> dict:
        if len(value['grantee_roles']) > 1:
            LOGGER.error('Too many grantees: %r', value['grantee_roles'])
            raise RuntimeError
        key = 'grant' if value['is_grant'] else 'revoke'
        return {
            'role': self._parse(value['grantee_roles'][0]),
            key: self._parse(value['granted_roles'])
        }

    def _grant_stmt(self, value: dict) -> dict:
        if value['targtype'] == 0:
            if len(value['grantees']) > 1:
                LOGGER.error('Too many targets: %r', value['grantees'])
                raise RuntimeError
        else:
            raise RuntimeError('Unsupported TargetType: {!r}'.format(value))
        if len(value['objects']) > 1:
            LOGGER.error('Too many grantees: %r', value['grantee_roles'])
            raise RuntimeError
        is_grant = value.get('is_grant')
        privs = self._parse(value.get('privileges', ['ALL']))
        if isinstance(privs, str):
            privs = [privs]
        return {
            'type': constants.REVOKE if not is_grant else constants.GRANT,
            'subject': {
                'type': _GrantObjectType(value['objtype']).name,
                'name': self._parse(value['objects'][0])
            },
            'privileges': [p.upper() for p in privs],
            'to': self._parse(value['grantees'][0])
        }

    def _index_elem(self, value: dict) -> dict:
        return {
            'name': value.get('name', self._parse(value.get('expr'))),
            'null_order': _NULLORDERING[value['nulls_ordering']],
            'order': _ORDERING[value['ordering']]
        }

    def _index_stmt(self, value: dict) -> dict:
        options = self._parse(value.get('options', {}))
        if not isinstance(options, list):
            options = [options] if options else []
        LOGGER.debug('Options: %r', options)
        return {
            'name': value['idxname'],
            'relation': self._relation_name(self._parse(value['relation'])),
            'type': value['accessMethod'],
            'columns': self._parse(value['indexParams']),
            'where': self._parse(value.get('whereClause')),
            'options': {r['name']: r['arg']
                        for r in options},
            'tablespace': value.get('tableSpace'),
            'unique': value.get('unique', False)
        }

    @staticmethod
    def _integer(value: dict) -> int:
        return value['ival']

    @staticmethod
    def _null(_value: dict) -> None:
        return None

    def _null_test(self, value: dict) -> str:
        return '{} {} NULL'.format(
            self._parse(value['arg']), _NULLTEST[value['nulltesttype']])

    def _object_with_args(self, value: dict) -> dict:
        return {
            'name': self._parse(value['objname']),
            'args': self._parse(value.get('objargs'))
        }

    def _range_var(self, value: dict) -> str:
        if isinstance(value, dict) and 'relname' in value:
            return self._relation_name(value)
        raise RuntimeError

    def _raw_stmt(self, value: dict) -> dict:
        return self._parse(value['stmt'])

    def _relation_name(self, value: typing.Union[dict, str]) -> str:
        if isinstance(value, str):
            return value
        parts = []
        if 'schemaname' in value:
            parts.append('{}.{}'.format(value['schemaname'], value['relname']))
        else:
            parts.append(value['relname'])
        if 'alias' in value:
            parts.append('AS')
            parts.append(self._parse(value['alias']))
        return ' '.join(parts)

    def _res_target(self, value: dict) -> str:
        name = value.get('name')
        if 'indirection' in value:
            name = '{}{}'.format(name, ''.join(
                self._ensure_list(self._parse(value['indirection']))))
        if value.get('mode') == 'update':
            if 'MultiAssignRef' in value['val']:
                return name
            return '{} = {}'.format(name, self._parse(value['val']))
        if name and 'val' in value:
            return '{} AS {}'.format(self._parse(value['val']), name)
        return name if name else self._parse(value['val'])

    @staticmethod
    def _role_spec(value: dict) -> str:
        if value['roletype'] == _RoleSpecType.CSTRING:
            return value['rolename']
        return _RoleSpecType(value['roletype']).name

    def _rule_stmt(self, value: dict) -> dict:
        return {
            'name': value['rulename'],
            'relation': self._relation_name(self._parse(value['relation'])),
            'event': _RULE_EVENTS[value['event']],
            'instead': value.get('instead', False),
            'actions': self._ensure_list(self._parse(value['actions']))
        }

    def _select_stmt(self, value: dict) -> dict:
        select = {
            'values':
            None,
            'value':
            None,
            'with':
            self._parse(value.get('withClause')),
            'columns': [
                '.'.join(str(t)) if isinstance(t, list) else str(t)
                for t in self._ensure_list(self._parse(value['targetList']))
            ],
            'into': (self._relation_name(self._parse(value.get('intoClause')))
                     if 'intoClause' in value else None),
            'from': [
                self._relation_name(f) for f in self._ensure_list(
                    self._parse(value.get('fromClause', [])))
            ],
            'where':
            self._ensure_list(self._parse(value.get('whereClause', []))),
            'group_by':
            self._ensure_list(self._parse(value.get('groupClause', []))),
            'having':
            self._ensure_list(self._parse(value.get('havingClause', []))),
            'sort_by':
            self._ensure_list(self._parse(value.get('sortClause', []))),
            'limit':
            self._parse(value.get('limitCount')),
            'offset':
            self._parse(value.get('limitOffset')),
        }
        if 'valuesLists' in value:
            parsed = self._parse(value['valuesLists'])
            if parsed and not all(isinstance(i, list) for i in parsed):
                parsed = [parsed]
            select['values'] = [row for row in parsed]
        if 'larg' in value and 'rarg' in value:
            parts = [self._parse(value['larg']), _SELECT_OP[value['op']]]
            if value.get('all') is True:
                parts.append('ALL')
            if value.get('groupClauses'):
                raise ValueError
            parts.append(self._parse(value['rarg']))
            select['value'] = ' '.join(parts)
        for unsupported in ['windowClause', 'lockingClause']:
            if unsupported in value:
                LOGGER.error('Unsupported keyword: %r', value)
                raise RuntimeError('{} in value'.format(unsupported))
        return select

    def _type_cast(self, value: dict) -> str:
        type_name = self._parse(value['typeName'])
        value = self._parse(value['arg'])
        if type_name == 'bool':
            return _BOOL_TEST[value]
        return '{}::{}'.format(value, type_name)

    def _type_name(self, value: dict) -> str:
        name = self._parse(value['names'])
        if isinstance(name, list):
            if 'pg_catalog' in name:
                name.remove('pg_catalog')
            if len(name) > 1:
                name = ['.'.join(name)]
            name = name[0]
        if name == 'bpchar':
            name = 'char'
        parts = [name]
        if 'typmods' in value:
            precision = self._parse(value['typmods'])
            if name == 'interval':
                parts.append(' ')
                parts.append(_INTERVAL_FIELDS[precision])
            else:
                parts.append('({})'.format(precision))
        if 'arrayBounds' in value:
            for _iter in range(0, len(value['arrayBounds'])):
                parts.append('[]')
        return ''.join(parts)

    @staticmethod
    def _string(value: dict) -> str:
        return value['str']

    def _variable_set_stmt(self, value: dict) -> dict:
        args = self._parse(value['args'])
        if isinstance(args, str) and args[0] == "'" and args[-1] == "'":
            args = args[1:-1]
        elif isinstance(args, list):
            args = [
                a[1:-1]
                if isinstance(a, str) and a[0] == "'" and a[-1] == "'" else a
                for a in args
            ]
        return {
            'name': value['name'],
            'type': _VariableSetStmt(value['kind']).name,
            'value': args
        }


@pglast.printer.node_printer('RuleStmt', override=True)
def rule_stmt_printer(node, output):
    output.write('CREATE RULE ')
    output.print_node(node.rulename)
    output.write(' AS')
    output.newline()
    output.space(2)
    with output.push_indent():
        if node.event == 1:
            output.write('ON SELECT TO ')
        elif node.event == 2:
            output.write('ON UPDATE TO ')
        elif node.event == 3:
            output.write('ON INSERT TO ')
        elif node.event == 4:
            output.write('ON DELETE TO ')
        output.print_name(node.relation)
        output.write(' DO')
        if node.instead:
            output.write(' INSTEAD')
        output.newline()
        output.space(2)
        with output.push_indent():
            output.print_list(node.actions, '', standalone_items=False)
