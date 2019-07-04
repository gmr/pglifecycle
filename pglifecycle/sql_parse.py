"""
Common utilities

"""
import collections
import logging
import typing

import pglast

from pglifecycle import constants

LOGGER = logging.getLogger(__name__)

_ACL_OBJECT_TYPE = {
    1: 'TABLE',
    10: 'SCHEMA'}
_ACL_ROLE_TYPE = {
    0: 'USER',
    3: 'PUBLIC'}
_BOOLOP = {0: ' AND ', 1: ' OR '}
_BOOL_TEST = {1: 'TRUE', 2: 'FALSE', "'t'": 'TRUE', "'f'": 'FALSE'}
_DEFINE_TYPES = {
    45: 'TYPE'}
_FK_ACTION = {
    'a': None,  # NO ACTION
    'c': 'CASCADE',
    'n': 'SET NULL',
    'r': 'RESTRICT'}
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
_JOIN_TYPE = {
    1: 'LEFT JOIN'
}
_NULLORDERING = {0: None, 1: 'FIRST', 2: 'LAST'}
_NULLTEST = {0: 'IS', 1: 'IS NOT'}
_ON_CONFLICT = {
    1: 'DO NOTHING',
    2: 'DO UPDATE SET'
}
_ORDERING = {0: None, 1: 'ASC', 2: 'DESC'}
_RULE_EVENTS = {
    1: constants.SELECT,
    2: constants.UPDATE,
    3: constants.INSERT,
    4: constants.DELETE,
}
_SELECT_OP = {
    1: 'UNION'
}
_SQL_VALUE_FUNCTION = {
    3: 'CURRENT_TIMESTAMP'
}
_TRIGGER_INSERT = (1 << 2)
_TRIGGER_DELETE = (1 << 3)
_TRIGGER_UPDATE = (1 << 4)
_TRIGGER_TRUNCATE = (1 << 5)
_TRIGGER_BEFORE = (1 << 1)
_TRIGGER_AFTER = 0x00000000
_TRIGGER_INSTEAD = (1 << 6)


def parse(value: str) -> typing.Union[dict, int, list, str]:
    """Parse SQL returning a more abbreviated data structure"""
    LOGGER.debug('SQL: %r', value)
    result = _parse(pglast.parse_sql(value))
    if isinstance(result, list) and len(result) == 1:
        return result[0]
    return result


def _capitalize_keywords(values: list) -> list:
    return [v.upper() if isinstance(v, str) and v in ['old', 'new'] else v
            for v in values]


def _ensure_list(value: typing.Any) -> list:
    return value if isinstance(value, list) else [value]


def _expr_type(value: dict) -> str:
    temp = list(value.keys())
    if 'location' in temp:
        temp.remove('location')
    return temp[0]


def _parse(value: typing.Union[dict, int, list, str, None]) \
        -> typing.Union[dict, int, list, str, None]:
    LOGGER.debug('_parse %r', value)
    if value is None:
        return None
    elif (isinstance(value, list) and
          len(value) == 1 and
          isinstance(value[0], dict)):
        value = value[0]
    if isinstance(value, dict):
        parsed = []
        for k, v in value.items():
            if isinstance(v, (int, str)):
                parsed.append(_parse(v))
            else:
                parsed.append(_PARSERS[k](v))
        if all(isinstance(r, dict) for r in parsed):
            flattened = dict(i for m in parsed for i in m.items())
            return flattened
        return parsed if len(parsed) > 1 else parsed[0]
    elif isinstance(value, list):
        for offset, entry in enumerate(value):
            if entry:
                value[offset] = _parse(entry)
        return value
    return value


def _parse_a_arrayexpr(value: dict) -> str:
    return 'ARRAY[{}]'.format(
        ', '.join(_parse(e) for e in value['elements']))


def _parse_a_const(value: dict) -> typing.Union[int, str, None]:
    value = _parse(value['val'])
    if isinstance(value, str):
        return repr(value)
    return value


def _parse_a_expr(value: dict) -> str:
    lexpr = _parse_a_expr_side(value['lexpr'])
    rexpr = _parse_a_expr_side(value['rexpr'])
    if 0 <= value['kind'] <= 1:
        return '{} {} {}'.format(lexpr, ''.join(_parse(value['name'])), rexpr)
    elif value['kind'] == 3:
        return '{} IS DISTINCT FROM {}'.format(lexpr, rexpr)
    elif value['kind'] == 8:
        return '{} LIKE {}'.format(lexpr, rexpr)
    LOGGER.error('Unsupported A_Expr: %r', value)
    raise RuntimeError


def _parse_a_expr_side(value: dict) -> str:
    if 'A_Expr' in value:
        lexpr = 'A_Expr' in value['A_Expr']['lexpr']
        rexpr = 'A_Expr' in value['A_Expr']['rexpr']
        if lexpr and rexpr:
            return '({})'.format(_parse(value))
        return _parse(value)
    parsed = _parse(value)
    if isinstance(parsed, list):
        if all(isinstance(v, str) for v in parsed):
            return '.'.join(_capitalize_keywords(parsed))
        LOGGER.error('Unsupported Format: %r', parsed)
        raise ValueError
    return parsed


def _parse_a_indices(value: dict) -> str:
    if value.get('is_slice'):
        return '[{}:{}]'.format(_parse(value['lidx']), _parse(value['uidx']))
    raise RuntimeError


def _parse_a_star(_value: dict) -> str:
    return '*'


def _parse_access_priv(value: dict) -> str:
    return value['priv_name'].upper()


def _parse_alias(value: dict) -> str:
    return value['aliasname']


def _parse_alter_table_cmd(value: dict) -> typing.Optional[dict]:
    if value['subtype'] == 14:  # Foreign Key
        return _parse(value['def'])
    elif value['subtype'] == 9:  # Column Storage
        return {
            'type': 'storage_mode',
            'column': _parse(value['name']),
            'storage': _parse(value['def'])
        }
    elif value['subtype'] == 28:  # Primary Key
        return None
    LOGGER.error('Unsupported Alter Table: %r', value)
    raise RuntimeError


def _parse_alter_table_stmt(value: dict) -> dict:
    return _parse(value['cmds'])


def _parse_bool_expr(value: dict) -> str:
    return _BOOLOP[value['boolop']].join(_parse(value['args']))


def _parse_boolean_test(value: dict) -> str:
    return ' {} '.format(
        _BOOLOP[value.get('boolop', 0)]).join(
            '{} IS {}'.format(_parse(f), _BOOL_TEST[value['booltesttype']])
            for f in value['arg'])


def _parse_case_expr(value: dict) -> dict:
    return collections.OrderedDict(
        [('conditions', _parse(value['args'])),
         ('else', _parse(value.get('defresult')))])


def _parse_case_when(value: dict) -> dict:
    return collections.OrderedDict(
        [('when', _parse(value['expr'])),
         ('then', _parse(value['result']))])


def _parse_column_def(value: dict):
    temp = _ensure_list(_parse(value.get('constraints', [])))
    constraints = {}
    for key in {'default', 'nullable', 'primary_key'}:
        constraints[key] = [c.get(key) for c in temp if key in c]
        if constraints[key]:
            constraints[key] = constraints[key][0]
            temp = [c for c in temp if key not in c]
    if len(temp) > 1:
        LOGGER.error('Temp: %r', temp)
        raise ValueError
    return {
        'name': value['colname'],
        'type': _parse(value['typeName']),
        'default': constraints.get('default') or None,
        'nullable': constraints.get('nullable', []) == [],
        'constraint': temp[0] if temp else None,
        'is_local': value['is_local'],
        'primary_key': constraints.get('primary_key', []) != []
    }


def _parse_column_ref(value: dict):
    if 'fields' in value:
        fields = _ensure_list(_parse(value['fields']))
        return '.'.join(fields)
    LOGGER.error('Unsupported ColumnRef: %r', value)
    raise RuntimeError


def _parse_comment_stmt(value: dict) -> dict:
    return {'owner': _parse(value['object']),
            'value': _parse(value['comment'])}


def _parse_common_table_expr(value: dict) -> str:
    parts = []
    if 'aliascolnames' in value:
        parts.append('{}({})'.format(
            value['ctename'], ', '.join(_parse(value['aliascolnames']))))
    else:
        parts.append(value['ctename'])
    parts.append('AS')
    parts.append('({})'.format(_parse(value['ctequery'])))
    return ' '.join(parts)


def _parse_composite_type_stmt(value: dict) -> dict:
    output = {
        'name': _parse(value['typevar'])
    }
    if 'coldeflist' in value:
        parsed = _ensure_list(_parse(value['coldeflist']))
        output['attributes'] = [{'name': e['name'], 'type': e['type']}
                                for e in parsed]
    return output


def _parse_constraint(value: dict) -> dict:
    if value['contype'] == 1:
        return {'nullable': False}
    elif value['contype'] == 2:
        return {'default': _parse(value['raw_expr'])}
    elif value['contype'] == 3:
        return {
            'generated': _GENERATED[value['generated_when']],
            'options': _parse(value.get('options', {}))
        }
    elif value['contype'] == 4:
        return {
            value.get('conname', 'check'): _parse(value['raw_expr']),
            'initially_valid': value.get('initially_valid', False)
        }
    elif value['contype'] == 5:
        if 'keys' in value:
            return {'primary_key': _parse(value['keys'])}
        elif 'conname' in value:
            return {'primary_key': value['conname']}
        else:
            return {'primary_key': True}
    elif value['contype'] == 8:
        fk_col = _ensure_list(_parse(value['fk_attrs']))
        ref_col = _ensure_list(_parse(value['pk_attrs']))
        return {
            'name': value['conname'],
            'fk_columns': fk_col,
            'ref_table': _parse(value['pktable']),
            'ref_columns': ref_col,
            'match': _FK_MATCH.get(value['fk_matchtype']),
            'on_delete': _FK_ACTION[value['fk_del_action']],
            'on_update': _FK_ACTION[value['fk_upd_action']],
            'deferrable': value.get('deferrable', None),
            'initially_deferred': value.get('initdeferred', None)
        }
    LOGGER.error('Unsupported constraint: %r', value)
    raise RuntimeError


def _parse_createdb_stmt(value: dict) -> dict:
    return {'name': value['dbname'],
            'options': {e['defname']: e['arg']
                        for e in _parse(value['options'])}}


def _parse_create_domain_stmt(value: dict) -> dict:
    return {
        'name': _parse(value['domainname']),
        'type': _parse(value['typeName']),
        'constraints': _parse(value['constraints'])
    }


def _parse_create_enum_stmt(value: dict) -> dict:
    return {'name': _parse(value['typeName']), 'values': _parse(value['vals'])}


def _parse_create_range_stmt(value: dict) -> dict:
    return {'name': _parse(value['typeName']),
            'range': {e['defname']: e['arg'] for e in _parse(value['params'])}}


def _parse_create_stmt(value: dict):
    return {k: _parse(v) for k, v in value.items()}


def _parse_create_trig_stmt(value: dict) -> dict:
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
    funcname = _parse(value['funcname'])
    if isinstance(funcname, list):
        funcname = '.'.join(funcname)
    transitions = _parse(value.get('transitionRels', []))
    if not isinstance(transitions, list):
        transitions = [transitions]
    return {
        'when': when,
        'events': events,
        'relation': _relation_name(_parse(value['relation'])),
        'name': value['trigname'],
        'row': value.get('row', False),
        'transitions': transitions,
        'condition': _parse(value.get('whenClause')),
        'function': '{}()'.format(funcname)
    }


def _parse_current_of_expr(value: dict) -> str:
    return 'CURRENT OF {}'.format(value['cursor_name'])


def _parse_def_elem(value: dict) -> dict:
    return {'arg': _parse(value['arg']), 'defname': value['defname']}


def _parse_define_stmt(value: dict) -> dict:
    return {
        'name': _parse(value['defnames']),
        'definition': {e['defname']: e['arg']
                       for e in _parse(value['definition'])}}


def _parse_delete_stmt(value: dict) -> str:
    parts = ['DELETE', 'FROM', _relation_name(_parse(value['relation']))]
    if 'usingClause' in value:
        parts.append('USING')
        parts.append(', '.join(_ensure_list(_parse(value['usingClause']))))
    if 'whereClause' in value:
        parts += ['WHERE', _parse(value['whereClause'])]
    if 'returningList' in value:
        parts += ['RETURNING', ', '.join(
            _ensure_list(_parse(value['returningList'])))]
    return ' '.join(parts)


def _parse_float(value: dict) -> int:
    return _parse(value)


def _parse_func_call(value: dict) -> str:
    args = [str(a) for a in _ensure_list(_parse(value.get('args', [])))]
    funcname = '.'.join(_ensure_list(_parse(value['funcname'])))
    return '{}({})'.format(funcname, ', '.join(args))


def _parse_grant_stmt(value: dict) -> dict:
    privileges = _ensure_list(_parse(value.get('privileges', 'ALL')))
    return {
        'action': 'GRANT' if value.get('is_grant') else 'REVOKE',
        'grantees': _parse(value['grantees']),
        'objects': _parse(value['objects']),
        'object_type': _ACL_OBJECT_TYPE[value['objtype']],
        'privileges': privileges
    }


def _parse_index_elem(value: dict) -> dict:
    return {
        'name': value.get('name', _parse(value.get('expr'))),
        'null_order': _NULLORDERING[value['nulls_ordering']],
        'order': _ORDERING[value['ordering']]
    }


def _parse_index_stmt(value: dict) -> dict:
    options = _parse(value.get('options', {}))
    if not isinstance(options, list):
        options = [options] if options else []
    return {
        'name': value['idxname'],
        'relation': _relation_name(_parse(value['relation'])),
        'type': value['accessMethod'],
        'columns': _ensure_list(_parse(value['indexParams'])),
        'where': _parse(value.get('whereClause')),
        'options': {r['defname']: r['arg'] for r in options},
        'tablespace': value.get('tableSpace'),
        'unique': value.get('unique', False)
    }


def _parse_infer_clause(value: dict) -> typing.Union[list, str]:
    if 'indexElems' in value:
        parts = ['({})'.format(', '.join(e['IndexElem']['name']
                                         for e in value['indexElems']))]
    elif 'conname' in value:
        parts = ['ON CONSTRAINT {}'.format(value['conname'])]
    if 'whereClause' in value:
        parts += ['WHERE', _parse(value['whereClause'])]
    return ' '.join(parts)


def _parse_insert_stmt(value: dict) -> str:
    parts = []
    if 'withClause' in value:
        parts.append(_parse(value['withClause']))
    parts.append('INSERT INTO')
    parts.append(_parse(value['relation']))
    if 'cols' in value:
        parts.append('({})'.format(', '.join(_parse(value['cols']))))
    if not value.get('selectStmt'):
        parts.append('DEFAULT')
        parts.append('VALUES')
    else:
        select = _parse(value['selectStmt'])
        if select.startswith('('):
            parts.append('VALUES')
        parts.append(select)
    if 'returningList' in value:
        parts += ['RETURNING', ', '.join(
            _ensure_list(_parse(value['returningList'])))]
    if 'onConflictClause' in value:
        on_conflict = _parse(value['onConflictClause'])
        parts.append(on_conflict)
    return ' '.join(parts)


def _parse_integer(value: dict) -> int:
    return value['ival']


def _parse_join_expr(value: dict) -> str:
    parts = []
    parts.append(_parse(value['larg']))
    parts.append(_JOIN_TYPE[value['jointype']])
    parts.append(_parse(value['rarg']))
    if 'quals' in value:
        parts.append('ON')
        parts.append(_parse(value['quals']))
    return ' '.join(parts)


def _parse_multi_assign_ref(value: dict) -> str:
    return ', '.join(_ensure_list(_parse(value['source'])))


def _parse_notify_stmt(value: dict) -> str:
    return 'NOTIFY {}'.format(value['conditionname'])


def _parse_null_test(value: dict) -> str:
    return '{} {} NULL'.format(
        _parse(value['arg']), _NULLTEST[value['nulltesttype']])


def _parse_on_conflict_clause(value: dict) -> str:
    parts = ['ON CONFLICT']
    if 'infer' in value:
        infer = _parse(value['infer'])
        if isinstance(infer, list):
            parts.append('({})'.format(', '.join(infer)))
        else:
            parts.append(infer)
    parts.append(_ON_CONFLICT[value['action']])
    if 'targetList' in value:
        _set_res_target_mode(value['targetList'], 'update')
        parts.append(', '.join(_ensure_list(_parse(value['targetList']))))
    if 'whereClause' in value:
        parts += ['WHERE', _parse(value['whereClause'])]
    return ' '.join(parts)


def _parse_options(values: list) -> dict:
    return {o['defname']: o['arg'] for o in [_parse(v) for v in values]}


def _parse_range_function(value: dict) -> str:
    parts = []
    if value.get('lateral') is True:
        parts.append('LATERAL')
    parts.append(', '.join(
        [f for f in _parse(value['functions'])[0] if f is not None]))
    if 'coldeflist' in value:
        parts.append('AS')
        parts.append('({})'.format(
            ', '.join(_ensure_list(['{} {}'.format(f['name'], f['type'])
                                    for f in _parse(value['coldeflist'])]))))
    if value.get('ordinality'):
        parts.append('WITH ORDINALITY')
    if value.get('alias'):
        parts.append('AS')
        parts.append(_parse(value['alias']))
    return ' '.join(parts)


def _parse_range_var(value: typing.Union[dict, int, str]) -> str:
    if isinstance(value, dict):
        if 'relname' in value:
            return _relation_name(value)
        raise RuntimeError
    return str(value)


def _parse_relation(value: dict):
    return _relation_name(_parse(value))


def _parse_raw_stmt(value: dict) -> str:
    return _parse(value['stmt'])


def _parse_res_target(value: dict) -> str:
    name = value.get('name')
    if 'indirection' in value:
        name = '{}{}'.format(
            name, ''.join(_ensure_list(_parse(value['indirection']))))
    if value.get('mode') == 'update':
        if 'MultiAssignRef' in value['val']:
            return name
        return '{} = {}'.format(name, _parse(value['val']))
    if name and 'val' in value:
        return '{} AS {}'.format(_parse(value['val']), name)
    return name if name else _parse(value['val'])


def _parse_role_spec(value: dict) -> str:
    return value.get('rolename', _ACL_ROLE_TYPE[value['roletype']])


def _parse_row_expr(value: dict) -> list:
    return _parse(value['args'])


def _parse_rule_stmt(value: dict) -> dict:
    return {
        'name': _parse(value['rulename']),
        'table': _relation_name(_parse(value['relation'])),
        'event': _RULE_EVENTS[value['event']],
        'instead': value.get('instead', False),
        'replace': value.get('replace', False),
        'where': _parse(value.get('whereClause')),
        'action': _parse(value['actions'])}


def _parse_select_stmt(value: dict) -> str:
    if 'valuesLists' in value:
        return ', '.join(
            '({})'.format(', '.join(str(c) for c in row))
            for row in _parse(value['valuesLists']))
    parts = []
    if 'withClause' in value:
        parts.append(_parse(value['withClause']))
    if 'larg' in value and 'rarg' in value:
        parts.append(_parse(value['larg']))
        parts.append(_SELECT_OP[value['op']])
        if value.get('all') is True:
            parts.append('ALL')
        if value.get('groupClauses'):
            raise ValueError
        parts.append(_parse(value['rarg']))
    else:
        parts.append('SELECT')
        parts.append(
            ', '.join(['.'.join(str(t)) if isinstance(t, list) else str(t)
                       for t in _ensure_list(_parse(value['targetList']))]))
    if 'fromClause' in value:
        from_clause = _ensure_list(_parse(value['fromClause']))
        parts += ['FROM', ', '.join([_relation_name(f) for f in from_clause])]
    if 'whereClause' in value:
        parts += ['WHERE', _parse(value['whereClause'])]
    if 'groupClause' in value:
        parts += ['GROUP BY', _parse(value['groupClause'])]
    if 'havingClause' in value:
        parts += ['HAVING', _parse(value['havingClause'])]
    if 'sortClause' in value:
        parts += ['ORDER BY', _parse(value['sortClause'])]
    for unsupported in ['intoClause', 'windowClause', 'valueLists',
                        'limitOffset', 'limitCount', 'lockingClause']:
        if unsupported in value:
            LOGGER.error('Unsupported keyword: %r', value)
            raise RuntimeError('{} in value'.format(unsupported))
    return ' '.join([str(p) for p in parts])


def _parse_set_to_default(_value: dict) -> str:
    return 'DEFAULT'


def _parse_sort_by(value: dict) -> str:
    return _parse(value['node'])


def _parse_stmt(value: dict) -> str:
    return _parse(value)


def _parse_string(value: dict) -> str:
    return value['str']


def _parse_sql_value_function(value: dict) -> str:
    return _SQL_VALUE_FUNCTION[value['op']]


def _parse_sublink(value: dict) -> str:
    if value['subLinkType'] == 2:
        return '{} IN ({})'.format(
            _parse(value['testexpr']), _parse(value['subselect']))
    elif value['subLinkType'] == 4:
        return '({})'.format(_parse(value['subselect']))
    raise RuntimeError


def _parse_trigger_transition(value: dict) -> dict:
    return {
        'name': value['name'],
        'is_new': value.get('isNew', False),
        'is_table': value.get('isTable', False)
    }


def _parse_type_cast(value: dict) -> str:
    type_name = _parse(value['typeName'])
    value = _parse(value['arg'])
    if type_name == 'bool':
        return _BOOL_TEST[value]
    return '{}::{}'.format(value, type_name)


def _parse_type_name(value: dict) -> str:
    name = _parse(value['names'])
    if isinstance(name, list):
        if 'pg_catalog' in name:
            name.remove('pg_catalog')
        if len(name) > 1:
            name = '.'.join(name)
        name = name[0]
    if name == 'bpchar':
        name = 'char'
    parts = [name]
    if 'typmods' in value:
        precision = _parse(value['typmods'])
        if name == 'interval':
            parts.append(' ')
            parts.append(_INTERVAL_FIELDS[precision])
        else:
            parts.append('({})'.format(precision))
    if 'arrayBounds' in value:
        for _iter in range(0, len(value['arrayBounds'])):
            parts.append('[]')
    return ''.join(parts)


def _parse_update_stmt(value: dict) -> str:
    parts = []
    if 'withClause' in value:
        parts.append(_parse(value['withClause']))
    parts.append('UPDATE')
    parts.append(_relation_name(_parse(value['relation'])))
    parts.append('SET')
    first_target = value['targetList'][0]
    if 'MultiAssignRef' in first_target['ResTarget'].get('val', {}).keys():
        parts.append('({})'.format(
            ', '.join(e['ResTarget']['name'] for e in value['targetList'])))
        parts.append('=')
        right = _parse(first_target['ResTarget']['val'])
        if right.startswith('(') and right.endswith(')'):
            right = right[1:-1]
        parts.append('({})'.format(right))
    else:
        _set_res_target_mode(value['targetList'], 'update')
        parts.append(
            ', '.join(['.'.join(str(t)) if isinstance(t, list) else str(t)
                       for t in _ensure_list(_parse(value['targetList']))]))
    if 'fromClause' in value:
        parts += ['FROM', _parse(value['fromClause'])]
    if 'whereClause' in value:
        parts += ['WHERE', _parse(value['whereClause'])]
    if 'returningList' in value:
        parts += ['RETURNING', ', '.join(
            _ensure_list(_parse(value['returningList'])))]
    return ' '.join(parts)


def _parse_with_clause(value: dict) -> str:
    ctes = []
    for cte in value.get('ctes', []):
        ctes.append(_parse(cte))
    parts = ['WITH']
    if value.get('recursive') is True:
        parts.append('RECURSIVE')
    parts.append(', '.join(ctes))
    return ' '.join(parts)


def _relation_name(value: typing.Union[dict, str]) -> str:
    if isinstance(value, str):
        return value
    parts = []
    if 'schemaname' in value:
        parts.append('{}.{}'.format(value['schemaname'], value['relname']))
    else:
        parts.append(value['relname'])
    if 'alias' in value:
        parts.append('AS')
        parts.append(_parse(value['alias']))
    return ' '.join(parts)


def _set_res_target_mode(values: dict, mode: str) -> typing.NoReturn:
    for offset in range(0, len(values)):
        values[offset]['ResTarget']['mode'] = mode


_PARSERS = {
    'A_ArrayExpr': _parse_a_arrayexpr,
    'A_Const': _parse_a_const,
    'A_Expr': _parse_a_expr,
    'A_Indices': _parse_a_indices,
    'A_Star': _parse_a_star,
    'AccessPriv': _parse_access_priv,
    'Alias': _parse_alias,
    'AlterTableCmd': _parse_alter_table_cmd,
    'AlterTableStmt': _parse_alter_table_stmt,
    'BoolExpr': _parse_bool_expr,
    'BooleanTest': _parse_boolean_test,
    'CaseExpr': _parse_case_expr,
    'CaseWhen': _parse_case_when,
    'ColumnDef': _parse_column_def,
    'ColumnRef': _parse_column_ref,
    'CommentStmt': _parse_comment_stmt,
    'CommonTableExpr': _parse_common_table_expr,
    'CompositeTypeStmt': _parse_composite_type_stmt,
    'Constraint': _parse_constraint,
    'CreatedbStmt': _parse_createdb_stmt,
    'CreateDomainStmt': _parse_create_domain_stmt,
    'CreateEnumStmt': _parse_create_enum_stmt,
    'CreateRangeStmt': _parse_create_range_stmt,
    'CreateStmt': _parse_create_stmt,
    'CreateTrigStmt': _parse_create_trig_stmt,
    'CurrentOfExpr': _parse_current_of_expr,
    'DefElem': _parse_def_elem,
    'DefineStmt': _parse_define_stmt,
    'DeleteStmt': _parse_delete_stmt,
    'Float': _parse_float,
    'FuncCall': _parse_func_call,
    'GrantStmt': _parse_grant_stmt,
    'IndexElem': _parse_index_elem,
    'IndexStmt': _parse_index_stmt,
    'InferClause': _parse_infer_clause,
    'InsertStmt': _parse_insert_stmt,
    'Integer': _parse_integer,
    'JoinExpr': _parse_join_expr,
    'MultiAssignRef': _parse_multi_assign_ref,
    'NotifyStmt': _parse_notify_stmt,
    'Null': lambda _x: None,
    'NullTest': _parse_null_test,
    'options': _parse_options,
    'OnConflictClause': _parse_on_conflict_clause,
    'relation': _parse_relation,
    'stmt': _parse_stmt,
    'RangeFunction': _parse_range_function,
    'RangeVar': _parse_range_var,
    'RawStmt': _parse_raw_stmt,
    'ResTarget': _parse_res_target,
    'RoleSpec': _parse_role_spec,
    'RowExpr': _parse_row_expr,
    'RuleStmt': _parse_rule_stmt,
    'SelectStmt': _parse_select_stmt,
    'SetToDefault': _parse_set_to_default,
    'SortBy': _parse_sort_by,
    'SQLValueFunction': _parse_sql_value_function,
    'String': _parse_string,
    'SubLink': _parse_sublink,
    'TriggerTransition': _parse_trigger_transition,
    'TypeCast': _parse_type_cast,
    'TypeName': _parse_type_name,
    'UpdateStmt': _parse_update_stmt,
    'WithClause': _parse_with_clause
}
