"""
Common Constants

"""
import enum
import pathlib

ALTER = 'ALTER'
COMMENT = 'COMMENT'
CREATE = 'CREATE'
CREATE_OR_REPLACE = 'CREATE OR REPLACE'
GRANT = 'GRANT'
REVOKE = 'REVOKE'
SET = 'SET'

AGGREGATE = 'AGGREGATE'
ACL = 'ACL'
CAST = 'CAST'
CHECK_CONSTRAINT = 'CHECK CONSTRAINT'
COLUMN = 'COLUMN'
COLLATION = 'COLLATION'
CONSTRAINT = 'CONSTRAINT'
CONVERSION = 'CONVERSION'
DATABASE = 'DATABASE'
DEFAULT = 'DEFAULT'
DIRECTIVE = 'DIRECTIVE'
DIRECTIVES = 'DIRECTIVES'
DML = 'DML'
DOMAIN = 'DOMAIN'
ENCODING = 'ENCODING'
EVENT_TRIGGER = 'EVENT TRIGGER'
EXTENSION = 'EXTENSION'
FOREIGN_DATA_WRAPPER = 'FOREIGN DATA WRAPPER'
FOREIGN_SERVER = 'FOREIGN SERVER'
FOREIGN_TABLE = 'FOREIGN TABLE'
FK_CONSTRAINT = 'FK CONSTRAINT'
FUNCTION = 'FUNCTION'
GROUP = 'GROUP'
INDEX = 'INDEX'
LARGE_OBJECT = 'LARGE OBJECT'
MATERIALIZED_VIEW = 'MATERIALIZED VIEW'
OPERATOR = 'OPERATOR'
OPERATOR_CLASS = 'OPERATOR CLASS'
POLICY = 'POLICY'
PROCEDURE = 'PROCEDURE'
PROCEDURAL_LANGUAGE = 'PROCEDURAL LANGUAGE'
PUBLICATION = 'PUBLICATION'
PUBLICATION_TABLE = 'PUBLICATION TABLE'
ROLE = 'ROLE'
RULE = 'RULE'
SEARCHPATH = 'SEARCHPATH'
SEQUENCE_OWNED_BY = 'SEQUENCE OWNED BY'
SEQUENCE_SET = 'SEQUENCE SET'
SCHEMA = 'SCHEMA'
SECURITY_LABEL = 'SECURITY LABEL'
SEQUENCE = 'SEQUENCE'
SERVER = 'SERVER'
SHELL_TYPE = 'SHELL TYPE'
STDSTRINGS = 'STDSTRINGS'
SUBSCRIPTION = 'SUBSCRIPTION'
TABLE = 'TABLE'
TABLESPACE = 'TABLESPACE'
TEXT_SEARCH_DICTIONARY = 'TEXT SEARCH DICTIONARY'
TEXT_SEARCH_CONFIGURATION = 'TEXT SEARCH CONFIGURATION'
TRIGGER = 'TRIGGER'
TYPE = 'TYPE'
USER = 'USER'
USER_MAPPING = 'USER MAPPING'
VIEW = 'VIEW'

INSERT = 'INSERT'
UPDATE = 'UPDATE'
DELETE = 'DELETE'
SELECT = 'SELECT'
TRUNCATE = 'TRUNCATE'

AFTER = 'AFTER'
BEFORE = 'BEFORE'
INSTEAD = 'INSTEAD OF'

PATHS = {
    AGGREGATE: pathlib.Path('aggregates'),
    CAST: pathlib.Path('casts'),
    COLLATION: pathlib.Path('collations'),
    CONVERSION: pathlib.Path('conversions'),
    DML: pathlib.Path('dml'),
    DOMAIN: pathlib.Path('domains'),
    EVENT_TRIGGER: pathlib.Path('event_triggers'),
    FOREIGN_DATA_WRAPPER: pathlib.Path('foreign_data_wrappers'),
    FUNCTION: pathlib.Path('functions'),
    GROUP: pathlib.Path('groups'),
    MATERIALIZED_VIEW: pathlib.Path('materialized_views'),
    OPERATOR: pathlib.Path('operators'),
    PROCEDURE: pathlib.Path('procedures'),
    PUBLICATION: pathlib.Path('publications'),
    ROLE: pathlib.Path('roles'),
    SCHEMA: pathlib.Path('schemata'),
    SEQUENCE: pathlib.Path('sequences'),
    SERVER: pathlib.Path('servers'),
    SUBSCRIPTION: pathlib.Path('subscriptions'),
    TABLE: pathlib.Path('tables'),
    TABLESPACE: pathlib.Path('tablespaces'),
    TEXT_SEARCH_CONFIGURATION: pathlib.Path('text_search'),
    TEXT_SEARCH_DICTIONARY: pathlib.Path('text_search'),
    TYPE: pathlib.Path('types'),
    USER: pathlib.Path('users'),
    USER_MAPPING: pathlib.Path('user_mappings'),
    VIEW: pathlib.Path('views'),
}

OBJ_KEYS = {
    'columns': COLUMN,
    'constraints': CONSTRAINT,
    'conversions': CONVERSION,
    'databases': DATABASE,
    'domains': DOMAIN,
    'extensions': EXTENSION,
    'foreign data wrappers': FOREIGN_DATA_WRAPPER,
    'foreign servers': FOREIGN_SERVER,
    'functions': FUNCTION,
    'groups': GROUP,
    'indexes': INDEX,
    'languages': PROCEDURAL_LANGUAGE,
    'large objects': LARGE_OBJECT,
    'operators': OPERATOR,
    'roles': ROLE,
    'sequences': SEQUENCE,
    'schemata': SCHEMA,
    'tables': TABLE,
    'tablespaces': TABLESPACE,
    'types': TYPE,
    'views': VIEW
}

GRANT_KEYS = {v: k for k, v in OBJ_KEYS.items()}

GRANT_SORT_WEIGHTS = {
    'ALL': -1,
    'SELECT': 0,
    'INSERT': 1,
    'UPDATE': 2,
    'DELETE': 3,
    'USAGE': 4,
    'TRUNCATE': 5,
    'REFERENCES': 6,
    'TRIGGER': 7,
    'CREATE': 0,
    'CONNECT': 1,
    'TEMPORARY': 2
}


OPTIONS_WEIGHTS = {
    'SUPERUSER': 0,
    'NOSUPERUSER': 0,
    'CREATEDB': 1,
    'NOCREATEDB': 1,
    'CREATEROLE': 2,
    'NOCREATEROLE': 2,
    'INHERIT': 3,
    'NOINHERIT': 3,
    'LOGIN': 4,
    'NOLOGIN': 4,
    'REPLICATION': 5,
    'NOREPLICATION': 5,
    'BYPASSRLS': 6,
    'NOBYPASSRLS': 6
}

TABLE_KEYS = {
    ACL: 'acls',
    CHECK_CONSTRAINT: 'check constraints',
    CONSTRAINT: 'constraints',
    DEFAULT: 'defaults',
    FK_CONSTRAINT: 'foreign keys',
    INDEX: 'indexes',
    RULE: 'rules',
    TRIGGER: 'triggers',
}

A_EXPR_KIND = {
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

ACL_OBJECT_TYPE = {
    1: 'TABLE',
    10: 'SCHEMA'}

ACL_ROLE_TYPE = {
    0: 'USER',
    3: 'PUBLIC'}

BOOL_OP = {0: 'AND', 1: 'OR', 2: 'NOT'}

BOOL_TEST = {1: 'TRUE', 2: 'FALSE', "'t'": 'TRUE', "'f'": 'FALSE'}

FK_ACTION = {
    'a': None,  # NO ACTION
    'c': 'CASCADE',
    'n': 'SET NULL',
    'r': 'RESTRICT'}

FK_MATCH = {'f': 'FULL', 'p': 'PARTIAL', 's': None}

GENERATED = {'a': 'ALWAYS', 'd': 'BY DEFAULT'}

GROUPING_SET = {
    0: None,
    1: 'SIMPLE',
    2: 'ROLLUP',
    3: 'CUBE',
    4: 'GROUPING SETS'
}

INTERVAL_FIELDS = {
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

JOIN_TYPE = {
    0: None,  # INNER
    1: 'LEFT',
    2: 'FULL',
    3: 'RIGHT',
    4: 'EXISTS',
    5: 'NOT EXISTS',
    6: 'UNIQUE OUTER',
    7: 'UNIQUE INNER'
}

NULL_ORDERING = {0: None, 1: 'FIRST', 2: 'LAST'}
NULL_TEST = {0: 'IS', 1: 'IS NOT'}

ON_CONFLICT = {
    1: 'DO NOTHING',
    2: 'DO UPDATE SET'
}

ORDERING = {0: None, 1: 'ASC', 2: 'DESC'}

ROW_COMPARE = {
    1: '<',
    2: '<=',
    3: '=',
    4: '>=',
    5: '>',
    6: '!=',
}

RULE_EVENTS = {
    1: SELECT,
    2: UPDATE,
    3: INSERT,
    4: DELETE
}

SELECT_OP = {
    0: None,
    1: 'UNION',
    2: 'INTERSECT',
    3: 'EXCEPT'
}

SQL_VALUE_FUNCTION = {
    3: 'CURRENT_TIMESTAMP'
}

SUBLINK_TYPE = {
    0: 'EXISTS',
    1: 'ALL',
    2: 'IN',   # is 'ANY' if operName is set
    3: 'ROW-COMPARE',     # ROWCOMPARE
    4: 'EXPRESSION',   # EXPR
    5: 'MULTI-EXPRESSION',   # MULTIEXPR
    6: 'ARRAY',
    7: 'WITH'  # CTE
}

TRIGGER_INSERT = (1 << 2)
TRIGGER_DELETE = (1 << 3)
TRIGGER_UPDATE = (1 << 4)
TRIGGER_TRUNCATE = (1 << 5)
TRIGGER_BEFORE = (1 << 1)
TRIGGER_AFTER = 0x00000000
TRIGGER_INSTEAD = (1 << 6)

VIEW_CHECK_OPTION = {0: None, 1: 'LOCAL', 2: 'CASCADED'}


class AlterTableType(enum.IntEnum):
    """Enum identifying the alter_table_cmd type"""
    ADD_COLUMN = 0
    ADD_COLUMN_RECURSE = 1
    ADD_COLUMN_TO_VIEW = 2
    COLUMN_DEFAULT = 3
    DROP_NOT_NULL = 4
    SET_NOT_NULL = 5
    SET_STATISTICS = 6
    SET_OPTIONS = 7
    RESET_OPTIONS = 8
    SET_STORAGE = 9
    DROP_COLUMN = 10
    DROP_COLUMN_RECURSE = 11
    ADD_INDEX = 12
    RE_ADD_INDEX = 13
    ADD_CONSTRAINT = 14
    ADD_CONSTRAINT_RECURSE = 15
    RE_ADD_CONSTRAINT = 16
    ALTER_CONSTRAINT = 17
    VALIDATE_CONSTRAINT = 18
    VALIDATE_CONSTRAINT_RECURSE = 19
    PROCESSED_CONSTRAINT = 20
    ADD_INDEX_CONSTRAINT = 21
    DROP_CONSTRAINT = 22
    DROP_CONSTRAINT_RECURSE = 23
    RE_ADD_COMMENT = 24
    ALTER_COLUMN_TYPE = 25
    ALTER_COLUMN_GENERIC_OPTIONS = 26
    CHANGE_OWNER = 27
    CLUSTER_ON = 28
    DROP_CLUSTER = 29
    SET_LOGGED = 30
    SET_UN_LOGGED = 31
    ADD_OIDS = 32
    ADD_OIDS_RECURSE = 33
    DROP_OIDS = 34
    SET_TABLE_SPACE = 35
    SET_REL_OPTIONS = 36
    RESET_REL_OPTIONS = 37
    REPLACE_REL_OPTIONS = 38
    ENABLE_TRIG = 39
    ENABLE_ALWAYS_TRIG = 40
    ENABLE_REPLICA_TRIG = 41
    DISABLE_TRIG = 42
    ENABLE_TRIG_ALL = 43
    DISABLE_TRIG_ALL = 44
    ENABLE_TRIG_USER = 45
    DISABLE_TRIG_USER = 46
    ENABLE_RULE = 47
    ENABLE_ALWAYS_RULE = 48
    ENABLE_REPLICA_RULE = 49
    DISABLE_RULE = 50
    ADD_INHERIT = 51
    DROP_INHERIT = 52
    ADD_OF = 53
    DROP_OF = 54
    REPLICA_IDENTITY = 55
    ENABLE_ROW_SECURITY = 56
    DISABLE_ROW_SECURITY = 57
    FORCE_ROW_SECURITY = 58
    NO_FORCE_ROW_SECURITY = 59
    GENERIC_OPTIONS = 60
    ATTACH_PARTITION = 61
    DETACH_PARTITION = 62
    ADD_IDENTITY = 63
    SET_IDENTITY = 64
    DROP_IDENTITY = 65


class DefElemAction(enum.IntEnum):
    """Actions for a DefElem"""
    UNSPECIFIED = 0
    SET = 1
    ADD = 2
    DROP = 3


class ObjectType(enum.IntEnum):
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
