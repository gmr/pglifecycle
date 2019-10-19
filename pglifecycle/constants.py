"""
Common Constants

"""
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

GRANT_KEYS = {
    COLUMN: 'columns',
    DATABASE: 'databases',
    DOMAIN: 'domains',
    FOREIGN_DATA_WRAPPER: 'foreign data wrappers',
    FOREIGN_SERVER: 'foreign servers',
    FUNCTION: 'functions',
    GROUP: 'groups',
    PROCEDURAL_LANGUAGE: 'languages',
    LARGE_OBJECT: 'large objects',
    ROLE: 'roles',
    SEQUENCE: 'sequences',
    SCHEMA: 'schemata',
    TABLE: 'tables',
    TABLESPACE: 'tablespaces',
    TYPE: 'types',
    VIEW: 'views'
}

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
