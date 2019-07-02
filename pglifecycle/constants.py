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
DOMAIN = 'DOMAIN'
ENCODING = 'ENCODING'
EVENT_TRIGGER = 'EVENT TRIGGER'
EXTENSION = 'EXTENSION'
FOREIGN_DATA_WRAPPER = 'FOREIGN DATA WRAPPER'
FOREIGN_TABLE = 'FOREIGN TABLE'
FK_CONSTRAINT = 'FK CONSTRAINT'
FUNCTION = 'FUNCTION'
INDEX = 'INDEX'
MATERIALIZED_VIEW = 'MATERIALIZED VIEW'
OPERATOR = 'OPERATOR'
OPERATORS = 'OPERATORS'
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

CHILD_OBJ_TYPES = [
    DEFAULT,
    COMMENT,
    POLICY,
    CHECK_CONSTRAINT,
    CONSTRAINT,
    FK_CONSTRAINT,
    INDEX,
    ACL,
    SECURITY_LABEL,
    SEQUENCE_OWNED_BY,
    SERVER,
    FOREIGN_TABLE,
    USER_MAPPING,
    PUBLICATION_TABLE
]

PATHS = {
    AGGREGATE: pathlib.Path('functions'),
    CAST: pathlib.Path('casts'),
    COLLATION: pathlib.Path('collations'),
    CONVERSION: pathlib.Path('conversions'),
    DOMAIN: pathlib.Path('domains'),
    EVENT_TRIGGER: pathlib.Path('event_triggers'),
    EXTENSION: pathlib.Path('extensions'),
    FOREIGN_DATA_WRAPPER: pathlib.Path('foreign_data_wrappers'),
    FUNCTION: pathlib.Path('functions'),
    MATERIALIZED_VIEW: pathlib.Path('materialized_views'),
    OPERATOR: pathlib.Path('operators'),
    PROCEDURE: pathlib.Path('procedures'),
    PROCEDURAL_LANGUAGE: pathlib.Path('extensions'),
    PUBLICATION: pathlib.Path('publications'),
    ROLE: pathlib.Path('roles'),
    RULE: pathlib.Path('rules'),
    SCHEMA: pathlib.Path('schemata'),
    SEQUENCE: pathlib.Path('sequences'),
    SERVER: pathlib.Path('servers'),
    SHELL_TYPE: pathlib.Path('types'),
    SUBSCRIPTION: pathlib.Path('subscriptions'),
    TABLE: pathlib.Path('tables'),
    TABLESPACE: pathlib.Path('tablespaces'),
    TEXT_SEARCH_CONFIGURATION: pathlib.Path('text_search'),
    TEXT_SEARCH_DICTIONARY: pathlib.Path('text_search'),
    TYPE: pathlib.Path('types'),
    VIEW: pathlib.Path('views')
}

SECTION_NONE: str = 'None'
SECTION_PRE_DATA: str = 'Pre-Data'
SECTION_DATA: str = 'DATA'
SECTION_POST_DATA: str = 'Post-Data'
