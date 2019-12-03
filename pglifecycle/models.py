"""
Models for database objects

"""
from __future__ import annotations

import dataclasses
import typing

from pglifecycle import constants

Dependencies = dataclasses.field(default_factory=lambda: [])
ACLList = typing.Optional[typing.Dict[str, typing.List[str]]]


@dataclasses.dataclass
class ACLs:
    """Represents role grant/revoke ACLs"""
    columns: ACLList = None
    databases: ACLList = None
    domains: ACLList = None
    foreign_data_wrappers: ACLList = None
    foreign_servers: ACLList = None
    functions: ACLList = None
    groups: typing.Optional[typing.List[str]] = None
    languages: ACLList = None
    large_objects: ACLList = None
    roles: typing.Optional[typing.List[str]] = None
    schemata: ACLList = None
    sequences: ACLList = None
    tables: ACLList = None
    tablespaces: ACLList = None
    types: ACLList = None
    views: ACLList = None


@dataclasses.dataclass
class Aggregate:
    """Represents the implementation of an aggregate for a data type"""
    name: str
    schema: str
    owner: str
    arguments: typing.List[Argument]
    sfunc: str
    state_data_type: str
    state_data_size: typing.Optional[int] = None
    ffunc: typing.Optional[str] = None
    finalfunc_extra: typing.Optional[bool] = None
    finalfunc_modify: typing.Optional[str] = None
    combinefunc: typing.Optional[str] = None
    serialfunc: typing.Optional[str] = None
    deserialfunc: typing.Optional[str] = None
    initial_condition: typing.Optional[str] = None
    msfunc: typing.Optional[str] = None
    minvfunc: typing.Optional[str] = None
    mstate_data_type: typing.Optional[str] = None
    mstate_data_size: typing.Optional[int] = None
    mffunc: typing.Optional[str] = None
    mfinalfunc_extra: typing.Optional[bool] = None
    mfinalfunc_modify: typing.Optional[str] = None
    minitial_condition: typing.Optional[str] = None
    sort_operator: typing.Optional[str] = None
    parallel: typing.Optional[str] = None
    hypothetical: typing.Optional[bool] = None
    sql: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Argument:
    """Represents an argument to a function"""
    data_type: str
    mode: str = 'IN'
    name: typing.Optional[str] = None


@dataclasses.dataclass
class Cast:
    """Represents an argument to a function"""
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    source_type: typing.Optional[str] = None
    target_type: typing.Optional[str] = None
    function: typing.Optional[str] = None
    inout: typing.Optional[bool] = None
    assignment: typing.Optional[bool] = None
    implicit: typing.Optional[bool] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class CheckConstraint:
    """Represents a Check Constraint in a Table"""
    name: str
    expression: str


@dataclasses.dataclass
class Collation:
    """Represents a Collation"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    locale: typing.Optional[str] = None
    lc_collate: typing.Optional[str] = None
    lc_ctype: typing.Optional[str] = None
    provider: typing.Optional[str] = None
    deterministic: typing.Optional[bool] = None
    version: typing.Optional[str] = None
    copy_from: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Column:
    """Represents a column in a table"""
    name: str
    data_type: str
    nullable: bool = True
    default: typing.Optional[typing.Any] = None
    collation: typing.Optional[str] = None
    check_constraint: typing.Optional[str] = None
    generated: typing.Optional[ColumnGenerated] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class ColumnGenerated:
    """Represents configuration of a generated column"""
    expression: typing.Optional[str] = None
    sequence: typing.Optional[str] = None
    sequence_behavior: typing.Optional[str] = None


@dataclasses.dataclass
class ConstraintColumns:
    """Defines constraint columns for various table constraints"""
    columns: typing.List[str]
    include: typing.List[str] = None


@dataclasses.dataclass
class Conversion:
    """Represents a Conversion"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    default: typing.Optional[bool] = None
    encoding_from: typing.Optional[str] = None
    encoding_to: typing.Optional[str] = None
    function: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Domain:
    """Represents a Domain"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    data_type: typing.Optional[str] = None
    collation: typing.Optional[str] = None
    default: typing.Optional[str] = None
    check_constraints: typing.Optional[typing.List[DomainConstraint]] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class DomainConstraint:
    """Represents a Check Constraint in a Domain"""
    name: typing.Optional[str] = None
    nullable: typing.Optional[bool] = None
    expression: typing.Optional[str] = None


@dataclasses.dataclass
class EventTrigger:
    """Represents an event trigger"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    event: typing.Optional[str] = None
    filter: typing.Optional[EventTriggerFilter] = None
    function: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class EventTriggerFilter:
    """An event trigger filter"""
    tags: typing.List[str]


@dataclasses.dataclass
class Extension:
    """Represents an extension"""
    name: str
    schema: typing.Optional[str] = None
    version: typing.Optional[str] = None
    cascade: typing.Optional[bool] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class ForeignDataWrapper:
    """Represents a Foreign Data Wrapper"""
    name: str
    owner: str
    handler: typing.Optional[str] = None
    validator: typing.Optional[str] = None
    options: typing.Optional[dict] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class ForeignKey:
    """Represents a Foreign Key on a Table"""
    name: str
    columns: typing.List[str]
    references: ForeignKeyReference
    match_type: typing.Optional[str] = None
    on_delete: str = 'NO ACTION'
    on_update: str = 'NO ACTION'
    deferrable: typing.Optional[bool] = None
    initially_deferred: typing.Optional[bool] = None


@dataclasses.dataclass
class ForeignKeyReference:
    """Represents the table a Foreign Key references"""
    name: str
    columns: typing.List[str]


@dataclasses.dataclass
class Function:
    """Represents a Function"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    parameters: typing.Optional[FunctionParameter] = None
    returns: typing.Optional[str] = None
    language: typing.Optional[str] = None
    transform_types: typing.Optional[typing.List[str]] = None
    window: typing.Optional[bool] = None
    immutable: typing.Optional[bool] = None
    stable: typing.Optional[bool] = None
    volatile: typing.Optional[bool] = None
    leak_proof: typing.Optional[bool] = None
    called_on_null_input: typing.Optional[bool] = None
    strict: typing.Optional[bool] = None
    security: typing.Optional[str] = None
    parallel: typing.Optional[str] = None
    cost: typing.Optional[int] = None
    rows: typing.Optional[int] = None
    support: typing.Optional[str] = None
    configuration: typing.Optional[dict] = None
    definition: typing.Optional[str] = None
    object_file: typing.Optional[str] = None
    link_symbol: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class FunctionParameter:
    """Represents a single parameter for a function"""
    mode: str
    data_type: str
    name: typing.Optional[str] = None
    default: typing.Optional[typing.Any] = None


@dataclasses.dataclass
class Group:
    """Represents a group"""
    name: str
    comment: typing.Optional[str] = None
    environments: typing.Optional[typing.List[str]] = None
    grants: typing.Optional[ACLs] = None
    revocations: typing.Optional[ACLs] = None
    options: typing.Optional[typing.Dict[str, bool]] = None


@dataclasses.dataclass
class Index:
    """Represents an Index on a table"""
    name: str
    sql: typing.Optional[str] = None
    unique: typing.Optional[bool] = None
    recurse: typing.Optional[bool] = None
    parent: typing.Optional[str] = None
    method: typing.Optional[str] = None
    columns: typing.Optional[typing.List[IndexColumn]] = None
    include: typing.Optional[typing.List[str]] = None
    where: typing.Optional[str] = None
    storage_parameters: typing.Optional[typing.Dict[str, str]] = None
    tablespace: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class IndexColumn:
    """Represents a column in an index on a table"""
    name: typing.Optional[str] = None
    expression: typing.Optional[str] = None
    collation: typing.Optional[str] = None
    opclass: typing.Optional[str] = None
    direction: typing.Optional[str] = None
    null_placement: typing.Optional[str] = None


@dataclasses.dataclass
class Language:
    """Represents a Procedural Language"""
    name: str
    replace: bool = False
    trusted: bool = False
    handler: typing.Optional[str] = None
    inline_handler: typing.Optional[str] = None
    validator: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class LikeTable:
    """Represents a the settings for creating a table using LIKE"""
    name: str
    include_comments: typing.Optional[bool] = None
    include_constraints: typing.Optional[bool] = None
    include_defaults: typing.Optional[bool] = None
    include_generated: typing.Optional[bool] = None
    include_identity: typing.Optional[bool] = None
    include_indexes: typing.Optional[bool] = None
    include_statistics: typing.Optional[bool] = None
    include_storage: typing.Optional[bool] = None
    include_all: typing.Optional[bool] = None


@dataclasses.dataclass
class MaterializedView:
    """Represents a MaterializedView"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    columns: typing.Optional[typing.List[str]] = None
    table_access_method: typing.Optional[str] = None
    storage_parameters: typing.Optional[typing.Dict[str, str]] = None
    tablespace: typing.Optional[str] = None
    query: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Operator:
    """Represents an operator used to compare values"""
    name: str
    schema: str
    owner: str
    function: str
    left_arg: typing.Optional[str] = None
    right_arg: typing.Optional[str] = None
    commutator: typing.Optional[str] = None
    negator: typing.Optional[str] = None
    restrict: typing.Optional[str] = None
    join: typing.Optional[str] = None
    hashes: typing.Optional[bool] = None
    merges: typing.Optional[bool] = None
    sql: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class PartitionKeyColumn:
    """Represents a column in a partition key"""
    column_name: typing.Optional[str] = None
    expression: typing.Optional[str] = None
    opclass: typing.Optional[str] = None


@dataclasses.dataclass
class Procedure:
    """Represents a Procedure"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    paramters: typing.Optional[FunctionParameter] = None
    language: typing.Optional[str] = None
    transform_types: typing.Optional[typing.List[str]] = None
    security: typing.Optional[str] = None
    configuration: typing.Optional[dict] = None
    definition: typing.Optional[str] = None
    object_file: typing.Optional[str] = None
    link_symbol: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Publication:
    """Represents a Publication"""
    name: str
    tables: typing.Optional[typing.List[str]] = None
    all_tables: typing.Optional[str] = None
    parameters: typing.Optional[typing.Dict[str, typing.List[str]]] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Role:
    """Represents a role"""
    name: str
    comment: typing.Optional[str] = None
    create: typing.Optional[bool] = None
    environments: typing.Optional[typing.List[str]] = None
    grants: typing.Optional[ACLs] = None
    revocations: typing.Optional[ACLs] = None
    options: typing.Optional[typing.Dict[str, bool]] = None
    settings: typing.Optional[typing.Dict[str, typing.Any]] = None


@dataclasses.dataclass
class Schema:
    """Represents a schema/namespace"""
    name: str
    owner: str
    authorization: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Sequence:
    """Represents a sequence"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    data_type: typing.Optional[str] = None
    increment_by: typing.Optional[int] = None
    min_value: typing.Optional[int] = None
    max_value: typing.Optional[int] = None
    start_with: typing.Optional[int] = None
    cache: typing.Optional[int] = None
    cycle: typing.Optional[bool] = None
    owned_by: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Server:
    """Represents a server"""
    name: str
    foreign_data_wrapper: str
    type: typing.Optional[str] = None
    version: typing.Optional[str] = None
    options: typing.Optional[dict] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Subscription:
    """Represents a logical replication subscription"""
    name: str
    connection: str
    publications: typing.List[str]
    parameters: typing.Optional[typing.Dict[str, typing.List[str]]] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Table:
    """Represents a table"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    unlogged: typing.Optional[bool] = None
    from_type: typing.Optional[str] = None
    parents: typing.Optional[typing.List[str]] = None
    like_table: typing.Optional[LikeTable] = None
    columns: typing.Optional[typing.List[Column]] = None
    indexes: typing.Optional[typing.List[Index]] = None
    primary_key: typing.Optional[ConstraintColumns] = None
    check_constraints: typing.Optional[typing.List[CheckConstraint]] = None
    unique_constraints: typing.Optional[typing.List[ConstraintColumns]] = None
    foreign_keys: typing.Optional[typing.List[ForeignKey]] = None
    triggers: typing.Optional[typing.List[Trigger]] = None
    partition: typing.Optional[TablePartitionBehavior] = None
    partitions: typing.Optional[typing.List[TablePartition]] = None
    access_method: typing.Optional[str] = None
    storage_parameters: typing.Optional[typing.Dict[str, str]] = None
    tablespace: typing.Optional[str] = None
    index_tablespace: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class TablePartition:
    """Defines a table partition"""
    name: str
    schema: str
    default: typing.Optional[bool] = None
    for_values_in: typing.Optional[typing.List[float, int, str]] = None
    for_values_from: typing.Optional[float, int, str] = None
    for_values_to: typing.Optional[float, int, str] = None
    for_values_with: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class TablePartitionBehavior:
    """Defines the data structure defining how a table is partitioned"""
    type: str
    columns: typing.List[TablePartitionColumn]


@dataclasses.dataclass
class TablePartitionColumn:
    """Defines the data structure defining table a partition column"""
    name: typing.Optional[str] = None
    expression: typing.Optional[str] = None
    collation: typing.Optional[str] = None
    opclass: typing.Optional[str] = None


@dataclasses.dataclass
class Tablespace:
    """Represents a tablespace"""
    name: str
    owner: str
    location: str
    options: typing.Optional[typing.Dict[str, float]] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class TextSearch:
    """Represents a complex object for text search"""
    schema: str
    configurations: typing.Optional[typing.List[TextSearchConfig]] = None
    dictionaries: typing.Optional[typing.List[TextSearchDict]] = None
    parsers: typing.Optional[typing.List[TextSearchParser]] = None
    templates: typing.Optional[typing.List[TextSearchTemplate]] = None


@dataclasses.dataclass
class TextSearchConfig:
    """Represents a configuration object for Text Search"""
    name: str
    sql: typing.Optional[str] = None
    parser: typing.Optional[str] = None
    source: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class TextSearchDict:
    """Represents a dictionary object for Text Search"""
    name: str
    sql: typing.Optional[str] = None
    template: typing.Optional[str] = None
    options: typing.Optional[typing.Dict[str, str]] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class TextSearchParser:
    """Represents a parser object for Text Search"""
    name: str
    sql: typing.Optional[str] = None
    start_function: typing.Optional[str] = None
    gettoken_function: typing.Optional[str] = None
    end_function: typing.Optional[str] = None
    lextypes_function: typing.Optional[str] = None
    headline_function: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class TextSearchTemplate:
    """Represents a template for Text Search"""
    name: str
    sql: typing.Optional[str] = None
    lexize_function: typing.Optional[str] = None
    init_function: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Trigger:
    """Table Triggers"""
    sql: typing.Optional[str] = None
    name: typing.Optional[str] = None
    when: typing.Optional[str] = None
    events: typing.Optional[typing.List[str]] = None
    for_each: typing.Optional[str] = None
    condition: typing.Optional[str] = None
    function: typing.Optional[str] = None
    arguments: typing.Optional[typing.List[float, int, str]] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Type:
    """Represents a user defined data type"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    type: typing.Optional[str] = None
    input: typing.Optional[str] = None
    output: typing.Optional[str] = None
    receive: typing.Optional[str] = None
    send: typing.Optional[str] = None
    typmod_in: typing.Optional[str] = None
    typmod_out: typing.Optional[str] = None
    analyze: typing.Optional[str] = None
    internal_length: typing.Union[None, int, str] = None
    passed_by_value: typing.Optional[bool] = None
    alignment: typing.Optional[str] = None
    storage: typing.Optional[str] = None
    like_type: typing.Optional[str] = None
    category: typing.Optional[str] = None
    preferred: typing.Optional[str] = None
    default: typing.Any = None
    element: typing.Optional[str] = None
    delimiter: typing.Optional[str] = None
    collatable: typing.Optional[bool] = None
    columns: typing.Optional[typing.List[TypeColumn]] = None
    enum: typing.Optional[typing.List[str]] = None
    subtype: typing.Optional[str] = None
    subtype_opclass: typing.Optional[str] = None
    collation: typing.Optional[str] = None
    canonical: typing.Optional[str] = None
    subtype_diff: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class TypeColumn:
    """Represents a column in a type"""
    name: str
    data_type: str
    collation: typing.Optional[str] = None


@dataclasses.dataclass
class User:
    """Represents a user"""
    name: str
    comment: typing.Optional[str] = None
    environments: typing.Optional[typing.List[str]] = None
    password: typing.Optional[str] = None
    valid_unitl: typing.Optional[str] = None
    grants: typing.Optional[ACLs] = None
    revocations: typing.Optional[ACLs] = None
    options: typing.Optional[typing.Dict[str, bool]] = None
    settings: typing.Optional[typing.Dict[str, typing.Any]] = None


@dataclasses.dataclass
class UserMapping:
    """Represents a user mapping"""
    name: str
    servers: typing.List[UserMappingServer]


@dataclasses.dataclass
class UserMappingServer:
    """Represents a server for a user mapping"""
    name: str
    options: typing.Optional[typing.Dict[str, typing.Any]] = None


@dataclasses.dataclass
class View:
    """Represents a View"""
    name: str
    schema: str
    owner: str
    sql: typing.Optional[str] = None
    recursive: typing.Optional[bool] = None
    columns: typing.Optional[typing.List[typing.Union[ViewColumn, str]]] = None
    check_option: typing.Optional[str] = None
    security_barrier: typing.Optional[bool] = None
    query: typing.Optional[str] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class ViewColumn:
    """Represents a column in a view or materialized view"""
    name: str
    comment: typing.Optional[str] = None


MAPPINGS = {
    constants.AGGREGATE: Aggregate,
    constants.CAST: Cast,
    constants.COLLATION: Collation,
    constants.CONVERSION: Conversion,
    constants.DOMAIN: Domain,
    constants.EVENT_TRIGGER: EventTrigger,
    constants.EXTENSION: Extension,
    constants.FOREIGN_DATA_WRAPPER: ForeignDataWrapper,
    constants.FUNCTION: Function,
    constants.GROUP: Group,
    constants.MATERIALIZED_VIEW: MaterializedView,
    constants.OPERATOR: Operator,
    constants.PROCEDURE: Procedure,
    constants.PROCEDURAL_LANGUAGE: Language,
    constants.PUBLICATION: Publication,
    constants.ROLE: Role,
    constants.SCHEMA: Schema,
    constants.SEQUENCE: Sequence,
    constants.SERVER: Server,
    constants.SUBSCRIPTION: Subscription,
    constants.TABLE: Table,
    constants.TABLESPACE: Tablespace,
    constants.TEXT_SEARCH: TextSearch,
    constants.TYPE: Type,
    constants.USER: User,
    constants.USER_MAPPING: UserMapping,
    constants.VIEW: View
}


Definition = typing.Union[
    Aggregate,
    Cast,
    Collation,
    Conversion,
    Domain,
    EventTrigger,
    Extension,
    ForeignDataWrapper,
    Function,
    Group,
    Index,
    Language,
    MaterializedView,
    Operator,
    Publication,
    Role,
    Schema,
    Sequence,
    Server,
    Subscription,
    Table,
    Tablespace,
    TextSearch,
    Type,
    User,
    UserMapping,
    View
]


@dataclasses.dataclass
class Item:
    """Represents an item in the project inventory"""
    id: int
    desc: str
    definition: Definition
    dependencies: typing.Set[int] = dataclasses.field(default_factory=set)
