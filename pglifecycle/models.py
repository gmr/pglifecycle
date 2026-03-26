"""
Models for database objects

"""

from __future__ import annotations

import dataclasses
import typing

from pglifecycle import constants

Dependencies = dataclasses.field(default_factory=lambda: [])
ACLList = dict[str, list[str]] | None


@dataclasses.dataclass
class ACLs:
    """Represents role grant/revoke ACLs"""

    columns: ACLList = None
    databases: ACLList = None
    domains: ACLList = None
    foreign_data_wrappers: ACLList = None
    foreign_servers: ACLList = None
    functions: ACLList = None
    groups: list[str] | None = None
    languages: ACLList = None
    large_objects: ACLList = None
    roles: list[str] | None = None
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
    arguments: list[Argument]
    sfunc: str
    state_data_type: str
    state_data_size: int | None = None
    ffunc: str | None = None
    finalfunc_extra: bool | None = None
    finalfunc_modify: str | None = None
    combinefunc: str | None = None
    serialfunc: str | None = None
    deserialfunc: str | None = None
    initial_condition: str | None = None
    msfunc: str | None = None
    minvfunc: str | None = None
    mstate_data_type: str | None = None
    mstate_data_size: int | None = None
    mffunc: str | None = None
    mfinalfunc_extra: bool | None = None
    mfinalfunc_modify: str | None = None
    minitial_condition: str | None = None
    sort_operator: str | None = None
    parallel: str | None = None
    hypothetical: bool | None = None
    sql: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class Argument:
    """Represents an argument to a function"""

    data_type: str
    mode: str = 'IN'
    name: str | None = None


@dataclasses.dataclass
class Cast:
    """Represents an argument to a function"""

    schema: str
    owner: str
    sql: str | None = None
    source_type: str | None = None
    target_type: str | None = None
    function: str | None = None
    inout: bool | None = None
    assignment: bool | None = None
    implicit: bool | None = None
    comment: str | None = None


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
    sql: str | None = None
    locale: str | None = None
    lc_collate: str | None = None
    lc_ctype: str | None = None
    provider: str | None = None
    deterministic: bool | None = None
    version: str | None = None
    copy_from: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class Column:
    """Represents a column in a table"""

    name: str
    data_type: str
    nullable: bool = True
    default: typing.Any | None = None
    collation: str | None = None
    check_constraint: str | None = None
    generated: ColumnGenerated | None = None
    comment: str | None = None


@dataclasses.dataclass
class ColumnGenerated:
    """Represents configuration of a generated column"""

    expression: str | None = None
    sequence: str | None = None
    sequence_behavior: str | None = None


@dataclasses.dataclass
class ConstraintColumns:
    """Defines constraint columns for various table constraints"""

    columns: list[str]
    include: list[str] = None


@dataclasses.dataclass
class Conversion:
    """Represents a Conversion"""

    name: str
    schema: str
    owner: str
    sql: str | None = None
    default: bool | None = None
    encoding_from: str | None = None
    encoding_to: str | None = None
    function: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class Domain:
    """Represents a Domain"""

    name: str
    schema: str
    owner: str
    sql: str | None = None
    data_type: str | None = None
    collation: str | None = None
    default: str | None = None
    check_constraints: list[DomainConstraint] | None = None
    comment: str | None = None


@dataclasses.dataclass
class DomainConstraint:
    """Represents a Check Constraint in a Domain"""

    name: str | None = None
    nullable: bool | None = None
    expression: str | None = None


@dataclasses.dataclass
class EventTrigger:
    """Represents an event trigger"""

    name: str
    sql: str | None = None
    event: str | None = None
    filter: EventTriggerFilter | None = None
    function: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class EventTriggerFilter:
    """An event trigger filter"""

    tags: list[str]


@dataclasses.dataclass
class Extension:
    """Represents an extension"""

    name: str
    schema: str | None = None
    version: str | None = None
    cascade: bool | None = None
    comment: str | None = None


@dataclasses.dataclass
class ForeignDataWrapper:
    """Represents a Foreign Data Wrapper"""

    name: str
    owner: str
    handler: str | None = None
    validator: str | None = None
    options: dict | None = None
    comment: str | None = None


@dataclasses.dataclass
class ForeignKey:
    """Represents a Foreign Key on a Table"""

    name: str
    columns: list[str]
    references: ForeignKeyReference
    match_type: str | None = None
    on_delete: str = 'NO ACTION'
    on_update: str = 'NO ACTION'
    deferrable: bool | None = None
    initially_deferred: bool | None = None


@dataclasses.dataclass
class ForeignKeyReference:
    """Represents the table a Foreign Key references"""

    name: str
    columns: list[str]


@dataclasses.dataclass
class Function:
    """Represents a Function"""

    name: str
    schema: str
    owner: str
    sql: str | None = None
    parameters: FunctionParameter | None = None
    returns: str | None = None
    language: str | None = None
    transform_types: list[str] | None = None
    window: bool | None = None
    immutable: bool | None = None
    stable: bool | None = None
    volatile: bool | None = None
    leak_proof: bool | None = None
    called_on_null_input: bool | None = None
    strict: bool | None = None
    security: str | None = None
    parallel: str | None = None
    cost: int | None = None
    rows: int | None = None
    support: str | None = None
    configuration: dict | None = None
    definition: str | None = None
    object_file: str | None = None
    link_symbol: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class FunctionParameter:
    """Represents a single parameter for a function"""

    mode: str
    data_type: str
    name: str | None = None
    default: typing.Any | None = None


@dataclasses.dataclass
class Group:
    """Represents a group"""

    name: str
    comment: str | None = None
    environments: list[str] | None = None
    grants: ACLs | None = None
    revocations: ACLs | None = None
    options: GroupOptions | None = None


@dataclasses.dataclass
class GroupOptions:
    """Options for a group"""

    create_db: bool | None = None
    create_role: bool | None = None
    inherit: bool | None = None
    superuser: bool | None = None


@dataclasses.dataclass
class Index:
    """Represents an Index on a table"""

    name: str
    sql: str | None = None
    unique: bool | None = None
    recurse: bool | None = None
    parent: str | None = None
    method: str | None = None
    columns: list[IndexColumn] | None = None
    include: list[str] | None = None
    where: str | None = None
    storage_parameters: dict[str, str] | None = None
    tablespace: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class IndexColumn:
    """Represents a column in an index on a table"""

    name: str | None = None
    expression: str | None = None
    collation: str | None = None
    opclass: str | None = None
    direction: str | None = None
    null_placement: str | None = None


@dataclasses.dataclass
class Language:
    """Represents a Procedural Language"""

    name: str
    replace: bool = False
    trusted: bool = False
    handler: str | None = None
    inline_handler: str | None = None
    validator: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class LikeTable:
    """Represents a the settings for creating a table using LIKE"""

    name: str
    include_comments: bool | None = None
    include_constraints: bool | None = None
    include_defaults: bool | None = None
    include_generated: bool | None = None
    include_identity: bool | None = None
    include_indexes: bool | None = None
    include_statistics: bool | None = None
    include_storage: bool | None = None
    include_all: bool | None = None


@dataclasses.dataclass
class MaterializedView:
    """Represents a MaterializedView"""

    name: str
    schema: str
    owner: str
    sql: str | None = None
    columns: list[str] | None = None
    table_access_method: str | None = None
    storage_parameters: dict[str, str] | None = None
    tablespace: str | None = None
    query: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class Operator:
    """Represents an operator used to compare values"""

    name: str
    schema: str
    owner: str
    function: str
    left_arg: str | None = None
    right_arg: str | None = None
    commutator: str | None = None
    negator: str | None = None
    restrict: str | None = None
    join: str | None = None
    hashes: bool | None = None
    merges: bool | None = None
    sql: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class PartitionKeyColumn:
    """Represents a column in a partition key"""

    column_name: str | None = None
    expression: str | None = None
    opclass: str | None = None


@dataclasses.dataclass
class Procedure:
    """Represents a Procedure"""

    name: str
    schema: str
    owner: str
    sql: str | None = None
    paramters: FunctionParameter | None = None
    language: str | None = None
    transform_types: list[str] | None = None
    security: str | None = None
    configuration: dict | None = None
    definition: str | None = None
    object_file: str | None = None
    link_symbol: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class Publication:
    """Represents a Publication"""

    name: str
    tables: list[str] | None = None
    all_tables: str | None = None
    parameters: dict[str, list[str]] | None = None
    comment: str | None = None


@dataclasses.dataclass
class Role:
    """Represents a role"""

    name: str
    comment: str | None = None
    create: bool | None = None
    environments: list[str] | None = None
    grants: ACLs | None = None
    revocations: ACLs | None = None
    options: dict[str, bool] | None = None
    settings: dict[str, typing.Any] | None = None


@dataclasses.dataclass
class RoleOptions:
    """Options for a role"""

    bypass_rls: bool | None = None
    connection_limit: int | None = None
    create_db: bool | None = None
    create_role: bool | None = None
    inherit: bool | None = None
    login: bool | None = None
    replication: bool | None = None
    superuser: bool | None = None


@dataclasses.dataclass
class Schema:
    """Represents a schema/namespace"""

    name: str
    owner: str
    authorization: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class Sequence:
    """Represents a sequence"""

    name: str
    schema: str
    owner: str
    sql: str | None = None
    data_type: str | None = None
    increment_by: int | None = None
    min_value: int | None = None
    max_value: int | None = None
    start_with: int | None = None
    cache: int | None = None
    cycle: bool | None = None
    owned_by: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class Server:
    """Represents a server"""

    name: str
    foreign_data_wrapper: str
    type: str | None = None
    version: str | None = None
    options: dict | None = None
    comment: str | None = None


@dataclasses.dataclass
class Subscription:
    """Represents a logical replication subscription"""

    name: str
    connection: str
    publications: list[str]
    parameters: dict[str, list[str]] | None = None
    comment: str | None = None


@dataclasses.dataclass
class Table:
    """Represents a table"""

    name: str
    schema: str
    owner: str
    sql: str | None = None
    unlogged: bool | None = None
    from_type: str | None = None
    parents: list[str] | None = None
    like_table: LikeTable | None = None
    columns: list[Column] | None = None
    indexes: list[Index] | None = None
    primary_key: ConstraintColumns | None = None
    check_constraints: list[CheckConstraint] | None = None
    unique_constraints: list[ConstraintColumns] | None = None
    foreign_keys: list[ForeignKey] | None = None
    triggers: list[Trigger] | None = None
    partition: TablePartitionBehavior | None = None
    partitions: list[TablePartition] | None = None
    access_method: str | None = None
    storage_parameters: dict[str, str] | None = None
    tablespace: str | None = None
    index_tablespace: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class TablePartition:
    """Defines a table partition"""

    name: str
    schema: str
    default: bool | None = None
    for_values_in: list[float, int, str] | None = None
    for_values_from: float | int | str | None = None
    for_values_to: float | int | str | None = None
    for_values_with: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class TablePartitionBehavior:
    """Defines the data structure defining how a table is partitioned"""

    type: str
    columns: list[TablePartitionColumn]


@dataclasses.dataclass
class TablePartitionColumn:
    """Defines the data structure defining table a partition column"""

    name: str | None = None
    expression: str | None = None
    collation: str | None = None
    opclass: str | None = None


@dataclasses.dataclass
class Tablespace:
    """Represents a tablespace"""

    name: str
    owner: str
    location: str
    options: dict[str, float] | None = None
    comment: str | None = None


@dataclasses.dataclass
class TextSearch:
    """Represents a complex object for text search"""

    schema: str
    configurations: list[TextSearchConfig] | None = None
    dictionaries: list[TextSearchDict] | None = None
    parsers: list[TextSearchParser] | None = None
    templates: list[TextSearchTemplate] | None = None


@dataclasses.dataclass
class TextSearchConfig:
    """Represents a configuration object for Text Search"""

    name: str
    sql: str | None = None
    parser: str | None = None
    source: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class TextSearchDict:
    """Represents a dictionary object for Text Search"""

    name: str
    sql: str | None = None
    template: str | None = None
    options: dict[str, str] | None = None
    comment: str | None = None


@dataclasses.dataclass
class TextSearchParser:
    """Represents a parser object for Text Search"""

    name: str
    sql: str | None = None
    start_function: str | None = None
    gettoken_function: str | None = None
    end_function: str | None = None
    lextypes_function: str | None = None
    headline_function: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class TextSearchTemplate:
    """Represents a template for Text Search"""

    name: str
    sql: str | None = None
    lexize_function: str | None = None
    init_function: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class Trigger:
    """Table Triggers"""

    sql: str | None = None
    name: str | None = None
    when: str | None = None
    events: list[str] | None = None
    for_each: str | None = None
    condition: str | None = None
    function: str | None = None
    arguments: list[float, int, str] | None = None
    comment: str | None = None


@dataclasses.dataclass
class Type:
    """Represents a user defined data type"""

    name: str
    schema: str
    owner: str
    sql: str | None = None
    type: str | None = None
    input: str | None = None
    output: str | None = None
    receive: str | None = None
    send: str | None = None
    typmod_in: str | None = None
    typmod_out: str | None = None
    analyze: str | None = None
    internal_length: None | int | str = None
    passed_by_value: bool | None = None
    alignment: str | None = None
    storage: str | None = None
    like_type: str | None = None
    category: str | None = None
    preferred: str | None = None
    default: typing.Any = None
    element: str | None = None
    delimiter: str | None = None
    collatable: bool | None = None
    columns: list[TypeColumn] | None = None
    enum: list[str] | None = None
    subtype: str | None = None
    subtype_opclass: str | None = None
    collation: str | None = None
    canonical: str | None = None
    subtype_diff: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class TypeColumn:
    """Represents a column in a type"""

    name: str
    data_type: str
    collation: str | None = None


@dataclasses.dataclass
class User:
    """Represents a user"""

    name: str
    comment: str | None = None
    environments: list[str] | None = None
    password: str | None = None
    valid_until: str | None = None
    grants: ACLs | None = None
    revocations: ACLs | None = None
    options: dict[str, bool] | None = None
    settings: dict[str, typing.Any] | None = None


@dataclasses.dataclass
class UserOptions:
    """Options for a user"""

    bypass_rls: bool | None = None
    connection_limit: int | None = None
    create_db: bool | None = None
    create_role: bool | None = None
    inherit: bool | None = None
    replication: bool | None = None
    superuser: bool | None = None


@dataclasses.dataclass
class UserMapping:
    """Represents a user mapping"""

    name: str
    servers: list[UserMappingServer]


@dataclasses.dataclass
class UserMappingServer:
    """Represents a server for a user mapping"""

    name: str
    options: dict[str, typing.Any] | None = None


@dataclasses.dataclass
class View:
    """Represents a View"""

    name: str
    schema: str
    owner: str
    sql: str | None = None
    recursive: bool | None = None
    columns: list[ViewColumn | str] | None = None
    check_option: str | None = None
    security_barrier: bool | None = None
    query: str | None = None
    comment: str | None = None


@dataclasses.dataclass
class ViewColumn:
    """Represents a column in a view or materialized view"""

    name: str
    comment: str | None = None


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
    constants.VIEW: View,
}


Definition = (
    Aggregate
    | Cast
    | Collation
    | Conversion
    | Domain
    | EventTrigger
    | Extension
    | ForeignDataWrapper
    | Function
    | Group
    | Index
    | Language
    | MaterializedView
    | Operator
    | Publication
    | Role
    | Schema
    | Sequence
    | Server
    | Subscription
    | Table
    | Tablespace
    | TextSearch
    | Trigger
    | Type
    | User
    | UserMapping
    | View
)


@dataclasses.dataclass
class Item:
    """Represents an item in the project inventory"""

    id: int
    desc: str
    definition: Definition
    dependencies: set[int] = dataclasses.field(default_factory=set)


class TableItem(Item):
    """Represents a table item in the project inventory"""

    id: int
    desc: str
    definition: Table
    dependencies: set[int] = dataclasses.field(default_factory=set)
