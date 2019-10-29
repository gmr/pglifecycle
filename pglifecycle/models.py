"""
Models for database objects

"""
from __future__ import annotations

import dataclasses
import typing

from pglifecycle import constants


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
    dependencies: typing.Optional[Extension, Function] = None


@dataclasses.dataclass
class Argument:
    """Represents an argument to a function"""
    mode: str
    name: str
    data_type: str


@dataclasses.dataclass
class Collation:
    """Represents a Collation"""
    name: str
    schema: str
    owner: str
    locale: typing.Optional[str] = None
    lc_collate: typing.Optional[str] = None
    lc_ctype: typing.Optional[str] = None
    provider: typing.Optional[str] = None
    deterministic: typing.Optional[bool] = None
    version: typing.Optional[str] = None
    copy_from: typing.Optional[str] = None
    comment: typing.Optional[str] = None
    dependencies: typing.Optional[Extension, Function] = None


@dataclasses.dataclass
class Constraint:
    """Represents a Constraint in a Domain"""
    check: str
    name: typing.Optional[str] = None


@dataclasses.dataclass
class Conversion:
    """Represents a Conversion"""
    name: str
    schema: str
    owner: str
    encoding_from: str
    encoding_to: str
    function: str
    comment: typing.Optional[str] = None
    dependencies: typing.Optional[Extension, Function] = None


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
    constraints: typing.Optional[typing.List[Constraint]] = None
    comment: typing.Optional[str] = None
    dependencies: typing.Optional[Collation, Extension, Function] = None


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
class Function:
    """Represents a Function"""
    name: str
    schema: str
    owner: str
    comment: typing.Optional[str] = None
    dependencies: typing.Optional[Language] = None


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
    dependencies: typing.Optional[Function] = None


@dataclasses.dataclass
class Role:
    """Represents a schema/namespace"""
    name: str


@dataclasses.dataclass
class Schema:
    """Represents a schema/namespace"""
    name: str
    owner: str
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Server:
    """Represents a server"""
    name: str
    owner: str
    foreign_data_wrapper: str
    dependencies: typing.Union[Extension, ForeignDataWrapper]
    type: typing.Optional[str] = None
    version: typing.Optional[str] = None
    options: typing.Optional[dict] = None
    comment: typing.Optional[str] = None


@dataclasses.dataclass
class Tablespace:
    """Represents a tablespace"""
    name: str
    owner: str
    location: str
    options: typing.Optional[typing.Dict[str, float]] = None
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
    alignment: typing.Optional[int] = None
    storage: typing.Optional[str] = None
    like_type: typing.Optional[str] = None
    category: typing.Optional[str] = None
    preferred: typing.Optional[str] = None
    default: typing.Any = None
    element: typing.Optional[str] = None
    delimiter: typing.Optional[str] = None
    collatable: typing.Optional[str] = None
    columns: typing.Optional[typing.List[typing.Dict[str, str]]] = None
    enum: typing.Optional[typing.List[str]] = None
    subtype: typing.Optional[str] = None
    subtype_opclass: typing.Optional[str] = None
    collation: typing.Optional[str] = None
    canonical: typing.Optional[str] = None
    subtype_diff: typing.Optional[str] = None
    comment: typing.Optional[str] = None
    dependencies: typing.Optional[Collation, Function, Extension] = None


MAPPINGS = {
    constants.AGGREGATE: Aggregate,
    constants.COLLATION: Collation,
    constants.CONVERSION: Conversion,
    constants.DOMAIN: Domain,
    constants.EXTENSION: Extension,
    constants.FOREIGN_DATA_WRAPPER: ForeignDataWrapper,
    constants.OPERATOR: Operator,
    constants.PROCEDURAL_LANGUAGE: Language,
    constants.SCHEMA: Schema,
    constants.SERVER: Server,
    constants.TABLESPACE: Tablespace,
    constants.TYPE: Type
}
