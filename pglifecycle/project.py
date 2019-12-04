"""
Core in-memory management of the project

"""
from __future__ import annotations

import dataclasses
import logging
import os
import pathlib
import typing

import toposort

from pglifecycle import constants, dump, models, utils, validation, yaml

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class _ItemDependency:
    """Represents a temporary view of item dependency"""
    desc: str
    namespace: str
    tag: str
    parent_desc: str
    parent_namespace: str
    parent_tag: str


class Project:
    """Represents the complete project including all database objects,
    and is a common way to interact with a project.

    :param str name: The name of the project, defaults to `postgres`
    :param str encoding: The database encoding to use, defaults to 'UTF8'
    :param bool stdstrings: Enable/disable stdstrings, defaults to True
    :param str superuser: The name of the superuser, defaults to `postgres`

    """
    _READ_ORDER = [
        constants.SCHEMA,
        constants.OPERATOR,
        constants.AGGREGATE,
        constants.COLLATION,
        constants.CONVERSION,
        constants.TYPE,
        constants.DOMAIN,
        constants.TABLESPACE,
        constants.TABLE,
        constants.SEQUENCE,
        constants.FUNCTION,
        constants.VIEW,
        constants.MATERIALIZED_VIEW,
        constants.CAST,
        constants.TEXT_SEARCH,
        constants.SERVER,
        constants.EVENT_TRIGGER,
        constants.PUBLICATION,
        constants.SUBSCRIPTION
    ]

    _PER_SCHEMA_FILES = [
        constants.CAST,
        constants.CONVERSION,
        constants.OPERATOR,
        constants.TEXT_SEARCH,
        constants.TYPE
    ]

    _OWNERLESS = [
        constants.EVENT_TRIGGER,
        constants.GROUP,
        constants.PUBLICATION,
        constants.ROLE,
        constants.SERVER,
        constants.SUBSCRIPTION,
        constants.TEXT_SEARCH,
        constants.USER,
        constants.USER_MAPPING
    ]

    _SCHEMALESS = [
        constants.EVENT_TRIGGER,
        constants.GROUP,
        constants.PUBLICATION,
        constants.ROLE,
        constants.SCHEMA,
        constants.SERVER,
        constants.SUBSCRIPTION,
        constants.TABLESPACE,
        constants.USER,
        constants.USER_MAPPING
    ]

    def __init__(self,
                 path: os.PathLike,
                 name: str = 'postgres',
                 encoding: str = 'UTF8',
                 stdstrings: bool = True,
                 superuser: str = 'postgres',
                 default_schema: str = 'public'):
        self.name: str = name
        self.default_schema = default_schema
        self.encoding: str = encoding
        self.inventory: typing.List[models.Item] = []
        self.path = pathlib.Path(path).absolute()
        self.stdstrings: bool = stdstrings
        self.superuser: str = superuser
        self._cached_dependencies = []
        self._load_errors = 0

    def __repr__(self) -> str:
        return '<Project path="{!s}">'.format(self.path)

    def build(self, path: os.PathLike) -> typing.NoReturn:
        """Build the project into a pg_restore -Fc compatible archive"""
        LOGGER.info('Saving build artifact to %s for %s', path, self.name)
        dump.Dump(self).save(path)

    def create(self, force: bool = False,
               gitkeep: bool = True) -> typing.NoReturn:
        """Create an empty, stub project"""
        self._create_directories(force, gitkeep)
        yaml.save(
            self.path / 'project.yaml', {
                'name': self.name,
                'encoding': self.encoding,
                'stdstrings': self.stdstrings,
                'superuser': self.superuser})

    def load(self) -> Project:
        """Load the project from the specified project directory

        :raises: RuntimeError

        """
        self._read_project_file()
        for ot in self._READ_ORDER:
            self._read_object_files(ot, models.MAPPINGS[ot])
        self._read_role_files(
            constants.GROUP, models.Group, models.GroupOptions)
        self._read_role_files(constants.ROLE, models.Role, models.RoleOptions)
        self._read_role_files(constants.USER, models.User, models.UserOptions)
        self._read_user_mapping_files()
        self._apply_cached_dependencies()
        if self._load_errors:
            LOGGER.critical('Project load failed with %i errors',
                            self._load_errors)
            raise RuntimeError('Project load failure')
        LOGGER.info('Project loaded')
        return self

    def save(self, path: os.PathLike) -> typing.NoReturn:
        """Save the project to the specified project directory"""
        pass

    @property
    def sorted_inventory(self):
        """Return the item inventory sorted topologically"""
        inventory = {item.id: item.dependencies for item in self.inventory}
        offsets = toposort.toposort_flatten(inventory, True)
        return [self.inventory[offset] for offset in offsets]

    def _add_item(self, desc: str, definition: models.Definition,
                  dependencies: typing.Optional[set] = None) -> int:
        if not self.inventory:
            item_id = 0
        else:
            item_id = max(item.id for item in self.inventory) + 1
        self.inventory.append(
            models.Item(item_id, desc, definition, dependencies or set({})))
        return item_id

    def _apply_cached_dependencies(self):
        for dep in self._cached_dependencies:
            item = self._lookup_item(dep.desc, dep.namespace, dep.tag)
            parent = self._lookup_item(
                dep.parent_desc, dep.parent_namespace, dep.parent_tag)
            if not item:
                raise RuntimeError(
                    'Failed to find {} {}.{} for {} {}.{}'.format(
                        dep.desc, dep.namespace, dep.tag,
                        dep.parent_desc, dep.parent_namespace, dep.parent_tag))
            if not parent:
                raise RuntimeError(
                    'Failed to find parent {} {}.{} for {} {}.{}'.format(
                        dep.parent_desc, dep.parent_namespace, dep.parent_tag,
                        dep.desc, dep.namespace, dep.tag))
            item.dependencies.add(parent.id)

    def _cache_and_remove_dependencies(self, desc: str,
                                       definition: dict) -> typing.NoReturn:
        for key in definition.get(constants.DEPENDENCIES, []):
            for name in definition[constants.DEPENDENCIES][key]:
                parent_namespace, parent_tag = utils.split_name(name)
                self._cached_dependencies.append(
                    _ItemDependency(
                        desc,
                        definition.get('schema', None),
                        definition['name'],
                        constants.OBJ_KEYS[key], parent_namespace, parent_tag))
        if constants.DEPENDENCIES in definition:
            del definition[constants.DEPENDENCIES]

    def _create_directories(self,
                            exist_ok: bool = False,
                            gitkeep: bool = True) -> typing.NoReturn:
        LOGGER.debug('Creating %s', self.path)
        self.path.mkdir(exist_ok=exist_ok)
        os.makedirs(self.path, exist_ok=exist_ok)
        for value in constants.PATHS.values():
            subdir_path = self.path / value
            subdir_path.mkdir(exist_ok=exist_ok)
            if gitkeep:
                gitkeep_path = subdir_path / '.gitkeep'
                gitkeep_path.touch(exist_ok=exist_ok)

    def _iterate_files(self, ot: str) -> typing.Generator[dict, None, None]:
        """Generator that will iterate over all of the subdirectories and
        files for the specified object type, parsing the YAML and returning
        a tuple of the schema name, object name, and a dict of values from the
        file.

        """
        path = self.path.joinpath(constants.PATHS[ot])
        if not path.exists():
            LOGGER.warning('No %s file found in project', ot)
            return
        for child in sorted(path.iterdir(), key=lambda p: str(p)):
            if child.is_dir():
                for s_child in sorted(child.iterdir(), key=lambda p: str(p)):
                    if yaml.is_yaml(s_child):
                        yield self._preprocess_definition(
                            ot, s_child.parent.name,
                            s_child.name.split('.')[0], yaml.load(s_child))
            elif yaml.is_yaml(child):
                yield self._preprocess_definition(
                    ot, child.name.split('.')[0], None, yaml.load(child))

    @staticmethod
    def _load_column_definition(defn: dict) -> models.Column:
        if 'generated' in defn:
            defn['generated'] = models.ColumnGenerated(**defn['generated'])
        return models.Column(**defn)

    @staticmethod
    def _load_fk_definition(defn: dict) -> models.ForeignKey:
        defn['references'] = models.ForeignKeyReference(**defn['references'])
        return models.ForeignKey(**defn)

    @staticmethod
    def _load_index_definition(defn: dict) -> models.Index:
        defn['columns'] = [models.IndexColumn(**c) for c in defn['columns']]
        return models.Index(**defn)

    @staticmethod
    def _load_partition_column(defn: typing.Union[dict, str]) \
            -> models.TablePartitionColumn:
        if isinstance(defn, dict):
            return models.TablePartitionColumn(**defn)
        return models.TablePartitionColumn(name=defn)

    def _load_table_definition(self, defn: dict) -> models.Table:
        if 'columns' in defn:
            defn['columns'] = [self._load_column_definition(c)
                               for c in defn['columns']]
        if 'indexes' in defn:
            defn['indexes'] = [self._load_index_definition(i)
                               for i in defn['indexes']]
        if 'like_table' in defn:
            defn['like_table'] = models.LikeTable(**defn['like_table'])
        if 'check_constraints' in defn:
            defn['check_constraints'] = [
                models.CheckConstraint(**cc)
                for cc in defn['check_constraints']]
        for constraint in ['primary_key', 'unique_constraints']:
            if constraint in defn:
                value = []
                for item in defn[constraint]:
                    if isinstance(item, dict):
                        value.append(models.ConstraintColumns(**item))
                    elif isinstance(item, list):
                        value.append(models.ConstraintColumns(columns=item))
                defn[constraint] = value
        if 'triggers' in defn:
            defn['triggers'] = [models.Trigger(**t) for t in defn['triggers']]
        if 'partition' in defn:
            defn['partition']['columns'] = [
                self._load_partition_column(c)
                for c in defn['partition']['columns']]
            defn['partition'] = models.TablePartitionBehavior(
                **defn['partition'])
        if 'partitions' in defn:
            defn['partitions'] = [models.TablePartition(**p)
                                  for p in defn['partitions']]
        if 'foreign_keys' in defn:
            defn['foreign_keys'] = [self._load_fk_definition(fk)
                                    for fk in defn['foreign_keys']]
        return models.Table(**defn)

    @staticmethod
    def _load_text_search_definition(defn: dict) -> typing.NoReturn:
        if defn.get('sql'):
            return models.TextSearch(defn['schema'], defn['sql'])
        configs, dicts, parsers, templates = None, None, None, None
        if defn.get('configurations'):
            configs = [models.TextSearchConfig(**c)
                       for c in defn['configurations']]
        if defn.get('dictionaries'):
            dicts = [models.TextSearchDict(**d)
                     for d in defn['dictionaries']]
        if defn.get('parsers'):
            parsers = [models.TextSearchParser(**p)
                       for p in defn['parsers']]
        if defn.get('templates'):
            templates = [models.TextSearchTemplate(**t)
                         for t in defn['templates']]
        return models.TextSearch(
            defn['schema'],
            configurations=configs,
            dictionaries=dicts,
            parsers=parsers,
            templates=templates)

    def _lookup_item(self, desc: str,
                     namespace: typing.Optional[str],
                     tag: str) -> typing.Optional[models.Item]:
        for item in self.inventory:
            if desc in self._SCHEMALESS \
                    and item.desc == desc \
                    and item.definition.name == tag:
                return item
            elif item.desc == desc \
                    and getattr(item.definition, 'schema', None) == namespace \
                    and item.definition.name == tag:
                return item

    @staticmethod
    def _object_name(definition: dict, schemaless: bool = False):
        if 'name' not in definition:
            LOGGER.critical('name missing from definition: %r', definition)
            raise RuntimeError('Missing object name')
        if schemaless or 'schema' not in definition:
            return definition['name']
        return '{}.{}'.format(definition['schema'], definition['name'])

    def _preprocess_definition(self, obj_type: str, schema: str,
                               name: typing.Optional[str],
                               definition: dict) -> dict:
        if obj_type not in self._SCHEMALESS and 'schema' not in definition:
            definition['schema'] = schema
        if name and 'name' not in definition:
            definition['name'] = name
        if obj_type not in self._OWNERLESS and 'owner' not in definition:
            definition['owner'] = self.superuser
        return definition

    def _read_object_files(self,
                           desc: str,
                           model: dataclasses.dataclass) -> typing.NoReturn:
        if desc in self._PER_SCHEMA_FILES:
            return self._read_objects_files(desc, model)
        LOGGER.debug('Reading %s definitions', desc)
        for defn in self._iterate_files(desc):
            name = self._object_name(defn)
            if not validation.validate_object(desc, name, defn):
                self._load_errors += 1
                continue
            self._cache_and_remove_dependencies(desc, defn)
            if desc == constants.AGGREGATE:
                defn['arguments'] = [
                    models.Argument(**a) for a in defn['arguments']]
                self._add_item(desc, model(**defn))
            elif desc == constants.DOMAIN and defn.get('check_constraints'):
                defn['check_constraints'] = [
                    models.DomainConstraint(**c)
                    for c in defn['check_constraints']]
                self._add_item(desc, model(**defn))
            elif desc == constants.EVENT_TRIGGER and defn.get('filter'):
                defn['filter'] = models.EventTriggerFilter(
                    defn['filter'].get('tags', []))
                self._add_item(desc, model(**defn))
            elif desc == constants.FUNCTION and defn.get('parameters'):
                defn['parameters'] = [
                    models.FunctionParameter(**p) for p in defn['parameters']]
                self._add_item(desc, model(**defn))
            elif desc in {constants.MATERIALIZED_VIEW, constants.VIEW} \
                    and defn.get('columns'):
                defn['columns'] = [
                    models.ViewColumn(c) if isinstance(c, str) else
                    models.ViewColumn(**c)
                    for c in defn['columns']]
                self._add_item(desc, model(**defn))
            elif desc == constants.TABLE:
                self._add_item(desc, self._load_table_definition(defn))
            else:
                self._add_item(desc, model(**defn))

    def _read_objects_files(self,
                            desc: str,
                            model: dataclasses.dataclass) -> typing.NoReturn:
        LOGGER.debug('Reading %s objects', desc)
        key = [k for k, v in constants.OBJ_KEYS.items() if v == desc][0]
        for defn in self._iterate_files(desc):
            if not validation.validate_object(key, defn['schema'], defn):
                self._load_errors += 1
                continue
            if desc == constants.TEXT_SEARCH:
                self._add_item(
                    constants.TEXT_SEARCH,
                    self._load_text_search_definition(defn))
                continue
            for entry in defn.get(key):
                if 'owner' not in entry:
                    entry['owner'] = defn['owner']
                if 'schema' not in entry:
                    entry['schema'] = defn['schema']
                if desc == constants.CAST:
                    name = '({} AS {})'.format(
                        entry.get('source_type'), entry.get('target_type'))
                else:
                    name = self._object_name(entry)
                if not validation.validate_object(desc, name, entry):
                    self._load_errors += 1
                    continue
                if desc == constants.TYPE and entry.get('columns'):
                    entry['columns'] = [
                        models.TypeColumn(**c) for c in entry['columns']]
                if 'dependencies' in entry:
                    self._cache_and_remove_dependencies(desc, entry)
                self._add_item(desc, model(**entry))

    def _read_project_file(self) -> typing.NoReturn:
        LOGGER.info('Loading project from %s', self.path)
        project_file = self.path / 'project.yaml'
        if not project_file.exists():
            raise RuntimeError('Missing project file')
        project = yaml.load(project_file)
        validation.validate_object('project', 'project.yaml', project)
        self.name = project.get('name', self.name)
        self.encoding = project.get('encoding', self.encoding)
        for extension in project.get('extensions'):
            self._add_item(constants.EXTENSION, models.Extension(**extension))
        for fdw in project.get('foreign_data_wrappers'):
            if 'owner' not in fdw:
                fdw['owner'] = self.superuser
            self._add_item(
                constants.FOREIGN_DATA_WRAPPER,
                models.ForeignDataWrapper(**fdw))
        for language in project.get('languages'):
            self._add_item(
                constants.PROCEDURAL_LANGUAGE, models.Language(**language))

    def _read_role_files(self, desc: str,
                         model: dataclasses.dataclass,
                         options: dataclasses.dataclass) -> typing.NoReturn:
        LOGGER.debug('Reading %s', constants.PATHS[desc].name.upper())
        for defn in self._iterate_files(desc):
            validation.validate_object(desc, defn['name'], defn)
            if 'grants' in defn:
                defn['grants'] = models.ACLs(**defn['grants'])
            if 'revocations' in defn:
                defn['revocations'] = models.ACLs(**defn['revocations'])
            if 'options' in defn:
                defn['options'] = options(**defn['options'])
            self._add_item(desc, model(**defn))

    def _read_user_mapping_files(self) -> typing.NoReturn:
        LOGGER.debug('Reading %s',
                     constants.PATHS[constants.USER_MAPPING].name.upper())
        for defn in self._iterate_files(constants.USER_MAPPING):
            validation.validate_object(
                constants.USER_MAPPING, defn['name'], defn)
            defn['servers'] = [models.UserMappingServer(**s)
                               for s in defn['servers']]
            self._add_item(constants.USER_MAPPING, models.UserMapping(**defn))


def load(path: os.PathLike) -> Project:
    """Load the project from the specified project directory, returning a
    :py:class:`~pglifecycle.project.Project` instance.

    """
    return Project(path).load()
