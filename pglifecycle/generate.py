# coding=utf-8
"""
Generates Project Structure

"""
import collections
import dataclasses
import logging
import os
import pathlib
import re
import tempfile
import typing

import arrow
import pgdumplib
from pgdumplib import dump

from pglifecycle import common, constants, parse, pgdump, storage

LOGGER = logging.getLogger(__name__)

DEFAULT_NAMESPACE = 'public'
SET_PATTERN = re.compile(r"SET .* = '(?P<value>.*)'")
YAML_EXTENSION = 'yaml'


def _filter(entries: typing.List[dump.Entry], desc: str,
            parent_id: int = None) -> typing.Generator[dump.Entry, None, None]:
    """Return a filtered list of the provided entries.

    Generator function that iterates over the entries provided and
    returns all matching entries. If ``parent_id`` is specified, it will
    filter down to only entries that have the ``parent_id`` value in their
    dependencies.

    """
    for e in [e for e in entries if e.desc == desc]:
        if parent_id is not None:
            if parent_id in e.dependencies:
                yield e
        else:
            yield e


def _function_filename(tag, filenames):
    """Create a filename for a function file, using an auto-incrementing
    value for duplicate functions with different parameters.

    :param str tag: The entity tag
    :param set filenames: Already used filenames
    :rtype: str

    """
    counter = 1
    base = tag.split('(')[0]
    parts = tag.count(',')
    if parts:
        filename = '{}-{}'.format(base, parts)
    else:
        filename = base

    while filename in filenames:
        if parts:
            filename = '{}-{}_{}'.format(base, parts, counter)
        else:
            filename = '{}_{}'.format(base, counter)
        counter += 1
    LOGGER.debug('Returning filename: %r', filename)
    return filename


_FILENAME_MAP = {
    constants.FUNCTION: _function_filename,
    constants.PROCEDURE: _function_filename
}


def _prettify(sql: str) -> str:
    return sql.strip().rstrip(';')


def _remove_null_values(values: dict) -> typing.NoReturn:
    for key, value in list(values.items()):
        if value is None or \
                isinstance(value, (dict, list, str)) and not value:
            del values[key]
        elif isinstance(value, dict):
            _remove_null_values(value)
            if not value:
                del values[key]


class Generate:
    """Generate Project Structure"""

    def __init__(self, args):
        self.args = args
        self.project_path = pathlib.Path(args.dest[0])
        self.tempdir = tempfile.TemporaryDirectory()
        self.dump = None
        self.dump_path = args.dump or pathlib.Path(self.tempdir.name) / \
            'pg-lifecycle-{}'.format(os.getpid())
        self.files_created = []
        self._processed = set()
        self._roles = {}
        self.structure = None

    @property
    def processed(self) -> set:
        """Returns the set of processed dump_ids"""
        return self._processed | self.structure.processed

    def run(self) -> typing.NoReturn:
        """Implement as core logic for generating the project"""
        if self.project_path.exists() and not self.args.force:
            common.exit_application('{} already exists'.format(
                self.project_path), 3)

        LOGGER.info('Generating project in %s', self.project_path)

        if not self.args.dump:
            pgdump.dump(self.args, self.dump_path)

        LOGGER.debug('Loading dump from %s', self.dump_path)
        self.dump = pgdumplib.load(self.dump_path)
        self.structure = Structure(self.dump.entries)

        self._create_directories()
        self._create_project_file()

        if self.args.extract_roles:
            self._extract_roles()

        self._process_acls()
        self._create_role_files()
        self._create_group_files()
        self._create_user_files()

        self._create_namespace_files(constants.CAST)
        self._create_namespace_files(constants.COLLATION)
        self._create_namespace_files(constants.CONVERSION)
        self._create_files(constants.DOMAIN)
        self._create_files(constants.EVENT_TRIGGER)
        self._create_files(constants.FOREIGN_DATA_WRAPPER)
        self._create_files(constants.FUNCTION)
        self._create_namespace_files(constants.MATERIALIZED_VIEW)
        self._create_operator_files()
        self._create_namespace_files(constants.PROCEDURE)
        self._create_namespace_files(constants.PUBLICATION)
        self._create_schema_files()
        self._create_files(constants.SEQUENCE)
        self._create_namespace_files(constants.SUBSCRIPTION)
        self._create_files(constants.SERVER)
        self._create_files(constants.TABLE)
        self._create_namespace_files(constants.TABLESPACE)
        self._create_namespace_files(constants.TYPE)
        self._create_namespace_files(constants.TEXT_SEARCH_CONFIGURATION)
        self._create_namespace_files(constants.TEXT_SEARCH_DICTIONARY)
        self._create_files(constants.USER_MAPPING)
        self._create_files(constants.VIEW)

        remaining = collections.Counter()
        for entry in [e for e in self.dump.entries
                      if e.dump_id not in self.processed
                      and e.desc != constants.SEARCHPATH]:
            remaining['{}:{}'.format(entry.section, entry.desc)] += 1

        for key in sorted(remaining.keys(), reverse=True):
            LOGGER.info('Remaining %s: %i', key, remaining[key])

        LOGGER.debug('Writing remaining.yaml')
        with open(self.project_path / 'remaining.yaml', 'w') as handle:
            storage.yaml_dump(handle, [
                dataclasses.asdict(e) for e in self.dump.entries
                if e.dump_id not in self.processed
            ])

        if self.args.gitkeep:
            storage.remove_unneeded_gitkeeps(self.project_path)
        if self.args.remove_empty_dirs:
            storage.remove_empty_directories(self.project_path)

    def _create_directories(self) -> typing.NoReturn:
        LOGGER.debug('Creating %s', self.project_path)
        os.makedirs(self.project_path, exist_ok=self.args.force)
        for value in constants.PATHS.values():
            subdir_path = self.project_path / value
            try:
                os.makedirs(subdir_path, exist_ok=self.args.force)
            except FileExistsError:
                pass
            if self.args.gitkeep:
                storage.create_gitkeep(subdir_path)

    def _create_project_file(self) -> typing.NoReturn:
        """Generates project.yaml"""
        LOGGER.debug('Creating the project file (project.yaml)')
        temp = [e for e in self.dump.entries if e.desc == constants.DATABASE]
        self._mark_processed(temp[0].dump_id)
        comments = {
            'pg_dump version':
            self.dump.dump_version,
            'postgres version':
            self.dump.server_version,
            'dumped at':
            arrow.get(self.dump.timestamp).format('YYYY-MM-DD HH:mm:ss ZZ')
        }
        project = {}
        for entry in self.dump.entries:
            if entry.defn.startswith('SET '):
                self._mark_processed(entry.dump_id)
                match = SET_PATTERN.match(entry.defn)
                project[entry.tag.lower()] = match.group(1)
        project.update({
            'extensions': self._find_extensions(),
            'shell_types': self._find_shell_types()
        })
        _remove_null_values(project)
        self.files_created.append(
            storage.save(self.project_path, 'project.yaml', constants.DATABASE,
                         self.dump.dbname, project, comments))

    def _create_files(self, object_type: str) -> typing.NoReturn:
        """Generate the schema files for the given object type"""
        LOGGER.info('Creating %s files', object_type.lower())
        formatter = getattr(self.structure,
                            object_type.lower().replace(' ', '_'),
                            self.structure.generic)
        for entry in _filter(self.dump.entries, object_type):
            self._mark_processed(entry.dump_id)
            data = formatter(entry)
            self._remove_empty_values(data)
            filename = None
            if entry.desc in _FILENAME_MAP:
                filename = _FILENAME_MAP[entry.desc](entry.tag,
                                                     self.files_created)
            self.files_created.append(
                storage.save(self.project_path,
                             self._object_path(entry, filename), entry.desc,
                             entry.tag, data))

    def _create_group_files(self) -> typing.NoReturn:
        """Generate the group files based upon the collected information"""
        LOGGER.info('Creating group files')
        for role in [
                r for r in self._roles.values() if r['type'] == constants.GROUP
        ]:
            data = {
                'name': role['role'],
                'grants': {
                    '{}s'.format(k.lower()): v
                    for k, v in role['grant'].items() if v
                },
                'revocations': {
                    '{}s'.format(k.lower()): v
                    for k, v in role['revoke'].items() if v
                },
                'options': role.get('options'),
                'settings': role.get('settings')
            }
            self.files_created.append(
                storage.save(self.project_path,
                             constants.PATHS[constants.GROUP] / '{}.{}'.format(
                                 role['role'], YAML_EXTENSION),
                             constants.GROUP, role['role'], data))

    def _create_namespace_files(self, object_type: str) -> typing.NoReturn:
        """Generate the schema files for the given object type"""
        LOGGER.info('Creating %s files', object_type.lower())
        formatter = getattr(self.structure,
                            object_type.lower().replace(' ', '_'),
                            self.structure.generic)
        namespace = {}
        for entry in _filter(self.dump.entries, object_type):
            self._mark_processed(entry.dump_id)
            if entry.namespace not in namespace.keys():
                namespace[entry.namespace] = []
            data = formatter(entry)
            self._remove_empty_values(data)
            namespace[entry.namespace].append(data)
        for value in namespace.keys():
            self.files_created.append(
                storage.save(self.project_path,
                             constants.PATHS[object_type] / '{}.{}'.format(
                                 value, YAML_EXTENSION),
                             '{}S'.format(object_type), value, {
                                 'sqls': namespace[value]
                             }))

    def _create_operator_files(self) -> typing.NoReturn:
        """Generate the schema files for operators"""
        LOGGER.info('Creating operator files')
        namespace = {}
        for obj_type in {constants.OPERATOR, constants.OPERATOR_CLASS}:
            for entry in _filter(self.dump.entries, obj_type):
                self._mark_processed(entry.dump_id)
                if entry.namespace not in namespace.keys():
                    namespace[entry.namespace] = []
                data = self.structure.operator(entry)
                self._remove_empty_values(data)
                namespace[entry.namespace].append(data)
            for value in namespace.keys():
                self.files_created.append(
                    storage.save(
                        self.project_path,
                        constants.PATHS[constants.OPERATOR] / '{}.{}'.format(
                            value, YAML_EXTENSION), '{}S'.format(
                                constants.OPERATOR), value, {
                                    'sqls': namespace[value]}))

    def _create_role_files(self) -> typing.NoReturn:
        """Generate the role files based upon the collected information"""
        LOGGER.info('Creating role file')
        for role in [
                r for r in self._roles.values()
                if not r.get('password') and r['type'] == constants.ROLE
        ]:
            data = {
                'name': role['role'],
                'grants': {
                    '{}s'.format(k.lower()): v
                    for k, v in role['grant'].items() if v
                },
                'revocations': {
                    '{}s'.format(k.lower()): v
                    for k, v in role['revoke'].items() if v
                },
                'options': role.get('options'),
                'settings': role.get('settings')
            }
            self.files_created.append(
                storage.save(self.project_path,
                             constants.PATHS[constants.ROLE] / '{}.{}'.format(
                                 role['role'], YAML_EXTENSION), constants.ROLE,
                             role['role'], data))

    def _create_schema_files(self) -> typing.NoReturn:
        """Generate the schema files for the given object type"""
        LOGGER.info('Creating schemata files')
        for entry in _filter(self.dump.entries, constants.SCHEMA):
            self._mark_processed(entry.dump_id)
            data = self.structure.schema(entry)
            self._remove_empty_values(data)
            self.files_created.append(
                storage.save(
                    self.project_path,
                    constants.PATHS[constants.SCHEMA] / '{}.{}'.format(
                        entry.tag, YAML_EXTENSION),
                    entry.desc, entry.tag, data))

    def _create_user_files(self) -> typing.NoReturn:
        """Generate the role files based upon the collected information"""
        LOGGER.info('Creating user files')
        for role in [r for r in self._roles.values()
                     if r.get('password') or r['type'] == constants.USER]:
            data = {
                'name': role['role'],
                'password': role['password'],
                'grants': {
                    '{}s'.format(k.lower()): v
                    for k, v in role['grant'].items() if v
                },
                'revocations': {
                    '{}s'.format(k.lower()): v
                    for k, v in role['revoke'].items() if v
                },
                'options': role.get('options'),
                'settings': role.get('settings')
            }
            self.files_created.append(
                storage.save(self.project_path,
                             constants.PATHS[constants.USER] / '{}.{}'.format(
                                 role['role'], YAML_EXTENSION), constants.USER,
                             role['role'], data))

    @staticmethod
    def _empty_grant() -> dict:
        return {
            constants.COLUMN: {},
            constants.TABLE: {},
            constants.SEQUENCE: {},
            constants.DATABASE: {},
            constants.DOMAIN: {},
            constants.FOREIGN_DATA_WRAPPER: {},
            constants.FOREIGN_SERVER: {},
            constants.FUNCTION: {},
            constants.PROCEDURAL_LANGUAGE: {},
            constants.LARGE_OBJECT: {},
            constants.SCHEMA: {},
            constants.ROLE: [],
            constants.TABLESPACE: {},
            constants.TYPE: {}
        }

    def _empty_role(self) -> dict:
        return {
            'role': None,
            'type': None,
            'grant': self._empty_grant(),
            'revoke': self._empty_grant(),
            'options': [],
            'settings': []
        }

    def _extract_roles(self) -> typing.NoReturn:
        LOGGER.debug('Dumping roles')
        dump_path = pathlib.Path(self.tempdir.name) / \
            'pg-lifecycle-{}-roles'.format(os.getpid())
        pgdump.dump_roles(self.args, dump_path)
        with open(dump_path, 'r') as handle:
            for line in handle.readlines():
                line = line.rstrip()
                if not line or line.startswith('--') or line.startswith('SET'):
                    continue
                parsed = parse.sql(line)
                if parsed['role'] not in self._roles:
                    self._roles[parsed['role']] = self._empty_role()
                if 'options' in parsed and parsed['options']:
                    self._roles[parsed['role']]['options'] += parsed['options']
                    del parsed['options']
                if 'settings' in parsed and parsed['settings']:
                    self._roles[parsed['role']]['settings'].append(
                        parsed['settings'])
                    del parsed['settings']
                if 'grant' in parsed and parsed['grant']:
                    self._roles[parsed['role']]['grant'][
                        constants.ROLE].append(parsed['grant'])
                    del parsed['grant']
                elif 'revoke' in parsed and parsed['revoke']:
                    self._roles[parsed['role']]['revoke'][
                        constants.ROLE].append(parsed['revoke'])
                    del parsed['revoke']
                for key, value in [(k, v) for k, v in parsed.items()
                                   if v is not None]:
                    if key not in self._roles[parsed['role']].keys() or \
                            not self._roles[parsed['role']][key]:
                        self._roles[parsed['role']][key] = value

    def _find_extensions(self) -> list:
        extensions = []
        for extension_type in {
                constants.EXTENSION, constants.PROCEDURAL_LANGUAGE
        }:
            for entry in _filter(self.dump.entries, extension_type):
                self._mark_processed(entry.dump_id)
                extensions.append(entry.defn.strip())
        return extensions

    def _find_shell_types(self) -> list:
        values = []
        for entry in _filter(self.dump.entries, constants.SHELL_TYPE):
            self._mark_processed(entry.dump_id)
            values.append(entry.defn.strip())
        return values

    def _mark_processed(self, dump_id: int) -> typing.NoReturn:
        self._processed.add(dump_id)

    @staticmethod
    def _object_path(entry: dump.Entry, name_override: str = None) -> str:
        return constants.PATHS[entry.desc] / entry.namespace / '{}.{}'.format(
            name_override or entry.tag, YAML_EXTENSION)

    def _process_acls(self) -> typing.NoReturn:
        def _maybe_ignore_revoke(acls: list) -> list:
            remove = []
            revokes = [a for a in acls if a['type'] == constants.REVOKE]
            for ga in [a for a in acls if a['type'] == constants.GRANT]:
                grant = dict(ga)
                del grant['type']
                for ra in revokes:
                    revoke = dict(ra)
                    del revoke['type']
                    if revoke == grant:
                        remove.append(ra)
            for record in remove:
                acls.remove(record)
            return acls

        for entry in _filter(self.dump.entries, constants.ACL):
            for acl in _maybe_ignore_revoke(parse.sql(entry.defn)):
                if acl['to'] not in self._roles:
                    self._roles[acl['to']] = self._empty_role()
                    self._roles[acl['to']]['role'] = acl['to']
                op = acl['type'].lower()
                subj = acl['subject']['type'].replace('_', ' ')
                if acl['subject']['type'] in [
                        constants.DATABASE, constants.SCHEMA,
                        constants.SEQUENCE, constants.TABLE
                ]:
                    self._roles[acl['to']][op][subj][acl['subject'][
                        'name']] = acl['privileges']
                elif acl['subject']['type'] == constants.FUNCTION:
                    if not isinstance(acl['subject']['name']['args'], list):
                        name = '{}({})'.format(
                            '.'.join(acl['subject']['name']['name']),
                            acl['subject']['name']['args'])
                    else:
                        name = '{}({})'.format('.'.join(
                            acl['subject']['name']['name']),
                            ', '.join(acl['subject']['name']['args']))
                    self._roles[acl['to']][op][subj][name] = acl['privileges']
                else:
                    raise ValueError('Unsupported ACL: {!r}'.format(acl))
            self._mark_processed(entry.dump_id)

    def _remove_empty_values(self, data: typing.Dict[str, typing.Any]) \
            -> typing.NoReturn:
        """Remove keys from a dict that are empty or null and values
        where it should be omitted due to cli args

        :param dict data: The dict to remove empty values from

        """
        for key, value in list(data.items()):
            if ((not value and not isinstance(value, int))
                    or value is None
                    or (key == 'owner' and self.args.no_owner)
                    or (key == 'tablespace' and self.args.no_tablespaces)
                    or (key == 'security label'
                        and self.args.no_security_labels)):
                del data[key]
        _remove_null_values(data)


class Structure:
    """Returns SQL sql based data structures"""

    def __init__(self, entries: list):
        self.dependency_cache = {}
        self.entries = entries
        self.processed = set()

    def generic(self, entry: dump.Entry) -> dict:
        """Return a data structure for for the entry"""
        self._mark_processed(entry.dump_id)
        return {
            'owner': entry.owner,
            'comment': self._find_comment(entry),
            'sql': _prettify(entry.defn),
            'dependencies': self._resolve_dependencies(entry.dependencies),
            'acls': self._find_acls(entry)
        }

    def operator(self, entry: dump.Entry) -> dict:
        """Return a data structure for for the entry"""
        self._mark_processed(entry.dump_id)
        return {
            'type': entry.desc,
            'owner': entry.owner,
            'comment': self._find_comment(entry),
            'sql': _prettify(entry.defn),
            'dependencies': self._resolve_dependencies(entry.dependencies)
        }

    def schema(self, entry: dump.Entry) -> dict:
        """Return a data structure for for the entry"""
        self._mark_processed(entry.dump_id)
        return {
            'tablespace': entry.tablespace,
            'owner': entry.owner,
            'comment': self._find_comment(entry),
            'sql': _prettify(entry.defn),
            'acls': self._find_acls(entry)
        }

    def sequence(self, entry: dump.Entry) -> dict:
        """Return a data structure for for the entry"""
        value = self.generic(entry)
        value['relation'] = self._find_children(entry,
                                                constants.SEQUENCE_OWNED_BY)
        if value['relation'] and len(value['relation']) == 1:
            value['relation'] = value['relation'][0]
        return value

    def server(self, entry: dump.Entry) -> dict:
        """Return a data structure for for the entry"""
        self._mark_processed(entry.dump_id)
        return {
            'tablespace': entry.tablespace,
            'owner': entry.owner,
            'comment': self._find_comment(entry),
            'sql': _prettify(entry.defn),
            'dependencies': self._resolve_dependencies(entry.dependencies),
            'acls': self._find_acls(entry)
        }

    def table(self, entry: dump.Entry) -> dict:
        """Return a data structure for for the entry"""
        self._mark_processed(entry.dump_id)
        return {
            'tablespace':
            entry.tablespace,
            'owner':
            entry.owner,
            'comment':
            self._find_comment(entry),
            'dependencies':
            self._resolve_dependencies(entry.dependencies),
            'sql':
            _prettify(entry.defn),
            'comments':
            self._find_column_comments(entry),
            'defaults':
            self._find_children(entry, constants.DEFAULT),
            'check constraints':
            self._find_children(entry, constants.CHECK_CONSTRAINT),
            'constraints':
            self._find_children(entry, constants.CONSTRAINT),
            'foreign keys':
            self._find_children(entry, constants.FK_CONSTRAINT),
            'indexes':
            self._find_children(entry, constants.INDEX),
            'rules':
            self._find_children(entry, constants.RULE),
            'triggers':
            self._find_children(entry, constants.TRIGGER),
            'acls':
            self._find_acls(entry)
        }

    def view(self, entry: dump.Entry) -> dict:
        """Return a data structure for for the entry"""
        self._mark_processed(entry.dump_id)
        return {
            'tablespace': entry.tablespace,
            'owner': entry.owner,
            'comment': self._find_comment(entry),
            'sql': _prettify(entry.defn),
            'comments': self._find_column_comments(entry),
            'dependencies': self._resolve_dependencies(entry.dependencies),
            'rules': self._find_children(entry, constants.RULE),
            'triggers': self._find_children(entry, constants.TRIGGER),
            'acls': self._find_acls(entry)
        }

    def _find_acls(self, parent: dump.Entry) -> list:
        acls = []
        for entry in _filter(self.entries, constants.ACL, parent.dump_id):
            if entry.tag.startswith(parent.tag):
                self._mark_processed(entry.dump_id)
                for line in entry.defn.splitlines(False):
                    if line:
                        acls.append(line.rstrip(';'))
        return acls

    def _find_children(self, parent: dump.Entry, entry_type) -> list:
        children = []
        parent_name = self._object_name(parent)
        ignore = {parent.desc: parent_name}
        for entry in _filter(self.entries, entry_type, parent.dump_id):
            add_child = False
            if entry.tag.startswith(parent.tag):
                add_child = True
            else:
                queries = parse.sql(entry.defn)
                if not isinstance(queries, list):
                    queries = [queries]
                for parsed in queries:
                    LOGGER.debug('Parsed: %r', parsed)
                    parsed_child = parsed.get('relation')
                    if parsed_child and '.' not in parsed_child:
                        parsed_child = '{}.{}'.format(entry.namespace,
                                                      parsed_child)
                    LOGGER.debug('Checking %r against %r', parsed_child,
                                 parent_name)
                    if parsed_child == parent_name:
                        add_child = True
                        break
                    elif parsed_child is None:
                        raise RuntimeError
            if add_child:
                LOGGER.debug('Adding %s child %s to %s', entry_type,
                             self._object_name(entry), parent_name)
                self._mark_processed(entry.dump_id)
                deps = self._resolve_dependencies(entry.dependencies)
                try:
                    deps.remove(ignore)
                except ValueError:
                    pass
                child = {
                    'tablespace': entry.tablespace,
                    'comment': self._find_comment(entry),
                    'sql': _prettify(entry.defn),
                    'dependencies': deps,
                    'acls': self._find_children(entry, constants.ACL)
                }
                _remove_null_values(child)
                children.append(child)
        return children

    def _find_column_comments(self, parent: dump.Entry) -> list:
        comments = []
        for entry in _filter(self.entries, constants.COMMENT, parent.dump_id):
            if entry.tag.startswith('COLUMN'):
                self._mark_processed(entry.dump_id)
                comments.append(_prettify(entry.defn))
        return comments

    def _find_comment(self, parent: dump.Entry) -> typing.Optional[str]:
        parent_name = parent.tag
        if '(' in parent_name:
            parent_name = parent_name[:parent_name.find('(')]
        if parent.desc == constants.TRIGGER:
            expectation = 'ON {}'.format(parent_name.split(' ')[0])
        else:
            expectation = '{} {}'.format(parent.desc, parent_name)
        for entry in _filter(self.entries, constants.COMMENT, parent.dump_id):
            LOGGER.debug('Expectation: %r / %r', expectation, entry.tag)
            if ((parent.desc == constants.TRIGGER and entry.tag.startswith(
                    constants.TRIGGER) and entry.tag.endswith(expectation))
                    or entry.tag.startswith(expectation)):
                LOGGER.debug('Comment matches expectation (%r): %r',
                             expectation, entry.dump_id)
                self._mark_processed(entry.dump_id)
                parsed = parse.sql(entry.defn)
                return parsed['comment']
        return None

    def _mark_processed(self, dump_id: int) -> typing.NoReturn:
        self.processed.add(dump_id)

    @staticmethod
    def _object_name(value: dump.Entry) -> str:
        if not value.namespace:
            return value.tag
        return '{}.{}'.format(value.namespace, value.tag)

    def _resolve_dependencies(self, dependencies: list) -> list:
        """Resolve the dependencies to a list of dictionaries describing
        the dependency types

        :param list dependencies: List of entry dependency dump_ids
        :rtype: list

        """
        key = ','.join(str(d) for d in dependencies)
        if key in self.dependency_cache:
            return self.dependency_cache[key]
        LOGGER.debug('Resolving dependencies: %r', dependencies)
        values = []
        for entry in [e for e in self.entries if e.dump_id in dependencies]:
            if entry.desc == constants.SCHEMA:
                continue
            values.append({entry.desc: self._object_name(entry)})
        self.dependency_cache[key] = values
        if len(self.dependency_cache.keys()) > 1024:
            del self.dependency_cache[list(self.dependency_cache.keys())[0]]
        return self.dependency_cache[key]
