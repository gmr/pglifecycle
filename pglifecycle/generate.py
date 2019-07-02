# coding=utf-8
"""
Generates Project Structure

"""
import collections
import logging
import os
import pathlib
import re
import subprocess
import tempfile
import typing

import arrow
import pgdumplib
from pgdumplib import dump
import yaml

from pglifecycle import common, constants, sql_parse, version

LOGGER = logging.getLogger(__name__)

SET_PATTERN = re.compile(r"SET .* = '(?P<value>.*)'")


def _str_representer(dumper, data):
    """Represent multi-line strings as a scalar

    :param dumper: The YAML dumper instance
    :param str data: The data to dump

    """
    if '\n' in data or len(data) > 80:
        return dumper.represent_scalar(
            'tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


def _represent_ordereddict(dumper, data):
    value = []
    for item_key, item_value in data.items():
        node_key = dumper.represent_data(item_key)
        node_value = dumper.represent_data(item_value)
        value.append((node_key, node_value))
    return yaml.nodes.MappingNode('tag:yaml.org,2002:map', value)


yaml.add_representer(str, _str_representer)
yaml.add_representer(collections.OrderedDict, _represent_ordereddict)


class Generate:
    """Generate Project Structure"""

    def __init__(self, args):
        self.args = args
        self.project_path = pathlib.Path(args.dest[0])
        self.tempdir = tempfile.TemporaryDirectory()
        self.dump = None
        self.dump_path = args.dump or pathlib.Path(self.tempdir.name) / \
            'pg-lifecycle-{}'.format(os.getpid())
        self.indexes = {}
        self.processed = set()

    def run(self) -> typing.NoReturn:
        """Implement as core logic for generating the project"""
        if self.project_path.exists() and not self.args.force:
            common.exit_application(
                '{} already exists'.format(self.project_path), 3)
        if not self.args.dump:
            self._dump_database()
        LOGGER.debug('Loading dump from %s', self.dump_path)
        self.dump = pgdumplib.load(self.dump_path)

        LOGGER.info('Generating project in %s', self.project_path)
        self._generate_files()

        LOGGER.warning('%i of %i objects remain unparsed',
                       len(self.dump.entries) - len(self.processed),
                       len(self.dump.entries))

        remaining = collections.Counter()
        for entry in [e for e in self.dump.entries
                      if e.dump_id not in self.processed]:
            remaining['{}:{}'.format(entry.section, entry.desc)] += 1

        for key in sorted(remaining.keys(), reverse=True):
            LOGGER.info('Remaining %s: %i', key, remaining[key])

        LOGGER.debug('Writing remaining.yaml')
        with open('test-project/remaining.yaml', 'w') as handle:
            yaml.dump([e for e in self.dump.entries
                       if e.dump_id not in self.processed], handle,
                      indent=2, default_flow_style=False,
                      explicit_start=True, encoding=self.dump.encoding)

        if self.args.gitkeep:
            self._remove_unneeded_gitkeeps()
        if self.args.remove_empty_dirs:
            self._remove_empty_directories()

    def _collect_indexes(self) -> typing.NoReturn:
        LOGGER.info('Collecting index information')
        count = 0
        for entry in self._get_entries(constants.INDEX):
            count += 1
            parsed = sql_parse.parse(entry.defn)
            if isinstance(parsed, list):
                parsed = parsed[0]
            LOGGER.debug('Parsed Index: %r', parsed)
            if not isinstance(parsed['columns'], list):
                parsed['columns'] = [parsed['columns']]
            index = collections.OrderedDict(
                columns=parsed['columns'],
                type=parsed['type'] if parsed['type'] != 'btree' else None,
                options=parsed.get('options'),
                transitions=parsed.get('transitions') or None,
                where=parsed.get('where'),
                tablespace=parsed.get('tablespace'),
                unique=parsed['unique'] or None)
            self._remove_null_values(index)
            for offset, col in enumerate(index['columns']):
                self._remove_null_values(index['columns'][offset])
                if len(col.keys()) > 1:
                    continue
                index['columns'][offset] = col['name']
            parent = self._parent_name(parsed['relation'])
            if parent not in self.indexes:
                self.indexes[parent] = []
            self.indexes[parent].append((entry.dump_id, entry.tag, index))
        LOGGER.debug('Collecting information on %i indexes', count)

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
                gitkeep_path = subdir_path / '.gitkeep'
                open(gitkeep_path, 'w').close()

    def _dump_command(self):
        """Return the pg_dump command to run to backup the database.

        :rtype: list

        """
        command = [
            'pg_dump',
            '-U', self.args.username,
            '-h', self.args.host,
            '-p', str(self.args.port),
            '-d', self.args.dbname,
            '-f', str(self.dump_path.resolve()),
            '-Fc', '--schema-only']
        for optional in {'no_owner',
                         'no_privileges',
                         'no_security_labels',
                         'no_tablespaces'}:
            if getattr(self.args, optional, False):
                command += ['--{}'.format(optional.replace('_', '-'))]
        if self.args.role:
            command += ['--role', self.args.role]
        LOGGER.debug('Dump command: %r', ' '.join(command))
        return command

    def _dump_database(self):
        """Return the pg_dump command to run to backup the database.


        """
        dsn = '{}:{}/{}'.format(
            self.args.host, self.args.port, self.args.dbname)
        LOGGER.info('Dumping %s to %s', dsn, self.dump_path)
        try:
            subprocess.check_output(
                self._dump_command(), stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as error:
            return common.exit_application(
                'Failed to dump {} ({}): {}'.format(
                    dsn, error.returncode,
                    error.stderr.decode('utf-8').strip()), 3)

    def _find_acls(self, parent: dump.Entry) -> list:
        groups = {'GRANT': {}, 'REVOKE': {}}
        for entry in self._get_entries(constants.ACL, parent.dump_id):
            if entry.tag.startswith(parent.tag):
                self._mark_processed(entry.dump_id)
                for acl in sql_parse.parse(entry.defn):
                    privileges = ','.join(acl['privileges'])
                    if privileges not in groups[acl['action']]:
                        groups[acl['action']][privileges] = []
                    groups[acl['action']][privileges].append(acl['grantees'])
        acls = []
        for key in ['REVOKE', 'GRANT']:
            for group in groups[key].keys():
                privileges = group.split(',')
                if len(privileges) == 1:
                    privileges = privileges[0]
                if len(groups[key][group]) == 1:
                    groups[key][group] = groups[key][group][0]
                acl = collections.OrderedDict(privileges=privileges)
                acl['from' if key == 'REVOKE' else 'to'] = groups[key][group]
                acls.append({key.lower(): acl})
        return acls

    def _find_comment(self, parent: dump.Entry) -> typing.Optional[str]:
        entries = [e for e in self._get_entries(constants.COMMENT,
                                                parent.dump_id)
                   if e.tag.startswith(parent.desc)]
        if entries:
            self._mark_processed(entries[0].dump_id)
            parsed = sql_parse.parse(entries[0].defn)
            return parsed['value']
        return None

    def _find_column_comments(self, parent: dump.Entry) -> dict:
        comments = {}
        for entry in self._get_entries(constants.COMMENT, parent.dump_id):
            if entry.tag.startswith('COLUMN {}'.format(parent.tag)):
                self._mark_processed(entry.dump_id)
                parsed = sql_parse.parse(entry.defn)
                comments[parsed['owner'][-1]] = parsed['value']
        return comments

    def _find_foreign_keys(self, parent: dump.Entry) -> dict:
        fks = {}
        for entry in self._get_entries(
                constants.FK_CONSTRAINT, parent.dump_id):
            if entry.tag.startswith(parent.tag) and \
                    'FOREIGN KEY' in entry.defn:
                self._mark_processed(entry.dump_id)
                parsed = sql_parse.parse(entry.defn)
                LOGGER.debug('Parsed: %r', parsed)
                fks[parsed['name']] = collections.OrderedDict(
                    columns=parsed['fk_columns'],
                    references=collections.OrderedDict(
                        table=self._parent_name(parsed['ref_table']),
                        columns=parsed['ref_columns']),
                    match=parsed['match'],
                    on_delete=parsed['on_delete'],
                    on_update=parsed['on_update'],
                    deferrable=parsed['deferrable'],
                    initially_deferred=parsed['initially_deferred'])
                self._remove_null_values(fks[parsed['name']])
        return fks

    def _find_indexes(self, entry: dump.Entry) -> typing.Optional[dict]:
        """Search through the other entries looking for the indexes for the
        passed table entry

        :param pgdumplib.dump.Entry entry: The entry to find indexes for
        :rtype: dict

        """
        parent = '{}.{}'.format(entry.namespace, entry.tag)
        if parent in self.indexes:
            indexes = collections.OrderedDict()
            for dump_id, name, index in self.indexes[parent]:
                self._mark_processed(dump_id)
                indexes[name] = index
            del self.indexes[parent]
            return indexes

    def _find_primary_key(self, parent: dump.Entry) -> typing.Union[list, str]:
        for entry in self._get_entries(constants.CONSTRAINT, parent.dump_id):
            if entry.tag.startswith(parent.tag) and \
                    'PRIMARY KEY' in entry.defn:
                self._mark_processed(entry.dump_id)
                parsed = sql_parse.parse(entry.defn)
                if isinstance(parsed, list):
                    parsed = parsed[0]
                return parsed['primary_key']

    def _find_triggers(self, parent: dump.Entry) -> dict:
        parent_name = self._parent_name(parent)
        triggers = {}
        for entry in self._get_entries(constants.TRIGGER, parent.dump_id):
            if parent_name in entry.defn:
                self._mark_processed(entry.dump_id)
                parsed = sql_parse.parse(entry.defn)
                triggers[parsed['name']] = collections.OrderedDict(
                    when=parsed['when'],
                    events=parsed['events'],
                    each='ROW' if parsed['row'] else 'STATEMENT',
                    transitions=parsed['transitions'] or None,
                    condition=parsed['condition'],
                    funtion=parsed['function']
                )
                self._remove_null_values(triggers[parsed['name']])
        return triggers

    @staticmethod
    def _function_filename(tag, filenames):
        """Create a filename for a function file, using an auto-incrementing
        value for duplicate functions with different parameters.

        :param str tag: The entity tag
        :param set filenames: Already used filenames
        :rtype: str

        """
        base = tag.split('(')[0]
        parts = tag.count(',')
        filename = '{}-{}.sql'.format(base, parts)
        if filename not in filenames:
            return filename
        counter = 2
        while True:
            filename = '{}-{}_{}.sql'.format(base, parts, counter)
            if filename not in filenames:
                return filename
            counter += 1

    def _generate_domains(self):
        LOGGER.info('Generating domains')
        for entry in self._get_entries(constants.DOMAIN):
            LOGGER.debug('Parsing %s.%s', entry.namespace, entry.tag)
            self._mark_processed(entry.dump_id)
            parsed = sql_parse.parse(entry.defn)
            data = collections.OrderedDict(
                define=constants.DOMAIN,
                name=entry.tag,
                namespace=entry.namespace,
                owner=entry.owner,
                comment=self._find_comment(entry),
                type=parsed['type'],
                constraints=parsed['constraints']
            )
            self._remove_null_values(data)
            self._yaml_dump(
                constants.PATHS[constants.DOMAIN] / entry.namespace /
                '{}.yaml'.format(entry.tag), data)

    def _generate_files(self) -> typing.NoReturn:
        """Generate all of the directories and files in the project"""
        self._create_directories()
        self._generate_project_file()
        self._generate_schema_files()
        self._generate_domains()
        self._generate_rules()
        self._collect_indexes()
        self._generate_table_files()
        self._generate_types()

    def _generate_project_file(self) -> typing.NoReturn:
        """Generates project.yaml"""
        LOGGER.debug('Generating project file')

        temp = [e for e in self.dump.entries if e.desc == 'DATABASE']
        self._mark_processed(temp[0].dump_id)
        database = sql_parse.parse(temp[0].defn)
        project = {
            'name': self.dump.dbname,
            'options': database['options'],
            'dump_version': self.dump.dump_version,
            'postgres_version': self.dump.server_version,
            'pgdumplib_version': version,
            'dumped_at': arrow.get(self.dump.timestamp).format(
                'YYYY-MM-DD HH:mm:ss ZZ'),
            'created_at': arrow.utcnow().format('YYYY-MM-DD HH:mm:ss ZZ')
        }
        for entry in self.dump.entries:
            if entry.defn.startswith('SET '):
                self._mark_processed(entry.dump_id)
                match = SET_PATTERN.match(entry.defn)
                project[entry.tag.lower()] = match.group(1)
        self._yaml_dump('project.yaml', project)

    def _generate_rules(self):
        LOGGER.info('Generating rules')
        for entry in self._get_entries(constants.RULE):
            LOGGER.debug('Parsing %s.%s', entry.namespace, entry.tag)
            self._mark_processed(entry.dump_id)
            parsed = sql_parse.parse(entry.defn)
            data = collections.OrderedDict(
                define=constants.RULE,
                name=entry.tag,
                namespace=entry.namespace,
                owner=entry.owner,
                comment=self._find_comment(entry),
                event=parsed['event'],
                instead=parsed['instead'],
                table=parsed['table'],
                where=parsed['where'],
                action=parsed['action']
            )
            self._remove_null_values(data)
            self._yaml_dump(
                constants.PATHS[constants.RULE] / entry.namespace /
                '{}.yaml'.format(entry.tag), data)

    def _generate_schema_files(self) -> typing.NoReturn:
        """Generate the schema files"""
        LOGGER.info('Generating schemas')
        for entry in [e for e in self.dump.entries
                      if e.section == constants.SECTION_PRE_DATA and
                      e.desc == constants.SCHEMA]:
            self._mark_processed(entry.dump_id)
            data = collections.OrderedDict(
                define=constants.SCHEMA,
                name=entry.tag,
                tablespace=entry.tablespace,
                owner=entry.owner,
                comment=self._find_comment(entry),
                acls=self._find_acls(entry))
            self._remove_null_values(data)
            self._yaml_dump(
                constants.PATHS[constants.SCHEMA] / '{}.yaml'.format(
                    entry.tag), data)

    def _generate_table_files(self) -> typing.NoReturn:
        """Generate YAML files for each table"""
        LOGGER.info('Generating tables')
        for entry in self._get_entries(constants.TABLE):
            LOGGER.debug('Parsing %s.%s', entry.namespace, entry.tag)
            self._mark_processed(entry.dump_id)
            columns, constraints, options = self._parse_table(
                entry.defn, self._find_column_comments(entry))
            data = collections.OrderedDict(
                define=constants.TABLE,
                schema=entry.namespace,
                name=entry.tag,
                owner=entry.owner,
                comment=self._find_comment(entry),
                dependencies=self._resolve_dependencies(entry.dependencies),
                columns=columns,
                primary_key=self._find_primary_key(entry),
                constraints=constraints,
                foreign_keys=self._find_foreign_keys(entry),
                triggers=self._find_triggers(entry),
                indexes=self._find_indexes(entry),
                rules=None,  # @TODO Replace _generate_rules
                acls=self._find_acls(entry),
                security_labels=[],
                tablespace=entry.tablespace,
                options=options)
            prefix = constants.PATHS[constants.TABLE] / entry.namespace
            self._yaml_dump(prefix / '{}.yaml'.format(entry.tag), data, True)

    def _generate_types(self):
        LOGGER.info('Generating types')
        for entry in self._get_entries(constants.TYPE):
            LOGGER.debug('Parsing %s.%s', entry.namespace, entry.tag)
            self._mark_processed(entry.dump_id)
            data = collections.OrderedDict(
                define=constants.TYPE,
                name=entry.tag,
                namespace=entry.namespace,
                owner=entry.owner,
                comment=self._find_comment(entry))
            parsed = sql_parse.parse(entry.defn)
            del parsed['name']
            data.update(parsed)
            self._remove_null_values(data)
            self._yaml_dump(
                constants.PATHS[constants.TYPE] / entry.namespace /
                '{}.yaml'.format(entry.tag), data)

    def _get_entries(self, desc, parent_id=None):
        for e in [e for e in self.dump.entries if e.desc == desc]:
            if parent_id is not None:
                if parent_id in e.dependencies:
                    yield e
            else:
                yield e

    def _mark_processed(self, dump_id):
        self.processed.add(dump_id)

    @staticmethod
    def _parent_name(value: typing.Union[dict, dump.Entry]) -> str:
        LOGGER.debug('Parent: %r', value)
        if isinstance(value, dump.Entry):
            return '{}.{}'.format(value.namespace, value.tag)
        if 'schemaname' in value:
            return '{}.{}'.format(value['schemaname'], value['relname'])
        return value['relname']

    @staticmethod
    def _parse_table(sql: str, comments: dict):
        """Parse the SQL returning a list of fields used to create a table

        :param str sql: The SQL definition of the table
        :rtype: list

        """
        columns = []
        constraints = []
        storage_modes = {}
        table = sql_parse.parse(sql)
        if isinstance(table, list):
            storage_modes = {r['column']: r['storage'] for r in table
                             if r.get('type') == 'storage_mode'}
            table = [t for t in table if 'relation' in t][0]
        if table.get('options') is None:
            table['options'] = []
        if not isinstance(table['options'], list):
            table['options'] = [table['options']]
        options = collections.OrderedDict(
            (r['defname'], r['arg'])
            for r in sorted(table['options'], key=lambda x: x['defname']))
        table_elts = table.get('tableElts', [])
        if not isinstance(table_elts, list):
            table_elts = [table_elts]
        for item in table_elts:
            if 'name' in item:
                column = collections.OrderedDict(
                    name=item['name'],
                    type=item['type'],
                    default=item['default'],
                    comment=comments.get(item['name']),
                    constraint=item['constraint'],
                    nullable=item['nullable'],
                    storage_mode=storage_modes.get(item['name']))
                for col in ['comment',
                            'constraint',
                            'default',
                            'storage_mode']:
                    if column[col] is None:
                        del column[col]
                if column['nullable'] is True:
                    del column['nullable']
                columns.append(column)
            else:
                constraints.append(item)
        return columns, constraints, options

    def _remove_empty_directories(self) -> typing.NoReturn:
        """Remove any empty directories"""
        for subdir in constants.PATHS.values():
            dir_path = self.project_path / subdir
            for root, dirs, files in os.walk(dir_path):
                if not len(dirs) and not len(files):
                    os.rmdir(root)

    def _remove_empty_values(self, data: typing.Dict[str, typing.Any]) \
            -> typing.NoReturn:
        """Remove keys from a dict that are empty or null and values
        where it should be omitted due to cli args

        :param dict data: The dict to remove empty values from

        """
        for key, value in list(data.items()):
            if ((not value and not isinstance(value, int)) or
                    value is None or
                    (key == 'owner' and self.args.no_owner) or
                    (key == 'tablespace' and self.args.no_tablespaces) or
                    (key == 'security label' and
                     self.args.no_security_labels)):
                del data[key]

    def _remove_null_values(self, values: dict) -> typing.NoReturn:
        LOGGER.debug('Removing None from %r', values)
        for key, value in list(values.items()):
            if value is None:
                del values[key]
            elif isinstance(value, dict):
                self._remove_null_values(value)
                if not value:
                    del values[key]

    def _remove_unneeded_gitkeeps(self) -> typing.NoReturn:
        """Remove any .gitkeep files in directories with subdirectories or
        files in the directory.

        """
        for subdir in constants.PATHS.values():
            for root, dirs, files in os.walk(self.project_path / subdir):
                if (len(dirs) or len(files) > 1) and '.gitkeep' in files:
                    gitkeep = pathlib.Path(root) / '.gitkeep'
                    LOGGER.debug('Removing %s', gitkeep)
                    gitkeep.unlink()

    def _resolve_dependencies(self, dependencies: list) -> list:
        """Resolve the dependencies to a list of dictionaries describing
        the dependency types

        :param list dependencies: List of entry dependency dump_ids
        :rtype: list

        """
        return [
            collections.OrderedDict(
                type=e.desc, schema=e.namespace, name=e.tag)
            for e in self.dump.entries
            if e.dump_id in dependencies and e.desc != constants.SCHEMA]

    def _yaml_dump(self, path: str, data: dict,
                   remove_empty=True) -> typing.NoReturn:
        """Write the data out to the specified path as YAML

        :param str path: The relative path to the file
        :param dict data: The data for the file

        """
        if remove_empty:
            self._remove_empty_values(data)
        LOGGER.debug('Writing to %s/%s', self.project_path, path)
        file_path = self.project_path / path
        if not file_path.parent.exists():
            file_path.parent.mkdir()
        with open(file_path, 'w') as handle:
            yaml.dump(
                data, handle, indent=2, default_flow_style=False,
                explicit_start=True, encoding=self.dump.encoding)
