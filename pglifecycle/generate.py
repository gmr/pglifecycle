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
    if '\n' in data or '::' in data or len(data) > 80:
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
        self.remaining = []

    def run(self) -> typing.NoReturn:
        """Implement as core logic for generating the project"""
        if self.project_path.exists() and not self.args.force:
            common.exit_application(
                '{} already exists'.format(self.project_path), 3)
        LOGGER.info('Generating project in %s', self.project_path)
        if not self.args.dump:
            self._dump_database()
        LOGGER.debug('Loading dump from %s', self.dump_path)
        self.dump = pgdumplib.load(self.dump_path)
        self.remaining = [e.dump_id for e in self.dump.entries]

        # Debug writing of dump info
        # self._yaml_dump(
        #     'test-project/entries.yaml', self.dump.entries, False)

        self._generate_files()

        LOGGER.debug('%i of %i objects remain unparsed',
                     len(self.remaining), len(self.dump.entries))

        if self.args.gitkeep:
            self._remove_unneeded_gitkeeps()
        if self.args.remove_empty_dirs:
            self._remove_empty_directories()

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

    def _find_comment(self, entry: dump.Entry) -> typing.Optional[str]:
        """Search through the other entries looking for a comment.

        :param pgdumplib.dump.Entry entry: The entry to find the comment for
        :rtype: str

        """
        entries = [e for e in self.dump.entries
                   if e.desc == constants.COMMENT and
                   entry.dump_id in e.dependencies and
                   e.tag.startswith(entry.desc)]
        if entries:
            self._mark_processed(entries[0].dump_id)
            parsed = sql_parse.parse(entries[0].defn)
            return parsed['value']
        return None

    def _find_column_comments(self, entry: dump.Entry) -> dict:
        """Search through the other entries looking for the column comments
        for the passed in table entry.

        :param pgdumplib.dump.Entry entry: The entry to find the comment for
        :rtype: dict

        """
        comments = {}
        entries = [e for e in self.dump.entries
                   if e.desc == constants.COMMENT and
                   entry.dump_id in e.dependencies and
                   e.tag.startswith('COLUMN {}'.format(entry.tag))]
        for comment in entries:
            parsed = sql_parse.parse(comment.defn)
            LOGGER.debug('parsed: %r', parsed)
            comments[parsed['owner'][-1]] = parsed['value']
            self._mark_processed(comment.dump_id)
        return comments

    def _find_foreign_keys(self, entry: dump.Entry) -> dict:
        """Search through the other entries looking for foreign keys

        :param pgdumplib.dump.Entry entry: The entry to find FKs for
        :rtype: dict

        """
        fks = {}
        for entry in [e for e in self.dump.entries
                      if entry.dump_id in e.dependencies and
                      e.desc == constants.FK_CONSTRAINT and
                      e.tag.startswith(entry.tag) and
                      'FOREIGN KEY' in e.defn]:
            parsed = sql_parse.parse(entry.defn)
            LOGGER.debug('Parsed: %r', parsed)
            fks[parsed['name']] = collections.OrderedDict(
                columns=parsed['fk_columns'],
                references=collections.OrderedDict(
                    table='{}.{}'.format(
                        parsed['ref_table']['schemaname'],
                        parsed['ref_table']['relname']),
                    columns=parsed['ref_columns']),
                match=parsed['match'],
                on_delete=parsed['on_delete'],
                on_update=parsed['on_update'],
                deferrable=parsed['deferrable'],
                initially_deferred=parsed['initially_deferred'])
            self._remove_null_values(fks[parsed['name']])
            self._mark_processed(entry.dump_id)
        return fks

    def _find_primary_key(self, entry: dump.Entry) -> typing.Union[list, str]:
        for entry in [e for e in self.dump.entries
                      if entry.dump_id in e.dependencies and
                      e.desc == constants.CONSTRAINT and
                      e.tag.startswith(entry.tag) and
                      'PRIMARY KEY' in e.defn]:
            self._mark_processed(entry.dump_id)
            parsed = sql_parse.parse(entry.defn)
            if isinstance(parsed, list):
                parsed = parsed[0]
            return parsed['primary_key']

    def _find_indexes(self, entry: dump.Entry) -> dict:
        """Search through the other entries looking for the indexes for the
        passed table entry

        :param pgdumplib.dump.Entry entry: The entry to find indexes for
        :rtype: dict

        """
        indexes = {}
        for entry in [e for e in self.dump.entries
                      if e.desc == constants.INDEX and
                      entry.dump_id in e.dependencies and
                      '{}.{}'.format(entry.namespace, entry.tag) in e.defn]:
            parsed = sql_parse.parse(entry.defn)
            if isinstance(parsed, list):
                parsed = parsed[0]
            self._mark_processed(entry.dump_id)
            LOGGER.debug('Parsed Index: %r', parsed)
            if not isinstance(parsed['columns'], list):
                parsed['columns'] = [parsed['columns']]
            indexes[parsed['name']] = collections.OrderedDict(
                columns=parsed['columns'],
                type=parsed['type'] if parsed['type'] != 'btree' else None,
                options=parsed.get('options'),
                transitions=parsed.get('transitions') or None,
                where=parsed.get('where'),
                tablespace=parsed.get('tablespace'),
                unique=parsed['unique'] or None)
            self._remove_null_values(indexes[parsed['name']])
            for offset, col in enumerate(indexes[parsed['name']]['columns']):
                self._remove_null_values(
                    indexes[parsed['name']]['columns'][offset])
                if len(col.keys()) > 1:
                    continue
                indexes[parsed['name']]['columns'][offset] = col['name']
        return indexes

    def _find_triggers(self, entry: dump.Entry) -> dict:
        """Search through the other entries looking for triggers on the
        passed table entry

        :param pgdumplib.dump.Entry entry: The entry to find triggers for
        :rtype: dict

        """
        triggers = {}
        for entry in [e for e in self.dump.entries
                      if e.desc == constants.TRIGGER and
                      entry.dump_id in e.dependencies and
                      '{}.{}'.format(entry.namespace, entry.tag) in e.defn]:
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
            self._mark_processed(entry.dump_id)
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

    def _generate_files(self) -> typing.NoReturn:
        """Generate all of the directories and files in the project"""
        self._create_directories()
        self._generate_project_file()
        self._generate_schema_files()
        self._generate_table_files()

    def _generate_project_file(self) -> typing.NoReturn:
        """Generates project.yaml"""
        LOGGER.debug('Generating project file')
        project = {
            'name': self.dump.dbname,
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

    def _generate_schema_files(self) -> typing.NoReturn:
        """Generate the schema files"""
        LOGGER.debug('Generating schema files')
        for entry in [e for e in self.dump.entries
                      if e.section == constants.SECTION_PRE_DATA and
                      e.desc == constants.SCHEMA]:
            self._mark_processed(entry.dump_id)
            data = {
                'name': entry.tag,
                'tablespace': entry.tablespace,
                'owner': entry.owner
            }
            self._yaml_dump(
                constants.PATHS[constants.SCHEMA] / '{}.yaml'.format(
                    entry.tag), data)

    def _generate_table_files(self) -> typing.NoReturn:
        """Generate YAML files for each table"""
        for entry in [e for e in self.dump.entries
                      if e.section == constants.SECTION_PRE_DATA and
                      e.desc == constants.TABLE]:
            LOGGER.debug('Parsing %s.%s', entry.namespace, entry.tag)
            self._mark_processed(entry.dump_id)
            columns, constraints, options = self._parse_table(
                entry.defn, self._find_column_comments(entry))
            data = collections.OrderedDict(
                type=constants.TABLE,
                schema=entry.namespace,
                name=entry.tag,
                owner=entry.owner,
                comment=self._find_comment(entry),
                dependencies=self._resolve_dependencies(entry.dependencies),
                definition=columns,
                primary_key=self._find_primary_key(entry),
                constraints=constraints,
                foreign_keys=self._find_foreign_keys(entry),
                triggers=self._find_triggers(entry),
                indexes=self._find_indexes(entry),
                grants=[],
                security_labels=[],
                tablespace=entry.tablespace,
                options=options)
            prefix = constants.PATHS[constants.TABLE] / entry.namespace
            self._yaml_dump(prefix / '{}.yaml'.format(entry.tag), data, True)

    def _mark_processed(self, dump_id):
        try:
            self.remaining.remove(dump_id)
        except ValueError:
            LOGGER.debug('Tried to double remove %i', dump_id)

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
