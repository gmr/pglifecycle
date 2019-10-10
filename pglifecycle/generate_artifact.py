# coding=utf-8
"""
Generates a pg_dump compatible build artifact

"""
import dataclasses
import logging
import pathlib
import typing

import pgdumplib
import ruamel.yaml as yaml

from pglifecycle import constants

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class _Project:
    name: str
    encoding: str
    stdstrings: str
    extensions: dict


class Generate:
    """Generate Project Structure"""

    def __init__(self, args):
        self._args = args
        self._inventory = {}
        self._processed = []
        self._project_path = pathlib.Path(args.project)
        self._project = self._read_project_file()
        self._schemas = {}
        self._dump = pgdumplib.new(self._project.name, self._project.encoding)

    def _read_project_file(self) -> _Project:
        project_file = self._project_path / 'project.yaml'
        if not project_file.exists():
            raise RuntimeError('Missing project file')
        with open(project_file, 'r') as handle:
            return _Project(**yaml.safe_load(handle))

    def run(self):
        """Generate the pg_dump compatible artifact from the project"""
        self._add_extensions()
        self._add_schemas()
        self._add_domains()
        self._add_foreign_data_wrappers()
        self._add_servers()
        self._add_sequences()
        self._add_types()
        self._save_dump()

    def _add_domains(self):
        LOGGER.info('Adding domains')
        for name, value in self._iterate_files(constants.DOMAIN):
            entry = self._dump.add_entry(
                tag=name,
                namespace=value['schema'],
                desc=constants.DOMAIN,
                section=constants.SECTION_PRE_DATA,
                defn='{}\n'.format(value['sql'].strip()),
                owner=value['owner'])
            self._processed.append(entry)

    def _add_extensions(self):
        LOGGER.info('Adding extensions')
        for name in self._project.extensions.keys():
            self._dump.add_entry(
                tag=name,
                desc=self._project.extensions[name]['type'],
                section=constants.SECTION_PRE_DATA,
                defn=self._project.extensions[name]['sql'])

    def _add_foreign_data_wrappers(self):
        LOGGER.info('Adding foreign data wrappers')
        for name, value in self._iterate_files(constants.FOREIGN_DATA_WRAPPER):
            entry = self._dump.add_entry(
                tag=name,
                desc=constants.DOMAIN,
                section=constants.SECTION_POST_DATA,
                defn='{}\n'.format(value['sql'].strip()),
                owner=value['owner'])
            self._processed.append(entry)

    def _add_schemas(self):
        LOGGER.info('Adding schemas')
        for name, value in self._iterate_files(constants.SCHEMA):
            entry = self._dump.add_entry(
                tag=name,
                desc=constants.SCHEMA,
                section=constants.SECTION_PRE_DATA,
                defn='{}\n'.format(value['sql'].strip()),
                owner=value['owner'])
            self._processed.append(entry)
            self._schemas[name] = entry.dump_id

    def _add_sequences(self):
        LOGGER.info('Adding sequences')
        for name, value in self._iterate_files(constants.SEQUENCE):
            entry = self._dump.add_entry(
                tag=name,
                namespace=value['schema'],
                desc=constants.SEQUENCE,
                section=constants.SECTION_PRE_DATA,
                defn='{}\n'.format(value['sql'].strip()),
                owner=value['owner'])
            self._processed.append(entry)

    def _add_servers(self):
        LOGGER.info('Adding servers')
        for name, value in self._iterate_files(constants.SERVER):
            entry = self._dump.add_entry(
                tag=name,
                desc=constants.SERVER,
                section=constants.SECTION_POST_DATA,
                defn='{}\n'.format(value['sql'].strip()),
                owner=value['owner'])
            self._processed.append(entry)

    def _add_types(self):
        LOGGER.info('Adding types')
        for name, value in self._iterate_files(constants.TYPE):
            for row in value.get('types', []):
                entry = self._dump.add_entry(
                    tag=row['name'],
                    namespace=value['schema'],
                    desc=constants.TYPE,
                    section=constants.SECTION_PRE_DATA,
                    owner=row['owner'],
                    defn='{}\n'.format(row['sql']).strip())
                self._processed.append(entry)
                if 'comment' in value:
                    self._dump.add_entry(
                        tag='{} {}'.format(constants.TYPE, row['name']),
                        desc=constants.COMMENT,
                        section=constants.SECTION_PRE_DATA,
                        defn='{}\n'.format(row['sql']).strip(),
                        owner=row['owner'],
                        dependencies=[entry.dump_id])

    def _iterate_files(self, file_type: str) \
            -> typing.Generator[typing.Tuple[str, dict], None, None]:
        """Generator that will iterate over all of the subdirectories and
        files for the specified `file_type`, parsing the YAML and returning
        the dict of values from the file.

        """
        path = self._project_path.joinpath(constants.PATHS[file_type])
        if not path.exists():
            LOGGER.warning('No %s file found in project', file_type)
            return
        for child in sorted(path.iterdir(), key=lambda p: str(p)):
            if child.is_dir():
                for s_child in sorted(child.iterdir(), key=lambda p: str(p)):
                    if self._is_yaml(s_child):
                        with s_child.open('r') as handle:
                            yield (s_child.name.split('.')[0],
                                   yaml.safe_load(handle))
                    else:
                        LOGGER.debug('Ignoring %r in %s', s_child.name, path)
            elif self._is_yaml(child):
                with child.open('r') as handle:
                    yield child.name.split('.')[0], yaml.safe_load(handle)
            else:
                LOGGER.debug('Ignoring %r in %s', child.name, path)

    @staticmethod
    def _is_yaml(file_path: pathlib.Path) -> bool:
        """Returns `True` if the file exists and ends with a YAML extension"""
        return (file_path.is_file()
                and (file_path.name.endswith('.yaml')
                     or file_path.name.endswith('.yml')))

    def _load_files(self) -> typing.NoReturn:
        LOGGER.debug('Loading in data from all files')
        for file_type in [constants.FUNCTION, constants.TABLE]:
            self._inventory[file_type] = {}
            for name, value in self._iterate_files(file_type):
                if value['schema'] not in self._inventory[file_type]:
                    self._inventory[file_type][value['schema']] = {}
                # self._inventory[file_type][value['schema']]

    def _save_dump(self) -> typing.NoReturn:
        LOGGER.debug('Saving dump')
        if self._args.file:
            path = pathlib.Path(self._args.file)
        else:
            path = self._project_path / '{}.dump'.format(self._project.name)
        if path.exists():
            path.unlink()
        self._dump.save(path)
        LOGGER.info('Project pg_dump artifact created at %s', path)
