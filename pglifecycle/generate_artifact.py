# coding=utf-8
"""
Generates a pg_dump compatible build artifact

"""
import dataclasses
import logging
import pathlib
import typing

import pgdumplib
import pgdumplib.dump as dump
import ruamel.yaml as yaml
import toposort

from pglifecycle import constants, utils

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class _Item:
    dump_id: int
    definition: typing.Optional[dict] = None
    parent: typing.Optional[int] = None


@dataclasses.dataclass
class _Project:
    name: str
    encoding: str
    stdstrings: str
    extensions: dict
    languages: dict
    superuser: str


class Generate:
    """Generate Project Structure"""

    def __init__(self, args):
        self._args = args
        self._dependencies = {}
        self._dump_id = None
        self._first_avail_id = None
        self._inventory = {}
        self._objects = 0
        self._processed = set({})
        self._project_path = pathlib.Path(args.project)
        self._project = self._read_project_file()
        self._reverse_lookup = {}
        self._dump = pgdumplib.new(self._project.name, self._project.encoding)

    def run(self):
        """Generate the pg_dump compatible artifact from the project"""
        LOGGER.info('Pre-processing items')
        self._process_schemas()
        self._process_extensions()
        self._process_languages()
        self._process_fdws()
        self._process_servers()
        self._process_types()

        LOGGER.info('Creating inventory')
        self._create_inventory()
        self._process_dependencies()

        LOGGER.info('Reticulating splines')
        self._process_inventory()

        LOGGER.info('Verifying build process')
        self._verify_data()

        LOGGER.info('Saving pg_dump compatible project file with %i objects',
                    len(self._dump.entries))
        self._save_dump()

    def _add_generic_item(self, obj_type: str, schema: str, name: str,
                          section: str, sql: str,
                          owner: typing.Optional[str] = None) -> dump.Entry:
        entry = self._dump.add_entry(
            dump_id=self._next_dump_id(),
            tag=name,
            namespace=schema,
            desc=obj_type,
            section=section,
            owner=owner or self._project.superuser,
            defn=sql)
        self._processed.add(entry.dump_id)
        if obj_type not in self._inventory:
            self._inventory[obj_type] = {}
        schema = self._maybe_replace_schema(schema, obj_type)
        if schema not in self._inventory[obj_type]:
            self._inventory[obj_type][schema] = {}
        self._inventory[obj_type][schema][name] = _Item(entry.dump_id)
        self._reverse_lookup[entry.dump_id] = obj_type, schema, name
        LOGGER.debug('Added %s %s.%s: %r', obj_type, schema, name,
                     self._inventory[obj_type][schema][name])
        self._objects += 1
        return entry

    def _add_item(self, obj_type: str, schema: str, name: str,
                  definition: typing.Optional[dict] = None,
                  parent: typing.Optional[int] = None) -> typing.NoReturn:
        # Prefer, but don't require, a specified value in the definition
        if definition:
            schema = definition.get('schema', schema)
            name = definition.get('name', name)
        schema = self._maybe_replace_schema(schema, obj_type)
        if obj_type not in self._inventory:
            self._inventory[obj_type] = {}
        if schema not in self._inventory[obj_type]:
            self._inventory[obj_type][schema] = {}
        dump_id = self._next_dump_id()
        self._inventory[obj_type][schema][name] = _Item(
            dump_id, definition, parent)
        self._reverse_lookup[dump_id] = obj_type, schema, name
        self._objects += 1
        LOGGER.debug('Added %s %s.%s: %r', obj_type, schema, name,
                     self._inventory[obj_type][schema][name])

    def _create_inventory(self) -> typing.NoReturn:
        counter = 0
        for obj_type in constants.PATHS:
            if obj_type in [constants.GROUP,
                            constants.FOREIGN_DATA_WRAPPER,
                            constants.ROLE,
                            constants.SCHEMA,
                            constants.SERVER,
                            constants.TYPE,
                            constants.USER]:
                continue
            for schema, name, definition in self._iterate_files(obj_type):
                self._add_item(obj_type, schema, name, definition)
                counter += 1
                if obj_type == constants.TABLE:
                    LOGGER.debug('%r, %r, %r', obj_type, schema, name)
                    dump_id = self._inventory[obj_type][schema][name].dump_id
                    for c_obj_type, key in constants.TABLE_KEYS.items():
                        for child in definition.get(key, []):
                            self._add_item(
                                c_obj_type, schema, child['name'],
                                None, dump_id)
                            counter += 1
        LOGGER.info('Processed %i files', counter)

    def _get_owner(self, definition):
        if not definition:
            return self._project.superuser
        return definition.get('owner', self._project.superuser)

    def _iterate_files(self, file_type: str) \
            -> typing.Generator[typing.Tuple[str, str, dict], None, None]:
        """Generator that will iterate over all of the subdirectories and
        files for the specified `file_type`, parsing the YAML and returning
        a tuple of the schema name, object name, and a dict of values from the
        file.

        """
        path = self._project_path.joinpath(constants.PATHS[file_type])
        if not path.exists():
            LOGGER.warning('No %s file found in project', file_type)
            return
        for child in sorted(path.iterdir(), key=lambda p: str(p)):
            if child.is_dir():
                for s_child in sorted(child.iterdir(), key=lambda p: str(p)):
                    if self._is_yaml(s_child):
                        yield (s_child.parent.name,
                               s_child.name.split('.')[0],
                               self._read_file(s_child))
                    else:
                        LOGGER.debug('Ignoring %r in %s', s_child.name, path)
            elif self._is_yaml(child):
                yield (child.name.split('.')[0],
                       None,
                       self._read_file(child))
            else:
                LOGGER.debug('Ignoring %r in %s', child.name, path)

    @staticmethod
    def _is_yaml(file_path: pathlib.Path) -> bool:
        """Returns `True` if the file exists and ends with a YAML extension"""
        return (file_path.is_file()
                and (file_path.name.endswith('.yaml')
                     or file_path.name.endswith('.yml')))

    def _lookup_entry(self, obj_type: str, schema: str, name: str) -> _Item:
        schema = obj_type if schema == '' else schema
        LOGGER.debug('Lookup %s %s.%s', obj_type, schema, name)
        try:
            return self._inventory[obj_type][schema][name]
        except KeyError:
            LOGGER.error('Could not find %s %s.%s in inventory (%r %r)',
                         obj_type, schema, name,
                         obj_type in self._inventory,
                         schema in self._inventory.get(obj_type, {}))
            raise RuntimeError

    def _maybe_add_comment(self,
                           entry: pgdumplib.dump.Entry,
                           data: dict) -> typing.NoReturn:
        if 'comment' in data:
            if isinstance(data['comment'], dict):
                sql = [data['comment']['sql'].strip()]
            else:
                sql = ['COMMENT ON', entry.desc]
                if entry.desc != constants.SCHEMA:
                    sql.append('{}.'.format(entry.namespace))
                sql.append(entry.tag)
                sql.append('IS')
                sql.append('$${}$$;'.format(data['comment']))
            self._dump.add_entry(
                dump_id=self._next_dump_id(),
                tag='{} {}'.format(entry.desc, entry.tag),
                desc=entry.desc,
                section=constants.SECTION_PRE_DATA,
                defn='{}\n'.format(' '.join(sql)),
                owner=entry.owner,
                dependencies=[entry.dump_id])
            self._objects += 1
        """
        elif 'comments' in data:
            for comment in data['comments']:
                if isinstance(comment, str):
                    sql = 'COMMENT ON {} {} IS $${}$$;\n'.format(
                        entry.desc, entry.tag, comment)
                else:
                    sql = '{}\n'.format(comment['sql']).strip()
                self._dump.add_entry(
                    dump_id=self._next_dump_id(),
                    tag='{} {}'.format(entry.desc, entry.tag),
                    desc=entry.desc,
                    section=constants.SECTION_PRE_DATA,
                    defn=sql,
                    owner=entry.owner,
                    dependencies=[entry.dump_id])
                self._objects += 1
        """

    @staticmethod
    def _maybe_replace_schema(schema, obj_type):
        if schema != '':
            return schema
        if obj_type in [constants.EXTENSION,
                        constants.FOREIGN_DATA_WRAPPER,
                        constants.PROCEDURAL_LANGUAGE,
                        constants.SCHEMA,
                        constants.SERVER]:
            LOGGER.debug('Overwriting schema for %s', obj_type)
            return obj_type
        LOGGER.debug('Returning public schema for %s', obj_type)
        return 'public'

    def _next_dump_id(self) -> int:
        if self._dump_id is None:
            self._dump_id = max(e.dump_id for e in self._dump.entries)
        self._dump_id += 1
        return self._dump_id

    def _process_dependencies(self):
        LOGGER.info('Processing dependencies')
        for obj_type in self._inventory:
            if obj_type == constants.SCHEMA:
                continue
            for schema in self._inventory[obj_type]:
                for name in self._inventory[obj_type][schema]:
                    dump_id = self._inventory[obj_type][schema][name].dump_id
                    self._dependencies[dump_id] = set({})
                    if self._inventory[obj_type][schema][name].definition:
                        self._process_item_dependencies(
                            schema, dump_id,
                            self._inventory[obj_type][schema][name].definition)

    def _process_item_dependencies(self, schema: str, dump_id: int,
                                   definition: dict) -> typing.NoReturn:
        if schema != 'public':
            LOGGER.debug('Adding schema %r as a dependency', schema)
            self._dependencies[dump_id].add(
                self._lookup_entry(constants.SCHEMA, '', schema).dump_id)
        for dep in definition.get('dependencies', []):
            obj_type, name = list(dep.items())[0]
            if '.' in name:
                d_schema, name = name.split('.')
            elif obj_type == constants.SCHEMA and name in {schema, 'public'}:
                continue
            elif obj_type == constants.SCHEMA and name != schema:
                d_schema = constants.SCHEMA
            else:
                d_schema = ''
            self._dependencies[dump_id].add(
                self._lookup_entry(obj_type, d_schema, name).dump_id)

    def _process_extensions(self) -> typing.NoReturn:
        for extension in self._project.extensions:
            schema = extension.get('schema', '')
            sql = [
                'CREATE EXTENSION IF NOT EXISTS',
                utils.quote_ident(extension['name'])]
            if schema:
                sql.append('WITH SCHEMA')
                sql.append(schema)
            entry = self._add_generic_item(
                constants.EXTENSION, schema, extension['name'],
                constants.SECTION_PRE_DATA,
                '{};\n'.format(' '.join(sql)))
            self._maybe_add_comment(entry, extension)

    def _process_fdws(self) -> typing.NoReturn:
        for _schema, name, definition in self._iterate_files(
                constants.FOREIGN_DATA_WRAPPER):
            if 'sql' in definition:
                sql = [definition['sql'].strip()]
            else:
                sql = ['CREATE FOREIGN DATA WRAPPER',
                       utils.quote_ident(definition.get('name', name))]
                if 'handler' in definition:
                    sql.append('HANDLER')
                    sql.append(utils.quote_ident(definition['handler']))
                else:
                    sql.append('NO HANDLER')
                if 'validator' in definition:
                    sql.append('VALIDATOR')
                    sql.append(utils.quote_ident(definition['validator']))
                else:
                    sql.append('NO VALIDATOR')
                if 'options' in definition:
                    sql.append('OPTIONS')
                    sql.append(
                        '({})'.format(', '.join(
                            ["{} '{}'".format(k, v)
                             for k, v in definition['options'].items()])))
            entry = self._add_generic_item(
                constants.FOREIGN_DATA_WRAPPER, '',
                definition.get('name', name),
                constants.SECTION_PRE_DATA, '{};\n'.format(' '.join(sql)),
                definition.get('owner', self._project.superuser))
            self._maybe_add_comment(entry, definition)
            self._objects += 1

    def _process_inventory(self) -> typing.NoReturn:
        for dump_id in toposort.toposort_flatten(self._dependencies, True):
            if dump_id in self._processed:
                continue
            obj_type, schema, name = self._reverse_lookup[dump_id]
            LOGGER.debug('Processing %s %s.%s', obj_type, schema, name)
            for dep in self._dependencies[dump_id]:
                if dep not in self._processed:
                    mt, ms, mn = self._reverse_lookup[dump_id]
                    LOGGER.error('Dependency for %i (%s, %s, %s), '
                                 '%i (%s, %s, %s) not processed',
                                 dump_id, obj_type, schema, name,
                                 dep, mt, ms, mn)
                    raise RuntimeError
            if self._inventory[obj_type][schema][name].parent:
                self._process_child(obj_type, schema, name, dump_id)
                continue
            definition = self._inventory[obj_type][schema][name].definition
            entry = self._dump.add_entry(
                schema, name, self._section(obj_type),
                self._get_owner(definition), obj_type,
                definition['sql'], dependencies=self._dependencies[dump_id],
                tablespace=definition.get('tablespace', ''), dump_id=dump_id)
            self._maybe_add_comment(entry, definition)
            self._processed.add(dump_id)
            self._objects += 1

    def _process_child(self, ct, cs, cn, dump_id):
        pt, ps, pn = self._reverse_lookup[self._inventory[ct][cs][cn].parent]
        parent = self._inventory[pt][ps][pn].definition
        for item in parent[constants.TABLE_KEYS[ct]]:
            if item['name'] != cn:
                continue
            tablespace = item.get('tablespace', parent.get('tablespace', ''))
            entry = self._dump.add_entry(
                cs, cn, self._section(ct),
                item.get('owner', self._get_owner(parent)),
                ct, item['sql'], dependencies=self._dependencies[dump_id],
                tablespace=tablespace, dump_id=dump_id)
            self._maybe_add_comment(entry, item)
            self._processed.add(dump_id)
            self._objects += 1
            return
        LOGGER.error('Failed to find the child %r, %r, %r, %r',
                     ct, cs, cn, dump_id)
        raise RuntimeError

    def _process_languages(self) -> typing.NoReturn:
        for language in self._project.languages:
            sql = ['CREATE']
            if language.get('replace'):
                sql.append('OR REPLACE')
            if language.get('trusted'):
                sql.append('TRUSTED')
            if language.get('procedural'):
                sql.append('PROCEDURAL')
            sql.append('LANGUAGE')
            sql.append(utils.quote_ident(language['name']))
            if language.get('handler'):
                sql.append('HANDLER')
                sql.append(utils.quote_ident(language['handler']))
            if language.get('inline_handler'):
                sql.append('INLINE')
                sql.append(utils.quote_ident(language['inline_handler']))
            if language.get('validator'):
                sql.append('VALIDATOR')
                sql.append(utils.quote_ident(language['validator']))
            entry = self._add_generic_item(
                constants.PROCEDURAL_LANGUAGE, '', language['name'],
                constants.SECTION_PRE_DATA, '{};'.format(' '.join(sql)))
            self._maybe_add_comment(entry, language)

    def _process_schemas(self) -> typing.NoReturn:
        for schema, _name, definition in self._iterate_files(constants.SCHEMA):
            name = definition.get('name', schema)
            if 'sql' in definition:
                sql = '{}\n'.format(definition['sql']).strip()
            else:
                sql = ['CREATE SCHEMA IF NOT EXISTS', utils.quote_ident(name)]
                if definition.get('authorization'):
                    sql.append('AUTHORIZATION')
                    sql.append(utils.quote_ident(definition['authorization']))
            entry = self._add_generic_item(
                constants.SCHEMA, '', name, constants.SECTION_PRE_DATA, sql,
                definition.get('owner'))
            self._maybe_add_comment(entry, definition)

    def _process_servers(self) -> typing.NoReturn:
        for _schema, name, definition in self._iterate_files(constants.SERVER):
            if 'sql' in definition:
                sql = '{}\n'.format(definition['sql']).strip()
            else:
                sql = ['CREATE SERVER IF NOT EXISTS {}'.format(
                    utils.quote_ident(definition.get('name', name)))]
                if 'type' in definition:
                    sql.append("TYPE '{}'".format(definition['type']))
                if 'version' in definition:
                    sql.append("VERSION '{}'".format(definition['version']))
                sql.append('FOREIGN DATA WRAPPER {}'.format(
                    utils.quote_ident(definition['fdw'])))
                if 'options' in definition:
                    sql.append(
                        'OPTIONS ({})'.format(', '.join(
                            ["{} '{}'".format(k, v)
                             for k, v in definition['options'].items()])))
            entry = self._add_generic_item(
                constants.FOREIGN_DATA_WRAPPER, '',
                definition.get('name', name),
                constants.SECTION_PRE_DATA, sql,
                definition.get('owner', self._project.superuser))
            self._maybe_add_comment(entry, definition)

    def _process_types(self) -> typing.NoReturn:
        for schema, _name, definition in self._iterate_files(constants.TYPE):
            for row in definition.get('types', []):
                entry = self._add_generic_item(
                    constants.TYPE, schema, row['name'],
                    constants.SECTION_PRE_DATA,
                    '{}\n'.format(row['sql']).strip(),
                    row['owner'])
                self._maybe_add_comment(entry, row)

    @staticmethod
    def _read_file(path: pathlib.Path) -> dict:
        with path.open('r') as handle:
            return yaml.safe_load(handle)

    def _read_project_file(self) -> _Project:
        project_file = self._project_path / 'project.yaml'
        if not project_file.exists():
            raise RuntimeError('Missing project file')
        with open(project_file, 'r') as handle:
            return _Project(**yaml.safe_load(handle))

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

    @staticmethod
    def _section(ot: str) -> str:
        return constants.OBJECT_SECTIONS.get(ot, constants.SECTION_PRE_DATA)

    def _verify_data(self) -> typing.NoReturn:
        errors = 0
        for obj_type in self._inventory:
            for schema in self._inventory[obj_type]:
                LOGGER.debug('Validating %s objects in %s', obj_type, schema)
                for name in self._inventory[obj_type][schema]:
                    if schema in [constants.EXTENSION,
                                  constants.FOREIGN_DATA_WRAPPER,
                                  constants.PROCEDURAL_LANGUAGE,
                                  constants.SCHEMA,
                                  constants.SERVER]:
                        schema = ''
                    entry = self._dump.lookup_entry(
                        schema, name, self._section(obj_type))
                    if not entry:
                        LOGGER.error(
                            'Missing %s %s.%s', obj_type, schema, name)
                        errors += 1
        LOGGER.info('Verified dump against inventory, %i errors', errors)
