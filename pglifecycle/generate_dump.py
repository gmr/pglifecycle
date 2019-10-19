# coding=utf-8
"""
Generates a pg_dump compatible build artifact

"""
import csv
import dataclasses
import logging
import pathlib
import typing

import pgdumplib
import pgdumplib.dump as dump
import ruamel.yaml as yaml
import toposort

from pglifecycle import constants, parse, utils

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
        self._process_user_mappings()

        LOGGER.info('Creating inventory')
        self._create_inventory()
        self._process_dependencies()

        LOGGER.info('Reticulating splines')
        self._process_inventory()

        LOGGER.info('Processing groups, roles, and users')
        self._process_roles()

        LOGGER.info('Processing DML')
        self._process_dml()

        LOGGER.info('Verifying build process')
        self._verify_data()

        LOGGER.info('Saving pg_dump compatible project file with %i objects',
                    len(self._dump.entries))
        self._save_dump()

    def _add_generic_item(self, desc: str, schema: str, name: str, sql: str,
                          owner: typing.Optional[str] = None) -> dump.Entry:
        entry = self._dump.add_entry(
            desc=desc,
            dump_id=self._next_dump_id(),
            tag=name,
            namespace=schema,
            owner=owner or self._project.superuser,
            defn=sql)
        self._processed.add(entry.dump_id)
        if desc not in self._inventory:
            self._inventory[desc] = {}
        schema = self._maybe_replace_schema(schema, desc)
        if schema not in self._inventory[desc]:
            self._inventory[desc][schema] = {}
        self._inventory[desc][schema][name] = _Item(entry.dump_id)
        self._reverse_lookup[entry.dump_id] = desc, schema, name
        LOGGER.debug('Added %s %s.%s: %r', desc, schema, name,
                     self._inventory[desc][schema][name])
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
            if obj_type in [constants.ACL,
                            constants.GROUP,
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
                                parent=dump_id)
                            counter += 1

        for desc in [constants.GROUP, constants.ROLE, constants.USER]:
            self._inventory[desc] = {desc: {}}
            for name, _ignore, definition in self._iterate_files(desc):
                dump_id, dependencies = self._next_dump_id(), set({})
                for dep_type in [constants.GROUP, constants.ROLE]:
                    for key in ['grants', 'revocations']:
                        for dep in definition.get(key, {}).get(
                                '{}s'.format(dep_type.lower()), []):
                            dependencies.add(':'.join([dep_type, dep]))
                definition['dependencies'] = list(dependencies)
                self._inventory[desc][desc][name] = _Item(dump_id, definition)
                self._reverse_lookup[dump_id] = desc, desc, name
                self._objects += 1
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

    @staticmethod
    def _lookup_desc_from_grant_key(key):
        for desc, value in constants.GRANT_KEYS.items():
            if key == value:
                return desc

    def _lookup_entry(self, obj_type: str, schema: str, name: str) -> _Item:
        schema = obj_type if schema == '' else schema
        LOGGER.debug('Lookup %s %s.%s', obj_type, schema, name)
        try:
            return self._inventory[obj_type][schema][name]
        except KeyError:
            LOGGER.debug('Could not find %s %s.%s in inventory',
                         obj_type, schema, name)
            raise RuntimeError

    def _maybe_add_comment(self,
                           entry: pgdumplib.dump.Entry,
                           data: dict) -> typing.NoReturn:
        if 'comment' in data:
            if isinstance(data['comment'], dict):
                sql = [data['comment']['sql'].strip()]
            else:
                sql = ['COMMENT ON', entry.desc]
                if entry.desc == constants.SCHEMA:
                    sql.append(entry.tag)
                elif entry.desc in [constants.CONSTRAINT,
                                    constants.POLICY,
                                    constants.RULE,
                                    constants.TRIGGER]:
                    parsed = parse.sql(data['sql'])
                    sql.append(parsed['name'])
                    sql.append('ON')
                    sql.append(parsed['relation'])
                else:
                    sql.append('{}.{}'.format(entry.namespace, entry.tag))
                sql.append('IS')
                sql.append('$${}$$;'.format(data['comment']))
            self._dump.add_entry(
                desc=constants.COMMENT,
                dump_id=self._next_dump_id(),
                tag='{} {}'.format(entry.desc, entry.tag),
                defn='{}\n'.format(' '.join(sql)),
                owner=entry.owner,
                dependencies=[entry.dump_id])
            self._objects += 1
        elif 'comments' in data:
            for comment in data['comments']:
                parsed = parse.sql(comment)
                self._dump.add_entry(
                    desc=constants.COMMENT,
                    dump_id=self._next_dump_id(),
                    tag='{} {}'.format(
                        parsed['object_type'], '.'.join(parsed['object'])),
                    defn='{}\n'.format(comment),
                    owner=entry.owner,
                    dependencies=[entry.dump_id])
                self._objects += 1

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

    def _process_child(self, ct, cs, cn, dump_id):
        pt, ps, pn = self._reverse_lookup[self._inventory[ct][cs][cn].parent]
        parent = self._inventory[pt][ps][pn].definition
        for item in parent[constants.TABLE_KEYS[ct]]:
            if item['name'] != cn:
                continue
            tablespace = item.get('tablespace', parent.get('tablespace', ''))
            entry = self._dump.add_entry(
                ct, cs, cn, item.get('owner', self._get_owner(parent)),
                item['sql'], dependencies=self._dependencies[dump_id],
                tablespace=tablespace, dump_id=dump_id)
            self._maybe_add_comment(entry, item)
            self._processed.add(dump_id)
            self._objects += 1
            return
        LOGGER.error('Failed to find the child %r, %r, %r, %r',
                     ct, cs, cn, dump_id)
        raise RuntimeError

    def _process_dependencies(self) -> typing.NoReturn:
        LOGGER.info('Processing dependencies')
        for obj_type in self._inventory:
            if obj_type in [constants.GROUP,
                            constants.ROLE,
                            constants.SCHEMA,
                            constants.USER]:
                continue
            for schema in self._inventory[obj_type]:
                for name in self._inventory[obj_type][schema]:
                    dump_id = self._inventory[obj_type][schema][name].dump_id
                    self._dependencies[dump_id] = set({})
                    if self._inventory[obj_type][schema][name].parent:
                        self._dependencies[dump_id].add(
                            self._inventory[obj_type][schema][name].parent)
                    if self._inventory[obj_type][schema][name].definition:
                        self._process_item_dependencies(
                            schema, dump_id,
                            self._inventory[obj_type][schema][name].definition)

    def _process_dml(self) -> typing.NoReturn:
        path = self._project_path.joinpath(constants.PATHS[constants.DML])
        if not path.exists():
            LOGGER.error('DML at %s not found', path)
            return
        for schema in sorted(path.iterdir(), key=lambda p: str(p)):
            if not schema.is_dir():
                continue
            for table in sorted(schema.iterdir(), key=lambda p: str(p)):
                if not table.name.endswith('.csv'):
                    continue
                self._process_dml_file(table)

    def _process_dml_file(self, path: pathlib.Path):
        schema = path.parent.name
        table = path.name[:-4]
        entry = self._dump.lookup_entry(constants.TABLE, schema, table)
        if not entry:
            LOGGER.error('Failed to find table definition for %s.%s',
                         schema, table)
            raise RuntimeError
        with path.open() as handle:
            fields = handle.readline().strip().split(',')
            reader = csv.reader(handle)
            with self._dump.table_data_writer(entry, fields) as writer:
                for row in reader:
                    LOGGER.debug('Row: %r', row)
                    writer.append(*row)

    def _process_extensions(self) -> typing.NoReturn:
        for extension in self._project.extensions:
            schema = extension.get('schema', '')
            sql = ['CREATE EXTENSION IF NOT EXISTS',
                   utils.quote_ident(extension['name'])]
            if schema:
                sql.append('WITH SCHEMA')
                sql.append(schema)
            entry = self._add_generic_item(
                constants.EXTENSION, schema, extension['name'],
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
                definition.get('name', name), '{};\n'.format(' '.join(sql)),
                definition.get('owner', self._project.superuser))
            self._maybe_add_comment(entry, definition)
            self._objects += 1

    def _process_inventory(self) -> typing.NoReturn:
        for dump_id in toposort.toposort_flatten(self._dependencies):
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
                obj_type, schema, name, self._get_owner(definition),
                definition['sql'], dependencies=self._dependencies[dump_id],
                tablespace=definition.get('tablespace', ''), dump_id=dump_id)
            self._maybe_add_comment(entry, definition)
            self._processed.add(dump_id)
            self._objects += 1

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
            try:
                self._dependencies[dump_id].add(
                    self._lookup_entry(obj_type, d_schema, name).dump_id)
            except RuntimeError:
                pot, pos, pon = self._reverse_lookup[dump_id]
                LOGGER.error('Failed to lookup %s %s.%s for %s %s.%s',
                             obj_type, d_schema, name, pot, pos, pon)
                raise

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
                '{};'.format(' '.join(sql)))
            self._maybe_add_comment(entry, language)

    def _process_role_dependencies(self) -> dict:
        dependencies = {}
        for desc in [constants.GROUP, constants.ROLE, constants.USER]:
            for name in self._inventory[desc][desc]:
                definition = self._inventory[desc][desc][name].definition
                dump_id = self._inventory[desc][desc][name].dump_id
                dependencies[dump_id] = set({})
                for dependency in definition['dependencies']:
                    dt, dn = dependency.split(':')
                    if dn == self._project.superuser:
                        continue
                    try:
                        dependencies[dump_id].add(
                            self._lookup_entry(dt, dt, dn).dump_id)
                    except RuntimeError:
                        LOGGER.error('%s %s has missing dependency: %s %s',
                                     desc, name, dt, dn)
                        raise
        return dependencies

    def _process_role_acls(self, dump_id: int,
                           grants: dict) -> typing.NoReturn:
        _desc, _schema, role_name = self._reverse_lookup[dump_id]
        for grant_type in grants:
            if grant_type in {'groups', 'roles', 'users'}:
                continue
            obj_type = self._lookup_desc_from_grant_key(grant_type)
            for name, perms in grants[grant_type].items():
                if 'ALL' in perms:
                    perms = ['ALL']
                sql = [constants.GRANT, ', '.join(perms), 'ON']
                if obj_type not in [constants.TABLE,
                                    constants.VIEW]:
                    sql.append(obj_type)
                sql.append(name)
                sql.append('TO')
                sql.append(role_name)
                if '.' in name:
                    schema = name[:name.find('.')]
                    name = name[name.find('.') + 1:]
                else:  # We dont care about public or the default
                    continue
                try:
                    item = self._lookup_entry(obj_type, schema, name)
                except RuntimeError:
                    if not self._args.suppress_warnings:
                        LOGGER.warning(
                            'Can not find %s %s.%s for %s, skipping',
                            obj_type, schema, name, role_name)
                    continue
                deps = [item.dump_id]
                acl = self._dump.add_entry(
                    constants.ACL, tag=role_name,
                    defn='{};\n'.format(' '.join(sql)),
                    dependencies=deps,
                    dump_id=self._next_dump_id())
                self._processed.add(acl.dump_id)
                self._objects += 1

    def _process_role_create(self, dump_id: int,
                             dependencies: dict) -> pgdumplib.dump.Entry:
        desc, schema, name = self._reverse_lookup[dump_id]
        definition = self._inventory[desc][schema][name].definition
        if definition.get('create', True):
            sql = [
                'CREATE', desc, definition.get('name', name), 'WITH'
            ]
            sql += definition.get('options', [])
            if 'password' in definition:
                if definition['password'].startswith('md5'):
                    sql.append('ENCRYPTED')
                sql.append('PASSWORD')
                sql.append('$${}$$'.format(definition['password']))
            entry = self._dump.add_entry(
                desc, tag=definition.get('name', name),
                defn='{};\n'.format(' '.join(sql)),
                dependencies=list(dependencies[dump_id]), dump_id=dump_id)
            self._maybe_add_comment(entry, definition)
            self._processed.add(dump_id)
            self._objects += 1
            return entry

    def _process_role_drop(self, dump_id: int, dependencies: dict) -> int:
        desc, schema, name = self._reverse_lookup[dump_id]
        definition = self._inventory[desc][schema][name].definition
        if definition.get('create', True):
            drop_if_exists = self._dump.add_entry(
                desc, tag=definition.get('name', name),
                defn='DROP {} IF EXISTS {};\n'.format(
                    desc, self._inventory[desc][schema][name].definition.get(
                        'name', name)),
                dependencies=list(dependencies[dump_id]),
                dump_id=self._next_dump_id())
            self._processed.add(drop_if_exists.dump_id)
            self._objects += 1
            return drop_if_exists.dump_id

    def _process_role_settings(self, entry: pgdumplib.dump.Entry,
                               settings: list) -> typing.NoReturn:
        for setting in settings:
            if setting['type'] == 'VALUE':
                if isinstance(setting['value'], list):
                    value = ', '.join(setting['value'])
                elif isinstance(setting['value'], str):
                    value = '$${}$$'.format(setting['value'])
                else:
                    LOGGER.error('Unsupported setting value: %r',
                                 setting)
                    raise RuntimeError
            else:
                LOGGER.error('Unsupported setting: %r', setting)
                raise RuntimeError
            alter_role = self._dump.add_entry(
                entry.desc, tag=entry.tag,
                defn='ALTER ROLE {} SET {} TO {};\n'.format(
                    entry.tag, setting['name'], value),
                dependencies=[entry.dump_id], dump_id=self._next_dump_id())
            self._processed.add(alter_role.dump_id)
            self._objects += 1

    def _process_roles(self) -> typing.NoReturn:
        dependencies = self._process_role_dependencies()
        for dump_id in toposort.toposort_flatten(dependencies):
            die_dump_id = self._process_role_drop(dump_id, dependencies)
            dependencies[dump_id].add(die_dump_id)
            entry = self._process_role_create(dump_id, dependencies)
            desc, schema, name = self._reverse_lookup[dump_id]
            definition = self._inventory[desc][schema][name].definition
            if definition.get('settings'):
                self._process_role_settings(entry, definition['settings'])
            self._process_role_acls(dump_id, definition.get('grants', {}))

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
                constants.SCHEMA, '', name, sql, definition.get('owner'))
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
                definition.get('name', name), sql,
                definition.get('owner', self._project.superuser))
            self._maybe_add_comment(entry, definition)

    def _process_types(self) -> typing.NoReturn:
        for schema, _name, definition in self._iterate_files(constants.TYPE):
            for row in definition.get('types', []):
                entry = self._add_generic_item(
                    constants.TYPE, schema, row['name'],
                    '{}\n'.format(row['sql']).strip(),
                    row['owner'])
                self._maybe_add_comment(entry, row)

    def _process_user_mappings(self) -> typing.NoReturn:
        for _schema, name, definition in self._iterate_files(
                constants.USER_MAPPING):
            if 'sql' in definition:
                sql = [definition['sql'].strip()]
            else:
                sql = ['CREATE USER MAPPING FOR ',
                       utils.quote_ident(definition['user']),
                       'SERVER', utils.quote_ident(definition['server'])]
                sql.append('SERVER')
                if 'options' in definition:
                    sql.append('OPTIONS')
                    sql.append(
                        '({})'.format(', '.join(
                            ["{} '{}'".format(k, v)
                             for k, v in definition['options'].items()])))
            entry = self._add_generic_item(
                constants.USER_MAPPING, '',
                definition.get('name', name), '{};\n'.format(' '.join(sql)),
                definition.get('owner', self._project.superuser))
            self._maybe_add_comment(entry, definition)
            self._objects += 1

    @staticmethod
    def _read_file(path: pathlib.Path) -> dict:
        with path.open() as handle:
            return yaml.safe_load(handle)

    def _read_project_file(self) -> _Project:
        project_file = self._project_path / 'project.yaml'
        if not project_file.exists():
            raise RuntimeError('Missing project file')
        with open(project_file) as handle:
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

    def _verify_data(self) -> typing.NoReturn:
        errors = 0
        for obj_type in self._inventory:
            for schema in self._inventory[obj_type]:
                LOGGER.debug('Validating %s objects in %s', obj_type, schema)
                for name in self._inventory[obj_type][schema]:
                    if schema in [constants.EXTENSION,
                                  constants.FOREIGN_DATA_WRAPPER,
                                  constants.GROUP,
                                  constants.PROCEDURAL_LANGUAGE,
                                  constants.ROLE,
                                  constants.SCHEMA,
                                  constants.SERVER,
                                  constants.USER]:
                        schema = ''
                    entry = self._dump.lookup_entry(obj_type, schema, name)
                    if not entry and name not in ['postgres', 'PUBLIC']:
                        LOGGER.error('Missing %s %s.%s',
                                     obj_type, schema, name)
                        errors += 1
        LOGGER.info('Verified dump against inventory, %i errors', errors)
