import argparse
import logging
import pathlib
import subprocess
import typing

from pglifecycle import common

LOGGER = logging.getLogger(__name__)


class _PGDump:
    """Wrapper around invoking pg_dump as a sub-process"""
    def __init__(self, args: argparse.Namespace):
        self.args = args

    def dump(self, path: pathlib.Path) -> typing.NoReturn:
        """Dump the database to the file specified

        If the dump fails, the error will be logged and the application will
        exit with a code of ``3``

        """
        LOGGER.debug('Dumping postgresql://%s:%s/%s to %s',
                     self.args.host, self.args.port, self.args.dbname, path)
        self._execute(self._dump_command(path))

    def _dump_command(self, path: pathlib.Path) -> list:
        """Return the pg_dump command to run to backup the database.

        :rtype: list

        """
        command = [
            'pg_dump',
            '-U', self.args.username,
            '-h', self.args.host,
            '-p', str(self.args.port),
            '-d', self.args.dbname,
            '-f', str(path.resolve()),
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

    @staticmethod
    def _execute(command: list) -> typing.NoReturn:
        LOGGER.debug('Executing %r', command)
        try:
            subprocess.check_output(command, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as error:
            return common.exit_application(
                'Failed to dump ({}): {}'.format(
                    error.returncode, error.stderr.decode('utf-8').strip()), 3)


class _PGDumpRoles(_PGDump):

    def _dump_command(self, path: pathlib.Path) -> list:
        """Return the pg_dump command to run to backup the database.

        :rtype: list

        """
        command = [
            'pg_dumpall',
            '-U', self.args.username,
            '-h', self.args.host,
            '-p', str(self.args.port),
            '-f', str(path.resolve()),
            '-r']
        if self.args.role:
            command += ['--role', self.args.role]
        LOGGER.debug('Dump command: %r', ' '.join(command))
        return command


def dump(args: argparse.Namespace, path: pathlib.Path) -> typing.NoReturn:
    """Dump the databased specified in the CLI args to the provided ``path``"""
    _PGDump(args).dump(path)


def dump_roles(args: argparse.Namespace,
               path: pathlib.Path) -> typing.NoReturn:
    """Dump the roles from the databased specified in the CLI args to the
    provided ``path``

    """
    _PGDumpRoles(args).dump(path)
