# coding=utf-8
"""
CLI Entry-point

"""
import argparse
import logging
import os
from os import path
import pwd

from pglifecycle import common, project, version

LOGGER = logging.getLogger(__name__)
LOGGING_FORMAT = '[%(asctime)-15s] %(levelname)-8s %(message)s'


def add_actions_to_parser(parser):
    """Add action CLI options to the parser.

    :param argparse.ArgumentParser parser: The parser to add the args to

    """
    sp = parser.add_subparsers(
        title='Action',
        description='The action or operation to perform',
        dest='action',
        required=True,
        metavar='ACTION')

    parser = sp.add_parser(
        'build',
        help='Generate a pg_restore -Fc compatible archive of the project')
    parser.add_argument(
        'project', metavar='PROJECT', nargs='?', action='store',
        help='The path to the pglifecycle project')
    parser.add_argument(
        'destination', metavar='DEST', nargs='?', action='store',
        help='The path save the build artifact to ')

    parser = sp.add_parser('generate', help='Generate a project')
    add_connection_options_to_parser(parser)
    add_ddl_options_to_parser(parser)
    parser.add_argument(
        '-D', '--dump', action='store',
        help='Use a pre-existing pg_dump file')
    parser.add_argument(
        '-e', '--extract', action='store_true',
        help='Extract schema from an existing database')
    parser.add_argument(
        '-r', '--extract-roles', action='store_true',
        help='Extract roles (and users) from an existing cluster')
    parser.add_argument(
        '-i', '--ignore', action='store',
        help='Specify a file with files skip writing')
    parser.add_argument(
        '--force', action='store_true',
        help='Write to destination path even if it already exists')
    parser.add_argument(
        '--gitkeep', action='store_true',
        help='Create a .gitkeep file in empty directories')
    parser.add_argument(
        '--remove-empty-dirs', action='store_true',
        help='Remove empty directories after generation')
    parser.add_argument(
        '--save-remaining', action='store_true',
        help='Save any unparsed/unprocessed dump items to remaining.yaml')
    parser.add_argument(
        'dest', nargs='?', metavar='DEST',
        help='Destination directory for the new project')


def add_connection_options_to_parser(parser):
    """Add PostgreSQL connection CLI options to the parser.

    :param argparse.ArgumentParser parser: The parser to add the args to

    """
    conn = parser.add_argument_group(
        'Connection Options', conflict_handler='resolve')
    conn.add_argument(
        '-d', '--dbname', action='store',
        default=os.environ.get('PGDATABASE', get_username()),
        help='database name to connect to')
    conn.add_argument(
        '-h', '--host', action='store',
        default=os.environ.get('PGHOST', 'localhost'),
        help='database server host or socket directory')
    conn.add_argument(
        '-p', '--port', action='store', type=int,
        default=int(os.environ.get('PGPORT', '5432')),
        help='database server port number')
    conn.add_argument(
        '-U', '--username', action='store',
        default=os.environ.get('PGUSER', get_username()),
        help='The PostgreSQL username to operate as')
    conn.add_argument(
        '-w', '--no-password', action='store_true',
        help='never prompt for password')
    conn.add_argument(
        '-W', '--password', action='store_true',
        help='force password prompt '
        '(should happen  automatically)')
    conn.add_argument(
        '--role', action='store',
        help='Role to assume when connecting to a database')


def add_ddl_options_to_parser(parser):
    """Add DDL creation options to the parser.

    :param argparse.ArgumentParser parser: The parser to add the args to

    """
    control = parser.add_argument_group('DDL Options')
    control.add_argument(
        '-O', '--no-owner', action='store_true',
        help='skip restoration of object ownership')
    control.add_argument(
        '-x', '--no-privileges', action='store_true',
        help='do not include privileges (grant/revoke)')
    control.add_argument(
        '--no-security-labels', action='store_true',
        help='do not include security label assignments')
    control.add_argument(
        '--no-tablespaces', action='store_true',
        help='do not include tablespace assignments')


def add_logging_options_to_parser(parser):
    """Add logging options to the parser.

    :param argparse.ArgumentParser parser: The parser to add the args to

    """
    group = parser.add_argument_group(title='Logging Options')
    group.add_argument(
        '-L', '--log-file', action='store',
        help='Log to the specified filename. If not specified, '
        'log output is sent to STDOUT')
    group.add_argument(
        '-v', '--verbose', action='store_true',
        help='Increase output verbosity')
    group.add_argument(
        '--debug', action='store_true', help='Extra verbose debug logging')


def configure_logging(args):
    """Configure Python logging.

    :param argparse.namespace args: The parsed cli arguments

    """
    level = logging.WARNING
    if args.verbose:
        level = logging.INFO
    elif args.debug:
        level = logging.DEBUG
    filename = args.log_file if args.log_file else None
    if filename:
        filename = path.abspath(filename)
        if not path.exists(path.dirname(filename)):
            filename = None
    logging.basicConfig(
        level=level, filename=filename, format=LOGGING_FORMAT)
    logging.getLogger('pgdumplib.dump').setLevel(logging.INFO)


def get_username():
    """Return the username of the current process.

    :rtype: str

    """
    return pwd.getpwuid(os.getuid())[0]


def parse_cli_arguments():
    """Create the CLI parser and parse the arguments.

    :return argparse.namespace args: The parsed cli arguments

    """
    parser = argparse.ArgumentParser(
        description='PostgreSQL Schema Management',
        conflict_handler='resolve',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add_logging_options_to_parser(parser)
    add_actions_to_parser(parser)
    parser.add_argument(
        '-V',
        '--version',
        action='store_true',
        help='output version information, then exit')
    return parser.parse_args()


def run():
    """Main entry-point to the pg_lifecycle application"""
    args = parse_cli_arguments()
    configure_logging(args)
    LOGGER.info('pglifecycle v%s running %s', version, args.action)
    if args.action == 'build':
        if not args.project:
            common.exit_application('Project not specified', 2)
        elif not args.destination:
            common.exit_application('Destination not specified', 2)
        try:
            project.load(args.project).build(args.destination)
        except RuntimeError as error:
            common.exit_application(str(error), 4)
    elif args.action == 'generate-project':
        if not args.dest:
            common.exit_application('Destination not specified', 2)
        if args.gitkeep and args.remove_empty_dirs:
            common.exit_application(
                'Can not specify --gitkeep and --remove-empty-dirs', 2)
        #  generate_project.Generate(args).run()
    else:
        common.exit_application('Invalid action specified', 1)
