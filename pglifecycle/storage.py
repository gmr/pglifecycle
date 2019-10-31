import datetime
import logging
import os
import pathlib
import typing

from dateutil import tz

from pglifecycle import constants, version, yaml

LOGGER = logging.getLogger(__name__)
LINE_LENGTH = 80
MAX_SINGLE_LINE_LENGTH = 120


def create_gitkeep(directory: pathlib.Path) -> typing.NoReturn:
    """Create a .gitkeep file in the specified directory

    :param pathlib.Path directory:

    """
    open(str(directory / '.gitkeep'), 'w').close()


def remove_empty_directories(path: pathlib.Path) -> typing.NoReturn:
    """Remove any empty directories from under the specified path

    :param pathlib.Path path: The project path to clean up

    """
    for subdir in constants.PATHS.values():
        for root, dirs, files in os.walk(path / subdir):
            if not len(dirs) and not len(files):
                os.rmdir(root)


def remove_unneeded_gitkeeps(path: pathlib.Path) -> typing.NoReturn:
    """Remove any .gitkeep files in directories with subdirectories or
    files in the directory.

    :param pathlib.Path path: The project path to clean up

    """
    for subdir in constants.PATHS.values():
        for root, dirs, files in os.walk(path / subdir):
            if (len(dirs) or len(files) > 1) and '.gitkeep' in files:
                gitkeep = pathlib.Path(root) / '.gitkeep'
                LOGGER.debug('Removing %s', gitkeep)
                gitkeep.unlink()


def save(base_path: pathlib.Path, path: str, doc_type: str, doc_name: str,
         data: dict, comments: dict = None) -> str:
    """Write the data out to the specified path as YAML

    :param str base_path: The base path to write files to
    :param str path: The relative path to the file
    :param str doc_type: The type of document being written
    :param str doc_name: The name of the object the document is written for
    :param dict data: The data for the file
    :param dict comments: Extra comments to throw in the header
    :returns: File path written

    """
    file_path = base_path / path
    LOGGER.debug('Writing to %s', file_path)
    if not file_path.parent.exists():
        file_path.parent.mkdir()
    with open(str(file_path), 'w') as handle:
        if doc_type and doc_name:
            handle.write('# {}: {}\n'.format(doc_type, doc_name))
        elif doc_type:
            handle.write('# {}\n'.format(doc_type))
        elif doc_name:
            handle.write('# {}\n'.format(doc_name))
        handle.write('# Created with pglifecycle v{} ({})\n'.format(
            version, datetime.datetime.now(tz=tz.UTC).isoformat(
                sep=' ', timespec='seconds')))
        for key, value in (comments or {}).items():
            handle.write('# {}: {}\n'.format(key, value))
        handle.write('---\n')
        yaml.dump(handle, data)
    return path
