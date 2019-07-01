# coding=utf-8
"""Common constants and shared methods"""
import logging
import sys

LOGGER = logging.getLogger(__name__)


def exit_application(message=None, code=0):
    """Exit the application displaying the message to either INFO or ERROR
    based upon the exist code.

    :param str message: The exit message
    :param int code: The exit code (default: 0)

    """
    if message:
        log_method = LOGGER.info if not code else LOGGER.error
        log_method(message.strip())
    sys.exit(code)
