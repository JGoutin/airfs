# coding=utf-8
"""Pycosio internal exceptions.

Allows to filter Pycosio generated exception and standard exceptions"""
from contextlib import contextmanager
from sys import exc_info

from pycosio._core.compat import (
    file_not_found_error, permission_error, file_exits_error)


class ObjectException(Exception):
    """Pycosio base exception"""


class ObjectNotFoundError(ObjectException):
    """Reraised as "FileNotFoundError" by handle_os_exceptions"""


class ObjectPermissionError(ObjectException):
    """Reraised as "PermissionError" by handle_os_exceptions"""


class ObjectExistsError(ObjectException):
    """Reraised as "FileExistsError" by handle_os_exceptions"""


@contextmanager
def handle_os_exceptions():
    """
    Handles pycosio exceptions and raise standard OS exceptions.
    """
    try:
        yield

    # Convert pycosio exception to equivalent OSError
    except ObjectException:
        exc_type, exc_value, exc_traceback = exc_info()

        # Raise OSError subclass for predefined exceptions
        try:
            raise {ObjectNotFoundError: file_not_found_error,
                   ObjectPermissionError: permission_error,
                   ObjectExistsError: file_exits_error
                   }[exc_type](exc_value)

        # Raise generic OSError for other exceptions
        except KeyError:
            raise OSError('(%s) %s' % (exc_type, exc_value))
