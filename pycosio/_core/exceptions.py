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


_OS_EXCEPTIONS = {
    ObjectNotFoundError: file_not_found_error,
    ObjectPermissionError: permission_error,
    ObjectExistsError: file_exits_error}


@contextmanager
def handle_os_exceptions():
    """
    Handles pycosio exceptions and raise standard OS exceptions.
    """
    try:
        yield

    # Convert pycosio exception to equivalent OSError
    except ObjectException:
        exc_type, exc_value, _ = exc_info()
        raise _OS_EXCEPTIONS.get(exc_type, OSError)(exc_value)

    # Raise generic OSError for other exceptions
    except Exception:
        exc_type, exc_value, _ = exc_info()
        raise OSError('%s%s' % (
            exc_type, (', %s' % exc_value) if exc_value else ''))
