# coding=utf-8
"""Pycosio internal exceptions.

Allows to filter Pycosio generated exception and standard exceptions"""
from contextlib import contextmanager

from pycosio._core.compat import file_not_found_error, permission_error


class ObjectException(Exception):
    """Pycosio base exception"""


class ObjectNotFoundError(ObjectException):
    """Reraised as "FileNotFoundError" by handle_os_exceptions"""


class ObjectPermissionError(ObjectException):
    """Reraised as "PermissionError" by handle_os_exceptions"""


@contextmanager
def handle_os_exceptions():
    """
    Handles pycosio exceptions and raise standard OS exceptions.
    """
    try:
        yield

    # Convert pycosio exception to equivalent OSError
    except ObjectException as exception:
        raise {ObjectNotFoundError: file_not_found_error,
               ObjectPermissionError: permission_error}.get(
                    type(exception), OSError)(exception.args[0])
