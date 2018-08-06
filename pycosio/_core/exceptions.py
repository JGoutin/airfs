# coding=utf-8
"""Pycosio internal exceptions"""
from contextlib import contextmanager

from pycosio._core.compat import (
    file_not_found_error, permission_error)


class ObjectException(Exception):
    """Pycosio base exception"""


class ObjectNotFoundError(ObjectException):
    """Object not found"""


class ObjectPermissionError(ObjectException):
    """PermissionError"""


@contextmanager
def handle_os_exceptions():
    """
    Handles pycosio exceptions and raise standard OS errors.
    """
    try:
        yield
    except ObjectNotFoundError as exception:
        raise file_not_found_error(exception.args[0])
    except ObjectPermissionError as exception:
        raise permission_error(exception.args[0])
