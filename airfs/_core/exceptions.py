"""airfs exceptions"""
from contextlib import contextmanager as _contextmanager
from io import UnsupportedOperation as _UnsupportedOperation
from os import getenv as _getenv
from shutil import SameFileError as _SameFileError
from sys import exc_info as _exc_info


# Publicly raised exceptions


class AirfsException(Exception):
    """airfs base exception

    .. versionadded:: 1.0.0
    """


class AirfsWarning(UserWarning):
    """airfs base warning

    .. versionadded:: 1.5.0
    """


class MountException(AirfsException):
    """airfs mount exception

    .. versionadded:: 1.5.0
    """


class ConfigurationException(AirfsException):
    """airfs configuration exception

    .. versionadded:: 1.5.0
    """


# Internal exceptions


class AirfsInternalException(Exception):
    """airfs internal base exception"""

    _PATH_MSG = "Error"

    def __init__(self, *args, path=None):
        if path:
            args = [f"{self._PATH_MSG}: '{path}'"]
        Exception.__init__(self, *args)


class ObjectNotFoundError(AirfsInternalException):
    """Reraised as "FileNotFoundError" by handle_os_exceptions"""

    _PATH_MSG = "No such file or directory"


class ObjectPermissionError(AirfsInternalException):
    """Reraised as "PermissionError" by handle_os_exceptions"""

    _PATH_MSG = "Permission denied"


class ObjectExistsError(AirfsInternalException):
    """Reraised as "FileExistsError" by handle_os_exceptions"""

    _PATH_MSG = "File exists"


class ObjectNotADirectoryError(AirfsInternalException):
    """Reraised as "NotADirectoryError" by handle_os_exceptions"""

    _PATH_MSG = "Not a directory"


class ObjectNotASymlinkError(AirfsInternalException):
    """Reraised as "OSError" by handle_os_exceptions"""

    _PATH_MSG = "Not a symbolic link"


class ObjectIsADirectoryError(AirfsInternalException):
    """Reraised as "IsADirectoryError" by handle_os_exceptions."""

    _PATH_MSG = "Is a directory"


class ObjectNotImplementedError(AirfsInternalException):
    """Reraised as "NotImplementedError" by handle_os_exceptions."""

    def __init__(self, *args, feature=None):
        if feature:
            args = [f"'{feature}' unavailable on this storage"]
        Exception.__init__(self, *args)


class ObjectSameFileError(AirfsInternalException):
    """Reraised as "shutil.SameFileError" by handle_os_exceptions."""

    def __init__(self, *, path1=None, path2=None):
        Exception.__init__(self, [f"'{path1}' and '{path2}' are the same file"])


class ObjectUnsupportedOperation(AirfsInternalException):
    """Reraised as "io.UnsupportedOperation" by handle_os_exceptions."""


_OS_EXCEPTIONS = {
    ObjectNotFoundError: FileNotFoundError,
    ObjectPermissionError: PermissionError,
    ObjectExistsError: FileExistsError,
    ObjectNotADirectoryError: NotADirectoryError,
    ObjectIsADirectoryError: IsADirectoryError,
    ObjectSameFileError: _SameFileError,
    ObjectNotImplementedError: NotImplementedError,
    ObjectUnsupportedOperation: _UnsupportedOperation,
}
_FULLTRACEBACK = True if _getenv("AIRFS_FULLTRACEBACK") else False


@_contextmanager
def handle_os_exceptions():
    """
    Handles airfs exceptions and raise standard OS exceptions.
    """
    try:
        yield

    except AirfsInternalException as exception:
        exc_type, exc_value, _ = _exc_info()
        raise _OS_EXCEPTIONS.get(exc_type, OSError)(exc_value) from (
            exception if _FULLTRACEBACK else None
        )

    except Exception:
        raise
