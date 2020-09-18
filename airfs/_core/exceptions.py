"""airfs internal exceptions.

Allows to filter airfs generated exception and standard exceptions"""
from contextlib import contextmanager
from io import UnsupportedOperation
from shutil import SameFileError
from sys import exc_info


# Publicly raised exceptions


class AirfsException(Exception):
    """airfs base exception

    .. versionadded:: 1.0.0
    """

    _PATH_MSG = "Error"

    def __init__(self, *args, path=None):
        if path:
            args = [f"{self._PATH_MSG}: '{path}'"]
        Exception.__init__(self, *args)


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


# Internal exceptions, should not be seen by users


class ObjectNotFoundError(AirfsException):
    """Reraised as "FileNotFoundError" by handle_os_exceptions"""

    _PATH_MSG = "No such file or directory"


class ObjectPermissionError(AirfsException):
    """Reraised as "PermissionError" by handle_os_exceptions"""

    _PATH_MSG = "Permission denied"


class ObjectExistsError(AirfsException):
    """Reraised as "FileExistsError" by handle_os_exceptions"""

    _PATH_MSG = "File exists"


class ObjectNotADirectoryError(AirfsException):
    """Reraised as "NotADirectoryError" by handle_os_exceptions"""

    _PATH_MSG = "Not a directory"


class ObjectNotASymlinkError(AirfsException):
    """Reraised as "OSError" by handle_os_exceptions"""

    _PATH_MSG = "Not a symbolic link"


class ObjectIsADirectoryError(AirfsException):
    """Reraised as "IsADirectoryError" by handle_os_exceptions."""

    _PATH_MSG = "Is a directory"


_OS_EXCEPTIONS = {
    ObjectNotFoundError: FileNotFoundError,
    ObjectPermissionError: PermissionError,
    ObjectExistsError: FileExistsError,
    ObjectNotADirectoryError: NotADirectoryError,
    ObjectIsADirectoryError: IsADirectoryError,
}


@contextmanager
def handle_os_exceptions():
    """
    Handles airfs exceptions and raise standard OS exceptions.
    """
    try:
        yield

    except AirfsException:
        exc_type, exc_value, _ = exc_info()
        raise _OS_EXCEPTIONS.get(exc_type, OSError)(exc_value)

    except (OSError, SameFileError, UnsupportedOperation):
        # TODO: Handle all errors with Airfs errors only
        raise

    except Exception:
        # TODO: Simply reraise without conversion: Should not occur
        exc_type, exc_value, _ = exc_info()
        raise OSError(str(exc_type) + (f", {exc_value}" if exc_value else ""))
