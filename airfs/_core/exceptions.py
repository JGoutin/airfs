"""airfs internal exceptions.

Allows to filter airfs generated exception and standard exceptions"""
from contextlib import contextmanager
from io import UnsupportedOperation
from shutil import SameFileError
from sys import exc_info


# Publicly raised exceptions


class AirfsException(Exception):
    """airfs base exception"""


class AirfsWarning(UserWarning):
    """airfs base warning"""


class MountException(Exception):
    """airfs mount exception"""


# Internal exceptions, should not be seen by users


class ObjectNotFoundError(AirfsException):
    """Reraised as "FileNotFoundError" by handle_os_exceptions"""


class ObjectPermissionError(AirfsException):
    """Reraised as "PermissionError" by handle_os_exceptions"""


class ObjectExistsError(AirfsException):
    """Reraised as "FileExistsError" by handle_os_exceptions"""


class ObjectNotADirectoryError(AirfsException):
    """Reraised as "NotADirectoryError" by handle_os_exceptions"""


class ObjectIsADirectoryError(AirfsException):
    """Reraised as "IsADirectoryError" by handle_os_exceptions"""


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

    # Convert airfs exception to equivalent OSError
    except AirfsException:
        exc_type, exc_value, _ = exc_info()
        raise _OS_EXCEPTIONS.get(exc_type, OSError)(exc_value)

    # Re-raise generic exceptions
    except (OSError, SameFileError, UnsupportedOperation):
        raise

    # Raise generic OSError for other exceptions
    except Exception:
        exc_type, exc_value, _ = exc_info()
        raise OSError("%s%s" % (exc_type, (", %s" % exc_value) if exc_value else ""))
