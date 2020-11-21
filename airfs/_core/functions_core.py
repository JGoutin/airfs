"""Base utilities to define storage functions"""

from contextlib import contextmanager
from functools import wraps
from os import fsdecode, fsencode

from airfs._core.exceptions import handle_os_exceptions, ObjectNotImplementedError


def is_storage(file, storage=None):
    """
    Check if file is a local file or a storage file.

    Args:
        file (str or int): file path, URL or file descriptor.
        storage (str): Storage name.

    Returns:
        bool: return True if file is not local.
    """
    if storage:
        return True
    elif isinstance(file, int):
        return False
    split_url = file.split("://", 1)
    if len(split_url) == 2 and split_url[0].lower() != "file":
        return True
    return False


def format_and_is_storage(path, file_obj_as_storage=False, storage=None):
    """
    Checks if path is storage and format it.

    If path is an opened file-like object, returns is storage as True.

    Args:
        path (path-like object or file-like object or int):
            Path, opened file or file descriptor.
        file_obj_as_storage (bool): If True, count file-like objects as storages.
            Useful if standard functions are not intended to support them.
        storage (str): Storage name.

    Returns:
        tuple: str or file-like object or int (Updated path),
            bool (True if is storage).
    """
    readable = hasattr(path, "read")
    if isinstance(path, int) or readable:
        return path, readable and file_obj_as_storage
    path = fsdecode(path).replace("\\", "/")
    return path, is_storage(path, storage)


def equivalent_to(std_function, keep_path_type=False):
    """
    Decorates an airfs object compatible function to provides fall back to standard
    function if used on local files.

    Args:
        std_function: standard function to use with local files.
        keep_path_type (bool): Convert returned result to bytes if path argument was
            bytes.

    Returns:
        function: new function
    """

    def decorate(cos_function):
        """Decorator argument handler

        Args:
            cos_function (function): Storage function to use with storage files.
        """

        @wraps(cos_function)
        def decorated(path, *args, **kwargs):
            """
            Decorated function.

            Args:
                path (path-like object or int): Path, URL or file descriptor.
            """
            if not isinstance(path, int):
                path_str = fsdecode(path).replace("\\", "/")
                if is_storage(path_str):
                    with handle_os_exceptions():
                        result = cos_function(path_str, *args, **kwargs)
                    if keep_path_type and isinstance(path, bytes):
                        result = fsencode(result)
                    return result

            return std_function(path, *args, **kwargs)

        return decorated

    return decorate


def raises_on_dir_fd(dir_fd):
    """
    Raise on use of dir_fd

    Args:
        dir_fd: Checks if None

    Raises:
        NotImplementedError: dir_fd is not None.
    """
    if dir_fd is not None:
        raise ObjectNotImplementedError(feature="dir_fd")


class SeatsCounter:
    """
    A simple counter keeping track of available seats.

    Args:
        max_seats (int or None): Maximum available seats. None if no maximum.
    """

    __slots__ = ("_seats",)

    def __init__(self, max_seats):
        self._seats = max_seats

    def take_seat(self):
        """
        Take a seat.
        """
        if self._seats:
            self._seats -= 1

    @property
    def seats_left(self):
        """
        Remaining seats.

        Returns:
            int or None: Remaining seats. None if no maximum.
        """
        if self._seats:
            return self._seats

    @property
    def full(self):
        """
        Check if seats are full.

        Returns:
            bool: True if no more seat available.
        """
        return self._seats == 0


@contextmanager
def ignore_exception(exception):
    """
    Convenient shorter method to ignore exception.
    """
    try:
        yield
    except exception:
        return
