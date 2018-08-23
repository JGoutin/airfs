"""Base utilities to define storage functions"""

from functools import wraps

from pycosio._core.compat import fsdecode
from pycosio._core.exceptions import handle_os_exceptions


def is_storage(url, storage=None):
    """
    Check if file is a local file or a storage file.

    File is considered local if:
        - URL is a local path.
        - URL starts by "file://"
        - a "storage" is provided.

    Args:
        url (str): file path or URL
        storage (str): Storage name.

    Returns:
        bool: return True if file is local.
    """
    if storage:
        return True
    split_url = url.split('://', 1)
    if len(split_url) == 2 and split_url[0].lower() != 'file':
        return True
    return False


def format_and_is_storage(path):
    """
    Checks if path is storage and format it.

    If path is an opened file-like object, returns is storage as True.

    Args:
        path (path-like object or file-like object):

    Returns:
        tuple: str or file-like object (Updated path),
            bool (True if is storage).
    """
    if not hasattr(path, 'read'):
        return fsdecode(path), is_storage(path)
    return path, True


def equivalent_to(std_function):
    """
    Decorates a cloud object compatible function
    to provides fall back to standard function if
    used on local files.

    Args:
        std_function (function): standard function to
            used with local files.

    Returns:
        function: new function
    """

    def decorate(cos_function):
        """Decorator argument handler"""

        @wraps(cos_function)
        def decorated(path, *args, **kwargs):
            """Decorated function"""

            # Handles path-like objects
            path = fsdecode(path)

            # Storage object: Handle with Cloud object storage
            # function
            if is_storage(path):
                with handle_os_exceptions():
                    return cos_function(path, *args, **kwargs)

            # Local file: Redirect to standard function
            return std_function(path, *args, **kwargs)

        return decorated

    return decorate
