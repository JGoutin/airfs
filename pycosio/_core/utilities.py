# coding=utf-8
"""Cloud storage abstract System"""
from contextlib import contextmanager
from functools import wraps

from pycosio._core.exceptions import (
    ObjectNotFoundError, ObjectPermissionError)
from pycosio._core.compat import (
    file_not_found_error, permission_error)


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


def memoizedmethod(method):
    """
    Caches method result.

    Target class needs as "_cache"
    attribute directory.

    Args:
        method (function): Method

    Returns:
        function: Memoized method.
    """
    method_name = method.__name__

    @wraps(method)
    def patched(self, *args, **kwargs):
        """Patched method"""
        # Gets value from cache
        try:
            return self._cache[method_name]

        # Evaluates and cache value
        except KeyError:
            result = self._cache[method_name] = method(
                self, *args, **kwargs)
            return result

    return patched
