# coding=utf-8
"""Cloud storage abstract System"""
from functools import wraps


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
            result = self._cache[method_name] = method(self, *args, **kwargs)
            return result

    return patched
