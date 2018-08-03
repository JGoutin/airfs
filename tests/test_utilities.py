# coding=utf-8
"""Test pycosio._core.utilities"""
from threading import Lock

import pytest


def test_handle_os_exceptions():
    """Tests pycosio._core.utilities.handle_os_exceptions"""
    from pycosio._core.utilities import handle_os_exceptions
    from pycosio._core.exceptions import (
        ObjectNotFoundError, ObjectPermissionError)
    from pycosio._core.compat import (
        file_not_found_error, permission_error)

    with pytest.raises(file_not_found_error):
        with handle_os_exceptions():
            raise ObjectNotFoundError('error')

    with pytest.raises(permission_error):
        with handle_os_exceptions():
            raise ObjectPermissionError('error')


def test_memoizedmethod():
    """Tests pycosio._core.utilities.memoizedmethod"""
    from pycosio._core.utilities import memoizedmethod

    # Tests _memoize
    class Dummy():

        def __init__(self):
            self._cache = {}
            self._cache_lock = Lock()

        @memoizedmethod
        def to_memoize(self, arg):
            """Fake method"""
            return arg

    dummy = Dummy()
    assert not dummy._cache
    value = 'value'
    assert dummy.to_memoize(value) == value
    assert dummy._cache == {'to_memoize': value}
    assert dummy.to_memoize(value) == value
