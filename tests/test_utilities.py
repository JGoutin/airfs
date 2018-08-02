# coding=utf-8
"""Test pycosio._core.utilities"""
from threading import Lock


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
