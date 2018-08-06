# coding=utf-8
"""Test pycosio._core.std_functions"""

import pytest


def test_equivalent_to():
    """Tests pycosio._core._equivalent_to"""
    from pycosio._core.std_functions import equivalent_to
    from pycosio._core.exceptions import ObjectNotFoundError
    from sys import version_info

    std = 'std'
    cos = 'cos'
    local_path = 'path'
    storage_path = 'http://path'
    dummy_args = (1, 2, 3)
    dummy_kwargs = {'args1': 1, 'args2': 2}
    raises_exception = False

    # Mocks a standard function and is storage equivalent

    def std_function(path, *args, **kwargs):
        """Checks arguments and returns fake result"""
        assert path == local_path
        assert args == dummy_args
        assert kwargs == dummy_kwargs
        return std

    @equivalent_to(std_function)
    def cos_function(path, *args, **kwargs):
        """Checks arguments and returns fake result"""
        assert path == storage_path
        assert args == dummy_args
        assert kwargs == dummy_kwargs
        if raises_exception:
            raise ObjectNotFoundError('Error')
        return cos

    # Tests storage function
    assert cos_function(
        storage_path, *dummy_args, **dummy_kwargs) == cos

    # Tests fall back to standard function
    assert cos_function(
        local_path, *dummy_args, **dummy_kwargs) == std

    # Tests path-like object compatibility
    if (version_info[0] == 3 and version_info[1] >= 6) or version_info[0] > 3:
        import pathlib

        assert cos_function(
            pathlib.Path(local_path), *dummy_args, **dummy_kwargs) == std

    # Tests exception conversion
    raises_exception = True
    with pytest.raises(OSError):
        cos_function(storage_path, *dummy_args, **dummy_kwargs)


def test_equivalent_functions():
    """Tests functions using pycosio._core._equivalent_to"""
    from pycosio._core.storage_manager import STORAGE
    import pycosio._core.std_functions as std

    # Mock system

    prefix = 'dummy://'
    dummy_path = prefix + 'dir1/dir2/dir3'
    result = 'result'

    class System:
        """dummy system"""

        @staticmethod
        def relpath(path):
            """Checks arguments and returns fake result"""
            assert path == dummy_path
            return path.split('://')[1]

    system = System()
    STORAGE[prefix] = dict(system_cached=system)

    # Tests
    try:
        # Functions that only call "get_instance(path).function(path)

        def basic_function(path):
            """"Checks arguments and returns fake result"""
            assert path == dummy_path
            return result

        for name in ('getsize', 'getmtime', 'isfile', 'listdir'):
            setattr(system, name, basic_function)
            assert getattr(std, name)(dummy_path) == result

        # relpath
        assert std.relpath(dummy_path) == 'dir1/dir2/dir3'
        assert std.relpath(dummy_path, start='dir1/') == 'dir2/dir3'

    # Clean up
    finally:
        del STORAGE[prefix]
