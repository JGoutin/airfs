# coding=utf-8
"""Test pycosio._core.std_functions"""

import pytest


def test_equivalent_to():
    """Tests pycosio._core.std_functions._equivalent_to"""
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
    """Tests functions using pycosio._core.std_functions._equivalent_to"""
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


def test_cos_open(tmpdir):
    """Tests  pycosio._core.std_functions.open"""
    from pycosio._core.std_functions import cos_open
    from pycosio._core.storage_manager import STORAGE
    from pycosio._core.io_raw import ObjectRawIOBase
    from pycosio._core.io_buffered import ObjectBufferedIOBase
    from io import TextIOWrapper

    prefix = 'dummy://'
    cos_path = prefix + 'path'
    content = b'dummy_content'
    size = len(content)

    # Mock storage

    class DummySystem:
        """Dummy system"""

        client = None

        def __init__(self, **_):
            """Do nothing"""

        @staticmethod
        def getsize(*_, **__):
            """Returns fake result"""
            return size

        @staticmethod
        def head(*_, **__):
            """Returns fake result"""
            return {}

        @staticmethod
        def relpath(path):
            """Returns fake result"""
            return path

        @staticmethod
        def get_client_kwargs(*_, **__):
            """Returns fake result"""
            return {}

    class DummyRawIO(ObjectRawIOBase):
        """Dummy IO"""
        _SYSTEM_CLASS = DummySystem

        def _flush(self):
            """Do nothing"""

        def read(self, *_, **__):
            """Read fake bytes"""
            return content

    class DummyBufferedIO(ObjectBufferedIOBase):
        """Dummy buffered IO"""
        _RAW_CLASS = DummyRawIO

        def _close_writable(self):
            """Do nothing"""

        def _flush(self):
            """Do nothing"""

        def read(self, *_, **__):
            """Read fake bytes"""
            return content

    STORAGE[prefix] = dict(
        raw=DummyRawIO,
        buffered=DummyBufferedIO,
        system=DummySystem,
        system_cached=DummySystem(),
        storage_parameters={})

    # Tests
    try:
        # Buffered Binary
        with cos_open(cos_path, 'rb') as file:
            assert isinstance(file, DummyBufferedIO)
            assert file.read() == content

        # Buffered Text
        with cos_open(cos_path, 'rt') as file:
            assert isinstance(file, TextIOWrapper)
            assert file.read() == content.decode()

        # Raw Binary
        with cos_open(cos_path, 'rb', buffering=0) as file:
            assert isinstance(file, DummyRawIO)
            assert file.read() == content

        # Raw Text
        with cos_open(cos_path, 'rt', buffering=0) as file:
            assert isinstance(file, TextIOWrapper)
            assert file.read() == content.decode()

        # Local file
        local_file = tmpdir.join('file.txt')
        local_file.write(content)
        with cos_open(str(local_file), 'rb') as file:
            assert file.read() == content

    # Clean up
    finally:
        del STORAGE[prefix]
