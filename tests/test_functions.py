# coding=utf-8
"""Test pycosio._core.function_*"""

from io import BytesIO

import pytest


def test_equivalent_to():
    """Tests pycosio._core.functions_core.equivalent_to"""
    from pycosio._core.functions_core import equivalent_to
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
    """Tests functions using pycosio._core.functions_core.equivalent_to"""
    from pycosio._core.storage_manager import MOUNTED
    import pycosio._core.functions_os_path as std

    # Mock system

    root = 'dummy://'
    relative = 'dir1/dir2/dir3'
    dummy_path = root + relative
    excepted_path = dummy_path
    result = 'result'

    class System:
        """dummy system"""

        @staticmethod
        def relpath(path):
            """Checks arguments and returns fake result"""
            if excepted_path:
                assert path.startswith(excepted_path)
            return path.split(root)[1].strip('/')

    system = System()
    MOUNTED[root] = dict(system_cached=system)

    # Tests
    try:
        # Functions that only call "get_instance(path).function(path)

        def basic_function(path):
            """"Checks arguments and returns fake result"""
            assert path == excepted_path
            return result

        for name in ('exists', 'getsize', 'getmtime', 'isdir', 'isfile'):
            setattr(system, name, basic_function)
            assert getattr(std, name)(dummy_path) == result

        # relpath
        assert std.relpath(dummy_path) == relative
        assert std.relpath(dummy_path, start='dir1/') == 'dir2/dir3'

        # ismount
        assert std.ismount(dummy_path) is False
        excepted_path = root
        assert std.ismount(root) is True

        # isabs
        excepted_path = dummy_path
        assert std.isabs(dummy_path) is True
        excepted_path = relative
        assert std.isabs(excepted_path) is False

        # splitdrive
        excepted_path = dummy_path
        assert std.splitdrive(dummy_path) == (root, relative)
        old_root = root
        old_relative = relative
        relative = 'dir2/dir3'
        root += 'dir1'
        assert std.splitdrive(dummy_path) == (root, '/' + relative)
        root = old_root
        relative = old_relative

        # samefile
        assert std.samefile(__file__, __file__)
        assert std.samefile(dummy_path, dummy_path)
        assert not std.samefile(dummy_path, relative)
        assert not std.samefile(relative, dummy_path)
        assert std.samefile(dummy_path, dummy_path + '/')
        assert not std.samefile(dummy_path, dummy_path + '/dir4')

        root2 = 'dummy2://'
        MOUNTED[root2] = dict(system_cached=System())
        excepted_path = ''
        assert not std.samefile(root2 + relative, dummy_path)

    # Clean up
    finally:
        del MOUNTED[root]


def test_cos_open(tmpdir):
    """
    Tests  pycosio._core.functions_io.cos_open and
    pycosio._core.functions_shutil.copy
    """

    from pycosio import copy
    from pycosio._core.functions_io import cos_open
    from pycosio._core.storage_manager import MOUNTED
    from io import TextIOWrapper

    root = 'dummy_read://'
    cos_path = root + 'file.txt'
    content = b'dummy_content'

    # Mock storage
    class DummyIO(BytesIO):
        """Dummy IO"""

        def __init__(self, *_, **__):
            BytesIO.__init__(self, content)

    class DummyRawIO(DummyIO):
        """Dummy raw IO"""

    class DummyBufferedIO(DummyIO):
        """Dummy buffered IO"""

    MOUNTED[root] = dict(
        raw=DummyRawIO, buffered=DummyBufferedIO,
        system_cached=None, storage_parameters={})

    # Tests
    try:
        # open: Buffered Binary
        with cos_open(cos_path, 'rb') as file:
            assert isinstance(file, DummyBufferedIO)
            assert file.read() == content

        # open: Buffered Text
        with cos_open(cos_path, 'rt') as file:
            assert isinstance(file, TextIOWrapper)
            assert file.read() == content.decode()

        # open: Raw Binary
        with cos_open(cos_path, 'rb', buffering=0) as file:
            assert isinstance(file, DummyRawIO)
            assert file.read() == content

        # open: Raw Text
        with cos_open(cos_path, 'rt', buffering=0) as file:
            assert isinstance(file, TextIOWrapper)
            assert file.read() == content.decode()

        # open: Stream Binary
        with cos_open(BytesIO(content), 'rb') as file:
            assert isinstance(file, BytesIO)
            assert file.read() == content

        # open: Stream Text
        with cos_open(BytesIO(content), 'rt') as file:
            assert isinstance(file, TextIOWrapper)
            assert file.read() == content.decode()

        # open: Local file
        local_file = tmpdir.join('file.txt')
        local_file.write(content)
        with cos_open(str(local_file), 'rb') as file:
            assert file.read() == content

        # copy: Local file to local file
        local_dst = tmpdir.join('file_dst.txt')
        assert not local_dst.check()
        copy(str(local_file), str(local_dst))
        assert local_dst.read_binary() == content
        local_dst.remove()

        # copy: storage file to local file
        assert not local_dst.check()
        copy(cos_path, str(local_dst))
        assert local_dst.read_binary() == content
        local_dst.remove()

        # copy: stream to local file
        assert not local_dst.check()
        copy(BytesIO(content), str(local_dst))
        assert local_dst.read_binary() == content
        local_dst.remove()

        # copy: local file to stream
        stream_dst = BytesIO()
        copy(str(local_file), stream_dst)
        stream_dst.seek(0)
        assert stream_dst.read() == content

        # copy: storage file to local directory
        local_dir = tmpdir.mkdir('sub_dir')
        copy(cos_path, str(local_dir))
        assert local_dir.join(
            cos_path.split('://')[1]).read_binary() == content

        # copy: Buffer size
        DummyIO._buffer_size = 1024
        assert not local_dst.check()
        copy(cos_path, str(local_dst))
        assert local_dst.read_binary() == content

    # Clean up
    finally:
        del MOUNTED[root]


def test_is_storage():
    """Tests pycosio._core.storage_manager.is_storage"""
    from pycosio._core.functions_core import is_storage

    # Remote paths
    assert is_storage('', storage='storage')
    assert is_storage('http://path')

    # Local paths
    assert not is_storage('path')
    assert not is_storage('file://path')
