# coding=utf-8
"""Test pycosio._core.io_raw"""
import io
import os

import pytest


def test_object_raw_base_io():
    """Tests pycosio._core.io_raw.ObjectRawIOBase"""
    from pycosio._core.io_base_raw import ObjectRawIOBase
    from pycosio._core.exceptions import ObjectNotFoundError
    from pycosio._core.io_random_write import ObjectRawIORandomWriteBase

    # Mock sub class
    name = 'name'
    size = 10000
    flushed = bytearray()
    raise_not_exists_exception = False

    class DummySystem:
        """Dummy system"""

        client = None

        def __init__(self, **_):
            """Do nothing"""

        @staticmethod
        def getsize(*_, **__):
            """Returns fake result"""
            if raise_not_exists_exception:
                raise ObjectNotFoundError
            return size

        @staticmethod
        def head(*_, **__):
            """Returns fake result"""
            if raise_not_exists_exception:
                raise ObjectNotFoundError
            return {}

        @staticmethod
        def relpath(path):
            """Returns fake result"""
            return path

        @staticmethod
        def get_client_kwargs(*_, **__):
            """Returns fake result"""
            return {}

    class DummyIO(ObjectRawIOBase):
        """Dummy IO"""
        _SYSTEM_CLASS = DummySystem

        def _flush(self, buffer, *_):
            """Flush in a buffer"""
            flushed[:] = buffer

        def _read_range(self, start, end=0):
            """Read fake bytes"""
            if raise_not_exists_exception:
                raise ObjectNotFoundError
            if end == 0:
                end = size
            return ((size if end > size else end) - start) * b'0'

    class DummyIORandomWrite(DummyIO, ObjectRawIORandomWriteBase):
        """Dummy IO with random write support"""

        def _flush(self, buffer, start, stop):
            """Flush in a buffer"""
            flushed[start:stop] = buffer

    # Test seek/tell
    object_io = DummyIO(name)
    assert object_io.tell() == 0
    assert object_io.seek(10) == 10
    assert object_io.tell() == 10
    assert object_io.seek(10) == 10
    assert object_io.tell() == 10
    assert object_io.seek(10, os.SEEK_CUR) == 20
    assert object_io.tell() == 20
    assert object_io.seek(-10, os.SEEK_END) == size - 10
    assert object_io.tell() == size - 10

    with pytest.raises(ValueError):
        object_io.seek(10, 10)

    object_io._seekable = False
    with pytest.raises(io.UnsupportedOperation):
        object_io.seek(0)
    with pytest.raises(io.UnsupportedOperation):
        object_io.tell()

    # Test readinto
    object_io = DummyIO(name)
    read_buffer = bytearray(100)
    assert object_io.readinto(read_buffer) == 100
    assert bytes(read_buffer) == 100 * b'0'
    assert object_io.tell() == 100
    assert object_io.readinto(bytearray(100)) == 100
    assert object_io.tell() == 200
    assert object_io.readinto(bytearray(size)) == size - 200

    # Test read with size (call readinto)
    object_io.seek(200)
    assert object_io.read(100) == 100 * b'0'
    assert object_io.tell() == 300

    # Test readall
    object_io.seek(300)
    assert object_io.readall() == (size - 300) * b'0'
    assert object_io.tell() == size

    # Test read without size (call readall)
    object_io.seek(300)
    assert object_io.read() == (size - 300) * b'0'
    assert object_io.tell() == size

    # Tests: flush should do nothing
    seek = object_io._seek
    object_io.flush()
    assert object_io._seek == seek

    # Test write in read mode
    with pytest.raises(io.UnsupportedOperation):
        object_io.write(b'0')

    # Test write
    object_io = DummyIO(name, mode='w')
    assert object_io.write(10 * b'0') == 10
    assert object_io.tell() == 10
    assert object_io.write(10 * b'0') == 10
    assert object_io.tell() == 20
    object_io.seek(10)
    assert object_io.write(10 * b'0') == 10
    assert object_io.tell() == 20
    assert object_io._get_buffer().tobytes() == 20 * b'0'

    # Test flush
    assert not len(flushed)
    object_io.flush()
    assert len(flushed) == 20

    # Test append
    object_io = DummyIO(name, mode='a')
    assert object_io.tell() == size
    assert bytes(object_io._write_buffer) == size * b'0'

    # Test append on non existing file
    raise_not_exists_exception = True
    object_io = DummyIO(name, mode='a')
    assert object_io.tell() == 0
    raise_not_exists_exception = False

    # Test exclusive creation
    with pytest.raises(OSError):
        DummyIO(name, mode='x')
    raise_not_exists_exception = True
    assert DummyIO(name, mode='x')
    raise_not_exists_exception = False

    # Test HTTP range
    assert ObjectRawIOBase._http_range(10, 50) == 'bytes=10-49'
    assert ObjectRawIOBase._http_range(10) == 'bytes=10-'

    # Random write access
    object_io = DummyIORandomWrite(name, mode='w')
    flushed[:] = b''
    assert not len(flushed)
    assert object_io.write(100 * b'0') == 100
    object_io.seek(50)
    assert len(flushed) == len(100 * b'0')
    assert object_io.write(50 * b'1') == 50
    object_io.flush()
    assert flushed == 50 * b'0' + 50 * b'1'

    object_io = DummyIORandomWrite(name, mode='a')
    assert object_io.tell() == size
    assert not len(object_io._write_buffer)
