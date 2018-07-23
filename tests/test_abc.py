# coding=utf-8
"""Test pycosio.abc"""
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import io
import os

import pytest


def test_object_base_io():
    """Tests pycosio.abc.ObjectIOBase"""
    from pycosio.abc import ObjectIOBase

    # Mock sub class
    size = 100
    name = 'name'

    class DummyIO(ObjectIOBase):
        """Dummy IO"""

        def getmtime(self):
            """Do nothing"""
            return 0.0

        def getsize(self):
            """Returns fake result"""
            return size

    # Tests mode
    object_io = DummyIO(name, mode='r')
    assert object_io.name == name
    assert object_io.mode == 'r'
    assert object_io.readable()
    assert object_io.seekable()
    assert not object_io.writable()

    object_io = DummyIO(name, mode='w')
    assert object_io.mode == 'w'
    assert not object_io.readable()
    assert object_io.seekable()
    assert object_io.writable()

    object_io = DummyIO(name, mode='a')
    assert object_io.mode == 'a'
    assert not object_io.readable()
    assert object_io.seekable()
    assert object_io.writable()

    with pytest.raises(ValueError):
        DummyIO(name, mode='z')

    # Test seek/tell
    object_io = DummyIO(name)
    assert object_io.tell() == 0
    assert object_io.seek(10) == 10
    assert object_io.tell() == 10
    assert object_io.seek(10, os.SEEK_SET) == 10
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


def test_object_raw_base_io():
    """Tests pycosio.abc.ObjectRawIOBase"""
    from pycosio.abc import ObjectRawIOBase

    # Mock sub class
    name = 'name'
    size = 10000
    flushed = bytearray()
    one_byte = b'0'

    class DummyIO(ObjectRawIOBase):
        """Dummy IO"""

        def getmtime(self):
            """Do nothing"""
            return 0.0

        def getsize(self):
            """Returns fake result"""
            return size

        def _flush(self):
            """Flush in a buffer"""
            flushed[:] = self._write_buffer

        def _read_range(self, start, end):
            """Read fake bytes"""
            return ((size if end > size else end) - start) * one_byte

    # Test readinto
    object_io = DummyIO(name, mode='r')
    buffer = bytearray(100)
    assert object_io.readinto(buffer) == 100
    assert bytes(buffer) == 100 * one_byte
    assert object_io.tell() == 100
    assert object_io.readinto(bytearray(100)) == 100
    assert object_io.tell() == 200
    assert object_io.readinto(bytearray(size)) == size - 200

    # Test read with size (call readinto)
    object_io.seek(200)
    assert object_io.read(100) == 100 * one_byte
    assert object_io.tell() == 300

    # Test readall
    object_io.seek(300)
    assert object_io.readall() == (size - 300) * one_byte
    assert object_io.tell() == size

    # Test read without size (call readall)
    object_io.seek(300)
    assert object_io.read() == (size - 300) * one_byte
    assert object_io.tell() == size

    # Test write in read mode
    with pytest.raises(io.UnsupportedOperation):
        object_io.write(one_byte)

    # Test write
    object_io = DummyIO(name, mode='w')
    assert object_io.write(10 * one_byte) == 10
    assert object_io.tell() == 10
    assert object_io.write(10 * one_byte) == 10
    assert object_io.tell() == 20
    object_io.seek(10)
    assert object_io.write(10 * one_byte) == 10
    assert object_io.tell() == 20

    # Test flush
    assert not len(flushed)
    object_io.flush()
    assert len(flushed) == 20

    # Test append
    object_io = DummyIO(name, mode='a')
    assert object_io.tell() == size
    assert bytes(object_io._write_buffer) == size * one_byte


def test_object_buffered_base_io():
    """Tests pycosio.abc.ObjectBufferedIOBase"""
    from pycosio.abc import ObjectBufferedIOBase, ObjectRawIOBase

    # Mock sub class
    name = 'name'
    size = 10000
    flushed = bytearray()
    one_byte = b'0'

    class DummyRawIO(ObjectRawIOBase):
        """Dummy IO"""

        def getmtime(self):
            """Do nothing"""
            return 0.0

        def getsize(self):
            """Returns fake result"""
            return size

        def _flush(self):
            """Do nothing"""

        def _read_range(self, start, end):
            """Read fake bytes"""
            return ((size if end > size else end) - start) * one_byte

    class DummyBufferedIO(ObjectBufferedIOBase):
        """Dummy buffered IO"""
        _RAW_CLASS = DummyRawIO
        DEFAULT_BUFFER_SIZE = 100

        def __init(self, *arg, **kwargs):
            ObjectBufferedIOBase.__init__(self, *arg, **kwargs)
            self.close_called = False

        def _close_writable(self):
            """"""
            self.close_called = True

        def _flush(self):
            """"""
            flushed.extend(self._write_buffer[:self._buffer_seek])

    # Test raw
    object_io = DummyBufferedIO(name, mode='r')
    assert isinstance(object_io.raw, object_io._RAW_CLASS)
    assert object_io.getsize() == object_io.raw.getsize()
    assert object_io.getmtime() == object_io.raw.getmtime()

    assert object_io.raw.tell() == 0
    assert object_io.peek(10) == 10 * one_byte
    assert object_io.raw.tell() == 0

    assert object_io.read1(10) == 10 * one_byte
    assert object_io.raw.tell() == 10

    buffer = bytearray(10)
    assert object_io.readinto1(buffer) == 10
    assert bytes(buffer) == 10 * one_byte
    assert object_io.raw.tell() == 20

    # Tests read
    object_io = DummyBufferedIO(name, mode='r')
    # TODO: implementation

    # Tests write
    assert not len(flushed)
    object_io = DummyBufferedIO(name, mode='w')
    assert object_io.write(250 * one_byte) == 250
    assert object_io._buffer_seek == 50
    assert object_io._seek == 2
    assert len(flushed) == 200

    object_io.flush()
    assert object_io._seek == 3
    assert len(flushed) == 250
    assert object_io._buffer_seek == 0

    # Test read in write mode
    object_io = DummyBufferedIO(name, mode='w')
    with pytest.raises(io.UnsupportedOperation):
        object_io.read()

    # Test write in read mode
    object_io = DummyBufferedIO(name, mode='r')
    with pytest.raises(io.UnsupportedOperation):
        object_io.write(one_byte)

    # Test workers type
    object_io = DummyBufferedIO(name, workers_type='thread')
    assert isinstance(object_io._workers, ThreadPoolExecutor)
    object_io = DummyBufferedIO(name, workers_type='process')
    assert isinstance(object_io._workers, ProcessPoolExecutor)
