# coding=utf-8
"""Test pycosio.io_base"""
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import io
import os
import time
from wsgiref.handlers import format_date_time

import pytest

from tests.utilities import BYTE, SIZE, check_head_methods


def test_object_base_io():
    """Tests pycosio.io_base.ObjectIOBase"""
    from pycosio.io_base import ObjectIOBase

    name = 'name'

    # Tests mode
    object_io = ObjectIOBase(name, mode='r')
    assert object_io.name == name
    assert object_io.mode == 'r'
    assert object_io.readable()
    assert object_io.seekable()
    assert not object_io.writable()

    object_io = ObjectIOBase(name, mode='w')
    assert object_io.mode == 'w'
    assert not object_io.readable()
    assert object_io.seekable()
    assert object_io.writable()

    object_io = ObjectIOBase(name, mode='a')
    assert object_io.mode == 'a'
    assert not object_io.readable()
    assert object_io.seekable()
    assert object_io.writable()

    with pytest.raises(ValueError):
        ObjectIOBase(name, mode='z')


def test_object_raw_base_io():
    """Tests pycosio.io_base.ObjectRawIOBase"""
    from pycosio.io_base import ObjectRawIOBase

    # Mock sub class
    name = 'name'
    size = 10000
    flushed = bytearray()
    m_time = time.time()

    class DummyIO(ObjectRawIOBase):
        """Dummy IO"""

        def _getsize(self):
            """Returns fake result"""
            return size

        def _flush(self):
            """Flush in a buffer"""
            flushed[:] = self._write_buffer

        def _head(self):
            """Returns fake result"""
            return {'Content-Length': str(SIZE),
                    'Last-Modified': format_date_time(m_time)}

        def _read_range(self, start, end=0):
            """Read fake bytes"""
            if end == 0:
                end = size
            return ((size if end > size else end) - start) * BYTE

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

    # Test readinto
    object_io = DummyIO(name, mode='r')
    buffer = bytearray(100)
    assert object_io.readinto(buffer) == 100
    assert bytes(buffer) == 100 * BYTE
    assert object_io.tell() == 100
    assert object_io.readinto(bytearray(100)) == 100
    assert object_io.tell() == 200
    assert object_io.readinto(bytearray(size)) == size - 200

    # Test read with size (call readinto)
    object_io.seek(200)
    assert object_io.read(100) == 100 * BYTE
    assert object_io.tell() == 300

    # Test readall
    object_io.seek(300)
    assert object_io.readall() == (size - 300) * BYTE
    assert object_io.tell() == size

    # Test read without size (call readall)
    object_io.seek(300)
    assert object_io.read() == (size - 300) * BYTE
    assert object_io.tell() == size

    # Tests _head
    check_head_methods(object_io, m_time, size)

    # Test write in read mode
    with pytest.raises(io.UnsupportedOperation):
        object_io.write(BYTE)

    # Test write
    object_io = DummyIO(name, mode='w')
    assert object_io.write(10 * BYTE) == 10
    assert object_io.tell() == 10
    assert object_io.write(10 * BYTE) == 10
    assert object_io.tell() == 20
    object_io.seek(10)
    assert object_io.write(10 * BYTE) == 10
    assert object_io.tell() == 20

    # Test flush
    assert not len(flushed)
    object_io.flush()
    assert len(flushed) == 20

    # Test append
    object_io = DummyIO(name, mode='a')
    assert object_io.tell() == size
    assert bytes(object_io._write_buffer) == size * BYTE

    # Test HTTP range
    assert ObjectRawIOBase._http_range(10, 50) == 'bytes=10-49'
    assert ObjectRawIOBase._http_range(10) == 'bytes=10-'


def test_object_buffered_base_io():
    """Tests pycosio.io_base.ObjectBufferedIOBase"""
    from pycosio.io_base import ObjectBufferedIOBase, ObjectRawIOBase

    # Mock sub class
    name = 'name'
    size = 10000
    flushed = bytearray()
    raw_flushed = bytearray()

    class DummyRawIO(ObjectRawIOBase):
        """Dummy IO"""

        def _getmtime(self):
            """Do nothing"""
            return 0.0

        def _getsize(self):
            """Returns fake result"""
            return size

        def _flush(self):
            """Do nothing"""
            raw_flushed.extend(self._write_buffer[:self._seek])

        def _read_range(self, start, end=0):
            """Read fake bytes"""
            return ((size if end > size else end) - start) * BYTE

    class DummyBufferedIO(ObjectBufferedIOBase):
        """Dummy buffered IO"""
        _RAW_CLASS = DummyRawIO
        DEFAULT_BUFFER_SIZE = 100
        MINIMUM_BUFFER_SIZE = 10

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
    assert object_io._getsize() == object_io.raw._getsize()
    assert object_io._getmtime() == object_io.raw._getmtime()

    assert object_io.raw.tell() == 0
    assert object_io.peek(10) == 10 * BYTE
    assert object_io.raw.tell() == 0

    assert object_io.read1(10) == 10 * BYTE
    assert object_io.raw.tell() == 10

    buffer = bytearray(10)
    assert object_io.readinto1(buffer) == 10
    assert bytes(buffer) == 10 * BYTE
    assert object_io.raw.tell() == 20

    # Tests read
    object_io = DummyBufferedIO(name, mode='r')
    # TODO: implementation

    # Tests write
    assert bytes(flushed) == b''
    object_io = DummyBufferedIO(name, mode='w')
    assert object_io.write(250 * BYTE) == 250
    assert object_io._buffer_seek == 50
    assert bytes(object_io._write_buffer) == 50 * BYTE + 50 * b'\x00'
    assert object_io._get_buffer().tobytes() == 50 * BYTE
    assert object_io._seek == 2
    assert len(flushed) == 200
    assert bytes(flushed) == 200 * BYTE

    object_io.flush()
    assert object_io._seek == 3
    assert bytes(flushed) == 250 * BYTE
    assert object_io._buffer_seek == 0

    assert bytes(raw_flushed) == b''
    object_io = DummyBufferedIO(name, mode='w')
    assert object_io.write(10 * BYTE) == 10
    object_io.close()
    assert bytes(raw_flushed) == 10 * BYTE

    # Test read in write mode
    object_io = DummyBufferedIO(name, mode='w')
    with pytest.raises(io.UnsupportedOperation):
        object_io.read()

    # Test write in read mode
    object_io = DummyBufferedIO(name, mode='r')
    with pytest.raises(io.UnsupportedOperation):
        object_io.write(BYTE)

    # Test workers type
    object_io = DummyBufferedIO(name, workers_type='thread')
    assert isinstance(object_io._workers, ThreadPoolExecutor)
    object_io = DummyBufferedIO(name, workers_type='process')
    assert isinstance(object_io._workers, ProcessPoolExecutor)

    # Test buffer size
    object_io = DummyBufferedIO(name, mode='w')
    assert object_io._buffer_size == DummyBufferedIO.DEFAULT_BUFFER_SIZE
    object_io = DummyBufferedIO(name, mode='w', buffer_size=1000)
    assert object_io._buffer_size == 1000
    object_io = DummyBufferedIO(name, mode='w', buffer_size=1)
    assert object_io._buffer_size == DummyBufferedIO.MINIMUM_BUFFER_SIZE
