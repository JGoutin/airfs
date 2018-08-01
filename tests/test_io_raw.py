# coding=utf-8
"""Test pycosio._core.io_raw"""
import io
import os
import time
from wsgiref.handlers import format_date_time

import pytest

from tests.utilities import BYTE, SIZE, check_head_methods


def test_object_raw_base_io():
    """Tests pycosio._core.io_raw.ObjectRawIOBase"""
    from pycosio._core.io_raw import ObjectRawIOBase

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
