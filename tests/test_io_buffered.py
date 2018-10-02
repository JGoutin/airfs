# coding=utf-8
"""Test pycosio._core.io_buffered"""
import io
import time

import pytest

from tests.utilities import BYTE


def test_object_buffered_base_io():
    """Tests pycosio._core.io_buffered.ObjectBufferedIOBase"""
    from pycosio._core.io_raw import ObjectRawIOBase
    from pycosio._core.io_buffered import ObjectBufferedIOBase

    # Mock sub class
    name = 'name'
    size = 10000
    flushed = bytearray()
    raw_flushed = bytearray()
    buffer_size = 100
    flush_sleep = 0

    def flush(data):
        """Dummy flush"""
        flushed.extend(data)
        time.sleep(flush_sleep)

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
            raw_flushed.extend(self._write_buffer)

        def _read_range(self, start, end=0):
            """Read fake bytes"""
            return ((size if end > size else end) - start) * BYTE

    class DummyBufferedIO(ObjectBufferedIOBase):
        """Dummy buffered IO"""
        _RAW_CLASS = DummyRawIO
        DEFAULT_BUFFER_SIZE = buffer_size
        MINIMUM_BUFFER_SIZE = 10

        def ensure_ready(self):
            """Ensure flush is complete"""
            while any(
                    1 for future in self._write_futures
                    if not future.done()):
                time.sleep(0.01)

        def __init(self, *arg, **kwargs):
            ObjectBufferedIOBase.__init__(self, *arg, **kwargs)
            self.close_called = False

        def _close_writable(self):
            """Checks called"""
            self.close_called = True
            self.ensure_ready()

        def _flush(self):
            """Flush"""
            self._write_futures.append(self._workers.submit(
                flush, self._write_buffer[:self._buffer_seek]))

    # Test raw
    object_io = DummyBufferedIO(name, mode='r')
    assert isinstance(object_io.raw, object_io._RAW_CLASS)
    assert object_io._size == object_io.raw._size

    assert object_io.raw.tell() == 0
    assert object_io.peek(10) == 10 * BYTE
    assert object_io.raw.tell() == 0

    assert object_io.read1(10) == 10 * BYTE
    assert object_io.raw.tell() == 10

    buffer = bytearray(10)
    assert object_io.readinto1(buffer) == 10
    assert bytes(buffer) == 10 * BYTE
    assert object_io.raw.tell() == 20

    # Tests: Read until end
    object_io = DummyBufferedIO(name, mode='r')
    assert object_io.read() == size * BYTE

    # Tests: Read, max buffer
    object_io = DummyBufferedIO(name, mode='r', max_buffers=0)
    assert object_io._max_buffers == size // buffer_size

    object_io = DummyBufferedIO(name, mode='r', max_buffers=5)
    assert object_io.read(100) == 100 * BYTE

    # Tests: Read by parts
    assert sorted(object_io._read_queue) == list(range(
        100, 100 + buffer_size * 5, buffer_size))
    assert object_io._seek == 100
    assert object_io.read(150) == 150 * BYTE
    assert sorted(object_io._read_queue) == list(range(
        200, 200 + buffer_size * 5, buffer_size))
    assert object_io._seek == 250
    assert object_io.read(50) == 50 * BYTE
    assert sorted(object_io._read_queue) == list(range(
        300, 300 + buffer_size * 5, buffer_size))
    assert object_io._seek == 300
    assert object_io.read() == (size - 300) * BYTE
    assert not object_io._read_queue

    # Tests: Read small parts
    part = buffer_size // 10
    object_io.seek(0)
    for index in range(1, 15):
        assert object_io.read(part) == part * BYTE
        assert object_io._seek == part * index

    # Tests: Read, change seek
    object_io.seek(450)
    assert sorted(object_io._read_queue) == list(range(
        450, 450 + buffer_size * 5, buffer_size))

    object_io.seek(700)
    assert sorted(object_io._read_queue) == list(range(
        700, 700 + buffer_size * 5, buffer_size))

    # Tests: flush should do nothing
    seek = object_io._seek
    object_io.flush()
    assert object_io._seek == seek

    # Tests: Read buffer size (No copy mode)
    object_io.seek(0)
    assert object_io.read(buffer_size) == buffer_size * BYTE

    object_io.seek(size - buffer_size // 2)
    assert object_io.read(buffer_size) == BYTE * (buffer_size // 2)
    object_io._seek = size

    # Tests: Read, EOF before theoretical EOF
    def read_range(*_, **__):
        """Returns empty bytes"""
        return b''

    object_io = DummyBufferedIO(name, mode='r', max_buffers=5)
    object_io._read_range = read_range
    assert object_io.read() == b''

    # Tests write (with auto flush)
    assert bytes(flushed) == b''
    object_io = DummyBufferedIO(name, mode='w')
    assert object_io.write(250 * BYTE) == 250
    object_io.ensure_ready()
    assert object_io._buffer_seek == 50
    assert bytes(object_io._write_buffer) == 50 * BYTE + 50 * b'\x00'
    assert object_io._get_buffer().tobytes() == 50 * BYTE
    assert object_io._seek == 2
    assert len(flushed) == 200
    assert bytes(flushed) == 200 * BYTE

    # Tests manual flush
    object_io.flush()
    object_io.ensure_ready()
    assert object_io._seek == 3
    assert bytes(flushed) == 250 * BYTE
    assert object_io._buffer_seek == 0

    # Tests write, only buffered should flush
    flushed = bytearray()
    raw_flushed = bytearray()
    assert bytes(flushed) == b''
    assert bytes(raw_flushed) == b''

    with DummyBufferedIO(name, mode='w') as object_io:
        assert object_io.write(150 * BYTE) == 150
        object_io.ensure_ready()
        assert len(flushed) == 100
        assert object_io._buffer_seek == 50
        assert len(object_io._get_buffer()) == 50
        object_io.raw._write_buffer = object_io._get_buffer()
        assert len(object_io.raw._get_buffer()) == 50
    assert len(flushed) == 150
    assert not len(raw_flushed)

    # Tests write small data flushed by raw
    object_io = DummyBufferedIO(name, mode='w')
    assert object_io.write(10 * BYTE) == 10
    object_io.close()
    assert bytes(raw_flushed) == 10 * BYTE

    # Test max buffer
    object_io = DummyBufferedIO(name, mode='w', max_buffers=2)
    flush_sleep = object_io._FLUSH_WAIT
    assert object_io.write(1000 * BYTE) == 1000
    flush_sleep = 0

    # Test read in write mode
    object_io = DummyBufferedIO(name, mode='w')
    with pytest.raises(io.UnsupportedOperation):
        object_io.read()

    # Test seek in write mode
    object_io = DummyBufferedIO(name, mode='w')
    with pytest.raises(io.UnsupportedOperation):
        object_io.seek(0)

    # Test write in read mode
    object_io = DummyBufferedIO(name, mode='r')
    with pytest.raises(io.UnsupportedOperation):
        object_io.write(BYTE)

    # Test buffer size
    object_io = DummyBufferedIO(name, mode='w')
    assert object_io._buffer_size == DummyBufferedIO.DEFAULT_BUFFER_SIZE
    object_io = DummyBufferedIO(name, mode='w', buffer_size=1000)
    assert object_io._buffer_size == 1000
    object_io = DummyBufferedIO(name, mode='w', buffer_size=1)
    assert object_io._buffer_size == DummyBufferedIO.MINIMUM_BUFFER_SIZE
