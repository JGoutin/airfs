# coding=utf-8
"""Cloud storage I/O"""

from abc import abstractmethod as _abstractmethod
import io as _io
import os as _os
import threading as _threading

# TODO: OSError for function depending on seekable, writable, ...
# TODO: Check full exception, and IO interface behavior
# TODO: Text IO wrapper
# TODO: Auto sub class selection (open)

# Generic functions to implement


def open():
    """"""


def copy():
    """"""


def getsize(path):
    """
    Return the size, in bytes, of path.

    Returns:
        int: Size in bytes.

    Raises:
         OSError: if the file does not exist or is inaccessible.
    """


def getmtime(path):
    """
    Return the time of last access of path.

    Returns:
        float: The number of seconds since the epoch
            (see the time module).

    Raises:
         OSError: if the file does not exist or is inaccessible.
    """


def listdir(self):
    """"""


def exists(self, path):
    """"""


# Storage classes
# TODO: Move to cloudstorageio.abc

class _ObjectBase(_io.IOBase):
    """Base class to handle cloud object."""

    def __init__(self, name, mode='r'):
        _io.IOBase.__init__(self)

        self._name = name
        self._mode = mode

        # Thread safe stream position
        self._seek = 0
        self._seek_lock = _threading.Lock()

        # Select supported features based on mode
        self._writable = False
        self._readable = False
        self._seekable = True

        if 'w' in mode or 'x' in mode or 'a' in mode:
            self._writable = True

        if 'r' in mode or '+' in mode:
            self._readable = True

    @_abstractmethod
    def getsize(self):
        """
        Return the size, in bytes, of path.

        Returns:
            int: Size in bytes.

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """

    @_abstractmethod
    def getmtime(self):
        """
        Return the time of last access of path.

        Returns:
            float: The number of seconds since the epoch
                (see the time module).

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """

    @property
    def mode(self):
        """
        The mode.

        Returns:
            str: Mode.
        """
        return self._mode

    @property
    def name(self):
        """
        The file name.

        Returns:
            str: Name.
        """
        return self._name

    def readable(self):
        """
        Return True if the stream can be read from.
        If False, read() will raise OSError.

        Returns:
            bool: Supports reading.
        """
        return self._readable

    def seekable(self):
        """
        Return True if the stream supports random access.
        If False, seek(), tell() and truncate() will raise OSError.

        Returns:
            bool: Supports random access.
        """
        return self._seekable

    def writable(self):
        """
        Return True if the stream supports writing.
        If False, write() and truncate() will raise OSError.

        Returns:
            bool: Supports writing.
        """
        return self._writable


class ObjectIOBase(_io.RawIOBase, _ObjectBase):
    """Base class for binary cloud storage object I/O.

    In write mode, this class needs enough memory to store the entire object
    to write. In append mode, the cloud object is read and stored in memory
    on instantiation.
    For big objects use BufferedObjectIOBase that can performs operations
    with less memory.

    In read mode, this class random access to the cloud object and
    require only the accessed data size in memory.

    In read + write mode, read and write data may not be synchronized, use
    "flush()" to save written data in cloud and access it with "read()"

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r' (default), 'w', 'x', 'a'
            for reading (default), writing, exclusive creation or appending
            writing, exclusive creation.
            Add a '+' to the mode to allow simultaneous reading and writing.
    """

    def __init__(self, name, mode='r', **_):
        _io.RawIOBase.__init__(self)
        _ObjectBase.__init__(self, name, mode=mode)

        if self._writable:
            # In write mode, since it is not possible
            # to random write on cloud storage,
            # The full file needs to be write at once.
            # This write buffer store data in wait to send
            # it on storage on "flush()" call.
            self._write_buffer = bytearray()

            if 'a' in mode:
                self._write_buffer[:] = self.readall()
            elif 'x' in mode:
                # TODO: 'x' mode
                pass

    @_abstractmethod
    def flush(self):
        """
        Flush the write buffers of the stream if applicable and
        save the object on the cloud.
        """

    @_abstractmethod
    def readinto(self, b):
        """
        Read bytes into a pre-allocated, writable bytes-like object b,
        and return the number of bytes read.

        Args:
            b (bytes-like object): buffer.

        Returns:
            int: number of bytes read
        """

    def seek(self, offset, whence=_os.SEEK_SET):
        """
        Change the stream position to the given byte offset.

        Args:
            offset: Offset is interpreted relative to the position indicated by whence.
            whence: The default value for whence is SEEK_SET. Values for whence are:
                SEEK_SET or 0 – start of the stream (the default);
                offset should be zero or positive
                SEEK_CUR or 1 – current stream position;
                offset may be negative
                SEEK_END or 2 – end of the stream;
                offset is usually negative

        Returns:
            int: The new absolute position.
        """
        with self._seek_lock:
            if whence == _os.SEEK_SET:
                self._seek = offset
            elif whence == _os.SEEK_CUR:
                self._seek += offset
            elif whence == _os.SEEK_END:
                self._seek = offset + self.getsize()
            else:
                raise ValueError(
                    'Unsupported whence "%s"' % whence)
            return self._seek

    def tell(self):
        """Return the current stream position.

        Returns:
            int: Stream position."""
        with self._seek_lock:
            return self._seek

    def write(self, b):
        """
        Write the given bytes-like object, b, to the underlying raw stream,
        and return the number of bytes written.

        Args:
            b (bytes-like object): Bytes to write.

        Returns:
            int: The number of bytes written.
        """
        # This function write data in a buffer
        # "flush()" need to be called really write content on
        # Cloud Storage
        size = len(b)
        with self._seek_lock:
            start = self._seek
            end = start + size
            self._seek = end
        self._write_buffer[start:end] = b
        return size


class BufferedObjectIOBase(_io.BufferedIOBase, _ObjectBase):
    """Base class for buffered binary cloud storage object I/O"""
    _RAW_CLASS = ObjectIOBase

    def __init__(self, name, mode='r', buffer_size=_io.DEFAULT_BUFFER_SIZE, **kwargs):
        self._raw = self._RAW_CLASS(name, mode=mode, **kwargs)
        _io.BufferedIOBase.__init__(self)
        _ObjectBase.__init__(self, name, mode=mode)

        if self._writable:
            self._write_buffer = bytearray(buffer_size)
            self._buffer_seek = 0
            self._buffer_size = buffer_size
            self._part_number = 1
            self._seekable = False
            self._write_initialized = False

    @property
    def raw(self):
        """The underlying raw stream

        Returns:
            ObjectIOBase subclass: Raw stream.
        """
        return self._raw

    @_abstractmethod
    def _flush(self, end_of_object=False):
        """Flush the write buffers of the stream.

        Args:
            end_of_object (bool): If True, mark the writing as completed.
                Any following write will start from the start of object.
        """

    def flush(self):
        """
        Flush the write buffers of the stream if applicable and
        save the object on the cloud.

        Once called, object is closed on the cloud and any
        new write will restart from the beginning of the file.
        """
        self._flush(end_of_object=True)
        self._write_initialized = False

    def peek(self, size=-1):
        """
        Return bytes from the stream without advancing the position.

        Args:
            size (int): Number of bytes to read. -1 to read the full
                stream.

        Returns:
            bytes: bytes read
        """
        with self._seek_lock:
            seek = self._raw.tell()
            try:
                return self._raw.read(size)
            finally:
                self._raw.seek(seek)

    @_abstractmethod
    def read(self, size=-1):
        """Read and return up to size bytes,
        with at most one call to the underlying raw stream’s.

        Use at most one call to the underlying raw stream’s read method.

        Args:
            size (int): Number of bytes to read. -1 to read the
                stream until end."""

    def read1(self, size=-1):
        """Read and return up to size bytes,
        with at most one call to the underlying raw stream’s.

        Use at most one call to the underlying raw stream’s read method.

        Args:
            size (int): Number of bytes to read. -1 to read the
                stream until end."""
        return self._raw.read(size)

    @_abstractmethod
    def readinto(self, b):
        """
        Read bytes into a pre-allocated, writable bytes-like object b,
        and return the number of bytes read.

        Args:
            b (bytes-like object): buffer.

        Returns:
            int: number of bytes read
        """

    def readinto1(self, b):
        """
        Read bytes into a pre-allocated, writable bytes-like object b,
        and return the number of bytes read.

        Use at most one call to the underlying raw stream’s readinto method.

        Args:
            b (bytes-like object): buffer.

        Returns:
            int: number of bytes read
        """
        return self._raw.readinto(b)

    def getsize(self):
        """
        Return the size, in bytes, of path.

        Returns:
            int: Size in bytes.

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        return self.raw.getsize()

    def getmtime(self):
        """
        Return the time of last access of path.

        Returns:
            float: The number of seconds since the epoch
                (see the time module).

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        return self.raw.getmtime()

    def write(self, b):
        """
        Write the given bytes-like object, b, to the underlying raw stream,
        and return the number of bytes written.

        Args:
            b (bytes-like object): Bytes to write.

        Returns:
            int: The number of bytes written.
        """
