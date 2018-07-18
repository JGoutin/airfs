# coding=utf-8
"""Cloud storage I/O"""

from abc import abstractmethod as _abstractmethod
import io as _io
import os as _os
import threading as _threading

# TODO: OSError for function depending on seekable, writable, ...
# TODO: Check full exception, and IO interface behavior
# TODO: Mode 'x'
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

class ObjectIOBase(_io.RawIOBase):
    """Base class for binary cloud storage object I/O.

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'x' for reading (default),
            writing, exclusive creation.
    """

    def __init__(self, name, mode='r', **_):
        _io.RawIOBase.__init__(self)
        self._name = name
        self._mode = mode

        # Thread safe stream position
        self._seek = 0
        self._seek_lock = _threading.Lock()

        # Select supported features based on mode
        if mode in ('w', 'x'):
            self._readable = False
            self._writable = True

            # In write mode, since it is not possible
            # to random write on cloud storage,
            # The full file needs to be write at once.
            # This write buffer store data in wait to send
            # it on storage on "flush()" call.
            self._write_buffer = bytearray()

        elif mode == 'r':
            self._readable = True
            self._writable = False

        else:
            raise ValueError(
                'Unsupported mode "%s"' % mode)

        self._seekable = True

    @_abstractmethod
    def flush(self):
        """
        Flush the write buffers of the stream if applicable.
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
            return self._seek

    def seekable(self):
        """
        Return True if the stream supports random access.
        If False, seek(), tell() and truncate() will raise OSError.

        Returns:
            bool: Supports random access.
        """
        return self._seekable

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

    def writable(self):
        """
        Return True if the stream supports writing.
        If False, write() and truncate() will raise OSError.

        Returns:
            bool: Supports writing.
        """
        return self._writable


class BufferedObjectIOBase(_io.BufferedIOBase):
    """Base class for buffered binary cloud storage object I/O"""
    # TODO: BufferedReader + BufferedIOBase interface
    _RAW_CLASS = ObjectIOBase

    def __init__(self, name, mode='r', **kwargs):
        _io.BufferedIOBase.__init__(self)

        self._raw = self._RAW_CLASS(name, mode, **kwargs)
        self.read1 = self._raw.read
        self.readinto1 = self._raw.readinto

    @property
    def raw(self):
        """
        The underlying raw stream (a RawIOBase instance) that Buffered IO deals with.

        Returns:
            io.RawIOBase subclass: Raw stream.
        """
        return self._raw

    def detach(self):
        """Separate the underlying raw stream from the buffer and return it.

        After the raw stream has been detached, the buffer is in an unusable state."""
        return self._raw
