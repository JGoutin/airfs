# coding=utf-8
"""Cloud storage I/O"""

from abc import abstractmethod as _abstractmethod
import io as _io
import os as _os

# TODO: OSError for function depending on seekable, writable, ...
# TODO: Check full exception, and IO interface behavior

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


# Storage classes

class RawStorageIO(_io.RawIOBase):
    """Raw binary storage I/O"""

    def __init__(self, name, mode='r'):
        _io.RawIOBase.__init__(self)
        self._name = name
        self._mode = mode
        self._seek = 0
        self._seekable = True

        if mode == 'w':
            self._write_buffer = bytearray()
            self._readable = False
            self._writable = True
        elif mode == 'r':
            self._readable = True
            self._writable = False
        else:
            raise ValueError(
                'Unsupported mode "%s"' % mode)

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
        # This buffer need to be flushed to write content on
        # Cloud Storage
        size = len(b)
        start = self._seek
        self._seek += size
        self._write_buffer[start:self._seek] = b
        return size

    def writable(self):
        """
        Return True if the stream supports writing.
        If False, write() and truncate() will raise OSError.

        Returns:
            bool: Supports writing.
        """
        return self._writable


class BufferedStorageIO(_io.BufferedIOBase):
    """Buffered binary storage I/O"""
