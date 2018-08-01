# coding=utf-8
"""Cloud storage abstract Raw IO class"""
from abc import abstractmethod
from email.utils import parsedate
from io import RawIOBase, UnsupportedOperation
from os import SEEK_CUR, SEEK_END, SEEK_SET
from time import mktime

from pycosio._core.io_base import ObjectIOBase


class ObjectRawIOBase(RawIOBase, ObjectIOBase):
    """Base class for binary cloud storage object I/O.

    In write mode, this class needs enough memory to store the entire object
    to write. In append mode, the cloud object is read and stored in memory
    on instantiation.
    For big objects use ObjectBufferedIOBase that can performs operations
    with less memory.

    In read mode, this class random access to the cloud object and
    require only the accessed data size in memory.

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
    """

    def __init__(self, name, mode='r', **storage_kwargs):
        RawIOBase.__init__(self)
        ObjectIOBase.__init__(self, name, mode=mode)

        # Get storage local path from URL
        for prefix in self._get_prefix(**storage_kwargs):
            try:
                self._path = name.split(prefix)[1]
                break
            except IndexError:
                continue
        else:
            self._path = name

        if self._writable:
            # In write mode, since it is not possible
            # to random write on cloud storage,
            # The full file needs to be write at once.
            # This write buffer store data in wait to send
            # it on storage on "flush()" call.
            if 'a' in mode:
                self._write_buffer = bytearray(self._getsize())
                memoryview(self._write_buffer)[:] = self.readall()
            else:
                self._write_buffer = bytearray()

    def flush(self):
        """
        Flush the write buffers of the stream if applicable and
        save the object on the cloud.
        """
        if self._writable:
            self._flush()

    @abstractmethod
    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """

    @ObjectIOBase._memoize
    def _getmtime(self):
        """
        Return the time of last access of path.

        Returns:
            float: The number of seconds since the epoch
                (see the time module).

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        # By default, assumes that information are in a standard HTTP header
        return mktime(parsedate({
            key.lower(): value
            for key, value in self._head().items()}['last-modified']))

    @staticmethod
    @abstractmethod
    def _get_prefix(*args, **kwargs):
        """Return URL prefixes for this storage.

        Args:
            args, kwargs: Storage specific arguments.

        Returns:
            tuple of str: URL prefixes"""

    @ObjectIOBase._memoize
    def _getsize(self):
        """
        Return the size, in bytes, of path.

        Returns:
            int: Size in bytes.

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        # By default, assumes that information are in a standard HTTP header
        return int({
            key.lower(): value
            for key, value in self._head().items()}['content-length'])

    @ObjectIOBase._memoize
    def _head(self):
        """
        Returns object HTTP header.

        Returns:
            dict: HTTP header.
        """
        # This is not an abstract method because this may not
        # be used every time

    @staticmethod
    def _http_range(start=0, end=0):
        """
        Returns an HTTP Range request for a specified python range.

        Args:
            start (int): Start of the range.
            end (int): End of the range.
                0 To not specify end.

        Returns:
            str: range.
        """
        if end:
            return 'bytes=%d-%d' % (start, end - 1)
        return 'bytes=%d-' % start

    def _peek(self, size=-1):
        """
        Return bytes from the stream without advancing the position.

        Args:
            size (int): Number of bytes to read. -1 to read the full
                stream.

        Returns:
            bytes: bytes read
        """
        with self._seek_lock:
            seek = self._seek
        return self._read_range(seek, seek + size)

    def readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        with self._seek_lock:
            # Get data starting from seek
            if self._seek and self._seekable:
                data = self._read_range(self._seek)

            # Get all data
            else:
                data = self._readall()

            # Update seek
            self._seek += len(data)
        return data

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        return self._read_range(0)

    def readinto(self, b):
        """
        Read bytes into a pre-allocated, writable bytes-like object b,
        and return the number of bytes read.

        Args:
            b (bytes-like object): buffer.

        Returns:
            int: number of bytes read
        """
        # Get and update stream positions
        size = len(b)
        with self._seek_lock:
            start = self._seek
            end = start + size
            self._seek = end

        # Read data range
        read_data = self._read_range(start, end)

        # Copy to bytes-like object
        read_size = len(read_data)
        if read_size:
            memoryview(b)[:read_size] = read_data

        # Update stream position if end of file
        if read_size != size:
            with self._seek_lock:
                self._seek = start + read_size

        # Return read size
        return read_size

    @abstractmethod
    def _read_range(self, start, end=0):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position.
                0 To not specify end.

        Returns:
            bytes: number of bytes read
        """

    def seek(self, offset, whence=SEEK_SET):
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
        if not self._seekable:
            raise UnsupportedOperation('seek')

        with self._seek_lock:
            if whence == SEEK_SET:
                self._seek = offset
            elif whence == SEEK_CUR:
                self._seek += offset
            elif whence == SEEK_END:
                self._seek = offset + self._getsize()
            else:
                raise ValueError(
                    'Unsupported whence "%s"' % whence)
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
        if not self._writable:
            raise UnsupportedOperation('write')

        # This function write data in a buffer
        # "flush()" need to be called to really write content on
        # Cloud Storage
        size = len(b)
        with self._seek_lock:
            start = self._seek
            end = start + size
            self._seek = end

        buffer = self._write_buffer
        if end <= len(buffer):
            buffer = memoryview(buffer)
        buffer[start:end] = b
        return size
