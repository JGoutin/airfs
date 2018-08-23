# coding=utf-8
"""Cloud storage abstract Raw IO class"""
from abc import abstractmethod
from io import RawIOBase, UnsupportedOperation
from os import SEEK_CUR, SEEK_END, SEEK_SET

from pycosio._core.compat import file_exits_error
from pycosio._core.exceptions import ObjectNotFoundError, handle_os_exceptions
from pycosio._core.io_base import ObjectIOBase, memoizedmethod
from pycosio._core.io_system import SystemBase


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
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a', 'x'
            for reading (default), writing or appending
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    # System I/O class
    _SYSTEM_CLASS = SystemBase

    def __init__(self, name, mode='r', storage_parameters=None, **kwargs):

        RawIOBase.__init__(self)
        ObjectIOBase.__init__(self, name, mode=mode)

        # Initializes system
        try:
            # Try to get cached system
            self._system = storage_parameters.pop('pycosio.system_cached')
        except (AttributeError, KeyError):
            self._system = None

        if not self._system:
            # If none cached, create a new system
            self._system = self._SYSTEM_CLASS(
                storage_parameters=storage_parameters, **kwargs)

        # Gets storage local path from URL
        self._path = self._system.relpath(name)
        self._client_kwargs = self._system.get_client_kwargs(name)

        # Mark as standalone RAW to avoid flush conflics on close
        self._is_raw_of_buffered = False

        # Configures write mode
        if self._writable:
            # In write mode, since it is not possible
            # to random write on cloud storage,
            # The full file needs to be write at once.
            # This write buffer store data in wait to send
            # it on storage on "flush()" call.
            if 'a' in mode:
                # Read existing file in buffer
                self._write_buffer = bytearray(self._size)
                memoryview(self._write_buffer)[:] = self.readall()
            elif 'x' in mode:
                # Checks if object exists,
                # and raise if it is the case
                try:
                    self._head()
                except ObjectNotFoundError:
                    pass
                else:
                    raise file_exits_error
            else:
                self._write_buffer = bytearray()

        # Configure read mode
        elif self._readable:
            # Get header and checks files exists
            with handle_os_exceptions():
                self._head()

    @property
    def _client(self):
        """
        Returns client instance.

        Returns:
            client
        """
        return self._system.client

    def close(self):
        """
        Flush the write buffers of the stream if applicable and
        close the object.
        """
        if self._writable and not self._is_raw_of_buffered:
            with self._seek_lock:
                self._flush()

    def flush(self):
        """
        Flush the write buffers of the stream if applicable and
        save the object on the cloud.
        """
        if self._writable:
            with handle_os_exceptions():
                self._flush()

    @abstractmethod
    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """

    def _get_buffer(self):
        """
        Get a memory view of the current write buffer
        until its seek value.

        Returns:
            memoryview: buffer view.
        """
        return memoryview(self._write_buffer)

    @property
    @memoizedmethod
    def _size(self):
        """
        Return the size, in bytes, of path.

        Returns:
            int: Size in bytes.
        """
        return self._system.getsize(header=self._head())

    @memoizedmethod
    def _head(self):
        """
        Return file header.

        Returns:
            dict: header.
        """
        return self._system.head(client_kwargs=self._client_kwargs)

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
            offset: Offset is interpreted relative to the position indicated by
                whence.
            whence: The default value for whence is SEEK_SET.
                Values for whence are:
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
                self._seek = offset + self._size
            else:
                raise ValueError('Unsupported whence "%s"' % whence)
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
