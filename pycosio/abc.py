# coding=utf-8
"""Cloud storage abstract classes"""

from abc import abstractmethod as _abstractmethod
import io as _io
import os as _os
import threading as _threading
import concurrent.futures as _futures


class ObjectIOBase(_io.IOBase):
    """
    Base class to handle cloud object.

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r' (default), 'w', 'a'
            for reading (default), writing or appending
    """

    def __init__(self, name, mode='r'):
        _io.IOBase.__init__(self)

        self._name = name
        self._mode = mode

        # Thread safe stream position
        self._seek = 0
        self._seek_lock = _threading.RLock()

        # Select supported features based on mode
        self._writable = False
        self._readable = False
        self._seekable = True

        if 'w' in mode or 'a' in mode:
            self._writable = True

        elif 'r' in mode:
            self._readable = True

        else:
            raise ValueError('Invalid mode "%s"' % mode)

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

    @_abstractmethod
    def getsize(self):
        """
        Return the size, in bytes, of path.

        Returns:
            int: Size in bytes.

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
        if not self._seekable:
            raise _io.UnsupportedOperation('seek')

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
        if not self._seekable:
            raise _io.UnsupportedOperation('tell')

        with self._seek_lock:
            return self._seek

    def writable(self):
        """
        Return True if the stream supports writing.
        If False, write() and truncate() will raise OSError.

        Returns:
            bool: Supports writing.
        """
        return self._writable


class ObjectRawIOBase(_io.RawIOBase, ObjectIOBase):
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
        mode (str): The mode can be 'r' (default), 'w', 'a'
            for reading (default), writing or appending
    """

    def __init__(self, name, mode='r', **_):
        _io.RawIOBase.__init__(self)
        ObjectIOBase.__init__(self, name, mode=mode)

        if self._writable:
            # In write mode, since it is not possible
            # to random write on cloud storage,
            # The full file needs to be write at once.
            # This write buffer store data in wait to send
            # it on storage on "flush()" call.
            self._write_buffer = bytearray()

            if 'a' in mode:
                self._write_buffer[:] = self.readall()

    def flush(self):
        """
        Flush the write buffers of the stream if applicable and
        save the object on the cloud.
        """
        if self._writable:
            self._flush()

    @_abstractmethod
    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """

    def readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        with self._seek_lock:
            data = self._readall()
            self._seek += len(data)
        return data

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        # This method is not abstract because it is possible to
        # use RawIOBase behavior By default
        # (uses "readinto" to performs "readall").
        # But this may no be optimal and should be overriden if
        # better specific implementation exists.
        return _io.RawIOBase.readall(self)

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
            b[:read_size] = read_data

            # Update stream position if end of file
            if read_size != size:
                with self._seek_lock:
                    self._seek = start + read_size

        # Return read size
        return read_size

    @_abstractmethod
    def _read_range(self, start, end):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position.

        Returns:
            bytes: number of bytes read
        """

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
            raise _io.UnsupportedOperation('write')

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


class ObjectBufferedIOBase(_io.BufferedIOBase, ObjectIOBase):
    """
    Base class for buffered binary cloud storage object I/O

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r' (default), 'w'.
            for reading (default) or writing
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        workers_type (str): Parallel workers type: 'thread' or 'process'.
        kwargs: RAW class extra keyword arguments.
    """
    _RAW_CLASS = ObjectRawIOBase

    #: Default buffer_size value in bytes (Default to io.DEFAULT_BUFFER_SIZE)
    DEFAULT_BUFFER_SIZE = _io.DEFAULT_BUFFER_SIZE

    def __init__(self, name, mode='r', buffer_size=None,
                 max_workers=None, workers_type='thread', **kwargs):

        # Instantiate raw IO
        self._raw = self._RAW_CLASS(name, mode=mode, **kwargs)

        # Initialize class
        _io.BufferedIOBase.__init__(self)
        ObjectIOBase.__init__(self, name, mode=mode)

        # Initialize parallel processing
        self._workers_pool = None
        self._workers_count = max_workers
        self._workers_type = workers_type

        # Initialize write mode
        if self._writable:
            self._buffer_seek = 0
            self._buffer_size = buffer_size or self.DEFAULT_BUFFER_SIZE
            self._write_buffer = bytearray(self._buffer_size)
            self._seekable = False

    def close(self):
        """
        Flush the write buffers of the stream if applicable and
        close the object.
        """
        if self._writable:
            with self._seek_lock:
                self._flush()
                self._close_writable()

    @_abstractmethod
    def _close_writable(self):
        """
        Close the object in write mode.
        """

    def flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """
        if self._writable:
            with self._seek_lock:
                self._flush()

    @_abstractmethod
    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """

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

    def getsize(self):
        """
        Return the size, in bytes, of path.

        Returns:
            int: Size in bytes.

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        return self.raw.getsize()

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

    @property
    def raw(self):
        """
        The underlying raw stream

        Returns:
            ObjectRawIOBase subclass: Raw stream.
        """
        return self._raw

    def read(self, size=-1):
        """
        Read and return up to size bytes,
        with at most one call to the underlying raw stream’s.

        Use at most one call to the underlying raw stream’s read method.

        Args:
            size (int): Number of bytes to read. -1 to read the
                stream until end.
        """
        if not self._readable:
            raise _io.UnsupportedOperation('read')

        # TODO: Implementation

    def read1(self, size=-1):
        """
        Read and return up to size bytes,
        with at most one call to the underlying raw stream’s.

        Use at most one call to the underlying raw stream’s read method.

        Args:
            size (int): Number of bytes to read. -1 to read the
                stream until end.
        """
        return self._raw.read(size)

    def readinto(self, b):
        """
        Read bytes into a pre-allocated, writable bytes-like object b,
        and return the number of bytes read.

        Args:
            b (bytes-like object): buffer.

        Returns:
            int: number of bytes read
        """
        # TODO: Implementation

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

    def tell(self):
        """Return the current stream position.

        Returns:
            int: Stream position."""
        with self._seek_lock:
            if self._writable:
                # Seek is number the number of buffer written
                # So current position in bytes is:
                # number_of_written_buffer * buffer_size + current_buffer_seek
                return self._seek * self._buffer_size + self._buffer_seek
            return self._seek

    @property
    def _workers(self):
        """Executor pool

        Returns:
            concurrent.futures.Executor: Executor pool"""
        # Lazy instantiate workers pool on first call
        if self._workers_pool is None:
            self._workers_pool = (
                _futures.ThreadPoolExecutor if self._workers_type == 'thread'
                else _futures.ProcessPoolExecutor)(
                max_worker=self._workers_count)

        # Get worker pool
        return self._workers_pool

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
            raise _io.UnsupportedOperation('write')

        # TODO: Implementation
