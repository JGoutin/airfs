# coding=utf-8
"""Cloud storage abstract buffered IO class"""
from abc import abstractmethod
from concurrent.futures import as_completed
from io import BufferedIOBase, UnsupportedOperation
from math import ceil
from os import SEEK_SET
from threading import Lock
from time import sleep

from airfs._core.io_base import ObjectIOBase, WorkerPoolBase
from airfs._core.io_base_raw import ObjectRawIOBase
from airfs._core.exceptions import handle_os_exceptions


class ObjectBufferedIOBase(BufferedIOBase, ObjectIOBase, WorkerPoolBase):
    """
    Base class for buffered binary cloud storage object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r' (default), 'w'.
            for reading (default) or writing
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode
            or awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    __slots__ = ('_raw', '_client_kwargs', '_buffer_size', '_max_buffers',
                 '_buffer_seek', '_raw_flush', '_size_synched', '_size',
                 '_size_lock', '_read_range', '_read_queue')

    # Raw I/O class
    _RAW_CLASS = ObjectRawIOBase

    #: Default buffer_size value in bytes (Default to 8MB)
    DEFAULT_BUFFER_SIZE = 8388608

    #: Minimal buffer_size value in bytes
    MINIMUM_BUFFER_SIZE = 1

    #: Maximum buffer_size value in bytes (0 for no limit)
    MAXIMUM_BUFFER_SIZE = 0

    # Time to wait before try a new flush
    # if number of buffer currently in flush > max_buffer
    _FLUSH_WAIT = 0.01

    def __init__(self, name, mode='r', buffer_size=None,
                 max_buffers=0, max_workers=None, **kwargs):

        if 'a' in mode:
            # TODO: Implement append mode and remove this exception
            raise NotImplementedError(
                'Not implemented yet in airfs')

        BufferedIOBase.__init__(self)
        ObjectIOBase.__init__(self, name, mode=mode)
        WorkerPoolBase.__init__(self, max_workers)

        # Instantiate raw IO
        self._raw = self._RAW_CLASS(
            name, mode=mode, **kwargs)
        self._raw._is_raw_of_buffered = True

        # Link to RAW methods
        self._mode = self._raw.mode
        self._name = self._raw.name
        self._client_kwargs = self._raw._client_kwargs

        # Initializes buffer
        if not buffer_size or buffer_size < 0:
            self._buffer_size = self.DEFAULT_BUFFER_SIZE
        elif buffer_size < self.MINIMUM_BUFFER_SIZE:
            self._buffer_size = self.MINIMUM_BUFFER_SIZE
        elif (self.MAXIMUM_BUFFER_SIZE and
              buffer_size > self.MAXIMUM_BUFFER_SIZE):
            self._buffer_size = self.MAXIMUM_BUFFER_SIZE
        else:
            self._buffer_size = buffer_size

        # Initialize write mode
        if self._writable:
            self._max_buffers = max_buffers
            self._buffer_seek = 0
            self._write_buffer = bytearray(self._buffer_size)
            self._seekable = False
            self._write_futures = []
            self._raw_flush = self._raw._flush

            # Size used only with random write access
            # Value will be lazy evaluated latter if needed.
            self._size_synched = False
            self._size = 0
            self._size_lock = Lock()

        # Initialize read mode
        else:
            self._size = self._raw._size
            self._read_range = self.raw._read_range
            if max_buffers:
                self._max_buffers = max_buffers
            else:
                self._max_buffers = ceil(self._size / self._buffer_size)
            self._read_queue = dict()

    @property
    def _client(self):
        """
        Returns client instance.

        Returns:
            client
        """
        return self._raw._client

    def close(self):
        """
        Flush the write buffers of the stream if applicable and
        close the object.
        """
        if self._writable and not self._closed:
            self._closed = True
            with self._seek_lock:
                self._flush_raw_or_buffered()
            if self._seek:
                with handle_os_exceptions():
                    self._close_writable()

    def _close_writable(self):
        """
        Closes the object in write mode.

        Performs any finalization operation required to
        complete the object writing on the cloud.
        """
        # Default implementation only wait for tasks termination
        for future in as_completed(self._write_futures):
            future.result()

    def flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """
        if self._writable:
            with self._seek_lock:
                self._flush_raw_or_buffered()

                # Clear the buffer
                self._write_buffer = bytearray(self._buffer_size)
                self._buffer_seek = 0

    def _flush_raw_or_buffered(self):
        """
        Flush using raw of buffered methods.
        """
        # Flush only if bytes written
        # This avoid no required process/thread
        # creation and network call.
        # This step is performed by raw stream.
        if self._buffer_seek and self._seek:
            self._seek += 1
            with handle_os_exceptions():
                self._flush()

        # If data lower than buffer size
        # flush data with raw stream to reduce IO calls
        elif self._buffer_seek:
            self._raw._write_buffer = self._get_buffer()
            self._raw._seek = self._buffer_seek
            self._raw.flush()

    @abstractmethod
    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.

        In write mode, send the buffer content to the cloud object.
        """

    def _get_buffer(self):
        """
        Get a memory view of the current write buffer
        until its seek value.

        Returns:
            memoryview: buffer view.
        """
        return memoryview(self._write_buffer)[:self._buffer_seek]

    def peek(self, size=-1):
        """
        Return bytes from the stream without advancing the position.

        Args:
            size (int): Number of bytes to read. -1 to read the full
                stream.

        Returns:
            bytes: bytes read
        """
        if not self._readable:
            raise UnsupportedOperation('read')

        with self._seek_lock:
            self._raw.seek(self._seek)
            return self._raw._peek(size)

    def _preload_range(self):
        """Preload data for reading"""
        queue = self._read_queue
        size = self._buffer_size
        start = self._seek
        end = int(start + size * self._max_buffers)
        workers_submit = self._workers.submit
        indexes = tuple(range(start, end, size))

        # Drops buffer out of current range
        for seek in tuple(queue):
            if seek not in indexes:
                del queue[seek]

        # Launch buffer preloading for current range
        read_range = self._read_range
        for seek in indexes:
            if seek not in queue:
                queue[seek] = workers_submit(read_range, seek, seek + size)

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

        Returns:
            bytes: Object content
        """
        if not self._readable:
            raise UnsupportedOperation('read')

        # Checks if EOF
        if self._seek == self._size:
            return b''

        # Returns existing buffer with no copy
        if size == self._buffer_size:
            queue_index = self._seek

            # Starts initial preloading on first call
            if queue_index == 0:
                self._preload_range()

            # Get buffer from future
            with handle_os_exceptions():
                buffer = self._read_queue.pop(queue_index).result()

            # Append another buffer preload at end of queue
            buffer_size = self._buffer_size
            index = queue_index + buffer_size * self._max_buffers
            if index < self._size:
                self._read_queue[index] = self._workers.submit(
                    self._read_range, index, index + buffer_size)

            # Update seek and return buffer
            self._seek += len(buffer)
            return buffer

        # Uses a prealocated buffer
        if size != -1:
            buffer = bytearray(size)

        # Uses a mutable buffer
        else:
            buffer = bytearray()

        read_size = self.readinto(buffer)
        return memoryview(buffer)[:read_size].tobytes()

    def read1(self, size=-1):
        """
        Read and return up to size bytes,
        with at most one call to the underlying raw stream’s.

        Use at most one call to the underlying raw stream’s read method.

        Args:
            size (int): Number of bytes to read. -1 to read the
                stream until end.

        Returns:
            bytes: Object content
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
        if not self._readable:
            raise UnsupportedOperation('read')

        with self._seek_lock:
            # Gets seek
            seek = self._seek

            # Initializes queue
            queue = self._read_queue
            if seek == 0:
                # Starts initial preloading on first call
                self._preload_range()

            # Initializes read data buffer
            size = len(b)
            if size:
                # Preallocated buffer:
                # Use memory view to avoid copies
                b_view = memoryview(b)
                size_left = size
            else:
                # Dynamic buffer:
                # Can't avoid copy, read until EOF
                b_view = b
                size_left = -1
            b_end = 0

            # Starts reading
            buffer_size = self._buffer_size
            while size_left > 0 or size_left == -1:

                # Finds buffer position in queue and buffer seek
                start = seek % buffer_size
                queue_index = seek - start

                # Gets preloaded buffer
                try:
                    buffer = queue[queue_index]
                except KeyError:
                    # EOF
                    break

                # Get buffer from future
                with handle_os_exceptions():
                    try:
                        queue[queue_index] = buffer = buffer.result()

                    # Already evaluated
                    except AttributeError:
                        pass
                buffer_view = memoryview(buffer)
                data_size = len(buffer)

                # Checks if end of file reached
                if not data_size:
                    break

                # Gets theoretical range to copy
                if size_left != -1:
                    end = start + size_left
                else:
                    end = data_size - start

                # Checks for end of buffer
                if end >= data_size:
                    # Adjusts range to copy
                    end = data_size

                    # Removes consumed buffer from queue
                    del queue[queue_index]

                    # Append another buffer preload at end of queue
                    index = queue_index + buffer_size * self._max_buffers
                    if index < self._size:
                        queue[index] = self._workers.submit(
                            self._read_range, index, index + buffer_size)

                # Gets read size, updates seek and updates size left
                read_size = end - start
                if size_left != -1:
                    size_left -= read_size
                seek += read_size

                # Defines read buffer range
                b_start = b_end
                b_end = b_start + read_size

                # Copy data from preload buffer to read buffer
                b_view[b_start:b_end] = buffer_view[start:end]

            # Updates seek and sync raw
            self._seek = seek
            self._raw.seek(seek)

        # Returns read size
        return b_end

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

        # Only read mode is seekable
        with self._seek_lock:
            # Set seek using raw method and
            # sync buffered seek with raw seek
            self.raw.seek(offset, whence)
            self._seek = seek = self.raw._seek

            # Preload starting from current seek
            self._preload_range()

        return seek

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

        size = len(b)
        b_view = memoryview(b)
        size_left = size
        buffer_size = self._buffer_size
        max_buffers = self._max_buffers

        with self._seek_lock:
            end = self._buffer_seek
            buffer_view = memoryview(self._write_buffer)

            while size_left > 0:
                # Get range to copy
                start = end
                end = start + size_left

                if end > buffer_size:
                    # End of buffer, need flush after copy
                    end = buffer_size
                    flush = True
                else:
                    flush = False

                buffer_range = end - start

                # Update not remaining data size
                b_start = size - size_left
                size_left -= buffer_range

                # Copy data
                buffer_view[start:end] = b_view[b_start: b_start + buffer_range]

                # Flush buffer if needed
                if flush:
                    # Update buffer seek
                    # Needed to write the good amount of data
                    self._buffer_seek = end

                    # Update global seek, this is the number
                    # of buffer flushed
                    self._seek += 1

                    # Block flush based on maximum number of
                    # buffers in flush progress
                    if max_buffers:
                        futures = self._write_futures
                        flush_wait = self._FLUSH_WAIT
                        while sum(1 for future in futures
                                  if not future.done()) >= max_buffers:
                            sleep(flush_wait)

                    # Flush
                    with handle_os_exceptions():
                        self._flush()

                    # Clear buffer
                    self._write_buffer = bytearray(buffer_size)
                    buffer_view = memoryview(self._write_buffer)
                    end = 0

            # Update buffer seek
            self._buffer_seek = end
            return size
