"""Cloud storage abstract IO classes with random write support"""
from abc import abstractmethod
from functools import partial
from io import UnsupportedOperation
from os import SEEK_SET
from time import sleep

from airfs._core.io_base_raw import ObjectRawIOBase
from airfs._core.io_base_buffered import ObjectBufferedIOBase
from airfs._core.exceptions import handle_os_exceptions, ObjectNotFoundError


class ObjectRawIORandomWriteBase(ObjectRawIOBase):
    """
    Base class for binary storage object I/O that support flushing parts of file
    instead of requiring flushing the full file at once.
    """

    def _init_append(self):
        """
        Initializes file on 'a' mode.
        """
        self._seek = self._size

    def flush(self):
        """
        Flush the write buffers of the stream if applicable and save the object on the
        storage.
        """
        if self._writable:
            with self._seek_lock:
                buffer = self._get_buffer()

                end = self._seek
                start = end - len(buffer)

                self._write_buffer = bytearray()

            with handle_os_exceptions():
                self._flush(buffer, start, end)

    @abstractmethod
    def _flush(self, buffer, start, end):
        """
        Flush the write buffers of the stream if applicable.

        Args:
            buffer (memoryview): Buffer content.
            start (int): Start of buffer position to flush.
                Supported only if random write supported.
            end (int): End of buffer position to flush.
                Supported only if random write supported.
        """

    def _create(self):
        """
        Create the file if not exists.
        """
        self._flush(memoryview(b""), 0, 0)

    def seek(self, offset, whence=SEEK_SET):
        """
        Change the stream position to the given byte offset.

        Args:
            offset (int): Offset is interpreted relative to the position indicated by
                whence.
            whence (int): The default value for whence is SEEK_SET. Values are:
                SEEK_SET or 0 – start of the stream (the default);
                offset should be zero or positive
                SEEK_CUR or 1 – current stream position; offset may be negative
                SEEK_END or 2 – end of the stream; offset is usually negative

        Returns:
            int: The new absolute position.
        """
        if not self._seekable:
            raise UnsupportedOperation("seek")

        self.flush()

        return self._update_seek(offset, whence)


class ObjectBufferedIORandomWriteBase(ObjectBufferedIOBase):
    """
    Buffered base class for binary storage object I/O that support flushing parts
    of file instead of requiring flushing the full file at once.
    """

    # Need to be flagged because it is not an abstract class
    __DEFAULT_CLASS = False

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.

        In write mode, send the buffer content to the storage object.
        """
        buffer = self._get_buffer()
        start = self._buffer_size * (self._seek - 1)
        end = start + len(buffer)

        future = self._workers.submit(
            self._flush_range, buffer=buffer, start=start, end=end
        )
        self._write_futures.append(future)
        future.add_done_callback(partial(self._update_size, end))

    def _update_size(self, size, future):
        """
        Keep track of the file size during writing.

        If specified size value is greater than the current size, update the current
        size using specified value.

        Used as callback in default "_flush" implementation for files supporting random
        write access.

        Args:
            size (int): Size value.
            future (concurrent.futures._base.Future): future.
        """
        with self._size_lock:
            if size > self._size and future.done:
                # Size can be lower if seek down on an 'a' mode open file.
                self._size = size

    def _flush_range(self, buffer, start, end):
        """
        Flush a buffer to a range of the file.

        Meant to be used asynchronously, used to provides parallel flushing of file
        parts when applicable.

        Args:
            buffer (memoryview): Buffer content.
            start (int): Start of buffer position to flush.
            end (int): End of buffer position to flush.
        """
        with self._size_lock:
            if not self._size_synched:
                self._size_synched = True
                try:
                    self._size = self.raw._size
                except (ObjectNotFoundError, UnsupportedOperation):
                    self._size = 0

        while start > self._size:
            sleep(self._FLUSH_WAIT)

        self._raw_flush(buffer, start, end)
