"""Cloud storage abstract Raw IO class"""
from abc import abstractmethod
from io import RawIOBase, UnsupportedOperation
from os import SEEK_CUR, SEEK_END, SEEK_SET

from airfs._core.exceptions import (
    ObjectNotFoundError,
    ObjectPermissionError,
    handle_os_exceptions,
)
from airfs._core.io_base import ObjectIOBase, memoizedmethod
from airfs._core.io_base_system import SystemBase


class ObjectRawIOBase(RawIOBase, ObjectIOBase):
    """Base class for binary storage object I/O.

    In write mode, this class needs enough memory to store the entire object to write.
    In append mode, the storage object is read and stored in memory on instantiation.
    For big objects use ObjectBufferedIOBase that can performs operations with less
    memory.

    In read mode, this class random access to the storage object and require only the
    accessed data size in memory.

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a', 'x' for reading (default), writing,
            appending or creation.
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    __slots__ = (
        "_system",
        "_client_kwargs",
        "_is_raw_of_buffered",
        "_write_buffer",
    )

    # System I/O class
    _SYSTEM_CLASS = SystemBase

    #: Maximum size of one flush operation (0 for no limit)
    MAX_FLUSH_SIZE = 0

    def __init__(self, name, mode="r", storage_parameters=None, **kwargs):

        RawIOBase.__init__(self)
        ObjectIOBase.__init__(self, name, mode=mode)

        if storage_parameters is not None:
            storage_parameters = storage_parameters.copy()

        try:
            self._cache["_head"] = storage_parameters.pop("airfs.raw_io._head")
        except (AttributeError, KeyError):
            pass

        try:
            self._system = storage_parameters.pop("airfs.system_cached")
        except (AttributeError, KeyError):
            self._system = None

        if not self._system:
            self._system = self._SYSTEM_CLASS(
                storage_parameters=storage_parameters, **kwargs
            )

        self._client_kwargs = self._system.get_client_kwargs(name)

        self._is_raw_of_buffered = False

        if self._writable:
            self._write_buffer = bytearray()

            if "a" in mode:
                if self._exists() == 1:
                    with handle_os_exceptions():
                        self._init_append()

                elif self._exists() == 0:
                    with handle_os_exceptions():
                        self._create()

                else:
                    raise PermissionError(
                        "Insufficient permission to check if file already exists."
                    )

            elif "x" in mode and self._exists() == 1:
                raise FileExistsError

            elif "x" in mode and self._exists() == -1:
                raise PermissionError(
                    "Insufficient permission to check if file already exists."
                )

            else:
                with handle_os_exceptions():
                    self._create()

        else:
            with handle_os_exceptions():
                self._head()

    def _init_append(self):
        """
        Initializes file on 'a' mode.
        """
        self._write_buffer[:] = self._readall()
        self._seek = self._size

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
        Flush the write buffers of the stream if applicable and close the object.
        """
        if self._writable and not self._is_raw_of_buffered and not self._closed:
            self._closed = True
            if self._write_buffer:
                self.flush()

    def flush(self):
        """
        Flush the write buffers of the stream if applicable and save the object on the
        storage.
        """
        if self._writable:
            with handle_os_exceptions():
                self._flush(self._get_buffer())

    @abstractmethod
    def _flush(self, buffer):
        """
        Flush the write buffers of the stream if applicable.

        Args:
            buffer (memoryview): Buffer content.
        """

    def _create(self):
        """
        Create the file if not exists.
        """
        self._flush(memoryview(b""))

    def _get_buffer(self):
        """
        Get a memory view of the current write buffer until its seek value.

        Returns:
            memoryview: buffer view.
        """
        return memoryview(self._write_buffer)

    @property  # type: ignore
    @memoizedmethod
    def _size(self):
        """
        Return the size, in bytes, of path.

        Returns:
            int: Size in bytes.
        """
        return self._system.getsize(header=self._head())

    def _reset_head(self):
        """
        Reset memoized head and associated values.
        """
        for key in ("_size", "_head"):
            try:
                del self._cache[key]
            except KeyError:
                continue

    @memoizedmethod
    def _head(self):
        """
        Return file header.

        Returns:
            dict: header.
        """
        return self._system.head(client_kwargs=self._client_kwargs)

    @memoizedmethod
    def _exists(self):
        """
        Checks if file exists.

        Returns:
            int: 1 if exists, 0 if not exists, -1 if can't determine file existence
            (Because no access permission)
        """
        try:
            self._head()
            return 1
        except ObjectNotFoundError:
            return 0
        except ObjectPermissionError:
            return -1

    @staticmethod
    def _http_range(start=0, end=0):
        """
        Returns an HTTP Range request for a specified python range.

        Args:
            start (int): Start of the range.
            end (int): End of the range. 0 To not specify end.

        Returns:
            str: range.
        """
        if end:
            return f"bytes={start}-{end - 1}"
        return f"bytes={start}-"

    def _peek(self, size=-1):
        """
        Return bytes from the stream without advancing the position.

        Args:
            size (int): Number of bytes to read. -1 to read the full stream.

        Returns:
            bytes: bytes read
        """
        with self._seek_lock:
            seek = self._seek
        with handle_os_exceptions():
            return self._read_range(seek, seek + size)

    def readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        if not self._readable:
            raise UnsupportedOperation("read")

        with self._seek_lock:
            with handle_os_exceptions():
                if self._seek and self._seekable:
                    data = self._read_range(self._seek)

                else:
                    data = self._readall()

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
        Read bytes into a pre-allocated, writable bytes-like object b, and return the
        number of bytes read.

        Args:
            b (bytes-like object): buffer.

        Returns:
            int: number of bytes read
        """
        if not self._readable:
            raise UnsupportedOperation("read")

        size = len(b)
        with self._seek_lock:
            start = self._seek
            end = start + size
            self._seek = end

        with handle_os_exceptions():
            read_data = self._read_range(start, end)

        read_size = len(read_data)
        if read_size:
            memoryview(b)[:read_size] = read_data

        if read_size != size:
            with self._seek_lock:
                self._seek = start + read_size

        return read_size

    @abstractmethod
    def _read_range(self, start, end=0):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position. 0 To not specify end.

        Returns:
            bytes: number of bytes read
        """

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

        seek = self._update_seek(offset, whence)

        if self._writable:
            size = len(self._write_buffer)
            if seek > size:
                self._write_buffer[seek:size] = b"\0" * (seek - size)

        return seek

    def _update_seek(self, offset, whence):
        """
        Update seek value.

        Args:
            offset (int): Offset.
            whence (int): Whence.

        Returns:
            int: Seek position.
        """
        with self._seek_lock:
            if whence == SEEK_SET:
                self._seek = offset
            elif whence == SEEK_CUR:
                self._seek += offset
            elif whence == SEEK_END:
                self._seek = offset + self._size
            else:
                raise ValueError(f"whence value {whence} unsupported")
        return self._seek

    def write(self, b):
        """
        Write the given bytes-like object, b, to the underlying raw stream, and return
        the number of bytes written.

        Args:
            b (bytes-like object): Bytes to write.

        Returns:
            int: The number of bytes written.
        """
        if not self._writable:
            raise UnsupportedOperation("write")

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
