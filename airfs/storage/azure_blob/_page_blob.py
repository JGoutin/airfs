"""Microsoft Azure Blobs Storage: Pages blobs"""
from os import SEEK_SET, SEEK_END

from azure.storage.blob import PageBlobService  # type: ignore
from azure.storage.blob.models import _BlobTypes  # type: ignore

from airfs.storage.azure import _AzureStorageRawIORangeWriteBase
from airfs._core.io_base import memoizedmethod
from airfs.io import ObjectBufferedIORandomWriteBase, ObjectRawIORandomWriteBase
from airfs.storage.azure_blob._base_blob import (
    AzureBlobRawIO,
    AzureBlobBufferedIO,
    AZURE_RAW,
    AZURE_BUFFERED,
)

_BLOB_TYPE = _BlobTypes.PageBlob


class AzurePageBlobRawIO(AzureBlobRawIO, _AzureStorageRawIORangeWriteBase):
    """Binary Azure Page Blobs Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a' for reading (default), writing or
            appending.
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
        content_length (int): Define the size to preallocate on new file creation.
            This is not mandatory, and file will be resized on needs but this allow to
            improve performance when file size is known in advance. Any value will be
            rounded to be page aligned. Default to 0.
        ignore_padding (bool): If True, strip null chars padding from end of
            read data and ignore padding when seeking from end (whence=os.SEEK_END).
            Default to True.
    """

    __slots__ = ("_ignore_padding",)

    __DEFAULT_CLASS = False

    #: Maximum size of one flush operation
    MAX_FLUSH_SIZE = PageBlobService.MAX_PAGE_SIZE

    def __init__(self, *args, **kwargs):
        self._ignore_padding = kwargs.get("ignore_padding", True)
        _AzureStorageRawIORangeWriteBase.__init__(self, *args, **kwargs)

    @property  # type: ignore
    @memoizedmethod
    def _client(self):
        """
        Returns client instance.

        Returns:
            client
        """
        return self._system.client[_BLOB_TYPE]

    @property  # type: ignore
    @memoizedmethod
    def _resize(self):
        """
        Azure storage function that resize an object.

        Returns:
            function: Resize function.
        """
        return self._client.resize_blob

    def _init_append(self):
        """
        Initializes file on 'a' mode.
        """
        self._align_page()
        _AzureStorageRawIORangeWriteBase._init_append(self)

        if self._ignore_padding:
            self._seek = self._seek_end_ignore_padding()

    def _align_page(self):
        """
        Ensure content length is page aligned.
        """
        if self._content_length % 512:
            self._content_length += 512 - self._content_length % 512

    def _create(self):
        """
        Create the file if not exists.
        """
        self._align_page()
        _AzureStorageRawIORangeWriteBase._create(self)

    @property  # type: ignore
    @memoizedmethod
    def _create_from_size(self):
        """
        Azure storage function that create an object.

        Returns:
            function: Create function.
        """
        return self._client.create_blob

    def _update_range(self, data, **kwargs):
        """
        Update range with data

        Args:
            data (bytes): data.
        """
        self._client.update_page(page=data, **kwargs)

    def _read_range(self, start, end=0, null_strip=None):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position. 0 To not specify end.

        Returns:
            bytes: number of bytes read
        """
        data = AzureBlobRawIO._read_range(self, start, end)

        if (null_strip is None and self._ignore_padding) or null_strip:
            return data.rstrip(b"\0")

        return data

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        data = AzureBlobRawIO._readall(self)
        if self._ignore_padding:
            return data.rstrip(b"\0")
        return data

    def seek(self, offset, whence=SEEK_SET):
        """
        Change the stream position to the given byte offset.

        Args:
            offset: Offset is interpreted relative to the position indicated by whence.
            whence: The default value for whence is SEEK_SET. Values are:
                SEEK_SET or 0 – start of the stream (the default);
                offset should be zero or positive
                SEEK_CUR or 1 – current stream position; offset may be negative
                SEEK_END or 2 – end of the stream; offset is usually negative

        Returns:
            int: The new absolute position.
        """
        if self._ignore_padding and whence == SEEK_END:
            offset = self._seek_end_ignore_padding(offset)
            whence = SEEK_SET

        return ObjectRawIORandomWriteBase.seek(self, offset, whence)

    def _seek_end_ignore_padding(self, offset=0):
        """
        Compute seek position if seeking from end ignoring null padding.

        Args:
            offset (int): relative position to seek.

        Returns:
            int: New seek value.
        """
        page_end = self._size
        page_seek = page_end + min(offset, 0)
        page_start = page_seek - (page_seek % 512 or 512)
        last_pages = self._read_range(page_start, page_end, null_strip=True)

        return page_start + len(last_pages) + offset

    def _flush(self, buffer, start, end):
        """
        Flush the write buffer of the stream if applicable.

        Args:
            buffer (memoryview): Buffer content.
            start (int): Start of buffer position to flush.
                Supported only with page blobs.
            end (int): End of buffer position to flush.
                Supported only with page blobs.
        """
        buffer_size = len(buffer)

        if buffer_size:
            end_page_diff = end % 512
            start_page_diff = start % 512
            if end_page_diff or start_page_diff:
                end_page_diff = 512 - end_page_diff

                end += end_page_diff
                start -= start_page_diff

                unaligned_buffer = buffer
                buffer_size = end - start
                buffer = memoryview(bytearray(buffer_size))

                if self._exists() == 1 and start < self._size:
                    buffer[:] = memoryview(
                        self._read_range(start, end, null_strip=False)
                    )

                buffer[start_page_diff:-end_page_diff] = unaligned_buffer

        _AzureStorageRawIORangeWriteBase._flush(self, buffer, start, end)


class AzurePageBlobBufferedIO(AzureBlobBufferedIO, ObjectBufferedIORandomWriteBase):
    """Buffered binary Azure Page Blobs Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer. If not 512 bytes aligned, will be round
            to be page aligned.
        max_buffers (int): The maximum number of buffers to preload in read mode or
            awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to execute the
            given calls.
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
        content_length (int): Define the size to preallocate on new file creation. This
            is not mandatory, and file will be resized on needs but this allow to
            improve performance when file size is known in advance. Any value will be
            rounded to be page aligned. Default to 0.
        null_strip (bool): If True, strip null chars from end of read data to remove
            page padding when reading, and ignore trailing null chars on last page when
            seeking from end. Default to True.
    """

    __DEFAULT_CLASS = False
    _RAW_CLASS = AzurePageBlobRawIO

    #: Maximal buffer_size value in bytes (Maximum upload page size)
    MAXIMUM_BUFFER_SIZE = PageBlobService.MAX_PAGE_SIZE

    #: Minimal buffer_size value in bytes (Page size)
    MINIMUM_BUFFER_SIZE = 512

    def __init__(self, *args, **kwargs):
        ObjectBufferedIORandomWriteBase.__init__(self, *args, **kwargs)

        if self._writable:
            page_diff = self._buffer_size % 512
            if page_diff:
                self._buffer_size = min(
                    self._buffer_size + 512 - page_diff, self.MAXIMUM_BUFFER_SIZE
                )

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.

        In write mode, send the buffer content to the storage object.
        """
        buffer = self._get_buffer()
        start = self._buffer_size * (self._seek - 1)

        self._write_futures.append(
            self._workers.submit(
                self._raw_flush, buffer=buffer, start=start, end=start + len(buffer)
            )
        )


AZURE_RAW[_BLOB_TYPE] = AzurePageBlobRawIO
AZURE_BUFFERED[_BLOB_TYPE] = AzurePageBlobBufferedIO
