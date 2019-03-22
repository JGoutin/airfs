# coding=utf-8
"""Microsoft Azure Blobs Storage: Pages blobs"""
from __future__ import absolute_import  # Python 2: Fix azure import

from os import SEEK_SET, SEEK_END
from threading import Lock as _Lock

from azure.storage.blob import PageBlobService
from azure.storage.blob.models import _BlobTypes

from pycosio.storage.azure import _handle_azure_exception
from pycosio._core.io_base import memoizedmethod as _memoizedmethod
from pycosio.io import ObjectBufferedIOBase
from pycosio.storage.azure_blob._base_blob import (
    AzureBlobRawIO, AzureBlobBufferedIO, AZURE_RAW, AZURE_BUFFERED)

_BLOB_TYPE = _BlobTypes.PageBlob
_MAX_PAGE_SIZE = PageBlobService.MAX_PAGE_SIZE


class AzurePageBlobRawIO(AzureBlobRawIO):
    """Binary Azure Page Blobs Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more
            information.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
        content_length (int): Define the size to preallocate on new file
            creation. This is not mandatory, and file will be resized on needs
            but this allow to improve performance when file size is known in
            advance. Any value will be rounded to be page aligned. Default to 0.
        null_strip (bool): If True, strip null chars from end of read data to
            remove page padding when reading, and ignore trailing null chars
            on last page when seeking from end. Default to True.
    """
    _SUPPORT_PART_FLUSH = True
    _DEFAULT_CLASS = False

    def __init__(self, *args, **kwargs):
        AzureBlobRawIO.__init__(self, *args, **kwargs)

        self._null_strip = kwargs.get('null_strip', True)

        if self._writable:

            # Create lock for resizing
            self._size_lock = _Lock()

            # If a content length is provided, allocate pages for this blob
            content_length = kwargs.get('content_length', 0)
            if content_length:

                if content_length % 512:
                    # Must be page aligned
                    content_length += 512 - content_length % 512

                if self._is_new_file:
                    self._create_null_page_blob(content_length)

                # On already existing blob, increase size if needed
                elif self._size < content_length:
                    with _handle_azure_exception():
                        self._client.resize_blob(content_length=content_length,
                                                 **self._client_kwargs)

    def _create_null_page_blob(self, content_length):
        """
        Create a new blob of a specified size containing null pages.

        Args:
            content_length (int): Blob content length.
                Must be 512 bytes aligned.
        """
        if self._is_new_file:
            self._is_new_file = False
            with _handle_azure_exception():
                self._client.create_blob(
                    content_length=content_length, **self._client_kwargs)

    @property
    @_memoizedmethod
    def _client(self):
        """
        Returns client instance.

        Returns:
            client
        """
        return self._system.client[_BLOB_TYPE]

    def _read_range(self, start, end=0, null_strip=None):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position.
                0 To not specify end.

        Returns:
            bytes: number of bytes read
        """
        data = AzureBlobRawIO._read_range(self, start, end)

        if (null_strip is None and self._null_strip) or null_strip:
            # Remove trailing Null chars (Empty page end)
            return data.rstrip(b'\x00')

        return data

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        data = AzureBlobRawIO._readall(self)
        if self._null_strip:
            # Remove trailing Null chars (Empty page end)
            return data.rstrip(b'\x00')
        return data

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
        seek = AzureBlobRawIO.seek(self, offset, whence)

        # In case of end seek, adjust to real blob end and not padded page end
        if whence == SEEK_END and self._null_strip:
            # Read last page
            page_end = seek - offset
            last_page = memoryview(self._read_range(
                page_end - 512, page_end - 1, null_strip=True))[:offset]

            # Move seek to last not null byte
            self._seek = seek = page_end - 512 + len(last_page)

        return seek

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
            # Buffer must be aligned on pages
            end_page_diff = end % 512
            start_page_diff = start % 512
            if end_page_diff or start_page_diff:
                # Create a new aligned buffer
                end_page_diff = 512 - end_page_diff

                end += end_page_diff
                start -= start_page_diff

                unaligned_buffer = buffer
                buffer_size = end - start
                buffer = memoryview(bytearray(buffer_size))

                # If exists, Get aligned range from current file
                if self._exists():
                    buffer[:] = memoryview(self._read_range(
                        start, end, null_strip=False))

                # Update with current buffer
                buffer[start_page_diff:-end_page_diff] = unaligned_buffer

            # Write first buffer and create blob simultaneously
            if start == 0 and self._is_new_file:
                self._is_new_file = False

                if buffer_size > _MAX_PAGE_SIZE:
                    # Can't send more at once, require to perform extra
                    # "update_page" steps
                    initial_buffer = buffer[:_MAX_PAGE_SIZE]
                    buffer = buffer[_MAX_PAGE_SIZE:]
                    start = _MAX_PAGE_SIZE
                else:
                    initial_buffer = buffer

                with _handle_azure_exception():
                    self._client.create_blob_from_bytes(
                        blob=initial_buffer.tobytes(), **self._client_kwargs)
                self._reset_head()

                # No more data to flush
                if not start:
                    return

            # Write page normally
            with self._size_lock:
                if end > self._size:
                    # Require to resize the blob if note enough space
                    with _handle_azure_exception():
                        self._client.resize_blob(
                            content_length=end, **self._client_kwargs)
                    self._reset_head()

            with _handle_azure_exception():
                self._client.update_page(
                    page=buffer.tobytes(), start_range=start,
                    end_range=end - 1,
                    **self._client_kwargs)

        # Flush a new empty blob
        elif start == 0 and self._is_new_file:
            self._create_null_page_blob(0)


class AzurePageBlobBufferedIO(AzureBlobBufferedIO):
    """Buffered binary Azure Page Blobs Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
            If not 512 bytes aligned, will be round to be page aligned.
        max_buffers (int): The maximum number of buffers to preload in read mode
            or awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more
            information.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
        content_length (int): Define the size to preallocate on new file
            creation. This is not mandatory, and file will be resized on needs
            but this allow to improve performance when file size is known in
            advance. Any value will be rounded to be page aligned. Default to 0.
        null_strip (bool): If True, strip null chars from end of read data to
            remove page padding when reading, and ignore trailing null chars
            on last page when seeking from end. Default to True.
    """
    _RAW_CLASS = AzurePageBlobRawIO
    _DEFAULT_CLASS = False
    MAXIMUM_BUFFER_SIZE = _MAX_PAGE_SIZE

    def __init__(self, *args, **kwargs):
        ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        if self._writable:
            page_diff = self._buffer_size % 512
            if page_diff:
                # Round buffer size if not multiple of page size
                self._buffer_size = min(
                    self._buffer_size + 512 - page_diff,
                    self.MAXIMUM_BUFFER_SIZE)

            # Initialize a blob with size equal one buffer,
            # if not already existing.
            self._raw._create_null_page_blob(self._buffer_size)

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.

        In write mode, send the buffer content to the cloud object.
        """
        buffer = self._get_buffer()
        start = self._buffer_size * (self._seek - 1)

        self._write_futures.append(self._workers.submit(
            self._raw_flush, buffer=buffer, start=start,
            end=start + len(buffer)))


AZURE_RAW[_BLOB_TYPE] = AzurePageBlobRawIO
AZURE_BUFFERED[_BLOB_TYPE] = AzurePageBlobBufferedIO
