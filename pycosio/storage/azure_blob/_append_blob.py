# coding=utf-8
"""Microsoft Azure Blobs Storage: Append blobs"""
from __future__ import absolute_import  # Python 2: Fix azure import

from azure.storage.blob.models import _BlobTypes

from pycosio.storage.azure import _handle_azure_exception
from pycosio._core.io_base import memoizedmethod
from pycosio.io import (
    ObjectBufferedIORandomWriteBase, ObjectRawIORandomWriteBase)
from pycosio.storage.azure_blob._base_blob import (
    AzureBlobRawIO, AzureBlobBufferedIO, AZURE_RAW, AZURE_BUFFERED)

_BLOB_TYPE = _BlobTypes.AppendBlob


class AzureAppendBlobRawIO(AzureBlobRawIO, ObjectRawIORandomWriteBase):
    """Binary Azure Append Blobs Storage Object I/O

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
    """
    _SUPPORT_PART_FLUSH = True
    _DEFAULT_CLASS = False

    def __init__(self, *args, **kwargs):
        AzureBlobRawIO.__init__(self, *args, **kwargs)

        if self._writable:
            # Not seekable in append mode
            self._seekable = False

            # Creates blob on write mode
            if self._is_new_file:
                with _handle_azure_exception():
                    self._client.create_blob(**self._client_kwargs)

    @property
    @memoizedmethod
    def _client(self):
        """
        Returns client instance.

        Returns:
            client
        """
        return self._system.client[_BLOB_TYPE]

    def _flush(self, buffer, *_):
        """
        Flush the write buffer of the stream if applicable.

        Args:
            buffer (memoryview): Buffer content.
        """
        with _handle_azure_exception():
            # Append mode: Append block at file end
            # Can't append an empty buffer
            if len(buffer):
                self._client.append_block(
                    block=buffer.tobytes(), **self._client_kwargs)


class AzureAppendBlobBufferedIO(AzureBlobBufferedIO,
                                ObjectBufferedIORandomWriteBase):
    """Buffered binary Azure Append Blobs Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
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
    """
    _RAW_CLASS = AzureAppendBlobRawIO
    _DEFAULT_CLASS = False

    def __init__(self, *args, **kwargs):
        ObjectBufferedIORandomWriteBase.__init__(self, *args, **kwargs)

        if self._writable:
            # Can't upload in parallel, always add data at end.
            self._workers_count = 1

    def _flush(self):
        """
        Flush the write buffer of the stream.
        """
        self._write_futures.append(self._workers.submit(
            self._client.append_block, block=self._get_buffer().tobytes(),
            **self._client_kwargs))


AZURE_RAW[_BLOB_TYPE] = AzureAppendBlobRawIO
AZURE_BUFFERED[_BLOB_TYPE] = AzureAppendBlobBufferedIO
