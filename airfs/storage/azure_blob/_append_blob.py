"""Microsoft Azure Blobs Storage: Append blobs"""
from azure.storage.blob.models import _BlobTypes  # type: ignore
from azure.storage.blob import AppendBlobService  # type: ignore

from airfs.storage.azure import _handle_azure_exception
from airfs._core.io_base import memoizedmethod
from airfs.io import ObjectBufferedIORandomWriteBase, ObjectRawIORandomWriteBase
from airfs.storage.azure_blob._base_blob import (
    AzureBlobRawIO,
    AzureBlobBufferedIO,
    AZURE_RAW,
    AZURE_BUFFERED,
)

_BLOB_TYPE = _BlobTypes.AppendBlob


class AzureAppendBlobRawIO(AzureBlobRawIO, ObjectRawIORandomWriteBase):
    """Binary Azure Append Blobs Storage Object I/O

    This blob type is not seekable in write mode.

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a' for reading (default), writing or
            appending.
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    __DEFAULT_CLASS = False

    #: Maximum size of one flush operation
    MAX_FLUSH_SIZE = AppendBlobService.MAX_BLOCK_SIZE

    def __init__(self, *args, **kwargs):
        AzureBlobRawIO.__init__(self, *args, **kwargs)

        if self._writable:
            self._seekable = False

    def _create(self):
        """
        Create the file if not exists.
        """
        with _handle_azure_exception():
            self._client.create_blob(**self._client_kwargs)

    @property  # type: ignore
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
        buffer_size = len(buffer)

        if buffer_size > self.MAX_FLUSH_SIZE:
            for part_start in range(0, buffer_size, self.MAX_FLUSH_SIZE):

                buffer_part = buffer[part_start : part_start + self.MAX_FLUSH_SIZE]

                with _handle_azure_exception():
                    self._client.append_block(
                        block=buffer_part.tobytes(), **self._client_kwargs
                    )

        elif buffer_size:
            with _handle_azure_exception():
                self._client.append_block(block=buffer.tobytes(), **self._client_kwargs)


class AzureAppendBlobBufferedIO(AzureBlobBufferedIO, ObjectBufferedIORandomWriteBase):
    """Buffered binary Azure Append Blobs Storage Object I/O

    This blob type is not seekable in write mode.

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode
            or awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to execute the
            given calls.
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    __DEFAULT_CLASS = False
    _RAW_CLASS = AzureAppendBlobRawIO

    def __init__(self, *args, **kwargs):
        ObjectBufferedIORandomWriteBase.__init__(self, *args, **kwargs)

        if self._writable:
            # Can't upload in parallel, but can still upload sequentially as a
            # background task
            self._workers_count = 1

    def _flush(self):
        """
        Flush the write buffer of the stream.
        """
        self._write_futures.append(
            self._workers.submit(
                self._client.append_block,
                block=self._get_buffer().tobytes(),
                **self._client_kwargs
            )
        )


AZURE_RAW[_BLOB_TYPE] = AzureAppendBlobRawIO
AZURE_BUFFERED[_BLOB_TYPE] = AzureAppendBlobBufferedIO
