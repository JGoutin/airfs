# coding=utf-8
"""Microsoft Azure Blobs Storage"""
from __future__ import absolute_import  # Python 2: Fix azure import

from io import BytesIO as _BytesIO, IOBase as _IOBase
from random import choice as _choice
import re as _re
from string import ascii_lowercase as _ascii_lowercase

from azure.storage.blob import (
    PageBlobService as _PageBlobService,
    BlockBlobService as _BlockBlobService,
    AppendBlobService as _AppendBlobService,
    BlobBlock as _BlobBlock)
from azure.storage.blob.models import _BlobTypes
from azure.common import AzureHttpError as _AzureHttpError

from pycosio.storage.azure import (
    _handle_azure_exception, _update_storage_parameters, _get_time,
    _update_listing_client_kwargs, _get_endpoint, _model_to_dict)
from pycosio._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError)
from pycosio._core.io_base import memoizedmethod as _memoizedmethod
from pycosio.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase)

# Blob types
_APPEND = _BlobTypes.AppendBlob
_BLOCK = _BlobTypes.BlockBlob
_PAGE = _BlobTypes.PageBlob


class _AzureBlobSystem(_SystemBase):
    """
    Azure Blobs Storage system.

    Args:
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more
            information.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    _MTIME_KEYS = ('last_modified',)
    _SIZE_KEYS = ('content_length',)

    def __init__(self, *args, **kwargs):
        self._endpoint = None
        _SystemBase.__init__(self, *args, **kwargs)

    def copy(self, src, dst):
        """
        Copy object of the same storage.

        Args:
            src (str): Path or URL.
            dst (str): Path or URL.
        """
        # Path is relative, require an absolute path.
        if self.relpath(src) == src:
            src = '%s/%s' % (self._endpoint, src)

        with _handle_azure_exception():
            self._client_block.copy_blob(
                copy_source=src, **self.get_client_kwargs(dst))

    def _get_client(self):
        """
        Azure blob service

        Returns:
            dict of azure.storage.blob.baseblobservice.BaseBlobService subclass:
            Service
        """
        parameters = _update_storage_parameters(
            self._storage_parameters, self._unsecure).copy()

        # Parameter added by pycosio and unsupported by blob services.
        try:
            del parameters['blob_type']
        except KeyError:
            pass

        return {_PAGE: _PageBlobService(**parameters),
                _BLOCK: _BlockBlobService(**parameters),
                _APPEND: _AppendBlobService(**parameters)}

    @staticmethod
    def _get_time(header, keys, name):
        """
        Get time from header

        Args:
            header (dict): Object header.
            keys (tuple of str): Header keys.
            name (str): Method name.

        Returns:
            float: The number of seconds since the epoch
        """
        return _get_time(header, keys, name)

    @property
    @_memoizedmethod
    def _client_block(self):
        """
        Storage client

        Returns:
            azure.storage.blob.blockblobservice.BlockBlobService: client
        """
        return self.client[_BLOCK]

    @property
    @_memoizedmethod
    def _default_blob_type(self):
        """
        Return default blob type to use when creating objects.

        Returns:
            str: Blob type.
        """
        return self._storage_parameters.get('blob_type', _BLOCK)

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        container_name, blob_name = self.split_locator(path)
        kwargs = dict(container_name=container_name)

        # Blob
        if blob_name:
            kwargs['blob_name'] = blob_name

        return kwargs

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """
        # URL:
        # - http://<account>.blob.core.windows.net/<container>/<blob>
        # - https://<account>.blob.core.windows.net/<container>/<blob>

        # Note: "core.windows.net" may be replaced by another "endpoint_suffix"
        account, suffix, endpoint = _get_endpoint(
            self._storage_parameters, self._unsecure, 'blob')
        self._endpoint = endpoint
        return _re.compile(r'https?://%s\.blob\.%s' % (account, suffix)),

    def _head(self, client_kwargs):
        """
        Returns object or bucket HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_azure_exception():
            # Blob
            if 'blob_name' in client_kwargs:
                result = self._client_block.get_blob_properties(**client_kwargs)

            # Container
            else:
                result = self._client_block.get_container_properties(
                    **client_kwargs)

        return _model_to_dict(result)

    def _list_locators(self):
        """
        Lists locators.

        Returns:
            generator of tuple: locator name str, locator header dict
        """
        with _handle_azure_exception():
            for container in self._client_block.list_containers():
                yield container.name, _model_to_dict(container)

    def _list_objects(self, client_kwargs, path, max_request_entries):
        """
        Lists objects.

        args:
            client_kwargs (dict): Client arguments.
            path (str): Path relative to current locator.
            max_request_entries (int): If specified, maximum entries returned
                by request.

        Returns:
            generator of tuple: object name str, object header dict
        """
        client_kwargs = _update_listing_client_kwargs(
            client_kwargs, max_request_entries)

        blob = None
        with _handle_azure_exception():
            for blob in self._client_block.list_blobs(
                    prefix=path, **client_kwargs):
                yield blob.name, _model_to_dict(blob)

        # None only if path don't exists
        if blob is None:
            raise _ObjectNotFoundError

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_azure_exception():
            # Blob
            if 'blob_name' in client_kwargs:
                return self._client_block.create_blob_from_bytes(
                    blob=b'', **client_kwargs)

            # Container
            return self._client_block.create_container(**client_kwargs)

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_azure_exception():
            # Blob
            if 'blob_name' in client_kwargs:
                return self._client_block.delete_blob(**client_kwargs)

            # Container
            return self._client_block.delete_container(**client_kwargs)


def _new_blob(cls, kwargs):
    """
    Used to initialize a blob class.

    Args:
        cls (class): Class to initialize.
        kwargs (dict): Initialization keyword arguments.

    Returns:
        str: Blob type.
    """
    # Try to get cached parameters
    try:
        storage_parameters = kwargs['storage_parameters'].copy()
        system = storage_parameters.get('pycosio.system_cached')

    # Or create new empty ones
    except KeyError:
        storage_parameters = dict()
        system = None

    # If none cached, create a new system
    if not system:
        system = cls._SYSTEM_CLASS(**kwargs)
        storage_parameters['pycosio.system_cached'] = system

    # Detect if file already exists
    try:
        # ALso cache file header to avoid double head call
        # (in __new__ and __init__)
        storage_parameters['pycosio.raw_io._head'] = head = system.head('name')
    except _ObjectNotFoundError:
        head = kwargs

    # Update file storage parameters
    kwargs['storage_parameters'] = storage_parameters

    # Return blob type
    return head.get('blob_type', system._default_blob_type)


class AzureBlobRawIO(_ObjectRawIOBase):
    """Binary Azure Blobs Storage Object I/O

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
        blob_type (str): Blob type to use on new file creation.
            Possibles values: BlockBlob (default), AppendBlob, PageBlob.
    """
    _SYSTEM_CLASS = _AzureBlobSystem

    def __new__(cls, name, mode='r', **kwargs):
        # If call from a subclass, instantiate this subclass directly
        if cls is not AzureBlobRawIO:
            return _IOBase.__new__(cls)

        # Get subclass
        return _IOBase.__new__(_AZURE_RAW[_new_blob(cls, kwargs)])

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
        stream = _BytesIO()
        try:
            with _handle_azure_exception():
                self._client.get_blob_to_stream(
                    stream=stream, start_range=start,
                    end_range=(end - 1) if end else None, **self._client_kwargs)

        # Check for end of file
        except _AzureHttpError as exception:
            if exception.status_code == 416:
                # EOF
                return bytes()
            raise

        return stream.getvalue()

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        stream = _BytesIO()
        with _handle_azure_exception():
            self._client.get_blob_to_stream(
                stream=stream, **self._client_kwargs)
        return stream.getvalue()


class AzureBlockBlobRawIO(AzureBlobRawIO):
    """Binary Azure BLock Blobs Storage Object I/O

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
    _SUPPORT_PART_FLUSH = False
    _DEFAULT_CLASS = False

    def __init__(self, *args, **kwargs):
        _ObjectRawIOBase.__init__(self, *args, **kwargs)

        # Creates blob on write mode
        if ('x' in self.mode or 'w' in self.mode or
                ('a' in self.mode and not self._exists())):
            with _handle_azure_exception():
                self._client.create_blob_from_bytes(
                    blob=b'', **self._client_kwargs)

    @property
    @_memoizedmethod
    def _client(self):
        """
        Returns client instance.

        Returns:
            azure.storage.blob.pageblobservice.PageBlobService: client
        """
        return self._system.client[_BLOCK]

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
        with _handle_azure_exception():
            # Write entire file at once
            self._client.create_blob_from_bytes(
                blob=buffer.tobytes(), **self._client_kwargs)


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
    """
    _SUPPORT_PART_FLUSH = True
    _DEFAULT_CLASS = False

    def __init__(self, *args, **kwargs):
        _ObjectRawIOBase.__init__(self, *args, **kwargs)

        # Creates blob on write mode
        if ('x' in self.mode or 'w' in self.mode or
                ('a' in self.mode and not self._exists())):
            with _handle_azure_exception():
                self._client.create_blob(content_length=0, **self._client_kwargs)

    @property
    @_memoizedmethod
    def _client(self):
        """
        Returns client instance.

        Returns:
            client
        """
        return self._system.client[_PAGE]

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
        with _handle_azure_exception():
            self._client.update_page(
                page=buffer, start_range=start, end_range=end,
                **self._client_kwargs)


class AzureAppendBlobRawIO(AzureBlobRawIO):
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
        _ObjectRawIOBase.__init__(self, *args, **kwargs)

        if self._writable:
            # Not seekable in append mode
            self._seekable = False

            # Creates blob on write mode
            if ('x' in self.mode or 'w' in self.mode or
                    ('a' in self.mode and not self._exists())):
                with _handle_azure_exception():
                    self._client.create_blob(
                        content_length=0, **self._client_kwargs)

    @property
    @_memoizedmethod
    def _client(self):
        """
        Returns client instance.

        Returns:
            client
        """
        return self._system.client[_APPEND]

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
        with _handle_azure_exception():
            # Append mode: Append block at file end
            self._client.append_block(block=buffer, **self._client_kwargs)


_AZURE_RAW = {
    _APPEND: AzureAppendBlobRawIO,
    _BLOCK: AzureBlockBlobRawIO,
    _PAGE: AzurePageBlobRawIO,
}


class AzureBlobBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary Azure Blobs Storage Object I/O

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
        blob_type (str): Blob type to use on new file creation.
            Possibles values: BlockBlob (default), AppendBlob, PageBlob.
    """
    _SYSTEM_CLASS = _AzureBlobSystem

    def __new__(cls, name, mode='r', buffer_size=None, max_buffers=0,
                max_workers=None, **kwargs):
        # If call from a subclass, instantiate this subclass directly
        if cls is not AzureBlobBufferedIO:
            return _IOBase.__new__(cls)

        # Get subclass
        return _IOBase.__new__(_AZURE_BUFFERED[_new_blob(cls, kwargs)])


class AzureBlockBlobBufferedIO(AzureBlobBufferedIO):
    """Buffered binary Azure Block Blobs Storage Object I/O

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
    _RAW_CLASS = AzureBlockBlobRawIO
    _DEFAULT_CLASS = False

    def __init__(self, *args, **kwargs):
        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)
        if self._writable:
            self._blocks = []

    @staticmethod
    def _get_random_block_id(length):
        """
        Generate a random ID.

        Args:
            length (int): ID length.

        Returns:
            str: Random block ID.
        """
        return ''.join(_choice(_ascii_lowercase) for _ in range(length))

    def _flush(self):
        """
        Flush the write buffer of the stream.
        """
        block_id = self._get_random_block_id(32)

        # Upload block with workers
        self._write_futures.append(self._workers.submit(
            self._client.put_block, block=self._get_buffer().tobytes(),
            block_id=block_id, **self._client_kwargs))

        # Save block information
        self._blocks.append(_BlobBlock(id=block_id))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        for future in self._write_futures:
            future.result()

        block_list = self._client.get_block_list(**self._client_kwargs)
        self._client.put_block_list(
            block_list=block_list.committed_blocks + self._blocks,
            **self._client_kwargs)


class AzurePageBlobBufferedIO(AzureBlobBufferedIO):
    """Buffered binary Azure Page Blobs Storage Object I/O

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
    _RAW_CLASS = AzurePageBlobRawIO
    _DEFAULT_CLASS = False

    def __init__(self, *args, **kwargs):
        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        if self._writable and self._buffer_size % 512:
            raise ValueError('"buffer_size" must be multiple of 512 bytes')


class AzureAppendBlobBufferedIO(AzureBlobBufferedIO):
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
        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        if self._writable:
            # Can't upload in parallel, always add data at end.
            self._workers_count = 1

    def _flush(self):
        """
        Flush the write buffer of the stream.
        """
        self._write_futures.append(self._workers.submit(
            self._client.append_block, block=self._get_buffer(),
            **self._client_kwargs))


_AZURE_BUFFERED = {
    _APPEND: AzureAppendBlobBufferedIO,
    _BLOCK: AzureBlockBlobBufferedIO,
    _PAGE: AzurePageBlobBufferedIO,
}
