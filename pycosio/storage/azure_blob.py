# coding=utf-8
"""Microsoft Azure Blobs Storage"""
from __future__ import absolute_import  # Python 2: Fix azure import

from io import BytesIO as _BytesIO
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

    def copy(self, src, dst):
        """
        Copy object of the same storage.

        Args:
            src (str): Path or URL.
            dst (str): Path or URL.
        """
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

        return {
            _BlobTypes.PageBlob: _PageBlobService(**parameters),
            _BlobTypes.BlockBlob: _BlockBlobService(**parameters),
            _BlobTypes.AppendBlob: _AppendBlobService(**parameters)
        }

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
            client
        """
        return self.client[_BlobTypes.BlockBlob]

    @property
    @_memoizedmethod
    def _default_blob_type(self):
        """
        Return default blob type to use when creating objects.

        Returns:
            str: Blob type.
        """
        return self._storage_parameters.get('blob_type', _BlobTypes.PageBlob)

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

        return _re.compile(
            r'https?://%s\.blob\.%s' % _get_endpoint(self._storage_parameters)),

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

        with _handle_azure_exception():
            for blob in self._client_block.list_blobs(
                    prefix=path, **client_kwargs):
                yield blob.name, _model_to_dict(blob)

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
            Possibles values: PageBlob (default), AppendBlob, BlockBlob.
    """
    _SYSTEM_CLASS = _AzureBlobSystem
    _SUPPORT_RANDOM_WRITE = None

    def __init__(self, *args, **kwargs):
        _ObjectRawIOBase.__init__(self, *args, **kwargs)

        # Detects blob type to use
        try:
            self._blob_type = self._head()['blob_type']
        except (_ObjectNotFoundError, KeyError):
            self._blob_type = kwargs.get(
                'blob_type', self._system._default_blob_type)

        # Page blob support random write
        if self._blob_type == _BlobTypes.PageBlob:
            self._SUPPORT_RANDOM_WRITE = True

        # Other blob types require to load file content
        elif 'a' in self.mode:
            self._init_append()

        # Creates blob on write mode
        if 'x' in self.mode or 'w' in self.mode:
            if self._blob_type == _BlobTypes.BlockBlob:
                self._client.create_blob_from_bytes(
                    blob=b'', **self._client_kwargs)
            else:
                self._client.create_blob(**self._client_kwargs)

    @property
    @_memoizedmethod
    def _client(self):
        """
        Returns client instance.

        Returns:
            client
        """
        return self._system.client[self._blob_type]

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
                    end_range=end if end else None, **self._client_kwargs)

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
            # Page blob: Support Random write access
            if self._blob_type == _BlobTypes.PageBlob:
                self._client.update_page(
                    page=buffer, start_range=start, end_range=end,
                    **self._client_kwargs)

            # Other blobs: Write entire file at once
            else:
                self._client.create_blob_from_bytes(
                    blob=buffer, **self._client_kwargs)


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
    """
    _RAW_CLASS = AzureBlobRawIO

    def __init__(self, *args, **kwargs):
        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        if self._writable:
            self._blob_type = self._raw._blob_type

            if (self._blob_type == _BlobTypes.PageBlob and
                    self._buffer_size % 512):
                raise ValueError('"buffer_size" must be multiple of 512 bytes')
            elif self._blob_type == _BlobTypes.BlockBlob:
                self._blocks = []
            elif self._blob_type == _BlobTypes.AppendBlob:
                # Can't upload in parallel, always add data at end.
                self._workers_count = 1

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

    @property
    @_memoizedmethod
    def _client(self):
        """
        Returns client instance.

        Returns:
            client
        """
        return self._raw._system.client[self._blob_type]

    def _flush(self):
        """
        Flush the write buffer of the stream.
        """
        buffer = self._get_buffer()

        # Page blob: Writes buffer as range of bytes
        if self._blob_type == _BlobTypes.PageBlob:
            _ObjectBufferedIOBase._flush(self)

        # Block blob: Writes buffer as a block
        elif self._blob_type == _BlobTypes.BlockBlob:
            block_id = self._get_random_block_id(32)

            # Upload block with workers
            self._write_futures.append(self._workers.submit(
                self._client.put_block, block=buffer,
                block_id=block_id, **self._client_kwargs))

            # Save block information
            self._blocks.append(_BlobBlock(id=block_id))

        # Append blob: Appends buffer as a block
        else:
            self._write_futures.append(self._workers.submit(
                self._client.append_block, block=buffer, **self._client_kwargs))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        for future in self._write_futures:
            future.result()

        # Block blob: Commit put blocks to blob
        if self._blob_type == _BlobTypes.BlockBlob:
            block_list = self._client.get_block_list(**self._client_kwargs)

            self._client.put_block_list(
                block_list=block_list.committed_blocks + self._blocks,
                **self._client_kwargs)
