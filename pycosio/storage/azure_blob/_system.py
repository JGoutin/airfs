# coding=utf-8
"""Microsoft Azure Blobs Storage"""
from __future__ import absolute_import  # Python 2: Fix azure import

import re as _re

from azure.storage.blob import (
    PageBlobService, BlockBlobService, AppendBlobService)
from azure.storage.blob.models import _BlobTypes

from pycosio.storage.azure import (
    _handle_azure_exception, _update_storage_parameters, _get_time,
    _update_listing_client_kwargs, _get_endpoint, _model_to_dict)
from pycosio._core.exceptions import ObjectNotFoundError
from pycosio._core.io_base import memoizedmethod
from pycosio.io import SystemBase

# Default blob type
_DEFAULT_BLOB_TYPE = _BlobTypes.BlockBlob


class _AzureBlobSystem(SystemBase):
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
        SystemBase.__init__(self, *args, **kwargs)

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

        return {_BlobTypes.PageBlob: PageBlobService(**parameters),
                _BlobTypes.BlockBlob: BlockBlobService(**parameters),
                _BlobTypes.AppendBlob: AppendBlobService(**parameters)}

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
    @memoizedmethod
    def _client_block(self):
        """
        Storage client

        Returns:
            azure.storage.blob.blockblobservice.BlockBlobService: client
        """
        return self.client[_DEFAULT_BLOB_TYPE]

    @property
    @memoizedmethod
    def _default_blob_type(self):
        """
        Return default blob type to use when creating objects.

        Returns:
            str: Blob type.
        """
        return self._storage_parameters.get('blob_type', _DEFAULT_BLOB_TYPE)

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
            raise ObjectNotFoundError

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
