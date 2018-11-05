# coding=utf-8
"""Microsoft Azure Blobs Storage"""
from contextlib import contextmanager as _contextmanager
import re as _re

from azure.storage.blob import (BlockBlobService as _BlockBlobService,
                                AppendBlobService as _AppendBlobService)

from pycosio._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectPermissionError as _ObjectPermissionError)
from pycosio.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase)


@_contextmanager
def _handle_azure_exception():
    """
    Handle Azure exception and convert to class IO exceptions

    Raises:
        OSError subclasses: IO error.
    """
    yield


class _AzureBlobsSystem(_SystemBase):
    """
    Azure Blobs Storage system.

    Args:
        storage_parameters (dict): ????
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """

    def copy(self, src, dst):
        """
        Copy object of the same storage.

        Args:
            src (str): Path or URL.
            dst (str): Path or URL.
        """

    def _get_client(self):
        """
        Google storage client

        Returns:
            google.cloud.storage.client.Client: client
        """
        return _BlockBlobService(**self._storage_parameters)

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """
        # URL:
        # - http://<account>.blob.core.windows.net/<container>/<blob>
        # - https://<account>.blob.core.windows.net/<container>/<blob>

        # Note: "core.windows.net" may be replaced by another endpoint

        return _re.compile(
            r'https?://%s\.blob\.%s' % (self._account, self.endpoint)),

    def _head(self, client_kwargs):
        """
        Returns object or bucket HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """

    def _list_locators(self):
        """
        Lists locators.

        Returns:
            generator of tuple: locator name str, locator header dict
        """

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

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """


class AzureBlobsRawIO(_ObjectRawIOBase):
    """Binary Azure Blobs Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
        storage_parameters (dict): ????
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    _SYSTEM_CLASS = _AzureBlobsSystem

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

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """


class AzureBlobsBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary Azure Blobs Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode
            or awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        storage_parameters (dict): ????
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    _RAW_CLASS = AzureBlobsRawIO

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """

    def _close_writable(self):
        """
        Close the object in write mode.
        """
