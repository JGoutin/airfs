# coding=utf-8
"""Microsoft Azure Files Storage"""
from __future__ import absolute_import  # Python 2: Fix azure import

from io import BytesIO as _BytesIO
import re as _re

from azure.storage.file import FileService as _FileService
from azure.common import AzureHttpError as _AzureHttpError

from pycosio.storage.azure import (
    _handle_azure_exception, _update_storage_parameters, _get_time,
    _update_listing_client_kwargs, _get_endpoint, _model_to_dict)
from pycosio.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase)


class _AzureFileSystem(_SystemBase):
    """
    Azure Files Storage system.

    Args:
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.file.fileservice.FileService" for more information.
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
            self.client.copy_file(
                copy_source=src, **self.get_client_kwargs(dst))

    copy_from_azure_blobs = copy  # Allows copy from Azure Blobs Storage

    def _get_client(self):
        """
        Azure file service

        Returns:
            azure.storage.file.fileservice.FileService: Service
        """
        return _FileService(**_update_storage_parameters(
            self._storage_parameters, self._unsecure))

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

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        share_name, relpath = self.split_locator(path)
        kwargs = dict(share_name=share_name)

        # Directory
        if relpath and relpath[-1] == '/':
            kwargs['directory_name'] = relpath.rstrip('/')

        # File
        elif relpath:
            try:
                kwargs['directory_name'], kwargs['file_name'] = relpath.rsplit(
                    '/', 1)
            except ValueError:
                kwargs['directory_name'] = ''
                kwargs['file_name'] = relpath

        # Else, Share only
        return kwargs

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """
        # SMB
        # - smb://<account>.file.core.windows.net/<share>/<file>

        # Mounted share
        # - //<account>.file.core.windows.net/<share>/<file>
        # - \\<account>.file.core.windows.net\<share>\<file>

        # URL:
        # - http://<account>.file.core.windows.net/<share>/<file>
        # - https://<account>.file.core.windows.net/<share>/<file>

        # Note: "core.windows.net" may be replaced by another endpoint

        return _re.compile(r'(https?://|smb://|//|\\)%s\.file\.%s' %
                           _get_endpoint(self._storage_parameters)),

    def _head(self, client_kwargs):
        """
        Returns object or bucket HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_azure_exception():
            # File
            if 'file_name' in client_kwargs:
                result = self.client.get_file_properties(**client_kwargs)

            # Directory
            elif 'directory_name' in client_kwargs:
                result = self.client.get_directory_properties(**client_kwargs)

            # Share
            else:
                result = self.client.get_share_properties(**client_kwargs)

        return _model_to_dict(result)

    def _list_locators(self):
        """
        Lists locators.

        Returns:
            generator of tuple: locator name str, locator header dict
        """
        with _handle_azure_exception():
            for share in self.client.list_shares():
                yield share.name, _model_to_dict(share)

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
            for obj in self.client.list_directories_and_files(
                    prefix=path, **client_kwargs):
                yield obj.name, _model_to_dict(obj)

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_azure_exception():
            # Directory
            if 'directory_name' in client_kwargs:
                return self.client.create_directory(
                    share_name=client_kwargs['share_name'],
                    directory_name=client_kwargs['directory_name'])

            # Share
            return self.client.create_share(**client_kwargs)

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_azure_exception():
            # File
            if 'file_name' in client_kwargs:
                return self.client.delete_file(
                    share_name=client_kwargs['share_name'],
                    directory_name=client_kwargs['directory_name'],
                    file_name=client_kwargs['file_name'])

            # Directory
            elif 'directory_name' in client_kwargs:
                return self.client.delete_directory(
                    share_name=client_kwargs['share_name'],
                    directory_name=client_kwargs['directory_name'])

            # Share
            return self.client.delete_share(
                share_name=client_kwargs['share_name'])


class AzureFileRawIO(_ObjectRawIOBase):
    """Binary Azure Files Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.file.fileservice.FileService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    _SYSTEM_CLASS = _AzureFileSystem
    _SUPPORT_RANDOM_WRITE = True

    def __init__(self, *args, **kwargs):
        _ObjectRawIOBase.__init__(self, *args, **kwargs)

        # Creates blob on write mode
        if 'x' in self.mode or 'w' in self.mode:
            self._client.create_file(**self._client_kwargs)

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
                self._client.get_file_to_stream(
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
            self._client.get_file_to_stream(
                stream=stream, **self._client_kwargs)
        return stream.getvalue()

    def _flush(self, buffer, start, end):
        """
        Flush the write buffers of the stream if applicable.

        Args:
            buffer (memoryview): Buffer content.
            start (int): Start of buffer position to flush.
            end (int): End of buffer position to flush.
        """
        with _handle_azure_exception():
            self._client.update_range(data=buffer, start_range=start,
                                      end_range=end, **self._client_kwargs)


class AzureFileBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary Azure Files Storage Object I/O

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
            "azure.storage.file.fileservice.FileService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    _RAW_CLASS = AzureFileRawIO
