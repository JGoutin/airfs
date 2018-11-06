# coding=utf-8
"""Microsoft Azure Files Storage"""
from io import BytesIO as _BytesIO
import re as _re

from azure.storage.file import FileService as _FileService

from pycosio.storage.azure_blobs import _handle_azure_exception
from pycosio.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase)

# TODO:
# - Common "azure" storage entry point that generate both blob and file storage
# - Proper "Truncate" support
# - Proper random write support
# - Move common code from blob and file to a parent class.
# - copy: adapt for copy from Azure blob to Azure file


class _AzureFilesSystem(_SystemBase):
    """
    Azure Files Storage system.

    Args:
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.file.fileservice.FileService" for more information.
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
        with _handle_azure_exception():
            self.client.copy_file(
                copy_source=src, **self.get_client_kwargs(dst))

    def _get_client(self):
        """
        Azure file service

        Returns:
            azure.storage.file.fileservice.FileService: Service
        """
        parameters = self._storage_parameters or dict()

        # Handles unsecure mode
        if self._unsecure:
            parameters = parameters.copy()
            parameters['protocol'] = 'http'

        return _FileService(**parameters)

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        # Convert path from Windows format
        # TODO: Must apply on all functions
        path.replace('\\', '/')

        share_name, relpath = self.split_locator(path)
        kwargs = dict(share_name=share_name)

        # Directory
        if relpath and relpath[-1] == '/':
            kwargs['directory_name'] = relpath

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

        return _re.compile(r'(https?://|smb://|//|\\)%s\.file\.%s' % (
            self._account, self.endpoint)),

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
                return self.client.get_file_metadata(**client_kwargs)

            # Directory
            elif 'directory_name' in client_kwargs:
                return self.client.get_directory_properties(**client_kwargs)

            # Share
            return self.client.get_share_properties(**client_kwargs)

    def _list_locators(self):
        """
        Lists locators.

        Returns:
            generator of tuple: locator name str, locator header dict
        """
        with _handle_azure_exception():
            for share in self.client.list_shares():
                yield share['Name'], share['Properties']

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
        client_kwargs = client_kwargs.copy()
        if max_request_entries:
            client_kwargs['num_results'] = max_request_entries

        with _handle_azure_exception():
            for obj in self.client.list_directories_and_files(
                    prefix=path, **client_kwargs):
                try:
                    properties = obj['Properties']
                except KeyError:
                    # Directories don't have properties
                    properties = {}
                yield obj['Name'], properties

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
            # Directory
            if 'directory_name' in client_kwargs:
                return self.client.delete_directory(
                    share_name=client_kwargs['share_name'],
                    directory_name=client_kwargs['share_name'])

            # Share
            return self.client.delete_share(
                share_name=client_kwargs['share_name'])


class AzureFilesRawIO(_ObjectRawIOBase):
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
    _SYSTEM_CLASS = _AzureFilesSystem

    def __init__(self, *args, **kwargs):
        _ObjectRawIOBase.__init__(self, *args, **kwargs)

        # Creates blob on write mode
        if 'x' in self.mode or 'w' in self.mode:
            self._client.create_blob(**self._client_kwargs)

    def _init_append(self):
        """
        Initializes data on 'a' mode
        """
        # Supported by default

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
        with _handle_azure_exception():
            self._client.get_file_to_stream(
                stream=stream, start_range=start,
                end_range=end if end else None, **self._client_kwargs)
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

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """
        with _handle_azure_exception():
            self._client.update_range(
                data=self._get_buffer(),
                # Append at end
                start_range=self._size - len(self._get_buffer()),
                end_range=self._size, **self._client_kwargs)


class AzureFilesBufferedIO(_ObjectBufferedIOBase):
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
    _RAW_CLASS = AzureFilesRawIO

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        start_range = self._buffer_size * self._seek
        end_range = start_range + self._buffer_size

        self._write_futures.append(self._workers.submit(
            self._client.update_range, data=self._get_buffer(),
            start_range=start_range, end_range=end_range,
            **self._client_kwargs))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        for future in self._write_futures:
            future.result()
