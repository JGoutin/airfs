"""Microsoft Azure Files Storage"""
import re as _re

from azure.storage.file import (  # type: ignore
    FileService as _FileService,
    FilePermissions as _FilePermissions,
)
from azure.storage.file.models import Directory as _Directory  # type: ignore

from airfs._core.io_base import memoizedmethod as _memoizedmethod
from airfs._core.exceptions import (
    ObjectNotImplementedError as _ObjectNotImplementedError,
)
from airfs.storage.azure import (
    _handle_azure_exception,
    _AzureBaseSystem,
    _AzureStorageRawIORangeWriteBase,
    _make_sas_url,
)
from airfs.io import (
    ObjectBufferedIORandomWriteBase as _ObjectBufferedIORandomWriteBase,
    SystemBase as _SystemBase,
)


class _AzureFileSystem(_AzureBaseSystem, _SystemBase):
    """
    Azure Files Storage system.

    Args:
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.file.fileservice.FileService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    def copy(self, src, dst, other_system=None):
        """
        Copy object of the same storage.

        Args:
            src (str): Path or URL.
            dst (str): Path or URL.
            other_system (airfs.storage.azure._AzureBaseSystem subclass): The source
                storage system.
        """
        with _handle_azure_exception():
            self.client.copy_file(
                copy_source=(other_system or self)._format_src_url(src, self),
                **self.get_client_kwargs(dst),
            )

    copy_from_azure_blobs = copy

    def _get_client(self):
        """
        Azure file service

        Returns:
            azure.storage.file.fileservice.FileService: Service
        """
        return _FileService(**self._secured_storage_parameters())

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        path = path.split("?", 1)[0]

        share_name, relpath = self.split_locator(path)
        kwargs = dict(share_name=share_name)

        if relpath and relpath[-1] == "/":
            kwargs["directory_name"] = relpath.rstrip("/")

        elif relpath:
            try:
                kwargs["directory_name"], kwargs["file_name"] = relpath.rsplit("/", 1)
            except ValueError:
                kwargs["directory_name"] = ""
                kwargs["file_name"] = relpath

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
        return (
            _re.compile(
                r"^(https?://|smb://|//|\\)%s\.file\.%s" % self._get_endpoint("file")
            ),
        )

    def _head(self, client_kwargs):
        """
        Returns object or bucket HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_azure_exception():
            if "file_name" in client_kwargs:
                result = self.client.get_file_properties(**client_kwargs)

            elif "directory_name" in client_kwargs:
                result = self.client.get_directory_properties(**client_kwargs)

            else:
                result = self.client.get_share_properties(**client_kwargs)

        return self._model_to_dict(result)

    def _list_locators(self, max_results):
        """
        Lists locators.

        args:
            max_results (int): The maximum results that should return the method.

        Yields:
            tuple: locator name str, locator header dict, has content bool
        """
        with _handle_azure_exception():
            for share in self.client.list_shares(num_results=max_results):
                yield share.name, self._model_to_dict(share), True

    def _list_objects(self, client_kwargs, path, max_results, first_level):
        """
        Lists objects.

        args:
            client_kwargs (dict): Client arguments.
            path (str): Path to list.
            max_results (int): The maximum results that should return the method.
            first_level (bool): It True, may only first level objects.

        Yields:
            tuple: object path str, object header dict, has content bool
        """
        client_kwargs = self._update_listing_client_kwargs(client_kwargs, max_results)

        with _handle_azure_exception():
            for obj in self.client.list_directories_and_files(**client_kwargs):
                yield obj.name, self._model_to_dict(obj), isinstance(obj, _Directory)

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_azure_exception():
            if "directory_name" in client_kwargs:
                return self.client.create_directory(
                    share_name=client_kwargs["share_name"],
                    directory_name=client_kwargs["directory_name"],
                )

            return self.client.create_share(**client_kwargs)

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_azure_exception():
            if "file_name" in client_kwargs:
                return self.client.delete_file(
                    share_name=client_kwargs["share_name"],
                    directory_name=client_kwargs["directory_name"],
                    file_name=client_kwargs["file_name"],
                )

            elif "directory_name" in client_kwargs:
                return self.client.delete_directory(
                    share_name=client_kwargs["share_name"],
                    directory_name=client_kwargs["directory_name"],
                )

            return self.client.delete_share(share_name=client_kwargs["share_name"])

    def _shareable_url(self, client_kwargs, expires_in):
        """
        Get a shareable URL for the specified path.

        Args:
            client_kwargs (dict): Client arguments.
            expires_in (int): Expiration in seconds.

        Returns:
            str: Shareable URL.
        """
        if "file_name" not in client_kwargs:
            raise _ObjectNotImplementedError(
                "Shared URLs to shares or directories are not supported on "
                "Azure Files Storage"
            )
        return _make_sas_url(
            client_kwargs,
            expires_in,
            self.client.generate_file_shared_access_signature,
            self.client.make_file_url,
            _FilePermissions,
        )


class AzureFileRawIO(_AzureStorageRawIORangeWriteBase):
    """Binary Azure Files Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a' for reading (default), writing or
            appending.
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.file.fileservice.FileService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
        content_length (int): Define the size to preallocate on new file creation.
            This is not mandatory, and file will be resized on needs but this allow to
            improve performance when file size is known in advance.
    """

    _SYSTEM_CLASS = _AzureFileSystem

    #: Maximum size of one flush operation
    MAX_FLUSH_SIZE = _FileService.MAX_RANGE_SIZE

    @property  # type: ignore
    @_memoizedmethod
    def _get_to_stream(self):
        """
        Azure storage function that read a range to a stream.

        Returns:
            function: Read function.
        """
        return self._client.get_file_to_stream

    @property  # type: ignore
    @_memoizedmethod
    def _resize(self):
        """
        Azure storage function that resize an object.

        Returns:
            function: Resize function.
        """
        return self._client.resize_file

    @property  # type: ignore
    @_memoizedmethod
    def _create_from_size(self):
        """
        Azure storage function that create an object.

        Returns:
            function: Create function.
        """
        return self._client.create_file

    def _update_range(self, data, **kwargs):
        """
        Update range with data

        Args:
            data (bytes): data.
        """
        self._client.update_range(data=data, **kwargs)


class AzureFileBufferedIO(_ObjectBufferedIORandomWriteBase):
    """Buffered binary Azure Files Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode or
            awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to execute the
            given calls.
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.file.fileservice.FileService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
        content_length (int): Define the size to preallocate on new file creation. This
            is not mandatory, and file will be resized on needs but this allow to
            improve performance when file size is known in advance.
    """

    _RAW_CLASS = AzureFileRawIO

    #: Maximal buffer_size value in bytes (Maximum upload range size)
    MAXIMUM_BUFFER_SIZE = _FileService.MAX_RANGE_SIZE
