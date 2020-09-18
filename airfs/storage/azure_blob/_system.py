"""Microsoft Azure Blobs Storage: System"""
import re

from azure.storage.blob import (  # type: ignore
    PageBlobService,
    BlockBlobService,
    AppendBlobService,
    BlobPermissions,
    ContainerPermissions,
)
from azure.storage.blob.models import _BlobTypes  # type: ignore

from airfs.storage.azure import _handle_azure_exception, _AzureBaseSystem, _make_sas_url
from airfs._core.exceptions import ObjectNotFoundError
from airfs._core.io_base import memoizedmethod

_DEFAULT_BLOB_TYPE = _BlobTypes.BlockBlob


class _AzureBlobSystem(_AzureBaseSystem):
    """
    Azure Blobs Storage system.

    Args:
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more information.
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
            self._client_block.copy_blob(
                copy_source=(other_system or self)._format_src_url(src, self),
                **self.get_client_kwargs(dst),
            )

    def _get_client(self):
        """
        Azure blob service

        Returns:
            dict of azure.storage.blob.baseblobservice.BaseBlobService subclass: Service
        """
        parameters = self._secured_storage_parameters().copy()

        try:
            del parameters["blob_type"]
        except KeyError:
            pass

        return {
            _BlobTypes.PageBlob: PageBlobService(**parameters),
            _BlobTypes.BlockBlob: BlockBlobService(**parameters),
            _BlobTypes.AppendBlob: AppendBlobService(**parameters),
        }

    @property  # type: ignore
    @memoizedmethod
    def _client_block(self):
        """
        Storage client

        Returns:
            azure.storage.blob.blockblobservice.BlockBlobService: client
        """
        return self.client[_DEFAULT_BLOB_TYPE]

    @property  # type: ignore
    @memoizedmethod
    def _default_blob_type(self):
        """
        Return default blob type to use when creating objects.

        Returns:
            str: Blob type.
        """
        return self._storage_parameters.get("blob_type", _DEFAULT_BLOB_TYPE)

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

        container_name, blob_name = self.split_locator(path)
        kwargs = dict(container_name=container_name)

        # Blob
        if blob_name:
            kwargs["blob_name"] = blob_name
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
        return (re.compile(r"^https?://%s\.blob\.%s" % self._get_endpoint("blob")),)

    def _head(self, client_kwargs):
        """
        Returns object or bucket HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_azure_exception():
            if "blob_name" in client_kwargs:
                result = self._client_block.get_blob_properties(**client_kwargs)

            else:
                result = self._client_block.get_container_properties(**client_kwargs)

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
            for container in self._client_block.list_containers(
                num_results=max_results
            ):
                yield container.name, self._model_to_dict(container), True

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
        prefix = self.split_locator(path)[1]
        index = len(prefix)
        client_kwargs = self._update_listing_client_kwargs(client_kwargs, max_results)

        blob = None
        with _handle_azure_exception():
            for blob in self._client_block.list_blobs(prefix=prefix, **client_kwargs):
                yield blob.name[index:], self._model_to_dict(blob), False

        if blob is None:
            raise ObjectNotFoundError(path=path)

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_azure_exception():
            if "blob_name" in client_kwargs:
                return self._client_block.create_blob_from_bytes(
                    blob=b"", **client_kwargs
                )

            return self._client_block.create_container(**client_kwargs)

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_azure_exception():
            if "blob_name" in client_kwargs:
                return self._client_block.delete_blob(**client_kwargs)

            return self._client_block.delete_container(**client_kwargs)

    def _shareable_url(self, client_kwargs, expires_in):
        """
        Get a shareable URL for the specified path.

        Args:
            client_kwargs (dict): Client arguments.
            expires_in (int): Expiration in seconds.

        Returns:
            str: Shareable URL.
        """
        if "blob_name" in client_kwargs:
            make_url = self._client_block.make_blob_url
            generate_sas = self._client_block.generate_blob_shared_access_signature
            permissions = BlobPermissions
        else:
            make_url = self._client_block.make_container_url
            generate_sas = self._client_block.generate_container_shared_access_signature
            permissions = ContainerPermissions

        return _make_sas_url(
            client_kwargs, expires_in, generate_sas, make_url, permissions
        )
