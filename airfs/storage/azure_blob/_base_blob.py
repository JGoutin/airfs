"""Microsoft Azure Blobs Storage: Base for all blob types"""
from io import IOBase

from airfs._core.io_base import memoizedmethod
from airfs._core.exceptions import AirfsInternalException
from airfs.io import ObjectBufferedIOBase
from airfs.storage.azure_blob._system import _AzureBlobSystem
from airfs.storage.azure import _AzureStorageRawIOBase

AZURE_BUFFERED = {}  # type: ignore
AZURE_RAW = {}  # type: ignore


def _new_blob(cls, name, kwargs):
    """
    Used to initialize a blob class.

    Args:
        cls (class): Class to initialize.
        name (str): Blob name.
        kwargs (dict): Initialization keyword arguments.

    Returns:
        str: Blob type.
    """
    try:
        storage_parameters = kwargs["storage_parameters"].copy()
        system = storage_parameters.get("airfs.system_cached")

    except KeyError:
        storage_parameters = dict()
        system = None

    if not system:
        system = cls._SYSTEM_CLASS(**kwargs)
        storage_parameters["airfs.system_cached"] = system

    try:
        storage_parameters["airfs.raw_io._head"] = head = system.head(name)
    except AirfsInternalException:
        # Unable to access to the file (May not exists, or may not have read access
        # permission), try to use arguments as blob type source.
        head = kwargs

    kwargs["storage_parameters"] = storage_parameters

    return head.get("blob_type", system._default_blob_type)


class AzureBlobRawIO(_AzureStorageRawIOBase):
    """Binary Azure Blobs Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a' for reading (default), writing or
            appending.
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
        blob_type (str): Blob type to use on new file creation.
            Possibles values: BlockBlob (default), AppendBlob, PageBlob.
    """

    _SYSTEM_CLASS = _AzureBlobSystem
    __DEFAULT_CLASS = True

    def __new__(cls, name, mode="r", **kwargs):
        if cls is not AzureBlobRawIO:
            return IOBase.__new__(cls)
        return IOBase.__new__(AZURE_RAW[_new_blob(cls, name, kwargs)])

    @property  # type: ignore
    @memoizedmethod
    def _get_to_stream(self):
        """
        Azure storage function that read a range to a stream.

        Returns:
            function: Read function.
        """
        return self._client.get_blob_to_stream


class AzureBlobBufferedIO(ObjectBufferedIOBase):
    """Buffered binary Azure Blobs Storage Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode or
            awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to execute
            the given calls.
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more information.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
        blob_type (str): Blob type to use on new file creation.
            Possibles values: BlockBlob (default), AppendBlob, PageBlob.
    """

    _SYSTEM_CLASS = _AzureBlobSystem
    __DEFAULT_CLASS = True

    def __new__(
        cls, name, mode="r", buffer_size=None, max_buffers=0, max_workers=None, **kwargs
    ):
        if cls is not AzureBlobBufferedIO:
            return IOBase.__new__(cls)
        return IOBase.__new__(AZURE_BUFFERED[_new_blob(cls, name, kwargs)])
