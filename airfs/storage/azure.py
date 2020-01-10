# coding=utf-8
"""Microsoft Azure Storage"""
from abc import abstractmethod as _abstractmethod
from contextlib import contextmanager as _contextmanager
from concurrent.futures import as_completed as _as_completed
from io import (
    UnsupportedOperation as _UnsupportedOperation, BytesIO as _BytesIO)
from threading import Lock as _Lock

from azure.common import AzureHttpError as _AzureHttpError

from airfs._core.io_base import WorkerPoolBase as _WorkerPoolBase
from airfs._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectPermissionError as _ObjectPermissionError)
from airfs.io import (
    SystemBase as _SystemBase, ObjectRawIOBase as _ObjectRawIOBase,
    ObjectRawIORandomWriteBase as _ObjectRawIORandomWriteBase)

#: 'azure' can be used to mount following storage at once with airfs.mount
MOUNT_REDIRECT = ('azure_blob', 'azure_file')

_ERROR_CODES = {
    403: _ObjectPermissionError,
    404: _ObjectNotFoundError}


@_contextmanager
def _handle_azure_exception():
    """
    Handles Azure exception and convert to class IO exceptions

    Raises:
        OSError subclasses: IO error.
    """
    try:
        yield

    except _AzureHttpError as exception:
        if exception.status_code in _ERROR_CODES:
            raise _ERROR_CODES[exception.status_code](str(exception))
        raise


def _properties_model_to_dict(properties):
    """
    Convert properties model to dict.

    Args:
        properties: Properties model.

    Returns:
        dict: Converted model.
    """
    result = {}
    for attr in properties.__dict__:
        value = getattr(properties, attr)

        if hasattr(value, '__module__') and 'models' in value.__module__:
            value = _properties_model_to_dict(value)

        if not (value is None or (isinstance(value, dict) and not value)):
            result[attr] = value

    return result


class _AzureBaseSystem(_SystemBase):
    """
    Common base for Azure storage systems.

    Args:
        storage_parameters (dict): Azure service keyword arguments.
            This is generally Azure credentials and configuration. See
            "azure.storage.blob.baseblobservice.BaseBlobService" for more
            information.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
    """
    __slots__ = ('_endpoint', '_endpoint_domain')

    _MTIME_KEYS = ('last_modified',)
    _SIZE_KEYS = ('content_length',)

    def __init__(self, *args, **kwargs):
        self._endpoint = None
        self._endpoint_domain = None
        _SystemBase.__init__(self, *args, **kwargs)

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
        for key in keys:
            try:
                return header.pop(key).timestamp()
            except KeyError:
                continue

        raise _UnsupportedOperation(name)

    def _get_endpoint(self, sub_domain):
        """
        Get endpoint information from storage parameters.

        Update system with endpoint information and return information required
        to define roots.

        Args:
            self (airfs._core.io_system.SystemBase subclass): System.
            sub_domain (str): Azure storage sub-domain.

        Returns:
            tuple of str: account_name, endpoint_suffix
        """
        storage_parameters = self._storage_parameters or dict()
        account_name = storage_parameters.get('account_name')

        if not account_name:
            raise ValueError('"account_name" is required for Azure storage')

        suffix = storage_parameters.get(
            'endpoint_suffix', 'core.windows.net')

        self._endpoint = 'http%s://%s.%s.%s' % (
            '' if self._unsecure else 's', account_name, sub_domain, suffix)

        return account_name, suffix.replace('.', r'\.')

    def _secured_storage_parameters(self):
        """
        Updates storage parameters with unsecure mode.

        Returns:
            dict: Updated storage_parameters.
        """
        parameters = self._storage_parameters or dict()

        # Handles unsecure mode
        if self._unsecure:
            parameters = parameters.copy()
            parameters['protocol'] = 'http'

        return parameters

    def _format_src_url(self, path, caller_system):
        """
        Ensure path is absolute and use the correct URL format for use with
        cross Azure storage account copy function.

        Args:
            path (str): Path or URL.
            caller_system (airfs.storage.azure._AzureBaseSystem subclass):
                System calling this method (Can be another Azure system).

        Returns:
            str: URL.
        """
        path = '%s/%s' % (self._endpoint, self.relpath(path))

        # If SAS token available, use it to give cross account copy access.
        if caller_system is not self:
            try:
                path = '%s?%s' % (path, self._storage_parameters['sas_token'])
            except KeyError:
                pass

        return path

    @staticmethod
    def _update_listing_client_kwargs(client_kwargs, max_request_entries):
        """
        Updates client kwargs for listing functions.

        Args:
            client_kwargs (dict): Client arguments.
            max_request_entries (int): If specified, maximum entries returned
                by request.

        Returns:
            dict: Updated client_kwargs
        """
        client_kwargs = client_kwargs.copy()
        if max_request_entries:
            client_kwargs['num_results'] = max_request_entries
        return client_kwargs

    @staticmethod
    def _model_to_dict(obj):
        """
        Convert object model to dict.

        Args:
            obj: Object model.

        Returns:
            dict: Converted model.
        """
        result = _properties_model_to_dict(obj.properties)
        for attribute in ('metadata', 'snapshot'):
            try:
                value = getattr(obj, attribute)
            except AttributeError:
                continue
            if value:
                result[attribute] = value
        return result


class _AzureStorageRawIOBase(_ObjectRawIOBase):
    """
    Common Raw IO for all Azure storage classes
    """

    @property
    @_abstractmethod
    def _get_to_stream(self):
        """
        Azure storage function that read a range to a stream.

        Returns:
            function: Read function.
        """

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
                self._get_to_stream(
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
            self._get_to_stream(stream=stream, **self._client_kwargs)
        return stream.getvalue()


class _AzureStorageRawIORangeWriteBase(_ObjectRawIORandomWriteBase,
                                       _AzureStorageRawIOBase,
                                       _WorkerPoolBase):
    """
    Common Raw IO for Azure storage classes that have write range ability.
    """
    __slots__ = ('_content_length',)

    _MAX_FLUSH_SIZE = None

    def __init__(self, *args, **kwargs):

        # If a content length is provided, allocate pages for this blob
        self._content_length = kwargs.get('content_length', 0)

        _ObjectRawIORandomWriteBase.__init__(self, *args, **kwargs)
        _WorkerPoolBase.__init__(self)

        if self._writable:

            # Create lock for resizing
            self._size_lock = _Lock()

    @property
    @_abstractmethod
    def _resize(self):
        """
        Azure storage function that resize an object.

        Returns:
            function: Resize function.
        """

    @property
    @_abstractmethod
    def _create_from_size(self):
        """
        Azure storage function that create an object with a specified size.

        Returns:
            function: Create function.
        """

    def _init_append(self):
        """
        Initializes file on 'a' mode.
        """
        if self._content_length:
            # Adjust size if content length specified
            with _handle_azure_exception():
                self._resize(
                    content_length=self._content_length, **self._client_kwargs)
                self._reset_head()

        # Make initial seek position to current end of file
        self._seek = self._size

    def _create(self):
        """
        Create the file if not exists.
        """
        # Create new file
        with _handle_azure_exception():
            self._create_from_size(
                content_length=self._content_length, **self._client_kwargs)

    @_abstractmethod
    def _update_range(self, data, **kwargs):
        """
        Update range with data

        Args:
            data (bytes): data.
        """

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
        buffer_size = len(buffer)
        if not buffer_size:
            return

        # Write range normally
        with self._size_lock:
            if end > self._size:
                # Require to resize the blob if note enough space
                with _handle_azure_exception():
                    self._resize(content_length=end, **self._client_kwargs)
                self._reset_head()

        if buffer_size > self.MAX_FLUSH_SIZE:
            # Too large buffer, needs to split in multiples requests
            futures = []
            for part_start in range(0, buffer_size, self.MAX_FLUSH_SIZE):

                # Split buffer
                buffer_part = buffer[
                      part_start:part_start + self.MAX_FLUSH_SIZE]
                if not len(buffer_part):
                    # No more data
                    break

                # Upload split buffer in parallel
                start_range = start + part_start
                futures.append(self._workers.submit(
                    self._update_range, data=buffer_part.tobytes(),
                    start_range=start_range,
                    end_range=start_range + len(buffer_part) - 1,
                    **self._client_kwargs))

            with _handle_azure_exception():
                # Wait for upload completion
                for future in _as_completed(futures):
                    future.result()

        else:
            # Buffer lower than limit, do one requests.
            with _handle_azure_exception():
                self._update_range(
                    data=buffer.tobytes(), start_range=start,
                    end_range=end - 1, **self._client_kwargs)
