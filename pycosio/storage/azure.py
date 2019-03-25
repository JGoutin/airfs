# coding=utf-8
"""Microsoft Azure Storage"""
from __future__ import absolute_import  # Python 2: Fix azure import

from contextlib import contextmanager as _contextmanager
from io import UnsupportedOperation as _UnsupportedOperation

from azure.common import AzureHttpError as _AzureHttpError

from pycosio._core.compat import to_timestamp as _to_timestamp
from pycosio._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectPermissionError as _ObjectPermissionError)
from pycosio.io import SystemBase as _SystemBase

#: 'azure' can be used to mount following storage at once with pycosio.mount
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

        if value:
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
                return _to_timestamp(header.pop(key))
            except KeyError:
                continue

        raise _UnsupportedOperation(name)

    def _get_endpoint(self, sub_domain):
        """
        Get endpoint information from storage parameters.

        Update system with endpoint information and return information required
        to define roots.

        Args:
            self (pycosio._core.io_system.SystemBase subclass): System.
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
            caller_system (pycosio.storage.azure._AzureBaseSystem subclass):
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
