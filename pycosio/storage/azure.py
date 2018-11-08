# coding=utf-8
"""Microsoft Azure Storage"""
from __future__ import absolute_import  # Python 2: Fix azure import

from contextlib import contextmanager as _contextmanager
from azure.common import AzureHttpError as _AzureHttpError

from pycosio._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectPermissionError as _ObjectPermissionError)

# TODO:
# - Proper "Truncate" support
# - Proper random write support

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


def _update_storage_parameters(storage_parameters, unsecure):
    """
    Updates storage parameters.

    Args:
        storage_parameters (dict): Azure service keyword arguments.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.

    Returns:
        dict: Updated storage_parameters.
    """
    parameters = storage_parameters or dict()

    # Handles unsecure mode
    if unsecure:
        parameters = parameters.copy()
        parameters['protocol'] = 'http'

    return parameters


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


def _get_endpoint(storage_parameters):
    """
    Get endpoint information from storage parameters.

    Args:
        storage_parameters (dict): Azure service keyword arguments.

    Returns:
        tuple of str: account_name, endpoint_suffix
    """
    storage_parameters = storage_parameters or dict()
    account_name = storage_parameters.get('account_name')

    if not account_name:
        raise ValueError('"account_name" is required for Azure storage')

    return account_name, storage_parameters.get(
            'endpoint_suffix', 'core.windows.net').replace('.', r'\.')
