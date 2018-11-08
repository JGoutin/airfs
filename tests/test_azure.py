# coding=utf-8
"""Test pycosio.storage.azure"""
import pytest


def test_handle_azure_exception():
    """Test pycosio.storage.azure._handle_azure_exception"""
    from pycosio.storage.azure import _handle_azure_exception
    from azure.common import AzureHttpError
    from pycosio._core.exceptions import (
        ObjectNotFoundError, ObjectPermissionError)

    # Any error
    with pytest.raises(AzureHttpError):
        with _handle_azure_exception():
            raise AzureHttpError(message='', status_code=400)

    # 404 error
    with pytest.raises(ObjectNotFoundError):
        with _handle_azure_exception():
            raise AzureHttpError(message='', status_code=404)

    # 403 error
    with pytest.raises(ObjectPermissionError):
        with _handle_azure_exception():
            raise AzureHttpError(message='', status_code=403)


def test_mount_redirect():
    """Test pycosio.storage.azure.MOUNT_REDIRECT"""
    from collections import OrderedDict
    import pycosio._core.storage_manager as manager

    # Mocks mounted
    manager_mounted = manager.MOUNTED
    manager.MOUNTED = OrderedDict()
    account_name = 'account_name'
    endpoint_suffix = 'endpoint_suffix'

    # Tests
    try:
        # Auto mount of all Azure services
        result = manager.mount(storage='azure', storage_parameters=dict(
            account_name=account_name, endpoint_suffix=endpoint_suffix))
        assert 'azure_blob' in result
        assert 'azure_file' in result

        # Incompatible extra root argument
        with pytest.raises(ValueError):
            manager.mount(
                storage='azure', extra_root='azure://', storage_parameters=dict(
                    account_name=account_name, endpoint_suffix=endpoint_suffix))

        # Mandatory arguments
        manager.MOUNTED = OrderedDict()
        with pytest.raises(ValueError):
            manager.mount(storage='azure_blob')

    # Restore Mounted
    finally:
        manager.MOUNTED = manager_mounted


def test_update_storage_parameters():
    """Test pycosio.storage.azure._update_storage_parameters"""
    from pycosio.storage.azure import _update_storage_parameters

    params = dict(arg=1)
    assert _update_storage_parameters(params, True) == dict(
        protocol='http', arg=1)
    assert _update_storage_parameters(params, False) == dict(arg=1)


def test_update_listing_client_kwargs():
    """Test pycosio.storage.azure._update_storage_parameters"""
    from pycosio.storage.azure import _update_listing_client_kwargs

    params = dict(arg=1)
    assert _update_listing_client_kwargs(params, 10) == dict(
        num_results=10, arg=1)
    assert _update_listing_client_kwargs(params, 0) == dict(arg=1)
