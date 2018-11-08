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

    manager_mounted = manager.MOUNTED
    manager.MOUNTED = OrderedDict()
    account_name = 'account_name'
    endpoint_suffix = 'endpoint_suffix'
    try:
        result = manager.mount(storage='azure', storage_parameters=dict(
            account_name=account_name, endpoint_suffix=endpoint_suffix))
        assert 'azure_blobs' in result
        assert 'azure_files' in result
    finally:
        manager.MOUNTED = manager_mounted
