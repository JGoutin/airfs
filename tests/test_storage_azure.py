# coding=utf-8
"""Test pycosio.storage.azure"""
from __future__ import absolute_import  # Python 2: Fix azure import

from datetime import datetime
from time import time

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


def test_model_to_dict():
    """Test pycosio.storage.azure._model_to_dict"""
    from pycosio.storage.azure import _model_to_dict
    from azure.storage.file import models

    last_modified = datetime.now()
    props = models.FileProperties()
    props.etag = 'etag'
    props.last_modified = last_modified
    file = models.File(props=props, metadata=dict(metadata1=0))

    assert _model_to_dict(file) == dict(
        etag='etag', last_modified=last_modified, metadata=dict(metadata1=0))


def test_get_time():
    """Test pycosio.storage.azure._get_time"""
    from pycosio.storage.azure import _get_time
    from io import UnsupportedOperation

    m_time = time()
    last_modified = datetime.fromtimestamp(m_time)

    assert (_get_time(
        {'last_modified': last_modified}, ('last_modified',), 'gettime') ==
            pytest.approx(m_time, 1))

    with pytest.raises(UnsupportedOperation):
        _get_time({}, ('last_modified',), 'gettime')
