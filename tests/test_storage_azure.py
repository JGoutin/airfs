"""Test airfs.storage.azure."""
from datetime import datetime
from time import time

import pytest

pytest.importorskip("azure.storage.blob")
pytest.importorskip("azure.storage.file")


def test_handle_azure_exception():
    """Test airfs.storage.azure._handle_azure_exception."""
    from airfs.storage.azure import _handle_azure_exception
    from azure.common import AzureHttpError  # type: ignore
    from airfs._core.exceptions import ObjectNotFoundError, ObjectPermissionError

    # Any error
    with pytest.raises(AzureHttpError):
        with _handle_azure_exception():
            raise AzureHttpError(message="", status_code=400)

    # 404 error
    with pytest.raises(ObjectNotFoundError):
        with _handle_azure_exception():
            raise AzureHttpError(message="", status_code=404)

    # 403 error
    with pytest.raises(ObjectPermissionError):
        with _handle_azure_exception():
            raise AzureHttpError(message="", status_code=403)


def test_mount_redirect():
    """Test airfs.storage.azure.MOUNT_REDIRECT."""
    from collections import OrderedDict
    import airfs._core.storage_manager as manager
    from airfs import MountException

    # Mocks mounted
    manager_mounted = manager.MOUNTED
    manager.MOUNTED = OrderedDict()
    account_name = "account_name"
    endpoint_suffix = "endpoint_suffix"

    # Tests
    try:
        # Auto mount of all Azure services
        result = manager.mount(
            storage="azure",
            storage_parameters=dict(
                account_name=account_name, endpoint_suffix=endpoint_suffix
            ),
        )
        assert "azure_blob" in result
        assert "azure_file" in result

        # Incompatible extra root argument
        with pytest.raises(MountException):
            manager.mount(
                storage="azure",
                extra_root="azure://",
                storage_parameters=dict(
                    account_name=account_name, endpoint_suffix=endpoint_suffix
                ),
            )

        # Mandatory arguments
        manager.MOUNTED = OrderedDict()
        with pytest.raises(ValueError):
            manager.mount(storage="azure_blob")

    # Restore Mounted
    finally:
        manager.MOUNTED = manager_mounted


def test_update_listing_client_kwargs():
    """Test airfs.storage.azure._AzureBaseSystem._update_listing_client_kwargs."""
    from airfs.storage.azure import _AzureBaseSystem

    params = dict(arg=1)
    assert _AzureBaseSystem._update_listing_client_kwargs(params, 10) == dict(
        num_results=10, arg=1
    )
    assert _AzureBaseSystem._update_listing_client_kwargs(params, 0) == dict(arg=1)


def test_model_to_dict():
    """Test airfs.storage.azure._AzureBaseSystem._model_to_dict."""
    from airfs.storage.azure import _AzureBaseSystem
    from azure.storage.file import models  # type: ignore

    last_modified = datetime.now()
    props = models.FileProperties()
    props.etag = "etag"
    props.last_modified = last_modified
    file = models.File(props=props, metadata=dict(metadata1=0))

    assert _AzureBaseSystem._model_to_dict(file) == dict(
        etag="etag", last_modified=last_modified, metadata=dict(metadata1=0)
    )


def test_get_time():
    """Test airfs.storage.azure._AzureBaseSystem._get_time."""
    from airfs.storage.azure import _AzureBaseSystem
    from airfs._core.exceptions import ObjectUnsupportedOperation

    m_time = time()
    last_modified = datetime.fromtimestamp(m_time)

    assert _AzureBaseSystem._get_time(
        {"last_modified": last_modified}, ("last_modified",), "gettime"
    ) == pytest.approx(m_time, 1)

    with pytest.raises(ObjectUnsupportedOperation):
        _AzureBaseSystem._get_time({}, ("last_modified",), "gettime")


def get_storage_mock():
    """Return storage mock configured for Azure.

    Returns:
        tests.storage_mock.ObjectStorageMock: Mocked storage
    """
    from azure.common import AzureHttpError
    from tests.storage_mock import ObjectStorageMock

    def raise_404():
        """Raise 404 error."""
        raise AzureHttpError(message="", status_code=404)

    def raise_416():
        """Raise 416 error."""
        raise AzureHttpError(message="", status_code=416)

    def raise_500():
        """Raise 500 error."""
        raise AzureHttpError(message="", status_code=500)

    return ObjectStorageMock(
        raise_404, raise_416, raise_500, format_date=datetime.fromtimestamp
    )
