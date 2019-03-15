# coding=utf-8
"""Test pycosio.storage.swift"""
import pytest

UNSUPPORTED_OPERATIONS = (
    # Not supported on some objects
    'getctime',
)


def test_handle_client_exception():
    """Test pycosio.swift._handle_client_exception"""
    from pycosio.storage.swift import _handle_client_exception
    from swiftclient import ClientException
    from pycosio._core.exceptions import (
        ObjectNotFoundError, ObjectPermissionError)

    # No error
    with _handle_client_exception():
        pass

    # 403 error
    with pytest.raises(ObjectPermissionError):
        with _handle_client_exception():
            raise ClientException('error', http_status=403)

    # 404 error
    with pytest.raises(ObjectNotFoundError):
        with _handle_client_exception():
            raise ClientException('error', http_status=404)

    # Any error
    with pytest.raises(ClientException):
        with _handle_client_exception():
            raise ClientException('error', http_status=500)


def test_mocked_storage():
    """Tests pycosio.swift with a mock"""
    from json import loads
    import swiftclient
    from pycosio.storage.swift import SwiftRawIO, _SwiftSystem, SwiftBufferedIO

    from tests.test_storage import StorageTester
    from tests.storage_mock import ObjectStorageMock

    # Mocks Swift client

    def raise_404():
        """Raise 404 error"""
        raise swiftclient.ClientException('error', http_status=404)

    def raise_416():
        """Raise 416 error"""
        raise swiftclient.ClientException('error', http_status=416)

    def raise_500():
        """Raise 500 error"""
        raise swiftclient.ClientException('error', http_status=500)

    storage_mock = ObjectStorageMock(
        raise_404, raise_416, raise_500, swiftclient.ClientException)

    class Connection:
        """swiftclient.client.Connection"""

        def __init__(self, *_, **kwargs):
            self.kwargs = kwargs

        @staticmethod
        def get_auth():
            """swiftclient.client.Connection.get_auth"""
            return '###',

        @staticmethod
        def get_object(container, obj, headers=None, **_):
            """swiftclient.client.Connection.get_object"""
            return (storage_mock.head_object(container, obj),
                    storage_mock.get_object(container, obj, header=headers))

        @staticmethod
        def head_object(container, obj, **_):
            """swiftclient.client.Connection.head_object"""
            return storage_mock.head_object(container, obj)

        @staticmethod
        def put_object(container, obj, contents, query_string=None, **_):
            """swiftclient.client.Connection.put_object"""
            # Concatenates object parts
            if query_string == 'multipart-manifest=put':
                manifest = loads(contents)
                parts = []
                for part in manifest:
                    path = part['path'].split(container + '/')[1]
                    parts.append(path)

                    # Check manifest format
                    assert path.startswith(obj)
                    assert part['etag']

                storage_mock.concat_objects(container, obj, parts)

            # Single object upload
            else:
                storage_mock.put_object(container, obj, contents)

            # Return Etag
            return '123'

        @staticmethod
        def delete_object(container, obj, **_):
            """swiftclient.client.Connection.delete_object"""
            storage_mock.delete_object(container, obj)

        @staticmethod
        def put_container(container, **_):
            """swiftclient.client.Connection.put_container"""
            storage_mock.put_locator(container)

        @staticmethod
        def head_container(container=None, **_):
            """swiftclient.client.Connection.head_container"""
            return storage_mock.head_locator(container)

        @staticmethod
        def delete_container(container, **_):
            """swiftclient.client.Connection.delete_container"""
            storage_mock.delete_locator(container)

        @staticmethod
        def copy_object(container=None, obj=None, destination=None, **_):
            """swiftclient.client.Connection.copy_object"""
            storage_mock.copy_object(obj, destination, src_locator=container)

        @staticmethod
        def get_container(container, limit=None, prefix=None, **_):
            """swiftclient.client.Connection.get_container"""
            objects = []

            for name, header in storage_mock.get_locator(
                    container, prefix=prefix, limit=limit).items():
                header['name'] = name
                objects.append(header)

            return storage_mock.head_locator(container), objects

        @staticmethod
        def get_account():
            """swiftclient.client.Connection.get_account"""
            objects = []
            for name, header in storage_mock.get_locators().items():
                header['name'] = name
                objects.append(header)

            return {}, objects

    swiftclient_client_connection = swiftclient.client.Connection
    swiftclient.client.Connection = Connection

    # Tests
    try:
        # Init mocked system
        system = _SwiftSystem()
        storage_mock.attach_io_system(system)

        # Tests
        with StorageTester(
                system, SwiftRawIO, SwiftBufferedIO, storage_mock,
                unsupported_operations=UNSUPPORTED_OPERATIONS) as tester:

            # Common tests
            tester.test_common()

            # Test: Unsecure mode
            with SwiftRawIO(
                    tester.base_dir_path + 'file0.dat', unsecure=True) as file:
                assert file._client.kwargs['ssl_compression'] is False

    # Restore mocked functions
    finally:
        swiftclient.client.Connection = swiftclient_client_connection
