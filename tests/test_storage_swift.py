# coding=utf-8
"""Test pycosio.storage.swift"""
import pytest


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


def test_swift_mocked():
    """Tests pycosio.swift with a mock"""
    from json import loads
    import swiftclient
    from pycosio.storage.swift import SwiftRawIO, _SwiftSystem, SwiftBufferedIO

    from tests.storage_common import StorageTester
    from tests.storage_mock import ObjectStorageMock

    # Mocks Swift client

    def raise_404():
        """Raise 404 error"""
        raise swiftclient.ClientException('error', http_status=404)

    def raise_416():
        """Raise 416 error"""
        raise swiftclient.ClientException('error', http_status=416)

    storage_mock = ObjectStorageMock(raise_404, raise_416)

    raises_exception = False

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
            # Simulate server exception
            if raises_exception:
                raise swiftclient.ClientException(
                    'error', http_status=500)

            # Get object
            if headers is not None:
                data_range = headers.get('Range')
            else:
                data_range = None
            content = storage_mock.get_object(container, obj, data_range)

            return dict(), content

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
        def delete_object(container, obj):
            """swiftclient.client.Connection.delete_object"""
            storage_mock.delete_object(container, obj)

        @staticmethod
        def put_container(container, **_):
            """swiftclient.client.Connection.put_container"""
            storage_mock.put_locator(container)

        @staticmethod
        def head_container(**kwargs):
            """swiftclient.client.Connection.head_container"""
            return storage_mock.head_locator(kwargs['container'])

        @staticmethod
        def delete_container(container, **_):
            """swiftclient.client.Connection.delete_container"""
            storage_mock.delete_locator(container)

        @staticmethod
        def copy_object(**kwargs):
            """swiftclient.client.Connection.copy_object"""
            storage_mock.copy_object(
                kwargs['container'], kwargs['obj'],
                kwargs['destination'])

        @staticmethod
        def get_container(container, limit=None, prefix=None, **_):
            """swiftclient.client.Connection.get_container"""
            objects = []
            index = 0
            for name, header in storage_mock.get_locator(
                    container, prefix=prefix).items():

                # max_request_entries
                if limit is not None and index >= limit:
                    break
                index += 1

                # File header
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
        swift_system = _SwiftSystem()
        storage_mock.attach_io_system(swift_system)

        # Tests
        with StorageTester(swift_system, SwiftRawIO, SwiftBufferedIO) as tester:

            # Common tests
            tester.run_common_tests()

            # Test: Read not block other exceptions
            file_path = tester.base_dir_path + 'file0.dat'

            raises_exception = True
            swift_object = SwiftRawIO(file_path)
            with pytest.raises(swiftclient.ClientException):
                swift_object.read(10)
            raises_exception = False

            # Test: Unsecure mode
            with SwiftRawIO(file_path, unsecure=True) as swift_object:
                assert swift_object._client.kwargs['ssl_compression'] is False

    # Restore mocked functions
    finally:
        swiftclient.client.Connection = swiftclient_client_connection
