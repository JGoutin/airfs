# coding=utf-8
"""Test pycosio.swift"""
import json
import time
from wsgiref.handlers import format_date_time

import pytest

from tests.utilities import (
    parse_range, check_head_methods, check_raw_read_methods, BYTE)


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


def test_swift_raw_io():
    """Tests pycosio.swift.SwiftRawIO _SwiftSystem"""
    import swiftclient
    from pycosio.storage.swift import SwiftRawIO, _SwiftSystem

    # Initializes some variables
    m_time = time.time()
    container_name = 'container'
    object_name = 'object'
    path = '/'.join((container_name, object_name))
    raises_exception = False
    put_object_called = []
    delete_object_called = []
    put_container_called = []
    delete_container_called = []

    # Mocks swiftclient

    class Connection:
        """Fake Connection"""

        def __init__(self, *_, **__):
            """Do nothing"""

        @staticmethod
        def get_auth():
            """Do Nothing"""
            return '###',

        @staticmethod
        def get_object(container, obj, headers=None, **_):
            """Check arguments and returns fake result"""
            assert container == container_name
            assert obj == object_name

            if raises_exception:
                raise swiftclient.ClientException(
                    'error', http_status=500)

            try:
                content = parse_range(headers)
            except ValueError:
                raise swiftclient.ClientException(
                    'error', http_status=416)

            return dict(), content

        @staticmethod
        def head_object(container, obj, **_):
            """Check arguments and returns fake result"""
            assert container == container_name
            assert obj == object_name

            return {'content-length': '100',
                    'last-modified': format_date_time(m_time)}

        @staticmethod
        def put_object(container, obj, contents, **_):
            """Check arguments and returns fake result"""
            assert container == container_name
            assert obj.startswith(object_name)
            if obj[-1] != '/':
                assert contents
            else:
                assert contents == b''
            put_object_called.append(1)

        @staticmethod
        def delete_object(container, obj):
            """Check arguments"""
            assert container == container_name
            assert obj.startswith(object_name)
            delete_object_called.append(1)

        @staticmethod
        def put_container(container, **_):
            """Check arguments and returns fake result"""
            assert container == container_name
            put_container_called.append(1)

        @staticmethod
        def head_container(**kwargs):
            """Check arguments and returns fake value"""
            assert 'obj' not in kwargs
            assert 'container' in kwargs
            return dict(container=kwargs['container'])

        @staticmethod
        def delete_container(container, **_):
            """Check arguments and returns fake result"""
            assert container == container_name
            delete_container_called.append(1)

        @staticmethod
        def copy_object(**kwargs):
            """Check arguments"""
            for key in ('container', 'obj', 'destination'):
                assert key in kwargs

    swiftclient_client_connection = swiftclient.client.Connection
    swiftclient.client.Connection = Connection

    # Tests
    try:
        swift_object = SwiftRawIO(path)
        swift_system = _SwiftSystem()

        # Tests head
        check_head_methods(swift_system, m_time, path=path)
        assert swift_system.head(
            path=container_name)['container'] == container_name

        # Tests create directory
        swift_system.make_dir(container_name)
        assert len(put_container_called) == 1
        swift_system.make_dir(path)
        assert len(put_object_called) == 1
        put_object_called = []

        # Tests remove
        swift_system.remove(container_name)
        assert len(delete_container_called) == 1
        swift_system.remove(path)
        assert len(delete_object_called) == 1
        put_object_called = []

        # Tests copy
        swift_system.copy(path, path)

        # Tests read
        check_raw_read_methods(swift_object)

        # Test read not block other exceptions
        raises_exception = True
        swift_object = SwiftRawIO(path)
        with pytest.raises(swiftclient.ClientException):
            swift_object.read(10)
        raises_exception = False

        # Tests _flush
        swift_object = SwiftRawIO(path, mode='w')
        assert not put_object_called
        swift_object.write(50 * BYTE)
        swift_object.flush()
        assert len(put_object_called) == 1

    # Restore mocked functions
    finally:
        swiftclient.client.Connection = swiftclient_client_connection


def test_swift_buffered_io():
    """Tests pycosio.swift.SwiftBufferedIO"""
    from pycosio.storage.swift import SwiftBufferedIO
    import swiftclient

    # Initializes some variables
    container_name = 'container'
    object_name = 'object'
    path = '/'.join((container_name, object_name))

    # Mocks swiftclient

    class Connection:
        """Fake Connection"""

        def __init__(self, *_, **kwargs):
            """Do nothing"""
            self.kwargs = kwargs
            self.called = 0

        @staticmethod
        def get_auth():
            """Do Nothing"""
            return '###',

        @staticmethod
        def get_object(*_, **__):
            """Do nothing"""

        @staticmethod
        def head_object(*_, **__):
            """Do nothing"""
            return {'content-length': '100',
                    'last-Modified': format_date_time(time.time())}

        def put_object(self, container, obj, contents, query_string=None, **_):
            """Check arguments and returns fake result"""
            assert container == container_name

            # Check manifest
            if query_string:
                assert obj == object_name
                manifest = json.loads(contents)
                assert len(manifest) == self.called
                for part in manifest:
                    assert part['etag'] == '123'
                    assert part['path'].startswith(
                        '/'.join((container, object_name)))

            # Check part
            else:
                assert obj.startswith(object_name)
                assert contents
                self.called += 1

            # Returns ETag
            return '123'

    swiftclient_client_connection = swiftclient.client.Connection
    swiftclient.client.Connection = Connection

    # Tests
    try:
        # Write and flush using multipart upload
        swift_object = SwiftBufferedIO(path, mode='w')
        swift_object._buffer_size = 10

        swift_object.write(BYTE * 95)
        swift_object.close()

        # Tests unsecure
        with SwiftBufferedIO(path, mode='w', unsecure=True) as swift_object:
            assert swift_object._client.kwargs['ssl_compression'] is False

        # Tests read mode instantiation
        SwiftBufferedIO(path, mode='r')

    # Restore mocked functions
    finally:
        swiftclient.client.Connection = swiftclient_client_connection
