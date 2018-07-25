# coding=utf-8
"""Test pycosio.swift"""
import time
from wsgiref.handlers import format_date_time

import pytest

from tests.utilities import parse_range, check_head_methods, check_raw_read_methods, BYTE


def test_handle_client_exception():
    """Test pycosio.swift._handle_client_exception"""
    from pycosio.swift import _handle_client_exception
    from swiftclient import ClientException

    # No error
    with _handle_client_exception():
        pass

    # 403 error
    with pytest.raises(OSError):
        with _handle_client_exception():
            raise ClientException('error', http_status=403)

    # 404 error
    with pytest.raises(OSError):
        with _handle_client_exception():
            raise ClientException('error', http_status=404)

    # Any error
    with pytest.raises(ClientException):
        with _handle_client_exception():
            raise ClientException('error', http_status=500)


def test_swift_raw_io():
    """Tests pycosio.swift.SwiftRawIO"""
    from pycosio.swift import SwiftRawIO
    import swiftclient

    # Initializes some variables
    m_time = time.time()
    container_name = 'container'
    object_name = 'object'
    path = '/'.join((container_name, object_name))
    raises_exception = False
    put_object_called = []

    # Mocks swiftclient

    class Connection:
        """Fake Connection"""
        def __init__(self, *_, **__):
            """Do nothing"""

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
                print(1)
                raise swiftclient.ClientException(
                    'error', http_status=416)

            return dict(), content

        @staticmethod
        def head_object(container, obj, **_):
            """Check arguments and returns fake result"""
            assert container == container_name
            assert obj == object_name

            return {'content-length': '100',
                    'last-Modified': format_date_time(m_time)}

        @staticmethod
        def put_object(container, obj, contents, **_):
            """Check arguments and returns fake result"""
            assert container == container_name
            assert obj == object_name
            put_object_called.append(1)


    swiftclient_client_connection = swiftclient.client.Connection
    swiftclient.client.Connection = Connection

    # Tests
    try:
        swift_object = SwiftRawIO(path)

        # Tests _head
        check_head_methods(swift_object, m_time)

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
        assert put_object_called == [1]

    # Restore mocked functions
    finally:
        swiftclient.client.Connection = swiftclient_client_connection
