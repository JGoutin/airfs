# coding=utf-8
"""Test pycosio.oss"""
import io
import time
from wsgiref.handlers import format_date_time

from tests.utilities import (BYTE, SIZE, parse_range, check_head_methods,
                             check_raw_read_methods)

import pytest


def test_handle_oss_error():
    """Test pycosio.oss._handle_oss_error"""
    from pycosio.storage.oss import _handle_oss_error
    from oss2.exceptions import OssError
    from pycosio._core.exceptions import (
        ObjectNotFoundError, ObjectPermissionError)

    kwargs = dict(headers={}, body=None, details={})

    # Any error
    with pytest.raises(OssError):
        with _handle_oss_error():
            raise OssError(416, **kwargs)

    # 404 error
    with pytest.raises(ObjectNotFoundError):
        with _handle_oss_error():
            raise OssError(404, **kwargs)

    # 403 error
    with pytest.raises(ObjectPermissionError):
        with _handle_oss_error():
            raise OssError(403, **kwargs)


def test_oss_raw_io():
    """Tests pycosio.oss.OSSRawIO"""
    from pycosio.storage.oss import OSSRawIO, _OSSSystem
    from oss2.exceptions import OssError
    import oss2

    # Initializes some variables
    bucket = 'bucket'
    key_value = 'key'
    client_args = dict(bucket_name=bucket, key=key_value)
    path = '%s/%s' % (bucket, key_value)
    url = 'oss://' + path
    oss_endpoint = 'https://oss-nowhere.aliyuncs.com'
    put_object_called = []
    m_time = time.time()
    ossobject = None
    raises_exception = False
    error_kwargs = dict(headers={}, body=None, details={})
    storage_kwargs = dict(endpoint=oss_endpoint, auth='auth')

    # Mocks oss2

    class Response:
        """Dummy head_object response"""
        headers = {'Content-Length': SIZE,
                   'Last-Modified': format_date_time(m_time)}

    class Auth:
        """Dummy OSS Auth"""
        def __init__(self, **kwargs):
            """Checks arguments"""
            assert kwargs == dict(auth='auth')

    class Bucket:
        """Dummy Bucket"""

        def __init__(self, auth, endpoint, bucket_name, **__):
            """Checks arguments"""
            assert isinstance(auth, Auth)
            assert endpoint == oss_endpoint
            assert bucket_name == bucket

        @staticmethod
        def get_object(key=None, headers=None, **_):
            """Check arguments and returns fake value"""
            assert key == key_value

            if raises_exception:
                raise OssError(500, **error_kwargs)
            try:
                content = parse_range(headers)
            except ValueError:
                # EOF reached
                raise OssError(416, **error_kwargs)

            return io.BytesIO(content)

        @staticmethod
        def head_object(key=None, **_):
            """Check arguments and returns fake value"""
            assert key == key_value
            return Response()

        @staticmethod
        def put_object(key=None, data=None, **_):
            """Check arguments and returns fake value"""
            assert key == key_value
            assert len(data) == len(ossobject._write_buffer)
            put_object_called.append(1)

    oss2_auth = oss2.Auth
    oss2_stsauth = oss2.StsAuth
    oss2_bucket = oss2.Bucket
    oss2.Auth = Auth
    oss2.StsAuth = Auth
    oss2.Bucket = Bucket

    # Tests
    try:
        # Tests path and URL handling
        ossobject = OSSRawIO(url, storage_parameters=storage_kwargs)
        assert ossobject._client_kwargs == client_args
        assert ossobject.name == url

        ossobject = OSSRawIO(path, storage_parameters=storage_kwargs)
        assert ossobject._client_kwargs == client_args
        assert ossobject.name == path

        # Tests head
        check_head_methods(_OSSSystem(
            storage_parameters=storage_kwargs), m_time, path=path)

        # Tests read
        check_raw_read_methods(ossobject)

        # Tests _read_range don't hide oss2 exceptions
        raises_exception = True
        with pytest.raises(OssError):
            assert ossobject.read(10)

        # Tests _flush
        ossobject = OSSRawIO(url, mode='w', storage_parameters=storage_kwargs)
        assert not put_object_called
        ossobject.write(50 * BYTE)
        ossobject.flush()
        assert put_object_called == [1]

        # Tests unsecure
        assert _OSSSystem(storage_parameters=storage_kwargs,
                          unsecure=False)._endpoint == oss_endpoint
        assert _OSSSystem(
            storage_parameters=storage_kwargs,
            unsecure=True)._endpoint == oss_endpoint.replace('https', 'http')

    # Restore mocked class
    finally:
        oss2.Auth = oss2_auth
        oss2.StsAuth = oss2_stsauth
        oss2.Bucket = oss2_bucket


def test_oss_buffered_io():
    """Tests pycosio.oss.OSSBufferedIO"""
    from pycosio.storage.oss import OSSBufferedIO
    import oss2

    # Mocks client
    bucket = 'bucket'
    key_value = 'key'
    path = '%s/%s' % (bucket, key_value)

    class Response:
        """Dummy response"""
        headers = {'Content-Length': SIZE,
                   'Last-Modified': format_date_time(time.time())}
        etag = 456
        upload_id = 123

    class Auth:
        """Dummy OSS Auth"""

        def __init__(self, *_, **__):
            """Do nothing"""

    class Bucket:
        """Dummy Bucket"""

        def __init__(self, *_, **__):
            """Do nothing"""

        @staticmethod
        def init_multipart_upload(key=None, **_):
            """Check arguments and returns fake value"""
            assert key == key_value
            return Response

        @staticmethod
        def complete_multipart_upload(
                key=None, upload_id=None, parts=None, **_):
            """Checks arguments and returns fake result"""
            assert key == key_value
            assert upload_id == 123

            assert 10 == len(parts)
            for index, part in enumerate(parts):
                assert part.part_number == index + 1
                assert part.etag == 456

        @staticmethod
        def upload_part(key=None, upload_id=None,
                        part_number=None, data=None, **_):
            """Checks arguments and returns fake result"""
            assert key == key_value
            assert upload_id == 123

            assert part_number > 0
            assert part_number <= 10
            assert data == BYTE * (
                5 if part_number == 10 else 10)
            return Response()

        @staticmethod
        def head_object(**_):
            """Returns fake value"""
            return Response()

    oss2_auth = oss2.Auth
    oss2_stsauth = oss2.StsAuth
    oss2_bucket = oss2.Bucket
    oss2.Auth = Auth
    oss2.StsAuth = Auth
    oss2.Bucket = Bucket

    # Tests
    try:
        # Write and flush using multipart upload
        with OSSBufferedIO(path, mode='w') as ossobject:
            ossobject._buffer_size = 10
            ossobject.write(BYTE * 95)

    # Restore mocked class
    finally:
        oss2.Auth = oss2_auth
        oss2.StsAuth = oss2_stsauth
        oss2.Bucket = oss2_bucket
