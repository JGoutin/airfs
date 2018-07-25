# coding=utf-8
"""Test pycosio.s3"""
from datetime import datetime
import io
import time
from tests.utilities import BYTE, SIZE, parse_range, check_head_methods, check_raw_read_methods

import pytest


def test_handle_io_exceptions():
    """Test pycosio.s3._handle_io_exceptions"""
    from pycosio.s3 import _handle_io_exceptions
    from botocore.exceptions import ClientError

    response = {'Error': {'Code': 'ErrorCode', 'Message': 'Error'}}

    # Any error
    with pytest.raises(ClientError):
        with _handle_io_exceptions():
            raise ClientError(response, 'testing')

    # 404 error
    response['Error']['Code'] = '404'
    with pytest.raises(OSError):
        with _handle_io_exceptions():
            raise ClientError(response, 'testing')

    # 403 error
    response['Error']['Code'] = '403'
    with pytest.raises(OSError):
        with _handle_io_exceptions():
            raise ClientError(response, 'testing')


def test_s3_raw_io():
    """Tests pycosio.s3.S3RawIO"""
    from pycosio.s3 import S3RawIO
    from botocore.exceptions import ClientError
    import boto3

    # Initializes some variables
    bucket = 'bucket'
    key_value = 'key'
    client_args = dict(Bucket=bucket, Key=key_value)
    path = '%s/%s' % (bucket, key_value)
    url = 's3://' + path
    put_object_called = []
    m_time = time.time()
    s3object = None
    raises_exception = False

    # Mocks boto3 client

    client_error_response = {
        'Error': {'Code': 'Error', 'Message': 'Error'}}

    class Client:
        """Dummy client"""

        def __init__(self, *_, **__):
            """Do nothing"""

        @staticmethod
        def get_object(**kwargs):
            """Mock boto3 get_object
            Check arguments and returns fake value"""
            if raises_exception:
                client_error_response['Error']['Code'] = 'Error'
                raise ClientError(client_error_response, 'Error')

            for key, value in client_args.items():
                assert key in kwargs
                assert kwargs[key] == value

            try:
                content = parse_range(kwargs)
            except ValueError:
                # EOF reached
                client_error_response['Error']['Code'] = 'InvalidRange'
                raise ClientError(client_error_response, 'EOF')

            return dict(Body=io.BytesIO(content))

        @staticmethod
        def head_object(**kwargs):
            """Mock boto3 head_object
            Check arguments and returns fake value"""
            assert kwargs == client_args
            return dict(
                ContentLength=SIZE,
                LastModified=datetime.fromtimestamp(m_time))

        @staticmethod
        def put_object(**kwargs):
            """Mock boto3 put_object
            Check arguments and returns fake value"""
            for key, value in client_args.items():
                assert key in kwargs
                assert kwargs[key] == value
            assert len(kwargs['Body']) == len(s3object._write_buffer)
            put_object_called.append(1)

    class Session:
        """Dummy Session"""
        client = Client

        def __init__(self, *_, **__):
            """Do nothing"""

    boto3_client = boto3.client
    boto3_session_session = boto3.session.Session
    boto3.client = Client
    boto3.session.Session = Session

    # Tests
    try:
        # Tests path and URL handling
        s3object = S3RawIO(url)
        assert s3object._client_kwargs == client_args
        assert s3object.name == url

        s3object = S3RawIO(path)
        assert s3object._client_kwargs == client_args
        assert s3object.name == url

        # Tests _head
        check_head_methods(s3object, m_time)

        # Tests read
        check_raw_read_methods(s3object)

        # Tests _read_range don't hide Boto exceptions
        raises_exception = True
        with pytest.raises(ClientError):
            assert s3object.read(10)

        s3object = S3RawIO(url, mode='w')

        # Tests _flush
        assert not put_object_called
        s3object.write(50 * BYTE)
        s3object.flush()
        assert put_object_called == [1]

    # Restore mocked class
    finally:
        boto3.client = boto3_client
        boto3.session.Session = boto3_session_session


def test_s3_buffered_io():
    """Tests pycosio.s3.S3BufferedIO"""
    from pycosio.s3 import S3BufferedIO, _upload_part
    import boto3

    # Mocks client
    bucket = 'bucket'
    key_value = 'key'
    client_args = dict(Bucket=bucket, Key=key_value)
    path = '%s/%s' % (bucket, key_value)

    class Client:
        """Dummy client"""

        def __init__(self, *_, **__):
            """Do nothing"""

        @staticmethod
        def create_multipart_upload(**kwargs):
            """Checks arguments and returns fake result"""
            for key, value in client_args.items():
                assert key in kwargs
                assert kwargs[key] == value
            return dict(UploadId=123)

        @staticmethod
        def complete_multipart_upload(**kwargs):
            """Checks arguments and returns fake result"""
            for key, value in client_args.items():
                assert key in kwargs
                assert kwargs[key] == value
            uploaded_parts = kwargs['MultipartUpload']['Parts']
            assert 10 == len(uploaded_parts)
            for index, part in enumerate(uploaded_parts):
                assert part['PartNumber'] == index + 1
                assert part['ETag'] == 456

        @staticmethod
        def upload_part(**kwargs):
            """Checks arguments and returns fake result"""
            print(1)
            assert kwargs['PartNumber'] > 0
            assert kwargs['PartNumber'] <= 10
            assert kwargs['Body'] == BYTE * (
                5 if kwargs['PartNumber'] == 10 else 10)
            assert kwargs['UploadId'] == 123
            for key, value in client_args.items():
                assert key in kwargs
                assert kwargs[key] == value
            return dict(ETag=456)

        @staticmethod
        def put_object(**_):
            """Do nothing"""

        @staticmethod
        def get_object(**_):
            """Do nothing"""

        @staticmethod
        def head_object(**_):
            """Do nothing"""

    class Session:
        """Dummy Session"""
        client = Client

        def __init__(self, *_, **__):
            """Do nothing"""

    boto3_client = boto3.client
    boto3_session_session = boto3.session.Session
    boto3.client = Client
    boto3.session.Session = Session

    # Tests
    try:
        # Write and flush using multipart upload
        s3object = S3BufferedIO(path, mode='w')
        assert s3object._upload_part is s3object._client.upload_part
        s3object._buffer_size = 10

        s3object.write(BYTE * 95)
        s3object.close()

        # Upload_part for ProcessPoolExecutor
        s3object = S3BufferedIO(path, mode='w', workers_type='process')
        assert s3object._upload_part is _upload_part

        assert _upload_part(
            Body=BYTE * 10, PartNumber=1, UploadId=123,
            **client_args) == dict(ETag=456)

    # Restore mocked class
    finally:
        boto3.client = boto3_client
        boto3.session.Session = boto3_session_session
