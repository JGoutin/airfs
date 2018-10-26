# coding=utf-8
"""Test pycosio.s3"""
from datetime import datetime
import io
import time

from tests.utilities import (BYTE, SIZE, parse_range, check_head_methods,
                             check_raw_read_methods)

import pytest


def test_handle_client_error():
    """Test pycosio.s3._handle_client_error"""
    from pycosio.storage.s3 import _handle_client_error
    from botocore.exceptions import ClientError
    from pycosio._core.exceptions import (
        ObjectNotFoundError, ObjectPermissionError)

    response = {'Error': {'Code': 'ErrorCode', 'Message': 'Error'}}

    # Any error
    with pytest.raises(ClientError):
        with _handle_client_error():
            raise ClientError(response, 'testing')

    # 404 error
    response['Error']['Code'] = '404'
    with pytest.raises(ObjectNotFoundError):
        with _handle_client_error():
            raise ClientError(response, 'testing')

    # 403 error
    response['Error']['Code'] = '403'
    with pytest.raises(ObjectPermissionError):
        with _handle_client_error():
            raise ClientError(response, 'testing')


def test_s3_raw_io():
    """Tests pycosio.s3.S3RawIO"""
    from io import UnsupportedOperation
    from pycosio.storage.s3 import S3RawIO, _S3System
    from pycosio._core.exceptions import ObjectNotFoundError
    from botocore.exceptions import ClientError
    import boto3

    # Initializes some variables
    bucket = 'bucket'
    key_value = 'key'
    client_args = dict(Bucket=bucket, Key=key_value)
    path = '%s/%s' % (bucket, key_value)
    url = 's3://' + path
    bucket_url = 's3://' + bucket
    put_object_called = []
    delete_object_called = []
    create_bucket_called = []
    delete_bucket_called = []
    m_time = time.time()
    s3object = None
    raises_exception = False
    no_head = False
    no_objects = False
    continuation_token = []
    max_request_entries = None

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
            return dict() if no_head else dict(
                ContentLength=SIZE,
                LastModified=datetime.fromtimestamp(m_time),
                CreationDate=datetime.fromtimestamp(m_time))

        @staticmethod
        def put_object(**kwargs):
            """Mock boto3 put_object
            Check arguments and returns fake value"""
            if kwargs['Key'][-1] == '/':
                assert kwargs['Body'] == b''
            else:
                for key, value in client_args.items():
                    assert key in kwargs
                    assert kwargs[key] == value
                assert len(kwargs['Body']) == len(s3object._write_buffer)
            put_object_called.append(1)

        @staticmethod
        def delete_object(**kwargs):
            """Mock boto3 delete_object
            Check arguments and returns fake value"""
            for key, value in client_args.items():
                assert key in kwargs
                assert kwargs[key] == value
            delete_object_called.append(1)

        @staticmethod
        def head_bucket(**kwargs):
            """Mock boto3 head_bucket
            Check arguments and returns fake value"""
            assert 'Key' not in kwargs
            assert 'Bucket' in kwargs
            return dict(Bucket=kwargs['Bucket'])

        @staticmethod
        def create_bucket(**kwargs):
            """Mock boto3 create_bucket
            Check arguments and returns fake value"""
            assert 'Key' not in kwargs
            assert 'Bucket' in kwargs
            create_bucket_called.append(1)

        @staticmethod
        def copy_object(CopySource=None, **kwargs):
            """Mock boto3 create_bucket
            Check arguments"""
            for key in ('Key', 'Bucket'):
                assert key in CopySource
                assert key in kwargs

        @staticmethod
        def delete_bucket(**kwargs):
            """Mock boto3 delete_bucket
            Check arguments and returns fake value"""
            assert 'Key' not in kwargs
            assert 'Bucket' in kwargs
            delete_bucket_called.append(1)

        @staticmethod
        def list_objects_v2(**kwargs):
            """Mock boto3 list_objects_v2
            Check arguments and returns fake value"""
            assert 'Bucket' in kwargs
            assert 'Prefix' in kwargs
            if max_request_entries:
                assert 'MaxKeys' in kwargs

            if no_objects:
                return dict()

            response = {'Contents': [{'Key': key_value, 'ContentLength': SIZE}]}
            if not continuation_token:
                response['NextContinuationToken'] = 'token'
                continuation_token.append(1)

            return response

        @staticmethod
        def list_buckets():
            """Mock boto3 list_buckets
            Check arguments and returns fake value"""
            return {'Buckets': [{'Name': bucket, 'ContentLength': SIZE}]}

    class Session:
        """Dummy Session"""
        client = Client
        region_name = ''

        def __init__(self, *_, **__):
            """Do nothing"""

    boto3_client = boto3.client
    boto3_session_session = boto3.session.Session
    boto3.client = Client
    boto3.session.Session = Session

    # Tests
    try:
        s3system = _S3System()

        # Tests head
        check_head_methods(s3system, m_time, c_time=m_time, path=path)
        assert s3system.head(path=bucket_url)['Bucket'] == bucket

        no_head = True
        with pytest.raises(UnsupportedOperation):
            s3system.getctime('path')
        with pytest.raises(UnsupportedOperation):
            s3system.getmtime('path')
        with pytest.raises(UnsupportedOperation):
            s3system.getsize('path')
        no_head = False

        # Tests create directory
        s3system.make_dir(bucket_url)
        assert len(create_bucket_called) == 1
        s3system.make_dir(url)
        assert len(put_object_called) == 1
        put_object_called = []

        # Tests remove
        s3system.remove(bucket_url)
        assert len(delete_bucket_called) == 1
        s3system.remove(url)
        assert len(delete_object_called) == 1
        put_object_called = []

        # Tests copy
        s3system.copy(url, url)

        # Tests _list_locators
        assert list(s3system._list_locators()) == [
            (bucket, dict(ContentLength=SIZE))]

        # Tests _list_objects
        assert list(s3system._list_objects(
            client_args, '', max_request_entries)) == [
                (key_value, dict(ContentLength=SIZE))] * 2

        max_request_entries = 10
        assert list(s3system._list_objects(
            client_args, '', max_request_entries)) == [
                   (key_value, dict(ContentLength=SIZE))]

        no_objects = True
        with pytest.raises(ObjectNotFoundError):
            assert list(s3system._list_objects(
                client_args, '', max_request_entries))

        # Tests path and URL handling
        s3object = S3RawIO(url)
        assert s3object._client_kwargs == client_args
        assert s3object.name == url

        s3object = S3RawIO(path)
        assert s3object._client_kwargs == client_args
        assert s3object.name == path

        # Tests read
        check_raw_read_methods(s3object)

        # Tests _read_range don't hide Boto exceptions
        raises_exception = True
        with pytest.raises(ClientError):
            assert s3object.read(10)

        # Tests _flush
        s3object = S3RawIO(url, mode='w')
        assert not put_object_called
        s3object.write(50 * BYTE)
        s3object.flush()
        assert len(put_object_called) == 1

    # Restore mocked class
    finally:
        boto3.client = boto3_client
        boto3.session.Session = boto3_session_session


def test_s3_buffered_io():
    """Tests pycosio.s3.S3BufferedIO"""
    from pycosio.storage.s3 import S3BufferedIO
    import boto3

    # Mocks client
    bucket = 'bucket'
    key_value = 'key'
    client_args = dict(Bucket=bucket, Key=key_value)
    path = '%s/%s' % (bucket, key_value)

    class Client:
        """Dummy client"""

        def __init__(self, *_, **kwargs):
            """Do nothing"""
            self.kwargs = kwargs

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
            return dict(
                ContentLength=SIZE,
                LastModified=datetime.fromtimestamp(time.time()))

    class Session:
        """Dummy Session"""
        client = Client
        region_name = ''

        def __init__(self, *_, **__):
            """Do nothing"""

    boto3_client = boto3.client
    boto3_session_session = boto3.session.Session
    boto3.client = Client
    boto3.session.Session = Session

    # Tests
    try:
        # Write and flush using multipart upload
        with S3BufferedIO(path, mode='w') as s3object:
            s3object._buffer_size = 10
            s3object.write(BYTE * 95)

        # Tests unsecure
        with S3BufferedIO(path, mode='w', unsecure=True) as s3object:
            assert s3object._client.kwargs['use_ssl'] is False

        # Tests read mode instantiation
        S3BufferedIO(path, mode='r')

    # Restore mocked class
    finally:
        boto3.client = boto3_client
        boto3.session.Session = boto3_session_session
