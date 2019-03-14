# coding=utf-8
"""Test pycosio.storage.s3"""
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


def test_s3_mocked():
    """Tests pycosio.s3 with a mock"""
    from datetime import datetime
    from io import BytesIO, UnsupportedOperation

    from pycosio.storage.s3 import S3RawIO, _S3System, S3BufferedIO

    from botocore.exceptions import ClientError
    import boto3

    from tests.storage_common import StorageTester
    from tests.storage_mock import ObjectStorageMock

    # Mocks boto3 client

    def raise_404():
        """Raise 404 error"""
        raise ClientError({
            'Error': {'Code': '404', 'Message': 'Error'}}, 'Error')

    def raise_416():
        """Raise 416 error"""
        raise ClientError({
            'Error': {'Code': 'InvalidRange', 'Message': 'Error'}}, 'Error')

    def raise_500():
        """Raise 500 error"""
        raise ClientError({
            'Error': {'Code': 'Error', 'Message': 'Error'}}, 'Error')

    storage_mock = ObjectStorageMock(
        raise_404, raise_416, raise_500, ClientError,
        format_date=datetime.fromtimestamp)

    no_head = False

    class Client:
        """boto3.client"""

        def __init__(self, *_, **kwargs):
            """boto3.client.__init__"""
            self.kwargs = kwargs

        @staticmethod
        def get_object(Bucket=None, Key=None, Range=None, **_):
            """boto3.client.get_object"""
            return dict(Body=BytesIO(
                storage_mock.get_object(Bucket, Key, header=dict(Range=Range))))

        @staticmethod
        def head_object(Bucket=None, Key=None, **_):
            """boto3.client.head_object"""
            if no_head:
                return dict()
            return storage_mock.head_object(Bucket, Key)

        @staticmethod
        def put_object(Bucket=None, Key=None, Body=None, **_):
            """boto3.client.put_object"""
            storage_mock.put_object(Bucket, Key, Body)

        @staticmethod
        def delete_object(Bucket=None, Key=None, **_):
            """boto3.client.delete_object"""
            storage_mock.delete_object(Bucket, Key)

        @staticmethod
        def head_bucket(Bucket=None, **_):
            """boto3.client.head_bucket"""
            return storage_mock.head_locator(Bucket)

        @staticmethod
        def create_bucket(Bucket=None, **_):
            """boto3.client.create_bucket"""
            storage_mock.put_locator(Bucket)

        @staticmethod
        def copy_object(Bucket=None, Key=None, CopySource=None, **_):
            """boto3.client.copy_object"""
            storage_mock.copy_object(
                CopySource['Key'], Key, dst_locator=Bucket,
                src_locator=CopySource['Bucket'])

        @staticmethod
        def delete_bucket(Bucket=None, **_):
            """boto3.client.delete_bucket"""
            storage_mock.delete_locator(Bucket)

        @staticmethod
        def list_objects_v2(Bucket=None, Prefix=None, MaxKeys=None, **_):
            """boto3.client.list_objects_v2"""
            objects = []

            for name, header in storage_mock.get_locator(
                    Bucket, prefix=Prefix, limit=MaxKeys,
                    raise_404_if_empty=False).items():

                header['Key'] = name
                objects.append(header)

            if not objects:
                return dict()
            return dict(Contents=objects)

        @staticmethod
        def list_buckets():
            """boto3.client.list_buckets"""
            objects = []
            for name, header in storage_mock.get_locators().items():
                header['Name'] = name
                objects.append(header)

            return dict(Buckets=objects)

        @staticmethod
        def create_multipart_upload(**_):
            """boto3.client.create_multipart_upload"""
            return dict(UploadId=123)

        @staticmethod
        def complete_multipart_upload(
                Bucket=None, Key=None, MultipartUpload=None,
                UploadId=None, **_):
            """boto3.client.complete_multipart_upload"""
            uploaded_parts = MultipartUpload['Parts']
            assert UploadId == 123

            parts = []
            for part in uploaded_parts:
                parts.append(Key + str(part['PartNumber']))
                assert part['ETag'] == 456

            storage_mock.concat_objects(Bucket, Key, parts)

        @staticmethod
        def upload_part(Bucket=None, Key=None, PartNumber=None,
                        Body=None, UploadId=None, **_):
            """boto3.client.upload_part"""
            assert UploadId == 123
            storage_mock.put_object(Bucket, Key + str(PartNumber), Body)
            return dict(ETag=456)

    class Session:
        """boto3.session.Session"""
        client = Client
        region_name = ''

        def __init__(self, *_, **__):
            """boto3.session.Session.__init__"""

    boto3_client = boto3.client
    boto3_session_session = boto3.session.Session
    boto3.client = Client
    boto3.session.Session = Session

    # Tests
    try:
        # Init mocked system
        system = _S3System()
        storage_mock.attach_io_system(system)

        # Tests
        with StorageTester(
                system, S3RawIO, S3BufferedIO, storage_mock) as tester:

            # Common tests
            tester.test_common()

            # Test: Unsecure mode
            file_path = tester.base_dir_path + 'file0.dat'
            with S3RawIO(file_path, unsecure=True) as file:
                assert file._client.kwargs['use_ssl'] is False

            # Test: Header values may be missing
            no_head = True
            with pytest.raises(UnsupportedOperation):
                system.getctime(file_path)
            with pytest.raises(UnsupportedOperation):
                system.getmtime(file_path)
            with pytest.raises(UnsupportedOperation):
                system.getsize(file_path)
            no_head = False

    # Restore mocked functions
    finally:
        boto3.client = boto3_client
        boto3.session.Session = boto3_session_session
