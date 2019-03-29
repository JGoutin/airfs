# coding=utf-8
"""Test pycosio.storage.oss"""
import pytest

UNSUPPORTED_OPERATIONS = (
    # Not supported on some objects
    'getctime',
)


def test_handle_oss_error():
    """Test pycosio.oss._handle_oss_error"""
    from pycosio.storage.oss import _handle_oss_error
    from oss2.exceptions import OssError
    from pycosio._core.exceptions import (
        ObjectNotFoundError, ObjectPermissionError)

    kwargs = dict(headers={}, body=None, details={'Message': ''})

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


def test_mocked_storage():
    """Tests pycosio.oss with a mock"""
    from io import BytesIO

    from pycosio.storage.oss import OSSRawIO, _OSSSystem, OSSBufferedIO

    from oss2.exceptions import OssError
    from oss2.models import HeadObjectResult
    import oss2

    from tests.test_storage import StorageTester
    from tests.storage_mock import ObjectStorageMock

    # Mocks client

    def raise_404():
        """Raise 404 error"""
        raise OssError(404, headers={}, body=None, details={'Message': ''})

    def raise_416():
        """Raise 416 error"""
        raise OssError(416, headers={}, body=None, details={'Message': ''})

    def raise_500():
        """Raise 500 error"""
        raise OssError(500, headers={}, body=None, details={'Message': ''})

    storage_mock = ObjectStorageMock(raise_404, raise_416, raise_500)

    class Auth:
        """oss2.Auth/oss2.StsAuth/oss2.AnonymousAuth"""

        def __init__(self, **_):
            """oss2.Auth.__init__"""

        @staticmethod
        def _sign_request(*_, **__):
            """oss2.Auth._sign_request"""

    class Response:
        """HTTP request response"""
        status = 200
        request_id = 0

        def __init__(self, **attributes):
            for name, value in attributes.items():
                setattr(self, name, value)

    class ListResult(Response):
        """
        oss2.models.ListBucketsResult
        oss2.models.ListObjectsResult
        """
        is_truncated = False
        next_marker = ''

    class Bucket:
        """oss2.Bucket"""

        def __init__(self, auth, endpoint, bucket_name=None, *_, **__):
            """oss2.Bucket.__init__"""
            self._bucket_name = bucket_name

        def get_object(self, key=None, headers=None, **_):
            """oss2.Bucket.get_object"""
            return BytesIO(storage_mock.get_object(
                self._bucket_name, key, header=headers))

        def head_object(self, key=None, **_):
            """oss2.Bucket.head_object"""
            return HeadObjectResult(Response(
                headers=storage_mock.head_object(self._bucket_name, key)))

        def put_object(self, key=None, data=None, **_):
            """oss2.Bucket.put_object"""
            storage_mock.put_object(self._bucket_name, key, data, new_file=True)

        def delete_object(self, key=None, **_):
            """oss2.Bucket.delete_object"""
            storage_mock.delete_object(self._bucket_name, key)

        def get_bucket_info(self, **_):
            """oss2.Bucket.get_bucket_info"""
            return Response(
                headers=storage_mock.head_locator(self._bucket_name))

        def copy_object(self, source_bucket_name=None, source_key=None,
                        target_key=None,**_):
            """oss2.Bucket.copy_object"""
            storage_mock.copy_object(
                src_path=source_key, src_locator=source_bucket_name,
                dst_path=target_key, dst_locator=self._bucket_name)

        def create_bucket(self, **_):
            """oss2.Bucket.create_bucket"""
            storage_mock.put_locator(self._bucket_name)

        def delete_bucket(self, **_):
            """oss2.Bucket.delete_bucket"""
            storage_mock.delete_locator(self._bucket_name)

        def list_objects(self, prefix=None, max_keys=None, **_):
            """oss2.Bucket.list_objects"""
            response = storage_mock.get_locator(
                self._bucket_name, prefix=prefix, limit=max_keys,
                raise_404_if_empty=False)
            object_list = []
            for key, headers in response.items():
                obj = HeadObjectResult(Response(headers=headers))
                obj.key = key
                object_list.append(obj)

            return ListResult(object_list=object_list)

        @staticmethod
        def init_multipart_upload(*_, **__):
            """oss2.Bucket.init_multipart_upload"""
            return Response(upload_id='123')

        def complete_multipart_upload(
                self, key=None, upload_id=None, parts=None, **_):
            """oss2.Bucket.complete_multipart_upload"""
            assert upload_id == '123'
            storage_mock.concat_objects(self._bucket_name, key, [
                key + str(part.part_number) for part in parts
            ])

        def upload_part(self, key=None, upload_id=None,
                        part_number=None, data=None, **_):
            """oss2.Bucket.upload_part"""
            assert upload_id == '123'
            return HeadObjectResult(Response(headers=storage_mock.put_object(
                self._bucket_name, key + str(part_number), data)))

    class Service:
        """oss2.Service"""

        def __init__(self, *_, **__):
            """oss2.Service.__init__"""

        @staticmethod
        def list_buckets(**_):
            """oss2.Service.list_buckets"""
            response = storage_mock.get_locators()
            buckets = []
            for name, headers in response.items():
                bucket = HeadObjectResult(Response(headers=headers))
                bucket.name = name
                buckets.append(bucket)
            return ListResult(buckets=buckets)

    oss2_auth = oss2.Auth
    oss2_stsauth = oss2.StsAuth
    oss2_anonymousauth = oss2.AnonymousAuth
    oss2_bucket = oss2.Bucket
    oss2_service = oss2.Service
    oss2.Auth = Auth
    oss2.StsAuth = Auth
    oss2.AnonymousAuth = Auth
    oss2.Bucket = Bucket
    oss2.Service = Service

    # Tests
    try:
        # Init mocked system
        endpoint = 'https://oss-region.aliyuncs.com'
        system_parameters = dict(storage_parameters=dict(endpoint=endpoint))
        system = _OSSSystem(**system_parameters)
        storage_mock.attach_io_system(system)

        # Tests
        with StorageTester(
                system, OSSRawIO, OSSBufferedIO, storage_mock,
                unsupported_operations=UNSUPPORTED_OPERATIONS,
                system_parameters=system_parameters) as tester:

            # Common tests
            tester.test_common()

            # Test: Missing endpoint
            with pytest.raises(ValueError):
                _OSSSystem()

            # Test: Unsecure mode
            assert _OSSSystem(
                unsecure=False, **system_parameters)._endpoint == endpoint
            assert (_OSSSystem(unsecure=True, **system_parameters)._endpoint ==
                    endpoint.replace('https', 'http'))

            # Test: Symlink
            # TODO: Remove and replace per proper function once implemented
            storage_mock.put_object(
                tester.locator, 'symlink', b'', headers={'type': 'Symlink'})
            assert system.islink(tester.locator + '/symlink')
            assert system.islink(header={'type': 'Symlink'})

    # Restore mocked functions
    finally:
        oss2.Auth = oss2_auth
        oss2.StsAuth = oss2_stsauth
        oss2.AnonymousAuth = oss2_anonymousauth
        oss2.Bucket = oss2_bucket
        oss2.Service = oss2_service
