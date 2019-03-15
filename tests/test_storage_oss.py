# coding=utf-8
"""Test pycosio.storage.oss"""
import pytest

UNSUPPORTED_OPERATIONS = (
    'copy',

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
    from pycosio.storage.oss import OSSRawIO, _OSSSystem, OSSBufferedIO

    from oss2.exceptions import OssError
    import oss2

    from tests.test_storage import StorageTester
    from tests.storage_mock import ObjectStorageMock

    # Mocks oss2 client

    def raise_404():
        """Raise 404 error"""
        raise OssError(404, headers={}, body=None, details={'Message': ''})

    def raise_416():
        """Raise 416 error"""
        raise OssError(416, headers={}, body=None, details={'Message': ''})

    def raise_500():
        """Raise 500 error"""
        raise OssError(500, headers={}, body=None, details={'Message': ''})

    storage_mock = ObjectStorageMock(
        raise_404, raise_416, raise_500, OssError)

    # TODO: OSS mock using storage mock

    class Auth:
        """oss2.Auth/oss2.StsAuth/oss2.AnonymousAuth"""

        def __init__(self, **_):
            """oss2.Auth.__init__"""

    class Bucket:
        """oss2.Bucket"""

        def __init__(self, *_, **__):
            """oss2.Bucket.__init__"""

        @staticmethod
        def get_object(key=None, headers=None, **_):
            """oss2.Bucket.get_object"""

        @staticmethod
        def head_object(key=None, **_):
            """oss2.Bucket.head_object"""

        @staticmethod
        def put_object(key=None, data=None, **_):
            """oss2.Bucket.put_object"""

        @staticmethod
        def delete_object(key=None, **_):
            """oss2.Bucket.delete_object"""

        @staticmethod
        def get_bucket_info(**_):
            """oss2.Bucket.get_bucket_info"""

        @staticmethod
        def create_bucket(**_):
            """oss2.Bucket.create_bucket"""

        @staticmethod
        def delete_bucket(**_):
            """oss2.Bucket.delete_bucket"""

        @staticmethod
        def list_objects(**_):
            """oss2.Bucket.list_objects"""

        @staticmethod
        def init_multipart_upload(key=None, **_):
            """oss2.Bucket.init_multipart_upload"""

        @staticmethod
        def complete_multipart_upload(
                key=None, upload_id=None, parts=None, **_):
            """oss2.Bucket.complete_multipart_upload"""

        @staticmethod
        def upload_part(key=None, upload_id=None,
                        part_number=None, data=None, **_):
            """oss2.Bucket.upload_part"""

    class Service:
        """oss2.Service"""

        def __init__(self, *_, **__):
            """oss2.Service.__init__"""

        @staticmethod
        def list_buckets(**_):
            """oss2.Service.list_buckets"""

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
        system = _OSSSystem()
        storage_mock.attach_io_system(system)

        # Tests
        with StorageTester(
                system, OSSRawIO, OSSBufferedIO, storage_mock,
                unsupported_operations=UNSUPPORTED_OPERATIONS) as tester:

            # Common tests
            tester.test_common()

    # Restore mocked functions
    finally:
        oss2.Auth = oss2_auth
        oss2.StsAuth = oss2_stsauth
        oss2.AnonymousAuth = oss2_anonymousauth
        oss2.Bucket = oss2_bucket
        oss2.Service = oss2_service


def test_oss_raw_io():
    """Tests pycosio.oss.OSSRawIO"""
    # TODO: remove tests once "test_mocked_storage" complete

    pytest.skip('Deprecated')
    import io
    import time
    from wsgiref.handlers import format_date_time
    from tests.utilities import (BYTE, SIZE, parse_range, check_head_methods,
                                 check_raw_read_methods)
    from pycosio.storage.oss import OSSRawIO, _OSSSystem
    from pycosio._core.exceptions import ObjectNotFoundError
    from oss2.exceptions import OssError
    import oss2

    # Initializes some variables
    bucket = 'bucket'
    key_value = 'key'
    client_args = dict(bucket_name=bucket, key=key_value)
    path = '%s/%s' % (bucket, key_value)
    url = 'oss://' + path
    bucket_url = 'oss://' + bucket
    oss_endpoint = 'https://oss-nowhere.aliyuncs.com'
    put_object_called = []
    delete_object_called = []
    create_bucket_called = []
    delete_bucket_called = []
    m_time = time.time()
    oss_object = None
    raises_exception = False
    error_kwargs = dict(headers={}, body=None, details={})
    storage_kwargs = dict(endpoint=oss_endpoint, auth='auth')
    object_header = {'Content-Length': SIZE,
                     'Last-Modified': format_date_time(m_time)}
    max_keys = None
    next_marker = []
    list_objects_header = dict(
        last_modified=int(m_time), etag='etag',
        type='type', size=SIZE, storage_class='storage_class')
    no_objects = []
    exists_in_parent = False

    # Mocks oss2

    class Response:
        """Dummy head_object response"""

        def __init__(self):
            """Returns fake result"""
            self.headers = object_header.copy()

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
            assert key.startswith(key_value)
            if key[-1] == '/':
                assert data == b''
            else:
                assert len(data) == len(oss_object._write_buffer)
            put_object_called.append(1)

        @staticmethod
        def delete_object(key=None, **_):
            """Check arguments and returns fake value"""
            assert key.startswith(key_value)
            delete_object_called.append(1)

        @staticmethod
        def get_bucket_info():
            """Returns fake value"""
            response = Response()
            response.headers = dict(bucket_name=bucket)
            return response

        @staticmethod
        def create_bucket():
            """Returns fake value"""
            create_bucket_called.append(1)

        @staticmethod
        def delete_bucket():
            """Returns fake value"""
            delete_bucket_called.append(1)

        @staticmethod
        def list_objects(**kwargs):
            """Returns fake value"""
            assert 'prefix' in kwargs
            if max_keys:
                assert 'max_keys' in kwargs

            response = ListObjectsResult()

            if not next_marker:
                response.next_marker = 'marker'
                next_marker.append(1)

            return response

    class ListBucketsResult:
        """Dummy oss2.models.ListBucketsResult"""

        def __init__(self):
            self.is_truncated = False
            self.next_marker = ''
            self.buckets = [oss2.models.SimplifiedBucketInfo(
                bucket, 'location', int(m_time), 'extranet_endpoint',
                'intranet_endpoint', 'storage_class')]

    class ListObjectsResult:
        """Dummy oss2.models.ListObjectsResult"""

        def __init__(self):
            self.is_truncated = False
            self.next_marker = ''

            self.object_list = [] if no_objects else [
                oss2.models.SimplifiedObjectInfo(
                    key=key_value, **list_objects_header)]
            # Parent exists
            if exists_in_parent and no_objects:
                no_objects.pop(0)
            self.prefix_list = []

    class Service:
        """Dummy Service"""

        def __init__(self, auth, endpoint, **__):
            """Checks arguments"""
            assert isinstance(auth, Auth)
            assert endpoint == oss_endpoint

        @staticmethod
        def list_buckets():
            """Returns fake value"""
            return ListBucketsResult()

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

        oss_system = _OSSSystem(storage_parameters=storage_kwargs)

        # Tests head
        check_head_methods(oss_system, m_time, path=path)
        assert oss_system.head(path=bucket_url)['bucket_name'] == bucket

        # Tests islink
        assert oss_system.islink(path=bucket_url) is False
        assert oss_system.islink(header={'type': 'Symlink'}) is True
        assert oss_system.islink(
            header={'x-oss-object-type': 'Symlink'}) is True

        # Tests create directory
        oss_system.make_dir(bucket_url)
        assert len(create_bucket_called) == 1
        oss_system.make_dir(url)
        assert len(put_object_called) == 1
        put_object_called = []

        # Tests remove
        oss_system.remove(bucket_url)
        assert len(delete_bucket_called) == 1
        oss_system.remove(url)
        assert len(delete_object_called) == 1
        put_object_called = []

        # Tests _list_locators
        assert list(oss_system._list_locators()) == [
            (bucket, dict(location='location', creation_date=int(m_time),
                          extranet_endpoint='extranet_endpoint',
                          intranet_endpoint='intranet_endpoint',
                          storage_class='storage_class'))]

        # Tests _list_objects
        assert list(oss_system._list_objects(
            client_args.copy(), '', max_keys)) == [
                   (key_value, list_objects_header)] * 2

        max_keys = 10
        assert list(oss_system._list_objects(
            client_args, '', max_keys)) == [
                   (key_value, list_objects_header)]

        no_objects.append(1)
        with pytest.raises(ObjectNotFoundError):
            assert list(oss_system._list_objects(
                client_args, '', max_keys))

        with pytest.raises(ObjectNotFoundError):
            # Checks parent dir
            assert list(oss_system._list_objects(
                client_args, 'dir1/dir2', max_keys))

        exists_in_parent = True
        assert list(oss_system._list_objects(
            client_args, 'dir1/dir2', max_keys)) == []

        # Tests path and URL handling
        oss_object = OSSRawIO(url, storage_parameters=storage_kwargs)
        assert oss_object._client_kwargs == client_args
        assert oss_object.name == url

        oss_object = OSSRawIO(path, storage_parameters=storage_kwargs)
        assert oss_object._client_kwargs == client_args
        assert oss_object.name == path

        # Tests read
        check_raw_read_methods(oss_object)

        # Tests _read_range don't hide oss2 exceptions
        raises_exception = True
        with pytest.raises(OssError):
            assert oss_object.read(10)

        # Tests _flush
        oss_object = OSSRawIO(url, mode='w', storage_parameters=storage_kwargs)
        assert not put_object_called
        oss_object.write(50 * BYTE)
        oss_object.flush()
        assert put_object_called == [1]

        # Tests unsecure
        assert _OSSSystem(storage_parameters=storage_kwargs,
                          unsecure=False)._endpoint == oss_endpoint
        assert _OSSSystem(
            storage_parameters=storage_kwargs,
            unsecure=True)._endpoint == oss_endpoint.replace('https', 'http')

        # Tests no endpoint
        with pytest.raises(ValueError):
            oss_object = OSSRawIO(url)

    # Restore mocked class
    finally:
        oss2.Auth = oss2_auth
        oss2.StsAuth = oss2_stsauth
        oss2.AnonymousAuth = oss2_anonymousauth
        oss2.Bucket = oss2_bucket
        oss2.Service = oss2_service


def test_oss_buffered_io():
    """Tests pycosio.oss.OSSBufferedIO"""
    # TODO: remove tests once "test_mocked_storage" complete

    pytest.skip('Deprecated')
    import time
    from wsgiref.handlers import format_date_time
    from tests.utilities import BYTE, SIZE
    from pycosio.storage.oss import OSSBufferedIO
    import oss2

    # Mocks client
    bucket = 'bucket'
    key_value = 'key'
    path = '%s/%s' % (bucket, key_value)
    storage_kwargs = dict(endpoint='https://oss-nowhere.aliyuncs.com')

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
    oss2_anonymousauth = oss2.AnonymousAuth
    oss2_bucket = oss2.Bucket
    oss2.Auth = Auth
    oss2.StsAuth = Auth
    oss2.AnonymousAuth = Auth
    oss2.Bucket = Bucket

    # Tests
    try:
        # Write and flush using multipart upload
        with OSSBufferedIO(path, mode='w',
                           storage_parameters=storage_kwargs) as oss_object:
            oss_object._buffer_size = 10
            oss_object.write(BYTE * 95)

        # Tests read mode instantiation
        OSSBufferedIO(path, mode='r', storage_parameters=storage_kwargs)

    # Restore mocked class
    finally:
        oss2.Auth = oss2_auth
        oss2.StsAuth = oss2_stsauth
        oss2.AnonymousAuth = oss2_anonymousauth
        oss2.Bucket = oss2_bucket