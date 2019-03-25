# coding=utf-8
"""Test pycosio.storage.azure_file"""
from __future__ import absolute_import  # Python 2: Fix azure import

UNSUPPORTED_OPERATIONS = (
    'symlink',

    # Not supported on some objects
    'getctime',
)


def test_mocked_storage():
    """Tests pycosio.azure_file with a mock"""
    from azure.storage.blob.models import (
        BlobProperties, ContainerProperties, Blob, Container, BlobBlockList,
        _BlobTypes)

    import pycosio.storage.azure_blob as azure_blob
    from pycosio.storage.azure_blob import (
        _AzureBlobSystem, AzureBlobRawIO, AzureBlockBlobRawIO,
        AzurePageBlobRawIO, AzureAppendBlobRawIO, AzureBlobBufferedIO,
        AzurePageBlobBufferedIO)

    from tests.test_storage import StorageTester
    from tests.test_storage_azure import get_storage_mock

    # Mocks client
    storage_mock = get_storage_mock()
    root = 'https://account.blob.core.windows.net'

    class BlobService:
        """azure.storage.blob.baseblobservice.BaseBlobService"""
        BLOB_TYPE = None

        def __init__(self, *_, **kwargs):
            """azure.storage.blob.baseblobservice.BaseBlobService.__init__"""
            self.kwargs = kwargs

        @staticmethod
        def copy_blob(container_name=None, blob_name=None, copy_source=None,
                      **_):
            """azure.storage.blob.baseblobservice.BaseBlobService.copy_blob"""
            copy_source = copy_source.split(root + '/')[1]
            storage_mock.copy_object(
                src_path=copy_source, dst_locator=container_name,
                dst_path=blob_name)

        def get_blob_properties(self, container_name=None, blob_name=None):
            """azure.storage.blob.baseblobservice.BaseBlobService.
            get_blob_properties"""
            args = container_name, blob_name
            props = BlobProperties()
            props.last_modified = storage_mock.get_object_mtime(*args)
            props.content_length = storage_mock.get_object_size(*args)
            props.blob_type = storage_mock.head_object(
                container_name, blob_name)['blob_type']
            return Blob(props=props, name=blob_name)

        @staticmethod
        def get_container_properties(container_name=None, **_):
            """azure.storage.blob.baseblobservice.BaseBlobService.
            get_container_properties"""
            props = ContainerProperties()
            props.last_modified = storage_mock.get_locator_mtime(container_name)
            return Container(props=props, name=container_name)

        @staticmethod
        def list_containers(**_):
            """azure.storage.blob.baseblobservice.BaseBlobService.
            list_containers"""
            containers = []
            for container_name in storage_mock.get_locators():
                props = ContainerProperties()
                props.last_modified = storage_mock.get_locator_mtime(
                    container_name)
                containers.append(Container(props=props, name=container_name))
            return containers

        @staticmethod
        def list_blobs(container_name=None, prefix=None, num_results=None, **_):
            """azure.storage.blob.baseblobservice.BaseBlobService.list_blobs"""
            blobs = []
            for blob_name in storage_mock.get_locator(
                    container_name, prefix=prefix, limit=num_results,
                    raise_404_if_empty=False):
                props = BlobProperties()
                props.last_modified = storage_mock.get_object_mtime(
                    container_name, blob_name)
                props.content_length = storage_mock.get_object_size(
                    container_name, blob_name)
                props.blob_type = storage_mock.head_object(
                    container_name, blob_name)['blob_type']
                blobs.append(Blob(props=props, name=blob_name))
            return blobs

        @staticmethod
        def create_container(container_name=None, **_):
            """azure.storage.blob.baseblobservice.BaseBlobService.
            create_container"""
            storage_mock.put_locator(container_name)

        @staticmethod
        def delete_container(container_name=None, **_):
            """azure.storage.blob.baseblobservice.BaseBlobService.
            delete_container"""
            storage_mock.delete_locator(container_name)

        @staticmethod
        def delete_blob(container_name=None, blob_name=None, **_):
            """azure.storage.blob.baseblobservice.BaseBlobService.delete_blob"""
            storage_mock.delete_object(container_name, blob_name)

        @staticmethod
        def get_blob_to_stream(
                container_name=None, blob_name=None, stream=None,
                start_range=None, end_range=None, **_):
            """azure.storage.blob.baseblobservice.BaseBlobService.
            get_blob_to_stream"""
            if end_range is not None:
                end_range += 1
            stream.write(storage_mock.get_object(
                container_name, blob_name, data_range=(start_range, end_range)))

    class PageBlobService(BlobService):
        """azure.storage.blob.pageblobservice.PageBlobService"""
        BLOB_TYPE = _BlobTypes.PageBlob

        def create_blob(self, container_name=None, blob_name=None,
                        content_length=None, **_):
            """azure.storage.blob.pageblobservice.PageBlobService.create_blob"""
            if content_length:
                # Must be page aligned
                assert not content_length % 512

                # Create null pages
                content = b'\0' * content_length
            else:
                content = None

            storage_mock.put_object(
                container_name, blob_name, content=content, headers=dict(
                    blob_type=self.BLOB_TYPE), new_file=True)

        def create_blob_from_bytes(
                self, container_name=None, blob_name=None, blob=None, **_):
            """azure.storage.blob.pageblobservice.PageBlobService.
            create_blob_from_bytes"""
            # Must be page aligned
            assert not len(blob) % 512

            storage_mock.put_object(
                container_name, blob_name, content=blob, headers=dict(
                    blob_type=self.BLOB_TYPE), new_file=True)

        def resize_blob(self, container_name=None, blob_name=None,
                        content_length=None, **_):
            """azure.storage.blob.pageblobservice.PageBlobService.
            resize_blob"""
            # Must be page aligned
            assert not content_length % 512

            # Add padding to resize blob
            size = storage_mock.get_object_size(container_name, blob_name)
            padding = content_length - size
            storage_mock.put_object(
                container_name, blob_name, content=b'\0' * padding,
                data_range=(content_length - padding, content_length))

        @staticmethod
        def update_page(container_name=None, blob_name=None,
                        page=None, start_range=None, end_range=None, **_):
            """azure.storage.blob.pageblobservice.PageBlobService.update_page"""
            # Don't use pythonic indexation
            end_range += 1

            # Must be page aligned
            assert not start_range % 512
            assert not end_range % 512

            storage_mock.put_object(
                container_name, blob_name, content=page,
                data_range=(start_range, end_range))

    class BlockBlobService(BlobService):
        """azure.storage.blob.blockblobservice.BlockBlobService"""
        BLOB_TYPE = _BlobTypes.BlockBlob

        def create_blob_from_bytes(
                self, container_name=None, blob_name=None, blob=None, **_):
            """azure.storage.blob.blockblobservice.BlockBlobService.
            create_blob_from_bytes"""
            storage_mock.put_object(
                container_name, blob_name, blob, headers=dict(
                    blob_type=self.BLOB_TYPE), new_file=True)

        @staticmethod
        def put_block(container_name=None, blob_name=None, block=None,
                      block_id=None, **_):
            """azure.storage.blob.blockblobservice.BlockBlobService.put_block"""
            storage_mock.put_object(
                container_name, '%s.%s' % (blob_name, block_id), content=block)

        @staticmethod
        def put_block_list(container_name=None, blob_name=None,
                           block_list=None, **_):
            """azure.storage.blob.blockblobservice.BlockBlobService.
            put_block_list"""
            blocks = []
            for block in block_list:
                blocks.append('%s.%s' % (blob_name, block.id))
            storage_mock.concat_objects(container_name, blob_name, blocks)

        @staticmethod
        def get_block_list(**_):
            """azure.storage.blob.blockblobservice.BlockBlobService.
            get_block_list"""
            return BlobBlockList()

    class AppendBlobService(BlobService):
        """azure.storage.blob.appendblobservice.AppendBlobService."""
        BLOB_TYPE = _BlobTypes.AppendBlob

        def create_blob(self, container_name=None, blob_name=None, **_):
            """azure.storage.blob.appendblobservice.AppendBlobService.
            create_blob"""
            storage_mock.put_object(container_name, blob_name, headers=dict(
                blob_type=self.BLOB_TYPE), new_file=True)

        @staticmethod
        def append_block(container_name=None, blob_name=None, block=None, **_):
            """azure.storage.blob.appendblobservice.AppendBlobService.
            append_block"""
            start = storage_mock.get_object_size(container_name, blob_name)
            storage_mock.put_object(
                container_name, blob_name, content=block,
                data_range=(start, start + len(block)))

    azure_block_blob_service = azure_blob._system.BlockBlobService
    azure_append_blob_service = azure_blob._system.AppendBlobService
    azure_page_blob_service = azure_blob._system.PageBlobService
    azure_blob._system.BlockBlobService = BlockBlobService
    azure_blob._system.AppendBlobService = AppendBlobService
    azure_blob._system.PageBlobService = PageBlobService

    # Tests
    try:
        # Init mocked system
        storage_parameters = dict(account_name='account')
        system_parameters = dict(storage_parameters=storage_parameters)
        tester_kwargs = dict(
            raw_io=AzureBlobRawIO,
            buffered_io=AzureBlobBufferedIO, storage_mock=storage_mock,
            unsupported_operations=UNSUPPORTED_OPERATIONS,
            system_parameters=system_parameters, root=root)

        # Block blobs tests (Default)
        blob_type = _BlobTypes.BlockBlob
        system = _AzureBlobSystem(**system_parameters)
        storage_mock.attach_io_system(system)
        with StorageTester(system, **tester_kwargs) as tester:

            # Common tests
            tester.test_common()

            # Test blob type
            assert system._default_blob_type == blob_type
            with AzureBlobRawIO(tester.base_dir_path + 'file0.dat',
                                **tester._system_parameters) as file:
                assert isinstance(file, AzureBlockBlobRawIO)

        # Page blobs tests
        blob_type = _BlobTypes.PageBlob
        storage_parameters['blob_type'] = blob_type
        system = _AzureBlobSystem(**system_parameters)
        storage_mock.attach_io_system(system)

        with StorageTester(system, **tester_kwargs) as tester:

            # Common tests
            tester.test_common()

            # Test blob type
            file_path = tester.base_dir_path + 'file0.dat'
            assert system._default_blob_type == blob_type
            with AzureBlobRawIO(file_path, **tester._system_parameters) as file:
                assert isinstance(file, AzurePageBlobRawIO)

            # Test pre-allocating pages with page aligned content length
            with AzureBlobRawIO(file_path, 'wb', content_length=1024,
                                **tester._system_parameters):
                pass

            with AzureBlobRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 1024

            # Test pre-allocating pages with page unaligned content length
            with AzureBlobRawIO(file_path, 'wb', content_length=1234,
                                **tester._system_parameters):
                pass

            with AzureBlobRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 1536

            # Test increase already existing blob size
            with AzureBlobRawIO(file_path, 'ab', content_length=2345,
                                **tester._system_parameters):
                pass

            with AzureBlobRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 2560

            # Test not truncate already existing blob with specified content
            # length
            with AzureBlobRawIO(file_path, 'ab', content_length=1024,
                                **tester._system_parameters):
                pass

            with AzureBlobRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 2560

            # Test Buffered IO: Page unaligned buffer size rounding
            with AzurePageBlobBufferedIO(file_path, 'wb', buffer_size=1234,
                                         **tester._system_parameters) as file:
                assert file._buffer_size == 1536

            # Test Buffered IO: initialization to one buffer size
            with AzureBlobRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 1536

            # Test Buffered IO: not truncate when initializing to one buffer
            with AzurePageBlobBufferedIO(file_path, 'ab', buffer_size=1024,
                                         **tester._system_parameters):
                pass

            with AzureBlobRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 1536

        # Append blobs tests
        blob_type = _BlobTypes.AppendBlob
        storage_parameters['blob_type'] = blob_type
        system = _AzureBlobSystem(**system_parameters)
        storage_mock.attach_io_system(system)

        with StorageTester(system, **tester_kwargs) as tester:

            # Common tests
            tester.test_common()

            # Test blob type
            assert system._default_blob_type == blob_type
            with AzureBlobRawIO(tester.base_dir_path + 'file0.dat',
                                **tester._system_parameters) as file:
                assert isinstance(file, AzureAppendBlobRawIO)

    # Restore mocked class
    finally:
        azure_blob._system.BlockBlobService = azure_block_blob_service
        azure_blob._system.AppendBlobService = azure_append_blob_service
        azure_blob._system.PageBlobService = azure_page_blob_service
