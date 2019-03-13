# coding=utf-8
"""Test pycosio.storage.azure_file"""
from __future__ import absolute_import  # Python 2: Fix azure import

from datetime import datetime
import time

from tests.utilities import (
    BYTE, SIZE, check_head_methods, check_raw_read_methods)


def test_azure_blob_raw_io():
    """Tests pycosio.storage.azure_blob.AzureBlobRawIO"""
    from pycosio.storage.azure_blob import AzureBlobRawIO, _AzureBlobSystem
    from pycosio._core.exceptions import ObjectNotFoundError
    import pycosio.storage.azure_blob as azure_blob
    from azure.storage.blob.models import (
        BlobProperties, ContainerProperties, Blob, Container)

    # Initializes some variables
    container_name = 'container'
    blob_name = 'blob'
    container_client_args = dict(container_name=container_name)
    blob_client_args = dict(container_name=container_name, blob_name=blob_name)
    account_name = 'account'
    root = 'http://%s.blob.core.windows.net' % account_name
    container_url = '/'.join((root, container_name))
    blob_path = '/'.join((container_name, blob_name))
    blob_url = '/'.join((root, container_name, blob_name))
    m_time = time.time()
    create_container_called = []
    delete_container_called = []
    delete_blob_called = []
    copy_blob_called = []
    create_blob_called = []
    write_blob_called = []
    blob_not_exists = False

    # Mocks Azure service client

    class BlobService:
        """Dummy BlobService"""

        def __init__(self, *_, **__):
            """Do nothing"""

        @staticmethod
        def copy_blob(**kwargs):
            """Do nothing"""
            assert kwargs['container_name'] == container_name
            assert kwargs['blob_name'] == blob_name
            assert kwargs['copy_source'] == blob_url
            copy_blob_called.append(1)

        @staticmethod
        def get_blob_properties(**kwargs):
            """Checks arguments and returns fake result"""
            assert kwargs['container_name'] == container_name
            assert kwargs['blob_name'] == blob_name
            if blob_not_exists:
                raise ObjectNotFoundError
            props = BlobProperties()
            props.last_modified = datetime.fromtimestamp(m_time)
            props.content_length = SIZE
            props.blob_type = 'BlockBlob'
            return Blob(props=props, metadata=blob_name)

        @staticmethod
        def get_container_properties(**kwargs):
            """Checks arguments and returns fake result"""
            assert kwargs['container_name'] == container_name
            assert 'blob_name' not in kwargs
            props = ContainerProperties()
            props.last_modified = datetime.fromtimestamp(m_time)
            return Container(props=props, metadata=container_name)

        @staticmethod
        def list_containers():
            """Returns fake result"""
            props = ContainerProperties()
            props.last_modified = datetime.fromtimestamp(m_time)
            return [Container(props=props, name=container_name)]

        @staticmethod
        def list_blobs(**kwargs):
            """Checks arguments and returns fake result"""
            assert kwargs['container_name'] == container_name
            assert kwargs['prefix'] == ''
            assert 'blob_name' not in kwargs
            props = BlobProperties()
            props.last_modified = datetime.fromtimestamp(m_time)
            props.content_length = SIZE
            return [Blob(props=props, name=blob_name)]

        @staticmethod
        def create_container(**kwargs):
            """Checks arguments"""
            assert kwargs['container_name'] == container_name
            assert 'blob_name' not in kwargs
            create_container_called.append(1)

        @staticmethod
        def create_blob(**kwargs):
            """Checks arguments"""
            assert kwargs['container_name'] == container_name
            assert kwargs['blob_name'] == blob_name
            create_blob_called.append(1)

        @staticmethod
        def create_blob_from_bytes(**kwargs):
            """Checks arguments"""
            assert kwargs['container_name'] == container_name
            assert kwargs['blob_name'] == blob_name
            create_blob_called.append(1)

        @staticmethod
        def create_blob_from_stream(**kwargs):
            """Checks arguments"""
            assert kwargs['container_name'] == container_name
            assert kwargs['blob_name'].strip('/') == blob_name
            stream = kwargs['stream']
            stream.seek(0)
            content = stream.read()
            if kwargs['blob_name'][-1] == '/':
                assert content == b''
            else:
                assert content == 50 * BYTE
                write_blob_called.append(1)

        @staticmethod
        def delete_container(**kwargs):
            """Checks arguments"""
            assert kwargs['container_name'] == container_name
            assert 'blob_name' not in kwargs
            delete_container_called.append(1)

        @staticmethod
        def delete_blob(**kwargs):
            """Checks arguments"""
            assert kwargs['container_name'] == container_name
            assert kwargs['blob_name'] == blob_name
            delete_blob_called.append(1)

        @staticmethod
        def get_blob_to_stream(**kwargs):
            """Checks arguments and returns fake result"""
            assert kwargs['container_name'] == container_name
            assert kwargs['blob_name'] == blob_name
            stream = kwargs['stream']
            end_range = kwargs.get('end_range') or SIZE
            if end_range > SIZE:
                end_range = SIZE
            start_range = kwargs.get('start_range') or 0
            stream.write(BYTE * (end_range - start_range))

    class PageBlobService(BlobService):
        @staticmethod
        def get_blob_properties(**kwargs):
            """Checks arguments and returns fake result"""
            blob = BlobService.get_blob_properties(**kwargs)
            blob.properties.blob_type = 'PageBlob'
            return blob

    class BlockBlobService(BlobService):
        @staticmethod
        def get_blob_properties(**kwargs):
            """Checks arguments and returns fake result"""
            blob = BlobService.get_blob_properties(**kwargs)
            blob.properties.blob_type = 'BlockBlob'
            return blob

    class AppendBlobService(BlobService):
        @staticmethod
        def get_blob_properties(**kwargs):
            """Checks arguments and returns fake result"""
            blob = BlobService.get_blob_properties(**kwargs)
            blob.properties.blob_type = 'AppendBlob'
            return blob

    azure_storage_block_blob_service = azure_blob._BlockBlobService
    azure_storage_append_blob_service = azure_blob._AppendBlobService
    azure_storage_page_blob_service = azure_blob._PageBlobService
    azure_blob._BlockBlobService = BlockBlobService
    azure_blob._AppendBlobService = AppendBlobService
    azure_blob._PageBlobService = PageBlobService
    # Tests
    try:
        azure_system = _AzureBlobSystem(
            storage_parameters=dict(account_name=account_name))

        # Tests head
        check_head_methods(azure_system, m_time, path=blob_url)
        assert azure_system.head(
            path=blob_url)['metadata'] == blob_name
        assert azure_system.head(
            path=container_url)['metadata'] == container_name

        # Tests create directory
        azure_system.make_dir(container_url)
        assert len(create_container_called) == 1
        azure_system.make_dir(blob_url)
        assert len(create_container_called) == 1

        # Tests remove
        azure_system.remove(container_url)
        assert len(delete_container_called) == 1
        azure_system.remove(blob_url)
        assert len(delete_blob_called) == 1

        # Tests copy
        azure_system.copy(blob_url, blob_url)
        assert len(copy_blob_called) == 1

        # Tests _list_locator
        assert list(azure_system._list_locators()) == [
            (container_name, dict(
                last_modified=datetime.fromtimestamp(m_time)))]

        # Tests _list_objects
        assert list(azure_system._list_objects(
            container_client_args, '', None)) == [
            (blob_name, dict(last_modified=datetime.fromtimestamp(m_time),
                             content_length=SIZE))]

        # Tests path and URL handling
        print("\n\n", azure_blob._BlockBlobService, "\n\n")
        azure_object = AzureBlobRawIO(
            blob_url, storage_parameters=dict(account_name=account_name))
        assert azure_object._client_kwargs == blob_client_args
        assert azure_object.name == blob_url
        assert azure_object._blob_type == 'BlockBlob'

        azure_object = AzureBlobRawIO(
            blob_path, storage_parameters=dict(account_name=account_name))
        assert azure_object._client_kwargs == blob_client_args
        assert azure_object.name == blob_path

        # Tests read
        check_raw_read_methods(azure_object)

        # Tests create page blob
        azure_blob._BlockBlobService = PageBlobService
        page_blob_not_exists = True
        len_create_blob_called = len(create_blob_called)
        azure_object = AzureBlobRawIO(
            blob_url, mode='w',
            storage_parameters=dict(account_name=account_name))
        assert azure_object._blob_type == 'PageBlob'
        assert len(create_blob_called) == len_create_blob_called + 1
        page_blob_not_exists = False

        # Tests create block blob
        azure_blob._BlockBlobService = BlockBlobService
        block_blob_not_exists = True
        len_create_blob_called = len(create_blob_called)
        azure_object = AzureBlobRawIO(
            blob_url, mode='w',
            storage_parameters=dict(account_name=account_name))
        assert azure_object._blob_type == 'BlockBlob'
        assert len(create_blob_called) == len_create_blob_called + 1
        block_blob_not_exists = False

        # Tests _flush
        azure_object.write(50 * BYTE)
        azure_object.flush()
        assert len(write_blob_called) == 1

    # Restore mocked class
    finally:
        azure_blob._BlockBlobService = azure_storage_block_blob_service
        azure_blob._AppendBlobService = azure_storage_append_blob_service
        azure_blob._PageBlobService = azure_storage_page_blob_service
