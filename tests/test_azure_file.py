# coding=utf-8
"""Test pycosio.storage.azure_file"""
from __future__ import absolute_import  # Python 2: Fix azure import

from datetime import datetime
import time

from tests.utilities import (
    BYTE, SIZE, check_head_methods, check_raw_read_methods)


def test_azure_file_raw_io():
    """Tests pycosio.storage.azure_file.AzureFileRawIO"""
    from pycosio.storage.azure_file import AzureFileRawIO, _AzureFileSystem
    from pycosio._core.exceptions import ObjectNotFoundError
    import pycosio.storage.azure_file as azure_file
    from azure.storage.file.models import (
        Share, File, Directory, ShareProperties, FileProperties,
        DirectoryProperties)

    # Initializes some variables
    share_name = 'share'
    file_name = 'file'
    directory_name = 'directory'
    share_client_args = dict(
        share_name=share_name, directory_name='')
    file_client_args = dict(
        share_name=share_name, directory_name='', file_name=file_name)
    account_name = 'account'
    root = '//%s.file.core.windows.net' % account_name
    share_url = '/'.join((root, share_name))
    directory_url = '/'.join((root, share_name, directory_name)) + '/'
    file_url = '/'.join((root, share_name, file_name))
    file_path = '/'.join((share_name, file_name))
    m_time = time.time()
    create_directory_called = []
    create_share_called = []
    delete_directory_called = []
    delete_share_called = []
    delete_file_called = []
    copy_file_called = []
    create_file_called = []
    write_file_called = []
    file_not_exists = False

    # Mocks Azure service client

    class FileService:
        """Dummy FileService"""

        def __init__(self, *_, **__):
            """Do nothing"""

        @staticmethod
        def copy_file(**kwargs):
            """Do nothing"""
            assert kwargs['share_name'] == share_name
            assert kwargs['directory_name'] == ''
            assert kwargs['file_name'] == file_name
            assert kwargs['copy_source'] == file_url
            copy_file_called.append(1)

        @staticmethod
        def get_file_properties(**kwargs):
            """Checks arguments and returns fake result"""
            assert kwargs['share_name'] == share_name
            assert kwargs['directory_name'] == ''
            assert kwargs['file_name'] == file_name
            if file_not_exists:
                raise ObjectNotFoundError
            props = FileProperties()
            props.last_modified = datetime.fromtimestamp(m_time)
            props.content_length = SIZE
            return File(props=props, metadata=file_name)

        @staticmethod
        def get_directory_properties(**kwargs):
            """Checks arguments and returns fake result"""
            assert kwargs['share_name'] == share_name
            assert kwargs['directory_name'] == directory_name
            assert 'file_name' not in kwargs
            props = DirectoryProperties()
            props.last_modified = datetime.fromtimestamp(m_time)
            return Directory(props=props, metadata=directory_name)

        @staticmethod
        def get_share_properties(**kwargs):
            """Checks arguments and returns fake result"""
            assert kwargs['share_name'] == share_name
            assert 'directory_name' not in kwargs
            assert 'file_name' not in kwargs
            props = ShareProperties()
            props.last_modified = datetime.fromtimestamp(m_time)
            return Share(props=props, metadata=share_name)

        @staticmethod
        def list_shares():
            """Returns fake result"""
            props = ShareProperties()
            props.last_modified = datetime.fromtimestamp(m_time)
            return [Share(props=props, name=share_name)]

        @staticmethod
        def list_directories_and_files(**kwargs):
            """Checks arguments and returns fake result"""
            assert kwargs['share_name'] == share_name
            assert kwargs['prefix'] == ''
            assert kwargs['directory_name'] == ''
            assert 'file_name' not in kwargs
            props = FileProperties()
            props.last_modified = datetime.fromtimestamp(m_time)
            props.content_length = SIZE
            return [File(props=props, name=file_name)]

        @staticmethod
        def create_directory(**kwargs):
            """Checks arguments"""
            assert kwargs['share_name'] == share_name
            assert kwargs['directory_name'] == directory_name
            assert 'file_name' not in kwargs
            create_directory_called.append(1)

        @staticmethod
        def create_share(**kwargs):
            """Checks arguments"""
            assert kwargs['share_name'] == share_name
            assert 'directory_name' not in kwargs
            assert 'file_name' not in kwargs
            create_share_called.append(1)

        @staticmethod
        def create_file(**kwargs):
            """Checks arguments"""
            assert kwargs['share_name'] == share_name
            assert kwargs['directory_name'] == ''
            assert kwargs['file_name'] == file_name
            create_file_called.append(1)

        @staticmethod
        def delete_directory(**kwargs):
            """Checks arguments"""
            assert kwargs['share_name'] == share_name
            assert kwargs['directory_name'] == directory_name
            assert 'file_name' not in kwargs
            delete_directory_called.append(1)

        @staticmethod
        def delete_share(**kwargs):
            """Checks arguments"""
            assert kwargs['share_name'] == share_name
            assert 'directory_name' not in kwargs
            assert 'file_name' not in kwargs
            delete_share_called.append(1)

        @staticmethod
        def delete_file(**kwargs):
            """Checks arguments"""
            assert kwargs['share_name'] == share_name
            assert kwargs['directory_name'] == ''
            assert kwargs['file_name'] == file_name
            delete_file_called.append(1)

        @staticmethod
        def get_file_to_stream(**kwargs):
            """Checks arguments and returns fake result"""
            assert kwargs['share_name'] == share_name
            assert kwargs['directory_name'] == ''
            assert kwargs['file_name'] == file_name
            stream = kwargs['stream']
            end_range = kwargs.get('end_range') or SIZE
            if end_range > SIZE:
                end_range = SIZE
            start_range = kwargs.get('start_range') or 0
            stream.write(BYTE * (end_range - start_range))

        @staticmethod
        def update_range(**kwargs):
            """Do nothing"""
            assert kwargs['share_name'] == share_name
            assert kwargs['directory_name'] == ''
            assert kwargs['file_name'] == file_name
            assert kwargs['data'] == 50 * BYTE
            write_file_called.append(1)

    azure_storage_file_file_service = azure_file._FileService
    azure_file._FileService = FileService
    # Tests
    try:
        azure_system = _AzureFileSystem(
            storage_parameters=dict(account_name=account_name))

        # Tests head
        check_head_methods(azure_system, m_time, path=file_url)
        assert azure_system.head(
            path=directory_url)['metadata'] == directory_name
        assert azure_system.head(path=file_url)['metadata'] == file_name
        assert azure_system.head(path=share_url)['metadata'] == share_name

        # Tests create directory
        azure_system.make_dir(share_url)
        assert len(create_share_called) == 1
        azure_system.make_dir(directory_url)
        assert len(create_directory_called) == 1

        # Tests remove
        azure_system.remove(share_url)
        assert len(delete_share_called) == 1
        azure_system.remove(directory_url)
        assert len(delete_directory_called) == 1
        azure_system.remove(file_url)
        assert len(delete_file_called) == 1

        # Tests copy
        azure_system.copy(file_url, file_url)
        assert len(copy_file_called) == 1

        # Tests _list_locator
        assert list(azure_system._list_locators()) == [
            (share_name, dict(last_modified=datetime.fromtimestamp(m_time)))]

        # Tests _list_objects
        assert list(azure_system._list_objects(
            share_client_args, '', None)) == [
            (file_name, dict(last_modified=datetime.fromtimestamp(m_time),
                             content_length=SIZE))]

        # Tests path and URL handling
        azure_object = AzureFileRawIO(
            file_url, storage_parameters=dict(account_name=account_name))
        assert azure_object._client_kwargs == file_client_args
        assert azure_object.name == file_url

        azure_object = AzureFileRawIO(
            file_path, storage_parameters=dict(account_name=account_name))
        assert azure_object._client_kwargs == file_client_args
        assert azure_object.name == file_path

        # Tests read
        check_raw_read_methods(azure_object)

        # Tests create blob
        file_not_exists = True
        azure_object = AzureFileRawIO(
            file_url, mode='w',
            storage_parameters=dict(account_name=account_name))
        assert len(create_file_called) == 1
        file_not_exists = False

        # Tests _flush
        azure_object.write(50 * BYTE)
        azure_object.flush()
        assert len(write_file_called) == 1

    # Restore mocked class
    finally:
        azure_file._FileService = azure_storage_file_file_service
