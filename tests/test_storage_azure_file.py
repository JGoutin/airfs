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
    from azure.storage.file.models import (
        Share, File, Directory, ShareProperties, FileProperties,
        DirectoryProperties)

    import pycosio.storage.azure_file as azure_file
    from pycosio.storage.azure_file import (
        AzureFileRawIO, _AzureFileSystem, AzureFileBufferedIO)

    from tests.test_storage import StorageTester
    from tests.test_storage_azure import get_storage_mock

    # Mocks client
    storage_mock = get_storage_mock()
    root = '//account.file.core.windows.net'

    def join(directory_name=None, file_name=None):
        """
        Join paths elements

        Args:
            directory_name (str): Directory.
            file_name (str): File.

        Returns:
            str: Path
        """
        if directory_name and file_name:
            return '%s/%s' % (directory_name, file_name)
        elif directory_name:
            return directory_name
        return file_name

    class FileService:
        """azure.storage.file.fileservice.FileService"""

        def __init__(self, *_, **kwargs):
            """azure.storage.file.fileservice.FileService.__init__"""
            self.kwargs = kwargs

        @staticmethod
        def copy_file(share_name=None, directory_name=None, file_name=None,
                      copy_source=None, **_):
            """azure.storage.file.fileservice.FileService.copy_file"""
            copy_source = copy_source.split(root + '/')[1]
            storage_mock.copy_object(
                src_path=copy_source, dst_locator=share_name,
                dst_path=join(directory_name, file_name))

        @staticmethod
        def get_file_properties(
                share_name=None, directory_name=None, file_name=None, **_):
            """azure.storage.file.fileservice.FileService.get_file_properties"""
            args = share_name, join(directory_name, file_name)
            props = FileProperties()
            props.last_modified = storage_mock.get_object_mtime(*args)
            props.content_length = storage_mock.get_object_size(*args)
            return File(props=props, name=file_name)

        @staticmethod
        def get_directory_properties(share_name=None, directory_name=None, **_):
            """
            azure.storage.file.fileservice.FileService.get_directory_properties
            """
            props = DirectoryProperties()
            props.last_modified = storage_mock.get_object_mtime(
                share_name, directory_name + '/')
            return Directory(props=props, name=directory_name)

        @staticmethod
        def get_share_properties(share_name=None, **_):
            """
            azure.storage.file.fileservice.FileService.get_share_properties
            """
            props = ShareProperties()
            props.last_modified = storage_mock.get_locator_mtime(share_name)
            return Share(props=props, name=share_name)

        @staticmethod
        def list_shares():
            """azure.storage.file.fileservice.FileService.list_shares"""
            shares = []
            for share_name in storage_mock.get_locators():
                props = ShareProperties()
                props.last_modified = storage_mock.get_locator_mtime(share_name)
                shares.append(Share(props=props, name=share_name))
            return shares

        @staticmethod
        def list_directories_and_files(
                share_name=None, directory_name=None, num_results=None, **_):
            """
            azure.storage.file.fileservice.FileService.
            list_directories_and_files
            """
            content = []
            for name in storage_mock.get_locator(
                    share_name, prefix=directory_name, limit=num_results,
                    first_level=True, relative=True):

                # This directory
                if not name:
                    continue

                # Directory
                elif name.endswith('/'):
                    content.append(Directory(
                        props=DirectoryProperties(), name=name))

                # File
                else:
                    props = FileProperties()
                    path = join(directory_name, name)
                    props.last_modified = storage_mock.get_object_mtime(
                        share_name, path)
                    props.content_length = storage_mock.get_object_size(
                        share_name, path)
                    content.append(File(props=props, name=name))

            return content

        @staticmethod
        def create_directory(share_name=None, directory_name=None, **_):
            """azure.storage.file.fileservice.FileService.create_directory"""
            storage_mock.put_object(share_name, directory_name + '/')

        @staticmethod
        def create_share(share_name=None, **_):
            """azure.storage.file.fileservice.FileService.create_share"""
            storage_mock.put_locator(share_name)

        @staticmethod
        def create_file(share_name=None, directory_name=None,
                        file_name=None, content_length=None, **_):
            """azure.storage.file.fileservice.FileService.create_file"""
            if content_length:
                # Create null padding
                content = b'\0' * content_length
            else:
                content = None

            storage_mock.put_object(
                share_name, join(directory_name, file_name), content=content,
                new_file=True)

        @staticmethod
        def delete_directory(share_name=None, directory_name=None, **_):
            """azure.storage.file.fileservice.FileService.delete_directory"""
            storage_mock.delete_object(share_name, directory_name)

        @staticmethod
        def delete_share(share_name=None, **_):
            """azure.storage.file.fileservice.FileService.delete_share"""
            storage_mock.delete_locator(share_name)

        @staticmethod
        def delete_file(share_name=None, directory_name=None,
                        file_name=None, **_):
            """azure.storage.file.fileservice.FileService.delete_file"""
            storage_mock.delete_object(
                share_name, join(directory_name, file_name))

        @staticmethod
        def get_file_to_stream(
                share_name=None, directory_name=None, file_name=None,
                stream=None, start_range=None, end_range=None, **_):
            """azure.storage.file.fileservice.FileService.get_file_to_stream"""
            if end_range is not None:
                end_range += 1
            stream.write(storage_mock.get_object(
                share_name, join(directory_name, file_name),
                data_range=(start_range, end_range)))

        @staticmethod
        def create_file_from_bytes(
                share_name=None, directory_name=None, file_name=None,
                file=None, **_):
            """azure.storage.file.fileservice.FileService.
            create_file_from_bytes"""
            storage_mock.put_object(
                share_name, join(directory_name, file_name), file,
                new_file=True)

        @staticmethod
        def update_range(share_name=None, directory_name=None, file_name=None,
                         data=None, start_range=None, end_range=None, **_):
            """azure.storage.file.fileservice.FileService.update_range"""
            if end_range is not None:
                end_range += 1
            storage_mock.put_object(
                share_name, join(directory_name, file_name), content=data,
                data_range=(start_range, end_range))

        @staticmethod
        def resize_file(share_name=None, directory_name=None,
                        file_name=None, content_length=None, **_):
            """azure.storage.file.fileservice.FileService.resize_file"""
            path = join(directory_name, file_name)
            # Add padding to resize file
            size = storage_mock.get_object_size(share_name, path)
            padding = content_length - size
            storage_mock.put_object(
                share_name, path, content=b'\0' * padding,
                data_range=(content_length - padding, content_length))

    azure_storage_file_file_service = azure_file._FileService
    azure_file._FileService = FileService

    # Tests
    try:
        # Init mocked system
        system_parameters = dict(
            storage_parameters=dict(account_name='account'))
        system = _AzureFileSystem(**system_parameters)
        storage_mock.attach_io_system(system)

        # Tests
        with StorageTester(
                system, AzureFileRawIO, AzureFileBufferedIO, storage_mock,
                unsupported_operations=UNSUPPORTED_OPERATIONS,
                system_parameters=system_parameters, root=root) as tester:

            # Common tests
            tester.test_common()

            # Test: Unsecure mode
            file_path = tester.base_dir_path + 'file0.dat'
            with AzureFileRawIO(file_path, unsecure=True,
                                **tester._system_parameters) as file:
                assert file._client.kwargs['protocol'] == 'http'

            # Test: Copy source formatted to use URL
            rel_path = '/container/file'
            assert (system._format_src_url(
                'smb://account.file.core.windows.net' + rel_path, system) ==
                    'https://account.file.core.windows.net' + rel_path)

            # Test: Cross account copy source URL with SAS token
            sas_token = 'sas_token'
            other_system = _AzureFileSystem(storage_parameters=dict(
                account_name='other', sas_token=sas_token))
            assert (other_system._format_src_url(
                'smb://other.file.core.windows.net' + rel_path, system) ==
                    'https://other.file.core.windows.net%s?%s' % (
                        rel_path, sas_token))

            # Test: Cross account copy source URL with no SAS token
            other_system = _AzureFileSystem(storage_parameters=dict(
                account_name='other'))
            assert (other_system._format_src_url(
                'smb://other.file.core.windows.net' + rel_path, system) ==
                    'https://other.file.core.windows.net' + rel_path)

            # Test pre-allocating file
            with AzureFileRawIO(file_path, 'wb', content_length=1024,
                                **tester._system_parameters):
                pass

            with AzureFileRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 1024

            # Test increase already existing blob size
            with AzureFileRawIO(file_path, 'ab', content_length=2048,
                                **tester._system_parameters):
                pass

            with AzureFileRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 2048

            # Test not truncate already existing blob with specified content
            # length
            with AzureFileRawIO(file_path, 'ab', content_length=1024,
                                **tester._system_parameters):
                pass

            with AzureFileRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 2048

            # Test Buffered IO: Page unaligned buffer size rounding
            with AzureFileBufferedIO(file_path, 'wb', buffer_size=1234,
                                     **tester._system_parameters) as file:
                assert file._buffer_size == 1234

            # Test Buffered IO: initialization to one buffer size
            with AzureFileRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 1234

            # Test Buffered IO: not truncate when initializing to one buffer
            with AzureFileBufferedIO(file_path, 'ab', buffer_size=1024,
                                     **tester._system_parameters):
                pass

            with AzureFileRawIO(file_path, ignore_padding=False,
                                **tester._system_parameters) as file:
                assert file.readall() == b'\0' * 1234

    # Restore mocked class
    finally:
        azure_file._FileService = azure_storage_file_file_service
