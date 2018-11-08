# coding=utf-8
"""Test pycosio.storage.azure_file"""
from datetime import datetime
import io
import time

from tests.utilities import (BYTE, SIZE, parse_range, check_head_methods,
                             check_raw_read_methods)

import pytest


def test_azure_file_raw_io():
    """Tests pycosio.storage.azure_file.AzureFilesRawIO"""
    from io import UnsupportedOperation
    from pycosio.storage.azure_file import AzureFilesRawIO, _AzureFilesSystem
    from pycosio._core.exceptions import ObjectNotFoundError
    import pycosio.storage.azure_file as azure_file

    # Initializes some variables
    share_name = 'share'
    file_name = 'file'
    directory_name = 'directory/'
    share_client_args = dict(share_name=share_name)
    file_client_args = dict(share_name=share_name, file_name=file_name)
    directory_client_args = dict(
        share_name=share_name, directory_name=directory_name)
    account_name = 'account'
    root = '//%s.file.core.windows.net' % account_name
    share_url = '/'.join((root, share_name))
    directory_url = '/'.join((root, share_name, directory_name))
    file_url = '/'.join((root, share_name, file_name))
    m_time = time.time()

    # Mocks Azure service client

    class FileService:
        """Dummy FileService"""

        def __init__(self, *_, **__):
            """Do nothing"""

        def copy_file(self, *_, **__):
            """Do nothing"""

        def get_file_metadata(self, *_, **__):
            """Returns fake result"""
            return {'Last-Modified': m_time, 'Content-Length': SIZE,
                    'Name': file_name}

        def get_directory_properties(self, *_, **__):
            """Returns fake result"""
            return {'Last-Modified': m_time, 'Name': directory_name}

        def get_share_properties(self, *_, **__):
            """Returns fake result"""
            return {'Last-Modified': m_time, 'Name': share_name}

        def list_shares(self, *_, **__):
            """Do nothing"""

        def list_directories_and_files(self, *_, **__):
            """Do nothing"""

        def create_directory(self, *_, **__):
            """Do nothing"""

        def create_share(self, *_, **__):
            """Do nothing"""

        def create_file(self, *_, **__):
            """Do nothing"""

        def delete_directory(self, *_, **__):
            """Do nothing"""

        def delete_share(self, *_, **__):
            """Do nothing"""

        def get_file_to_stream(self, *_, **__):
            """Do nothing"""

        def update_range(self, *_, **__):
            """Do nothing"""

    azure_storage_file_file_service = azure_file._FileService
    azure_file._FileService = FileService
    # Tests
    try:
        azure_system = _AzureFilesSystem(
            storage_parameters=dict(account_name=account_name))

        # Tests head
        # TODO:
        pytest.xfail('WIP')
        check_head_methods(azure_system, m_time, path=file_url)
        assert azure_system.head(path=directory_url)['Name'] == directory_name
        assert azure_system.head(path=file_url)['Name'] == file_name
        assert azure_system.head(path=share_url)['Name'] == share_name

    # Restore mocked class
    finally:
        azure_file._FileService = azure_storage_file_file_service
