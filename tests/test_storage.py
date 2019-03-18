# coding=utf-8
"""Test pycosio.storage"""
from io import UnsupportedOperation as _UnsupportedOperation
from os import urandom as _urandom
from time import time as _time
from uuid import uuid4 as _uuid

import pytest as _pytest


class StorageTester:
    """
    Class that contain common set of tests for storage.

    Args:
        system (pycosio._core.io_system.SystemBase instance):
            System to test.
        raw_io (pycosio._core.io_raw.ObjectRawIOBase subclass):
            Raw IO class.
        buffered_io (pycosio._core.io_buffered.ObjectBufferedIOBase subclass):
            Buffered IO class.
        storage_mock (tests.storage_mock.ObjectStorageMock instance):
            Storage mock in use, if any.
        storage_info (dict): Storage information from pycosio.mount.
    """

    def __init__(self, system=None, raw_io=None, buffered_io=None,
                 storage_mock=None, unsupported_operations=None,
                 storage_info=None, system_parameters=None):

        if system is None:
            system = storage_info['system_cached']
        if raw_io is None:
            raw_io = storage_info['raw']
        if buffered_io is None:
            buffered_io = storage_info['buffered']
        if system_parameters is None and storage_info:
            system_parameters = storage_info['system_parameters']

        self._system_parameters = system_parameters or dict()
        self._system = system
        self._raw_io = raw_io
        self._buffered_io = buffered_io
        self._storage_mock = storage_mock
        self._unsupported_operations = unsupported_operations or tuple()

        # Get storage root
        root = system.roots[0]

        # Defines randomized names for locator and objects
        self.locator = self._get_id()
        self.locator_url = '/'.join((root, self.locator))
        self.base_dir_path = '%s/%s/' % (self.locator, self._get_id())
        self.base_dir_url = '/'.join((root, self.base_dir_path))

        # Run test sequence
        self._objects = set()
        self._to_clean = self._objects.add

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.__del__()

    def __del__(self):
        from pycosio._core.exceptions import ObjectNotFoundError

        # Remove objects, and once empty the locator
        for obj in list(self._objects) + [self.locator]:
            self._objects.discard(obj)
            try:
                self._system.remove(obj, relative=True)
            except ObjectNotFoundError:
                continue

    def test_common(self):
        """
        Common set of tests
        """
        self._test_system_locator()
        self._test_system_objects()
        self._test_raw_io()
        self._test_buffered_io()

        # Only if mocked
        if self._storage_mock is not None:
            self._test_mock_only()

    def _is_supported(self, feature):
        """
        Return True if a feature is supported.

        Args:
            feature (str): Feature to support.

        Returns:
            bool: Feature is supported.
        """
        return feature not in self._unsupported_operations

    @staticmethod
    def _get_id():
        """
        Return an unique ID.

        Returns:
            str: id
        """
        return 'pycosio%s' % (str(_uuid()).replace('-', ''))

    def _test_raw_io(self):
        """
        Tests raw IO.
        """
        from os import SEEK_END

        size = 100
        file_path = self.base_dir_path + 'sample_100B.dat'
        self._to_clean(file_path)
        content = _urandom(size)

        # Open file in write mode
        file = self._raw_io(file_path, 'wb', **self._system_parameters)
        try:
            # Test: Write
            file.write(content)

            # Test: tell
            assert file.tell() == size

            # Test: _flush
            file.flush()

        finally:
            file.close()

        # Open file in read mode
        file = self._raw_io(file_path, **self._system_parameters)
        try:
            # Test: _read_all
            assert file.readall() == content
            assert file.tell() == size

            assert file.seek(10) == 10
            assert file.readall() == content[10:]
            assert file.tell() == size

            # Test: _read_range
            assert file.seek(0) == 0
            buffer = bytearray(40)
            assert file.readinto(buffer) == 40
            assert bytes(buffer) == content[:40]
            assert file.tell() == 40

            buffer = bytearray(40)
            assert file.readinto(buffer) == 40
            assert bytes(buffer) == content[40:80]
            assert file.tell() == 80

            buffer = bytearray(40)
            assert file.readinto(buffer) == 20
            assert bytes(buffer) == content[80:] + b'\x00' * 20
            assert file.tell() == size

            buffer = bytearray(40)
            assert file.readinto(buffer) == 0
            assert bytes(buffer) == b'\x00' * 40
            assert file.tell() == size

            file.seek(-10, SEEK_END)
            buffer = bytearray(20)
            assert file.readinto(buffer) == 10
            assert bytes(buffer) == content[90:] + b'\x00' * 10
            assert file.tell() == size

        finally:
            file.close()

    def _test_buffered_io(self):
        """
        Tests buffered IO.
        """
        # Set buffer size
        minimum_buffer_zize = 16 * 1024
        buffer_size = self._buffered_io.MINIMUM_BUFFER_SIZE
        if buffer_size < minimum_buffer_zize:
            buffer_size = minimum_buffer_zize

        # Define data to write
        file_path = self.base_dir_path + 'buffered_file.dat'
        self._to_clean(file_path)
        size = int(4.5 * buffer_size)
        data = _urandom(size)

        # Test: write data, not multiple of buffer
        with self._buffered_io(file_path, 'wb', buffer_size=buffer_size,
                               **self._system_parameters) as file:
            file.write(data)

        # Test: Read data, not multiple of buffer
        with self._buffered_io(file_path, 'rb', buffer_size=buffer_size,
                               **self._system_parameters) as file:
            assert data == file.read()

        size = int(4.5 * buffer_size)
        data = _urandom(size)

        # Test: write data, multiple of buffer
        with self._buffered_io(file_path, 'wb', buffer_size=buffer_size,
                               **self._system_parameters) as file:
            file.write(data)

        # Test: Read data, multiple of buffer
        with self._buffered_io(file_path, 'rb', buffer_size=buffer_size,
                               **self._system_parameters) as file:
            assert data == file.read()

    def _test_system_locator(self):
        """
        Test system internals related to locators.
        """
        system = self._system

        # Test: Create locator
        system.make_dir(self.locator_url)

        # Test: Check locator listed
        for name, header in system._list_locators():
            if name == self.locator and isinstance(header, dict):
                break
        else:
            _pytest.fail('Locator "%s" not found' % self.locator)

        # Test: Check locator header return a mapping
        assert hasattr(system.head(path=self.locator), '__getitem__')

        # Test: remove locator
        tmp_locator = self._get_id()
        self._to_clean(tmp_locator)
        system.make_dir(tmp_locator)
        assert tmp_locator in [name for name, _ in system._list_locators()]

        system.remove(tmp_locator)
        assert tmp_locator not in [name for name, _ in system._list_locators()]

    def _test_system_objects(self):
        """
        Test system internals related to objects.
        """
        from pycosio._core.exceptions import ObjectNotFoundError

        system = self._system

        # Write a sample file
        file_name = 'sample_16B.dat'
        file_path = self.base_dir_path + file_name
        self._to_clean(file_path)
        file_url = self.base_dir_url + file_name
        size = 16
        content = _urandom(size)

        with self._raw_io(file_path, mode='w',
                          **self._system_parameters) as file:
            # Write content
            file.write(content)

            # Estimate creation time
            create_time = _time()

        # Test: Check file header
        assert hasattr(system.head(path=file_path), '__getitem__')

        # Test: Check file size
        assert system.getsize(file_path) == size

        # Test: Check file modification time
        try:
            assert system.getmtime(file_path) == _pytest.approx(create_time, 2)
        except _UnsupportedOperation:
            # May not be supported on all files, if supported
            if self._is_supported('getmtime'):
                raise

        # Test: Check file creation time
        try:
            assert system.getctime(file_path) == _pytest.approx(create_time, 2)
        except _UnsupportedOperation:
            # May not be supported on all files, if supported
            if self._is_supported('getctime'):
                raise

        # Test: Check path and URL handling
        with self._raw_io(file_path, **self._system_parameters) as file:
            assert file.name == file_path

        with self._raw_io(file_url, **self._system_parameters) as file:
            assert file.name == file_url

        # Write some files
        files = set()
        files.add(file_path)
        for i in range(10):
            path = self.base_dir_path + 'file%d.dat' % i
            files.add(path)
            self._to_clean(path)
            with self._raw_io(
                    path, mode='w', **self._system_parameters) as file:
                file.flush()

        # Test: List objects
        objects = tuple(system.list_objects(self.locator))
        assert files == set(
            '%s/%s' % (self.locator, name) for name, _ in objects)
        for _, header in objects:
            assert isinstance(header, dict)

        # Test: List objects, with limited output
        max_request_entries = 5
        entries = len(tuple(system.list_objects(
            max_request_entries=max_request_entries)))
        assert entries == max_request_entries

        # Test: List objects, no objects found
        with _pytest.raises(ObjectNotFoundError):
            list(system.list_objects(self.base_dir_path + 'dir_not_exists/'))

        # Test: List objects on locator root, no objects found
        with _pytest.raises(ObjectNotFoundError):
            list(system.list_objects(self.locator + '/dir_not_exists/'))

        # Test: List objects, locator not found
        with _pytest.raises(ObjectNotFoundError):
            list(system.list_objects(self._get_id()))

        # Test: copy
        copy_path = file_path + '.copy'
        self._to_clean(copy_path)
        if self._is_supported('copy'):
            system.copy(file_path, copy_path)
            assert system.getsize(copy_path) == size
        else:
            with _pytest.raises(_UnsupportedOperation):
                system.copy(file_path, copy_path)

        # Test: Make a directory (With trailing /)
        dir_path0 = self.base_dir_path + 'directory0/'
        system.make_dir(dir_path0)
        self._to_clean(dir_path0)
        assert dir_path0 in self._list_objects_names()

        # Test: Make a directory (Without trailing /)
        dir_path1 = self.base_dir_path + 'directory1'
        system.make_dir(dir_path1)
        dir_path1 += '/'
        self._to_clean(dir_path1)
        assert dir_path1 in self._list_objects_names()

        # Test: Listing empty directory
        assert len(tuple(system.list_objects(dir_path0))) == 0

        # Test: Normal file is not symlink
        assert not system.islink(file_path)

        # Test: Symlink
        if self._is_supported('symlink'):
            link_path = self.base_dir_path + 'symlink'
            # TODO: Tests once create symlink implemented

            # Test: Is symlink
            #assert system.islink(link_path)
            #assert system.islink(header=system.head(link_path)

        # Test: Remove file
        assert file_path in self._list_objects_names()
        system.remove(file_path)
        assert file_path not in self._list_objects_names()

    def _test_mock_only(self):
        """
        Tests that can only be performed on mocks
        """
        # Create a file
        file_path = self.base_dir_path + 'mocked.dat'

        self._to_clean(file_path)
        with self._raw_io(
                file_path, mode='w', **self._system_parameters) as file:
            file.write(_urandom(20))
            file.flush()

        # Test: Read not block other exceptions
        with self._storage_mock.raise_server_error():
            with _pytest.raises(self._storage_mock.base_exception):
                self._raw_io(file_path, **self._system_parameters).read(10)

    def _list_objects_names(self):
        """
        List objects names.

        Returns:
            set of str: objects names.
        """
        return set('%s/%s' % (self.locator, name)
                   for name, _ in self._system.list_objects(self.locator))


def test_user_storage(storage):
    """
    Test specified storage.

    Test cases are automatically generated base on user configuration,
    see "tests.conftest.pytest_generate_tests"

    Args:
        storage (dict): Storage information.
    """
    # Get list of unsupported operations
    from importlib import import_module
    module = import_module('tests.test_storage_%s' % storage['storage'])
    try:
        unsupported_operations = module.UNSUPPORTED_OPERATIONS
    except AttributeError:
        unsupported_operations = None

    # Run tests
    with StorageTester(
            storage_info=storage,
            unsupported_operations=unsupported_operations) as tester:
        tester.test_common()
