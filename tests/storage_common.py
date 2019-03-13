# coding=utf-8
"""Test pycosio.storage"""
from copy import deepcopy
from os import urandom
from time import time
from uuid import uuid4 as uuid
from wsgiref.handlers import format_date_time


import pytest


class ObjectStorageMock:
    """
    Mocked Object storage.
    """

    class LocatorNotFound(Exception):
        """Locator not found exception"""

    class ObjectNotFound(Exception):
        """Object not found exception"""

    class UnsupportedOperation(Exception):
        """Unsupported operation exception"""

    class EndOfObject(Exception):
        """End of object reached"""

    def __init__(self):
        self._system = None
        self._locators = {}
        self._header_size = None
        self._header_mtime = None
        self._header_ctime = None

    def attach_io_system(self, system):
        """
        Attach IO system to use.

        Args:
            system (pycosio._core.io_system.SystemBase subclass):
                IO system to use.
        """
        self._system = system
        try:
            self._header_size = system._SIZE_KEYS[0]
        except IndexError:
            pass
        try:
            self._header_mtime = system._MTIME_KEYS[0]
        except IndexError:
            pass
        try:
            self._header_ctime = system._CTIME_KEYS[0]
        except IndexError:
            pass

    def put_locator(self, locator):
        """
        Put a locator.

        Args:
            locator (str): locator name
        """
        self._locators[locator] = dict(
            content=dict())

    def _get_locator(self, locator):
        """
        Get a locator.

        Args:
            locator (str): locator name
        """
        try:
            return self._locators[locator]
        except KeyError:
            raise self.LocatorNotFound

    def get_locator(self, locator):
        """
        Get locator content.

        Args:
            locator (str): locator name

        Returns:
            dict: objects names, objects headers.
        """
        headers = dict()
        for name, header in self._get_locator_content(locator).items():
            headers[name] = header.copy()
            del headers[name]['content']
        return headers

    def get_locators(self):
        """
        Get locators headers.

        Returns:
            dict: locators names, locators headers.
        """
        headers = dict()
        for name, header in self._locators.items():
            headers[name] = header.copy()
            del headers[name]['content']
        return headers

    def _get_locator_content(self, locator):
        """
        Get locator content.

        Args:
            locator (str): locator name

        Returns:
            dict: objects names, objects with header.
        """
        return self._get_locator(locator)['content']

    def head_locator(self, locator):
        """
        Get locator header

        Args:
            locator (str): locator name
        """
        header = self._get_locator(locator).copy()
        del header['content']
        return header

    def delete_locator(self, locator):
        """
        Delete locator.

        Args:
            locator (str): locator name
        """
        try:
            del self._locators[locator]
        except KeyError:
            raise self.LocatorNotFound

    def put_object(self, locator, path, content):
        """
        Put object.

        Args:
            locator (str): locator name
            path (str): Object path..
            content (bytes like-object): File content.
        """
        try:
            file = self._get_locator_content(locator)[path]
        except KeyError:
            file = dict(content=bytearray())
            self._get_locator_content(locator)[path] = file

            if self._header_ctime:
                file[self._header_ctime] = format_date_time(time())

        file['content'][:] = content

        if self._header_size:
            file[self._header_size] = len(file['content'])

        if self._header_mtime:
            file[self._header_mtime] = format_date_time(time())

    def copy_object(self, locator, src_path, dst_path):
        """
        Copy object.

        Args:
            locator (str): locator name
            src_path (str): Source object path.
            dst_path (str): Destination object path.
        """
        file = deepcopy(self._get_object(locator, src_path))
        self._get_locator_content(locator)[dst_path] = file
        if self._header_mtime:
            file[self._header_mtime] = format_date_time(time())

    def _get_object(self, locator, path):
        """
        Get object.

        Args:
            locator (str): locator name
            path (str): Object path..

        Returns:
            dict: Object
        """
        try:
            return self._get_locator_content(locator)[path]
        except KeyError:
            raise self.ObjectNotFound

    def get_object(self, locator, path):
        """
        Get object content.

        Args:
            locator (str): locator name
            path (str): Object path..

        Returns:
            bytes: File content.
        """
        # TODO: Range support
        return self._get_object(locator, path)['content']

    def head_object(self, locator, path):
        """
        Get object header.

        Args:
            locator (str): locator name
            path (str): Object path..

        Returns:
            dict: header.
        """
        header = self._get_object(locator, path).copy()
        del header['content']
        return header

    def delete_object(self, locator, path):
        """
        Delete object.

        Args:
            locator (str): locator name
            path (str): Object path..
        """
        try:
            del self._get_locator_content(locator)[path]
        except KeyError:
            raise self.ObjectNotFound


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
    """

    def __init__(self, system, raw_io, buffered_io):
        self._system = system
        self._raw_io = raw_io
        self._buffered_io = buffered_io

        # Get storage root
        root = system.roots[0]

        # Defines randomized names for locator and objects
        self._locator = self._get_id()
        self._locator_url = '/'.join((root, self._locator))
        self._base_dir_path = '%s/%s/' % (self._locator, self._get_id())
        self._base_dir_url = '/'.join((root, self._base_dir_path))

        # Run test sequence
        self._objects = set()
        self._to_clean = self._objects.add
        try:
            self._test_system_locator()
            self._test_system_objects()
            self._test_raw_io()
            self._test_buffered_io()

        finally:
            self._clean_up()

    def _clean_up(self):
        """
        Clean up storage from testing files
        """
        for obj in list(self._objects) + [self._locator]:
            try:
                self._system.remove(obj, relative=True)
            except Exception:
                continue

    @staticmethod
    def _get_id():
        """
        Return an unique ID.

        Returns:
            str: id
        """
        return 'pytest_pycosio_%s' % (str(uuid()).replace('-', ''))

    def _test_raw_io(self):
        """
        Tests raw IO.
        """
        size = 100
        file_path = self._base_dir_path + 'sample_100B.dat'
        self._to_clean(file_path)
        content = urandom(size)

        # Open file in write mode
        file = self._raw_io(file_path, 'wb')
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
        file = self._raw_io(file_path)
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

        finally:
            file.close()

    def _test_buffered_io(self):
        """
        Tests buffered IO.
        """
        # TODO: Implement

    def _test_system_locator(self):
        """
        Test system internals related to locators.
        """
        system = self._system

        # Test: Create locator
        system.make_dir(self._locator_url)

        # Test: Check locator listed
        for name, header in system._list_locators():
            if name == self._locator and isinstance(header, dict):
                break
        else:
            pytest.fail('Locator "%s" not found' % self._locator)

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
        file_path = self._base_dir_path + file_name
        self._to_clean(file_path)
        file_url = self._base_dir_url + file_name
        size = 16
        content = urandom(size)

        with self._raw_io(file_path, mode='w') as file:
            # Write content
            file.write(content)

            # Estimate creation time
            create_time = time()

        # Test: Check file header
        assert isinstance(system.head(path=file_path), dict)

        # Test: Check file size
        assert system.getsize(file_path) == size

        # Test: Check file modification time
        if system._MTIME_KEYS:
            assert system.getmtime(file_path) == pytest.approx(create_time, 2)

        # Test: Check file creation time
        if system._CTIME_KEYS:
            assert system.getctime(file_path) == pytest.approx(create_time, 2)

        # Test: Check path and URL handling
        with self._raw_io(file_path) as file:
            assert file.name == file_path

        with self._raw_io(file_url) as file:
            assert file.name == file_url

        # Write some files
        files = set()
        files.add(file_path)
        for i in range(10):
            path = self._base_dir_path + 'file_num_%d.dat' % i
            files.add(path)
            self._to_clean(path)
            with self._raw_io(path, mode='w') as file:
                file.flush()

        # Test: List objects
        objects = tuple(self._list_objects())
        assert files == set(name for name, _ in objects)
        for _, header in objects:
            assert isinstance(header, dict)

        # Test: List objects, with limited output
        max_request_entries = 5
        assert len(tuple(self._list_objects(
            max_request_entries=max_request_entries))) == max_request_entries

        # Test: List objects, no objects found
        with pytest.raises(ObjectNotFoundError):
            self._list_objects(path=self._base_dir_path + 'dir_not_exists/')

        # Test: List objects, locator not found
        with pytest.raises(ObjectNotFoundError):
            self._list_objects(locator=self._get_id())

        # Test: copy
        copy_path = file_path + '.copy'
        self._to_clean(copy_path)
        system.copy(file_path, copy_path)
        assert system.getsize(copy_path) == size

        # Test: Make a directory (With trailing /)
        dir_path0 = self._base_dir_path + 'directory0/'
        system.make_dir(dir_path0)
        self._to_clean(dir_path0)
        assert dir_path0 in self._list_objects_names()

        # Test: Make a directory (Without trailing /)
        dir_path1 = self._base_dir_path + 'directory1'
        system.make_dir(dir_path1)
        dir_path1 += '/'
        self._to_clean(dir_path1)
        assert dir_path1 in self._list_objects_names()

        # Test: Remove file
        assert file_path in self._list_objects_names()
        system.remove(file_path)
        assert file_path not in self._list_objects_names()

    def _list_objects(self, locator=None, path='', max_request_entries=None):
        """
        List objects with headers.

        Args:
            max_request_entries (int): Max entry count.

        Returns:
            list of tuple: objects.
        """
        if not locator:
            locator = self._locator
        client_args = self._system.get_client_kwargs(locator)
        return self._system._list_objects(
            client_args, path, max_request_entries)

    def _list_objects_names(self):
        """
        List objects names.

        Returns:
            set of str: objects names.
        """
        return set(name for name, _ in self._list_objects())
