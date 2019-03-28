# coding=utf-8
"""Test pycosio.storage"""
from contextlib import contextmanager as _contextmanager
from copy import deepcopy as _deepcopy
from threading import Lock as _Lock
from time import time as _time
from uuid import uuid4 as _uuid


class ObjectStorageMock:
    """
    Mocked Object storage.

    Args:
        raise_404 (callable): Function to call to raise a 404 error
            (Not found).
        raise_416 (callable): Function to call to raise a 416 error
            (End Of File/Out of range).
        raise_500 (callable): Function to call to raise a 500 error
            (Server exception).
        base_exception (Exception subclass): Type of exception raised by the
            500 error.
    """

    def __init__(self, raise_404, raise_416, raise_500, base_exception,
                 format_date=None):
        self._put_lock = _Lock()
        self._system = None
        self._locators = {}
        self._header_size = None
        self._header_mtime = None
        self._header_ctime = None
        self._raise_404 = raise_404
        self._raise_416 = raise_416
        self._raise_500 = raise_500
        self.base_exception = base_exception
        self._raise_server_error = False
        if format_date is None:
            from wsgiref.handlers import format_date_time
            format_date = format_date_time
        self._format_date = format_date

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

    @_contextmanager
    def raise_server_error(self):
        """Context manager that force sotrage to raise server exception."""
        self._raise_server_error = True
        try:
            yield
        finally:
            self._raise_server_error = False

    def put_locator(self, locator):
        """
        Put a locator.

        Args:
            locator (str): locator name
        """
        self._locators[locator] = locator = dict(_content=dict(),)
        if self._header_ctime:
            locator[self._header_ctime] = self._format_date(_time())
        if self._header_mtime:
            locator[self._header_mtime] = self._format_date(_time())

    def _get_locator(self, locator):
        """
        Get a locator.

        Args:
            locator (str): locator name
        """
        try:
            return self._locators[locator]
        except KeyError:
            self._raise_404()

    def get_locator(self, locator, prefix=None, limit=None,
                    raise_404_if_empty=True, first_level=False, relative=False):
        """
        Get locator content.

        Args:
            locator (str): locator name
            prefix (str): Filter returned object with this prefix.
            limit (int): Maximum number of result to return.
            raise_404_if_empty (bool): Raise 404 Error if empty.
            first_level (bool): If True, return only first level after prefix.
            relative (bool): If True, return objects names relative to prefix.

        Returns:
            dict: objects names, objects headers.
        """
        if prefix is None:
            prefix = ''
        headers = dict()
        for name, header in self._get_locator_content(locator).items():
            if name.startswith(prefix):

                if (relative and prefix) or first_level:
                    # Relative path
                    if prefix:
                        name = name.split(prefix)[1].lstrip('/')

                    # Get first level element
                    if first_level and '/' in name.rstrip('/'):
                        name = name.split('/', 1)[0].rstrip('/')
                        if name:
                            name += '/'

                    # Set name absolute
                    if not relative and prefix and name:
                        name = '%s/%s' % (prefix.rstrip('/'), name)

                    # Skip already existing
                    if first_level and name in headers:
                        continue

                headers[name] = header.copy()
                del headers[name]['_content']

                if len(headers) == limit:
                    break

        if not headers and raise_404_if_empty:
            self._raise_404()

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
            del headers[name]['_content']

        if not headers:
            self._raise_404()

        return headers

    def _get_locator_content(self, locator):
        """
        Get locator content.

        Args:
            locator (str): locator name

        Returns:
            dict: objects names, objects with header.
        """
        return self._get_locator(locator)['_content']

    def head_locator(self, locator):
        """
        Get locator header

        Args:
            locator (str): locator name
        """
        header = self._get_locator(locator).copy()
        del header['_content']
        return header

    def get_locator_ctime(self, locator):
        """
        Get locator creation time.

        Args:
            locator (str): locator name

        Returns:
            object: Creation time.
        """
        return self._get_locator(locator)[self._header_ctime]

    def get_locator_mtime(self, locator):
        """
        Get locator modification time.

        Args:
            locator (str): locator name

        Returns:
            object: Modification time.
        """
        return self._get_locator(locator)[self._header_mtime]

    def get_locator_size(self, locator):
        """
        Get locator size.

        Args:
            locator (str): locator name

        Returns:
            int: Size.
        """
        return self._get_locator(locator)[self._header_size]

    def delete_locator(self, locator):
        """
        Delete locator.

        Args:
            locator (str): locator name
        """
        try:
            del self._locators[locator]
        except KeyError:
            self._raise_404()

    def put_object(self, locator, path, content=None, headers=None,
                   data_range=None, new_file=False):
        """
        Put object.

        Args:
            locator (str): locator name
            path (str): Object path.
            content (bytes like-object): File content.
            headers (dict): Header to put with the file.
            data_range (tuple of int): Range of position of content.
            new_file (bool): If True, force new file creation.

        Returns:
            dict: File header.
        """
        with self._put_lock:
            if new_file:
                self.delete_object(locator, path, not_exists_ok=True)
            try:
                # Existing file
                file = self._get_locator_content(locator)[path]
            except KeyError:
                # New file
                self._get_locator_content(locator)[path] = file = {
                    'Accept-Ranges': 'bytes',
                    'ETag': str(_uuid()),
                    '_content': bytearray(),
                    '_lock': _Lock()
                }

                if self._header_size:
                    file[self._header_size] = 0

                if self._header_ctime:
                    file[self._header_ctime] = self._format_date(_time())

        # Update file
        with file['_lock']:
            if content:
                file_content = file['_content']

                # Write full content
                if not data_range or (
                        data_range[0] is None and data_range[1] is None):
                    file_content[:] = content

                # Write content range
                else:
                    # Define range
                    start, end = data_range
                    if start is None:
                        start = 0
                    if end is None:
                        end = start + len(content)

                    # Add padding if missing data
                    if start > len(file_content):
                        file_content[len(file_content):start] = (
                            start - len(file_content)) * b'\0'

                    # Flush new content
                    file_content[start:end] = content

            if headers:
                file.update(headers)

            if self._header_size:
                file[self._header_size] = len(file['_content'])

            if self._header_mtime:
                file[self._header_mtime] = self._format_date(_time())

            # Return Header
            header = file.copy()
        del header['_content']
        return header

    def concat_objects(self, locator, path, parts):
        """
        Concatenates objects as one object.

        Args:
            locator (str): locator name
            path (str): Object path.
            parts (iterable of str): Paths of objects to concatenate.

        Returns:
            dict: File header.
        """
        content = bytearray()
        for part in parts:
            content += self.get_object(locator, part)
        return self.put_object(locator, path, content)

    def copy_object(self, src_path, dst_path, src_locator=None,
                    dst_locator=None):
        """
        Copy object.

        Args:
            src_path (str): Source object path.
            dst_path (str): Destination object path.
            src_locator (str): Source locator.
            dst_locator (str): Destination locator.
        """
        if src_locator is None:
            src_locator, src_path = src_path.split('/', 1)

        if dst_locator is None:
            dst_locator, dst_path = dst_path.split('/', 1)

        file = self._get_object(src_locator, src_path).copy()
        del file['_lock']
        file = _deepcopy(file)
        file['_lock'] = _Lock()

        self._get_locator_content(dst_locator)[dst_path] = file

        if self._header_mtime:
            file[self._header_mtime] = self._format_date(_time())

    def _get_object(self, locator, path):
        """
        Get object.

        Args:
            locator (str): locator name
            path (str): Object path.

        Returns:
            dict: Object
        """
        # Get object
        try:
            return self._get_locator_content(locator)[path]
        except KeyError:
            self._raise_404()

    def get_object(self, locator, path, data_range=None, header=None):
        """
        Get object content.

        Args:
            locator (str): locator name.
            path (str): Object path.
            data_range (tuple of int): Range as (start, end) tuple.
            header (dict): HTTP header that can contain Range.

        Returns:
            bytes: File content.
        """
        # Simulate server error
        if self._raise_server_error:
            self._raise_500()

        # Read file
        content = self._get_object(locator, path)['_content']
        size = len(content)

        if header and header.get('Range'):
            # Return object part
            data_range = header['Range'].split('=')[1]
            start, end = data_range.split('-')
            start = int(start)
            try:
                end = int(end) + 1
            except ValueError:
                end = size
        elif data_range is not None:
            start, end = data_range
        else:
            start = None
            end = None

        if start is None:
            start = 0
        elif start >= size:
            # EOF reached
            self._raise_416()

        if end is None or end > size:
            end = size

        return content[start:end]

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
        del header['_content']
        return header

    def get_object_ctime(self, locator, path):
        """
        Get object creation time.

        Args:
            locator (str): locator name
            path (str): Object path.

        Returns:
            object: Creation time.
        """
        return self._get_object(locator, path)[self._header_ctime]

    def get_object_mtime(self, locator, path):
        """
        Get object modification time.

        Args:
            locator (str): locator name
            path (str): Object path.

        Returns:
            object: Modification time.
        """
        return self._get_object(locator, path)[self._header_mtime]

    def get_object_size(self, locator, path):
        """
        Get object size.

        Args:
            locator (str): locator name
            path (str): Object path.

        Returns:
            int: Size.
        """
        return self._get_object(locator, path)[self._header_size]

    def delete_object(self, locator, path, not_exists_ok=False):
        """
        Delete object.

        Args:
            locator (str): locator name
            path (str): Object path.
            not_exists_ok (bool): If True, do not raise if object not exist.
        """
        try:
            del self._get_locator_content(locator)[path]
        except KeyError:
            if not not_exists_ok:
                self._raise_404()
