# coding=utf-8
"""Test pycosio.storage"""
from copy import deepcopy as _deepcopy
from time import time as _time
from wsgiref.handlers import format_date_time as _format_date_time


class ObjectStorageMock:
    """
    Mocked Object storage.

    Args:
        raise_404 (callable): Function to call to raise a 404 error
            (Not found).
        raise_416 (callable): Function to call to raise a 416 error
            (End Of File).
    """

    def __init__(self, raise_404, raise_416):
        self._system = None
        self._locators = {}
        self._header_size = None
        self._header_mtime = None
        self._header_ctime = None
        self._raise_404 = raise_404
        self._raise_416 = raise_416

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
            self._raise_404()

    def get_locator(self, locator, prefix=None):
        """
        Get locator content.

        Args:
            locator (str): locator name
            prefix (str): Filter returned object with this prefix.

        Returns:
            dict: objects names, objects headers.
        """
        if prefix is None:
            prefix = ''
        headers = dict()
        for name, header in self._get_locator_content(locator).items():
            if name.startswith(prefix):
                headers[name] = header.copy()
                del headers[name]['content']

        if not headers:
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
            del headers[name]['content']

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
            self._raise_404()

    def put_object(self, locator, path, content):
        """
        Put object.

        Args:
            locator (str): locator name
            path (str): Object path.
            content (bytes like-object): File content.
        """
        try:
            file = self._get_locator_content(locator)[path]
        except KeyError:
            file = dict(content=bytearray())
            self._get_locator_content(locator)[path] = file

            if self._header_ctime:
                file[self._header_ctime] = _format_date_time(_time())

        file['content'][:] = content

        if self._header_size:
            file[self._header_size] = len(file['content'])

        if self._header_mtime:
            file[self._header_mtime] = _format_date_time(_time())

    def concat_objects(self, locator, path, parts):
        """
        Concatenates objects as one object.

        Args:
            locator (str): locator name
            path (str): Object path.
            parts (iterable of str): Paths of objects to concatenate.
        """
        content = bytearray()
        for part in parts:
            content += self.get_object(locator, part)
        self.put_object(locator, path, content)

    def copy_object(self, src_locator, src_path, dst_path):
        """
        Copy object.

        Args:
            src_locator (str): Source locator.
            src_path (str): Source object path.
            dst_path (str): Destination object path.
        """
        dst_locator, dst_path = dst_path.split('/', 1)

        file = _deepcopy(self._get_object(src_locator, src_path))
        self._get_locator_content(dst_locator)[dst_path] = file

        if self._header_mtime:
            file[self._header_mtime] = _format_date_time(_time())

    def _get_object(self, locator, path):
        """
        Get object.

        Args:
            locator (str): locator name
            path (str): Object path.

        Returns:
            dict: Object
        """
        try:
            return self._get_locator_content(locator)[path]
        except KeyError:
            self._raise_404()

    def get_object(self, locator, path, data_range=None):
        """
        Get object content.

        Args:
            locator (str): locator name.
            path (str): Object path.
            data_range (str or tuple or dict):
                Range. in HTTP format or as (start, end) tuple.

        Returns:
            bytes: File content.
        """
        content = self._get_object(locator, path)['content']
        size = len(content)

        if isinstance(data_range, str):
            # Return object part
            data_range = data_range.split('=')[1]
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
            self._raise_404()
