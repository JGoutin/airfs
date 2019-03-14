# coding=utf-8
"""Test pycosio.storage"""
from contextlib import contextmanager as _contextmanager
from copy import deepcopy as _deepcopy
from time import time as _time


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

    def get_locator(self, locator, prefix=None, limit=None,
                    raise_404_if_empty=True):
        """
        Get locator content.

        Args:
            locator (str): locator name
            prefix (str): Filter returned object with this prefix.
            limit (int): Maximum number of result to return.
            raise_404_if_empty (bool): Raise 404 Error if empty.

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
                file[self._header_ctime] = self._format_date(_time())

        file['content'][:] = content

        if self._header_size:
            file[self._header_size] = len(file['content'])

        if self._header_mtime:
            file[self._header_mtime] = self._format_date(_time())

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

        file = _deepcopy(self._get_object(src_locator, src_path))
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
        content = self._get_object(locator, path)['content']
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
