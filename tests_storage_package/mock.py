"""airfs storage based on tests.storage_mock"""

from airfs.io import (
    SystemBase as _SystemBase,
    ObjectRawIORandomWriteBase as _ObjectRawIORandomWriteBase,
    ObjectBufferedIORandomWriteBase as _ObjectBufferedIORandomWriteBase,
)
from airfs._core import exceptions as _exc

from tests.storage_mock import ObjectStorageMock as _ObjectStorageMock


def _raise_404():
    """Raise 404 error"""
    raise _exc.ObjectNotFoundError("Object not found")


class _Error416(_exc.AirfsException):
    """416 Error"""


def _raise_416():
    """Raise 416 error"""
    raise _Error416("Invalid range or End of file")


def _raise_500():
    """Raise 500 error"""
    raise _exc.AirfsException("Server error")


class MockSystem(_SystemBase):
    """Mock System"""

    _CTIME_KEYS = ("Created",)

    def _get_client(self):
        """
        Storage client

        Returns:
            tests.storage_mock.ObjectStorageMock: client
        """
        storage_mock = _ObjectStorageMock(_raise_404, _raise_416, _raise_500)
        storage_mock.attach_io_system(self)
        return storage_mock

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        locator, path = self.split_locator(path)
        kwargs = dict(locator=locator)
        if path:
            kwargs["path"] = path
        return kwargs

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """
        return ("mock://",)

    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        if "path" in client_kwargs:
            return self.client.head_object(**client_kwargs)
        return self.client.head_locator(**client_kwargs)

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """
        if "path" in client_kwargs:
            return self.client.put_object(**client_kwargs)
        return self.client.put_locator(**client_kwargs)

    def copy(self, src, dst, other_system=None):
        """
        Copy object of the same storage.

        Args:
            src (str): Path or URL.
            dst (str): Path or URL.
            other_system (airfs._core.io_system.SystemBase subclass):
                Other storage system. May be required for some storage.
        """
        self.client.copy_object(src_path=self.relpath(src), dst_path=self.relpath(dst))

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """
        if "path" in client_kwargs:
            return self.client.delete_object(**client_kwargs)
        return self.client.delete_locator(**client_kwargs)

    def _list_locators(self, max_results):
        """
        Lists locators.

        Yields:
            tuple: locator name str, locator header dict, has content bool
        """
        for locator, headers in self.client.get_locators().items():
            yield locator, headers, True

    def _list_objects(self, client_kwargs, path, max_results, first_level):
        """
        Lists objects.

        args:
            client_kwargs (dict): Client arguments.
            path (str): Path to list.
            max_results (int): The maximum results that should return the method.
            first_level (bool): It True, may only first level objects.

        Yields:
            tuple: object path str, object header dict, has content bool
        """
        prefix = self.split_locator(path)[1]
        objects = tuple(
            self.client.get_locator(
                prefix=prefix,
                limit=max_results,
                raise_404_if_empty=False,
                **client_kwargs,
            ).items()
        )
        if not objects:
            _raise_404()

        index = len(prefix)
        for name, headers in objects:
            yield name[index:], headers, False

    def _shareable_url(self, client_kwargs, expires_in):
        """
        Get a shareable URL for the specified path.

        Args:
            client_kwargs (dict): Client arguments.
            expires_in (int): Expiration in seconds.

        Returns:
            str: Shareable URL.
        """
        return (
            f"https://{client_kwargs['locator']}/"
            f"{client_kwargs['path']}#token=123456"
        )


class MockRawIO(_ObjectRawIORandomWriteBase):
    """Mock Raw IO"""

    _SYSTEM_CLASS = MockSystem

    def _flush(self, buffer, start, end):
        """
        Flush the write buffers of the stream if applicable.

        Args:
            buffer (memoryview): Buffer content.
            start (int): Start of buffer position to flush.
                Supported only if random write supported.
            end (int): End of buffer position to flush.
                Supported only if random write supported.
        """
        self._client.put_object(
            content=buffer, data_range=(start, end), **self._client_kwargs
        )

    def _create(self):
        """
        Create the file if not exists.
        """
        self._client.put_object(new_file=True, **self._client_kwargs)

    def _read_range(self, start, end=0):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position. 0 To not specify end.

        Returns:
            bytes: number of bytes read
        """
        try:
            return self._client.get_object(
                data_range=(start, end or None), **self._client_kwargs
            )
        except _Error416:
            return b""


class MockBufferedIO(_ObjectBufferedIORandomWriteBase):
    """Mock Buffered IO"""

    _RAW_CLASS = MockRawIO
