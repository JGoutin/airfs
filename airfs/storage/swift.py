"""OpenStack Swift"""
from contextlib import contextmanager as _contextmanager
from json import dumps as _dumps

import swiftclient as _swift  # type: ignore
from swiftclient.utils import generate_temp_url as _generate_temp_url  # type: ignore
from swiftclient.exceptions import ClientException as _ClientException  # type: ignore

from airfs._core.io_base import memoizedmethod as _memoizedmethod
from airfs._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectPermissionError as _ObjectPermissionError,
    ConfigurationException as _ConfigurationException,
    ObjectNotImplementedError as _ObjectNotImplementedError,
)
from airfs.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase,
)

_ERROR_CODES = {403: _ObjectPermissionError, 404: _ObjectNotFoundError}


@_contextmanager
def _handle_client_exception():
    """
    Handle Swift exception and convert to class IO exceptions

    Raises:
        OSError subclasses: IO error.
    """
    try:
        yield

    except _ClientException as exception:
        if exception.http_status in _ERROR_CODES:
            raise _ERROR_CODES[exception.http_status](exception.http_reason)
        raise


class _SwiftSystem(_SystemBase):
    """
    Swift system.

    Args:
        storage_parameters (dict): Swift connection keyword arguments.
            This is generally OpenStack credentials and configuration.
            (see "swiftclient.client.Connection" for more information)
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    __slots__ = ("_temp_url_key",)
    _SIZE_KEYS = ("content-length", "content_length", "bytes")
    _MTIME_KEYS = ("last-modified", "last_modified")

    def copy(self, src, dst, other_system=None):
        """
        Copy object of the same storage.

        Args:
            src (str): Path or URL.
            dst (str): Path or URL.
            other_system (airfs._core.io_system.SystemBase subclass): Unused.
        """
        container, obj = self.split_locator(src)
        with _handle_client_exception():
            self.client.copy_object(
                container=container, obj=obj, destination=self.relpath(dst)
            )

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        container, obj = self.split_locator(path)
        kwargs = dict(container=container)
        if obj:
            kwargs["obj"] = obj
        return kwargs

    def _get_client(self):
        """
        Swift client

        Returns:
            swiftclient.client.Connection: client
        """
        kwargs = self._storage_parameters

        try:
            self._temp_url_key = kwargs.pop("temp_url_key")
        except KeyError:
            self._temp_url_key = None

        if self._unsecure:
            kwargs = kwargs.copy()
            kwargs["ssl_compression"] = False

        return _swift.client.Connection(**kwargs)

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """
        # URL (May have other format):
        # - https://<endpoint>/v1/AUTH_<project>/<container>/<object>
        return (self.client.get_auth()[0],)

    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_client_exception():
            if "obj" in client_kwargs:
                return self.client.head_object(**client_kwargs)

            return self.client.head_container(**client_kwargs)

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_client_exception():
            if "obj" in client_kwargs:
                return self.client.put_object(
                    client_kwargs["container"], client_kwargs["obj"], b""
                )

            return self.client.put_container(client_kwargs["container"])

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """
        with _handle_client_exception():
            if "obj" in client_kwargs:
                return self.client.delete_object(
                    client_kwargs["container"], client_kwargs["obj"]
                )

            return self.client.delete_container(client_kwargs["container"])

    def _list_locators(self, max_results):
        """
        Lists locators.

        args:
            max_results (int): The maximum results that should return the method.

        Yields:
            tuple: locator name str, locator header dict, has content bool
        """
        kwargs = dict()
        if max_results:
            kwargs["limit"] = max_results
        else:
            kwargs["full_listing"] = True

        with _handle_client_exception():
            response = self.client.get_account(**kwargs)

        for container in response[1]:
            yield container.pop("name"), container, True

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
        index = len(prefix)
        kwargs = dict(prefix=prefix)
        if max_results:
            kwargs["limit"] = max_results
        else:
            kwargs["full_listing"] = True

        with _handle_client_exception():
            response = self.client.get_container(client_kwargs["container"], **kwargs)

        for obj in response[1]:
            yield obj.pop("name")[index:], obj, False

    def _shareable_url(self, client_kwargs, expires_in):
        """
        Get a shareable URL for the specified path.

        Args:
            client_kwargs (dict): Client arguments.
            expires_in (int): Expiration in seconds.

        Returns:
            str: Shareable URL.
        """
        if "obj" not in client_kwargs:
            raise _ObjectNotImplementedError(
                "Shared URLs to containers are not supported on Openstack Swift"
            )

        if not self._temp_url_key:
            raise _ConfigurationException(
                'The "temp_url_key" storage parameter is not defined.'
            )

        self._head(client_kwargs)

        scheme, full_path = self._get_roots()[0].split("://", 1)
        netloc, account_path = full_path.split("/", 1)
        temp_path = _generate_temp_url(
            path=f"/{account_path}/{client_kwargs['container']}/{client_kwargs['obj']}",
            seconds=expires_in,
            key=self._temp_url_key,
            method="GET",
        )
        return f"{scheme}://{netloc}{temp_path}"


class SwiftRawIO(_ObjectRawIOBase):
    """Binary OpenStack Swift Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a' for reading (default), writing or
            appending.
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode or
            awaiting flush in write mode. 0 for no limit.
        storage_parameters (dict): Swift connection keyword arguments.
            This is generally OpenStack credentials and configuration.
            (see "swiftclient.client.Connection" for more information)
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    _SYSTEM_CLASS = _SwiftSystem

    @property  # type: ignore
    @_memoizedmethod
    def _client_args(self):
        """
        Client arguments as tuple.

        Returns:
            tuple of str: Client args.
        """
        return (self._client_kwargs["container"], self._client_kwargs["obj"])

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
            with _handle_client_exception():
                return self._client.get_object(
                    *self._client_args, headers=dict(Range=self._http_range(start, end))
                )[1]

        except _ClientException as exception:
            if exception.http_status == 416:
                # EOF
                return b""
            raise

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        with _handle_client_exception():
            return self._client.get_object(*self._client_args)[1]

    def _flush(self, buffer):
        """
        Flush the write buffers of the stream if applicable.

        Args:
            buffer (memoryview): Buffer content.
        """
        container, obj = self._client_args
        with _handle_client_exception():
            self._client.put_object(container, obj, buffer)


class SwiftBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary OpenStack Swift Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        max_workers (int): The maximum number of threads that can be used to execute the
            given calls.
        storage_parameters (dict): Swift connection keyword arguments.
            This is generally OpenStack credentials and configuration.
            (see "swiftclient.client.Connection" for more information)
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
    """

    __slots__ = ("_container", "_object_name", "_segment_name")

    _RAW_CLASS = SwiftRawIO

    def __init__(self, *args, **kwargs):

        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        self._container, self._object_name = self._raw._client_args

        if self._writable:
            self._segment_name = self._object_name + ".%03d"

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        name = self._segment_name % self._seek
        response = self._workers.submit(
            self._client.put_object, self._container, name, self._get_buffer()
        )

        self._write_futures.append(
            dict(etag=response, path="/".join((self._container, name)))
        )

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        for segment in self._write_futures:
            segment["etag"] = segment["etag"].result()

        with _handle_client_exception():
            self._client.put_object(
                self._container,
                self._object_name,
                _dumps(self._write_futures),
                query_string="multipart-manifest=put",
            )
