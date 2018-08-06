# coding=utf-8
"""OpenStack Swift"""
from contextlib import contextmanager as _contextmanager
from json import dumps as _dumps

import swiftclient as _swift
from swiftclient.exceptions import ClientException as _ClientException

from pycosio._core.exceptions import ObjectNotFoundError, ObjectPermissionError
from pycosio.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase)


@_contextmanager
def _handle_client_exception():
    """
    Handle Swift exception and convert to class
    IO exceptions

    Raises:
        OSError subclasses: IO error.
    """
    try:
        yield

    except _ClientException as exception:
        if exception.http_status in (403, 404):
            raise {403: ObjectPermissionError,
                   404: ObjectNotFoundError}[exception.http_status](
                exception.http_reason)
        raise


class _SwiftSystem(_SystemBase):
    """
    Swift system.

    Args:
        storage_parameters (dict): Swift connection keyword arguments.
            This is generally OpenStack credentials and configuration.
            (see "swiftclient.client.Connection" for more information)
    """

    def __init__(self, *args, **kwargs):
        _SystemBase.__init__(self, *args, **kwargs)

        # Head function
        self._head_object = self._client.head_object

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        container, obj = self.relpath(path).split('/', 1)
        return dict(container=container, obj=obj)

    def _get_client(self):
        """
        Swift client

        Returns:
            swiftclient.client.Connection: client
        """
        return _swift.client.Connection(**self._storage_parameters)

    def _get_prefixes(self):
        """
        Return URL prefixes for this storage.

        Returns:
            tuple of str: URL prefixes
        """
        return self._client.get_auth()[0] + '/', 'swift://'

    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        with _handle_client_exception():
            return self._head_object(**client_kwargs)


class SwiftRawIO(_ObjectRawIOBase):
    """Binary OpenStack Swift Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
        storage_parameters (dict): Swift connection keyword arguments.
            This is generally OpenStack credentials and configuration.
            (see "swiftclient.client.Connection" for more information)
    """
    _SYSTEM_CLASS = _SwiftSystem

    def __init__(self, *args, **kwargs):

        # Initializes storage
        _ObjectRawIOBase.__init__(self, *args, **kwargs)

        # Prepares Swift I/O functions and common arguments
        self._get_object = self._client.get_object
        self._put_object = self._client.put_object
        self._client_args = (
            self._client_kwargs['container'], self._client_kwargs['obj'])

    def _read_range(self, start, end=0):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position.
            end (int): End stream position.
                0 To not specify end.

        Returns:
            bytes: number of bytes read
        """
        try:
            with _handle_client_exception():
                return self._get_object(*self._client_args, headers=dict(
                    Range=self._http_range(start, end)))[1]

        except _ClientException as exception:
            if exception.http_status == 416:
                # EOF
                return b''
            raise

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        with _handle_client_exception():
            return self._get_object(*self._client_args)[1]

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """
        container, obj = self._client_args
        with _handle_client_exception():
            self._put_object(container, obj, memoryview(self._write_buffer))


class SwiftBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary OpenStack Swift Object I/O

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        workers_type (str): Parallel workers type: 'thread' or 'process'.
        storage_parameters (dict): Swift connection keyword arguments.
            This is generally OpenStack credentials and configuration.
            (see "swiftclient.client.Connection" for more information)
    """

    _RAW_CLASS = SwiftRawIO

    def __init__(self, *args, **kwargs):

        _ObjectBufferedIOBase.__init__(self, *args, **kwargs)

        # Use same client as RAW class, but keep theses names
        # protected to this module
        self._put_object = self.raw._put_object
        self._container, self._object_name = self._raw._client_args

        if self._writable:
            self._segment_name = self._object_name + '.%03d'

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        # Upload segment with workers
        name = self._segment_name % self._seek
        response = self._workers.submit(self._put_object, self._container, name,
                                        self._get_buffer().tobytes())

        # Save segment information in manifest
        self._write_futures.append(dict(
            etag=response, path='/'.join((self._container, name))))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        # Wait segments upload completion
        for segment in self._write_futures:
            segment['etag'] = segment['etag'].result()

        # Upload manifest file
        with _handle_client_exception():
            self._put_object(self._container, self._object_name, _dumps(
                self._write_futures), query_string='multipart-manifest=put')
