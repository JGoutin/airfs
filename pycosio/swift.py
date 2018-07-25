# coding=utf-8
"""OpenStack Swift"""
from contextlib import contextmanager as _contextmanager
from json import dumps as _dumps

import swiftclient as _swift
from swiftclient.exceptions import ClientException as _ClientException

from pycosio.io_base import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase)


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
            raise OSError(exception.http_reason)
        raise


class SwiftRawIO(_ObjectRawIOBase):
    """Binary OpenStack Swift Object I/O

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
        swift_connection_kwargs: Swift connection keyword arguments.
            This is generally OpenStack credentials and configuration.
            (see "swiftclient.client.Connection" for more information)
    """

    def __init__(self, name, mode='r', **swift_connection_kwargs):

        # Splits URL scheme if any
        try:
            path = name.split('://')[1]
        except IndexError:
            path = name
            name = 'swift://' + path

        # Initializes storage
        _ObjectRawIOBase.__init__(self, name, mode)

        # Instantiates Swift connection
        self._connection = _swift.client.Connection(
            **swift_connection_kwargs)

        # Prepares Swift I/O functions and common arguments
        self._get_object = self._connection.get_object
        self._put_object = self._connection.put_object
        self._head_object = self._connection.head_object

        container, object_name = path.split('/', 1)
        self._client_args = (container, object_name)

    def _head(self):
        """
        Returns object HTTP header.

        Returns:
            dict: HTTP header.
        """
        with _handle_client_exception():
            return self._head_object(*self._client_args)

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
                return self._get_object(
                    *self._client_args,
                    headers=dict(
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
            self._put_object(
                container, obj, memoryview(self._write_buffer))


class SwiftBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary OpenStack Swift Object I/O

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w' for reading (default) or writing
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        workers_type (str): Parallel workers type: 'thread' or 'process'.
        swift_connection_kwargs: Swift connection keyword arguments.
            This is generally OpenStack credentials and configuration.
            (see "swiftclient.client.Connection" for more information)
    """

    _RAW_CLASS = SwiftRawIO

    def __init__(self, name, mode='r', buffer_size=None,
                 max_workers=None, workers_type='thread',
                 **swift_connection_kwargs):

        _ObjectBufferedIOBase.__init__(
            self, name, mode=mode, buffer_size=buffer_size,
            max_workers=max_workers, workers_type=workers_type,
            **swift_connection_kwargs)

        # Use same client as RAW class, but keep theses names
        # protected to this module
        self._put_object = self.raw._put_object
        self._container, self._object_name = self._raw._client_args

        if self._writable:
            self._manifest = []
            self._segment_name = self._object_name + '.%03d'

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        # Upload segment with workers
        name = self._segment_name % self._seek
        response = self._workers.submit(
            self._put_object, self._container, name,
            self._get_buffer().tobytes())

        # Save segment information in manifest
        self._manifest.append(dict(etag=response, path='/'.join((
            self._container, name))))

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        # Wait segments upload completion
        for segment in self._manifest:
            segment['etag'] = segment['etag'].result()

        # Upload manifest file
        with _handle_client_exception():
            self._put_object(
                self._container, self._object_name,
                _dumps(self._manifest), query_string='multipart-manifest=put')
