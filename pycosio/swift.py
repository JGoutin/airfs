# coding=utf-8
"""OpenStack Swift"""

import swiftclient as _swift

from pycosio.io_base import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase)

# TODO: Exception handling


class SwiftRawIO(_ObjectRawIOBase):
    """Binary OpenStack Swift Object I/O

    Args:
        name (str): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a'
            for reading (default), writing or appending
    """

    # Default OpenStack auth-URL to use (str)
    OPENSTACK_AUTH_URL = None

    # Default Interface to use (str)
    OPENSTACK_INTERFACE = None

    def __init__(self, name, mode='r'):

        # Splits URL scheme is any
        try:
            path = name.split('://')[1]
        except IndexError:
            path = name
            name = 'swift://' + path

        # Initializes storage
        _ObjectRawIOBase.__init__(self, name, mode)

        # Instantiates Swift connection
        self._connection = _swift.client.Connection(
            # TODO: args
        )

        # Prepares Swift I/O functions and common arguments
        self._get_object = self._connection.get_object
        self._put_object = self._connection.put_object
        self._head_object = self._connection.head_object

        container, object_name = path.split('/', 1)
        self._client_args = (container, object_name)

    def _head(self):
        """
        Returns object metadata.

        Returns:
            dict: Object metadata.
        """
        return self._head_object(*self._client_args)

    def getsize(self):
        """
        Return the size, in bytes, of path.

        Returns:
            int: Size in bytes.

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        # TODO:

    def getmtime(self):
        """
        Return the time of last access of path.

        Returns:
            float: The number of seconds since the epoch
                (see the time module).

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        # TODO:

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
        return self._get_object(
            *self._client_args,
            headers=dict(Range=self._http_range(start, end))
            )[1]
        # TODO: EOF handling

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        return self._get_object(*self._client_args)[1]

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """
        container, obj = self._client_args
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
    """

    _RAW_CLASS = SwiftRawIO

    def _flush(self):
        """
        Flush the write buffers of the stream.
        """
        # TODO:

    def _close_writable(self):
        """
        Close the object in write mode.
        """
        # TODO:
