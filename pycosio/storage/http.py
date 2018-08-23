# coding=utf-8
"""Access file over HTTP"""

from io import UnsupportedOperation as _UnsupportedOperation

import requests as _requests

from pycosio._core.exceptions import ObjectNotFoundError, ObjectPermissionError
from pycosio.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase)


def _handle_http_errors(response):
    """
    Check for HTTP errors and raise
    OSError if relevant.

    Args:
        response (requests.Response):

    Returns:
        requests.Response: response
    """
    code = response.status_code
    if 200 <= code < 400:
        return response
    elif code in (403, 404):
        raise {403: ObjectPermissionError,
               404: ObjectNotFoundError}[code](response.reason)
    response.raise_for_status()


class _HTTPSystem(_SystemBase):
    """
    HTTP system.
    """

    # Request Timeout (seconds)
    _TIMEOUT = 5

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        return dict(url=path)

    def _get_client(self):
        """
        HTTP client

        Returns:
            requests.Session: client
        """
        return _requests.Session()

    def _get_prefixes(self):
        """
        Return URL prefixes for this storage.

        Returns:
            tuple of str or re.Pattern: URL prefixes
        """
        return 'http://', 'https://'

    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        return _handle_http_errors(
            self.client.request(
                'HEAD', timeout=self._TIMEOUT, **client_kwargs)).headers


class HTTPRawIO(_ObjectRawIOBase):
    """Binary HTTP Object I/O

    Args:
        name (path-like object): URL to the file which will be opened.
        mode (str): The mode can be 'r' for reading (default)
    """
    _SYSTEM_CLASS = _HTTPSystem
    _TIMEOUT = _HTTPSystem._TIMEOUT

    def __init__(self, *args, **kwargs):

        _ObjectRawIOBase.__init__(self, *args, **kwargs)

        # Only support readonly
        if 'r' not in self._mode:
            raise _UnsupportedOperation('write')

        # Check if object support random read
        self._seekable = self._head().get('Accept-Ranges') == 'bytes'

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
        # Get object part
        response = self._client.request(
            'GET', self.name, headers=dict(Range=self._http_range(start, end)),
            timeout=self._TIMEOUT)

        if response.status_code == 416:
            # EOF
            return b''

        # Get object content
        return _handle_http_errors(response).content

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        return _handle_http_errors(
            self._client.request(
                'GET', self.name, timeout=self._TIMEOUT)).content

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """


class HTTPBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary HTTP Object I/O

    Args:
        name (path-like object): URL to the file which will be opened.
        mode (str): The mode can be 'r' for reading.
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode
            or awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
    """

    _RAW_CLASS = HTTPRawIO

    def _close_writable(self):
        """
        Closes the object in write mode.

        Performs any finalization operation required to
        complete the object writing on the cloud.
        """

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.

        In write mode, send the buffer content to the cloud object.
        """
