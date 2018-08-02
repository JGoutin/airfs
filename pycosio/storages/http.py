# coding=utf-8
"""Access file over HTTP"""

from io import UnsupportedOperation as _UnsupportedOperation

import requests as _requests

from pycosio.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase)


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
        raise OSError(response.reason)
    response.raise_for_status()


class HTTPRawIO(_ObjectRawIOBase):
    """Binary HTTP Object I/O

    Args:
        name (str): URL to the file which will be opened.
        mode (str): The mode can be 'r' for reading (default)
    """

    def __init__(self, *args, **kwargs):

        _ObjectRawIOBase.__init__(self, *args, **kwargs)

        # Only support readonly
        if 'r' not in self._mode:
            raise _UnsupportedOperation('write')

        # HTTP session
        self._session = _requests.Session()
        self._request = self._session.request

        # Check if object support random read
        self._seekable = self._head().get('Accept-Ranges') == 'bytes'

    @_ObjectRawIOBase._memoize
    def _head(self):
        """
        Returns object HTTP header.

        Returns:
            dict: HTTP header.
        """
        return _handle_http_errors(
            self._request('HEAD', self._name)).headers

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
        response = self._request(
            'GET', self.name, headers=dict(
                Range=self._http_range(start, end)))

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
            self._request('GET', self.name)).content

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.
        """

    @staticmethod
    def _get_prefix(**__):
        """Return URL prefixes for this storage.

        Returns:
            tuple of str: URL prefixes"""
        return 'http://', 'https://'


class HTTPBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary HTTP Object I/O

    Args:
        name (str): URL to the file which will be opened.
        mode (str): The mode can be 'r' for reading.
        max_workers (int): The maximum number of threads that can be used to
            execute the given calls.
        workers_type (str): Parallel workers type: 'thread' or 'process'.
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
