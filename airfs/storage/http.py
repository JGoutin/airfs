"""Access file over HTTP"""
from io import UnsupportedOperation as _UnsupportedOperation

from requests import Session as _Session

from airfs._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectPermissionError as _ObjectPermissionError,
)
from airfs.io import (
    ObjectRawIOBase as _ObjectRawIOBase,
    ObjectBufferedIOBase as _ObjectBufferedIOBase,
    SystemBase as _SystemBase,
)

_CODES_CONVERSION = {403: _ObjectPermissionError, 404: _ObjectNotFoundError}


def _handle_http_errors(response, codes_conversion=None):
    """
    Check for HTTP errors and raise
    OSError if relevant.

    Args:
        response (requests.Response): Request response.
        codes_conversion (dict): Override default return codes conversion to exceptions.

    Returns:
        requests.Response: response
    """
    code = response.status_code
    if 200 <= code < 400:
        return response

    if codes_conversion is None:
        codes_conversion = _CODES_CONVERSION

    if code in codes_conversion:
        raise codes_conversion[code](response.reason)
    response.raise_for_status()


class _HTTPSystem(_SystemBase):
    """
    HTTP system.
    """

    #: Request Timeout (seconds)
    _TIMEOUT = 5

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a specific path.

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
        return _Session()

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """
        return "http://", "https://"

    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        return _handle_http_errors(
            self.client.request("HEAD", timeout=self._TIMEOUT, **client_kwargs)
        ).headers


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
        if "r" not in self._mode:
            raise _UnsupportedOperation("write")
        self._seekable = self._head().get("Accept-Ranges") == "bytes"

    def _read_range(self, start, end=0):
        """
        Read a range of bytes in stream.

        Args:
            start (int): Start stream position. 0 To not specify end.

        Returns:
            bytes: number of bytes read
        """
        response = self._client.request(
            "GET",
            self.name,
            headers=dict(Range=self._http_range(start, end)),
            timeout=self._TIMEOUT,
        )

        if response.status_code == 416:
            return b""

        return _handle_http_errors(response).content

    def _readall(self):
        """
        Read and return all the bytes from the stream until EOF.

        Returns:
            bytes: Object content
        """
        return _handle_http_errors(
            self._client.request("GET", self.name, timeout=self._TIMEOUT)
        ).content

    def _flush(self, *_):
        """
        Flush the write buffers of the stream if applicable.
        """


class HTTPBufferedIO(_ObjectBufferedIOBase):
    """Buffered binary HTTP Object I/O

    Args:
        name (path-like object): URL to the file which will be opened.
        mode (str): The mode can be 'r' for reading.
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode or
            awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to execute
            the given calls.
    """

    _RAW_CLASS = HTTPRawIO

    def _close_writable(self):
        """
        Closes the object in write mode.

        Performs any finalization operation required to complete the object writing on
        the storage.
        """

    def _flush(self):
        """
        Flush the write buffers of the stream if applicable.

        In write mode, send the buffer content to the storage object.
        """
