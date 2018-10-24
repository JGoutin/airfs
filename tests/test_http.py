# coding=utf-8
"""Test pycosio.http"""
import io
import time
from wsgiref.handlers import format_date_time

import pytest

from tests.utilities import (
    parse_range, check_head_methods, check_raw_read_methods)


def test_handle_http_errors():
    """Test pycosio.http._handle_http_errors"""
    from pycosio.storage.http import _handle_http_errors
    from pycosio._core.exceptions import (
        ObjectNotFoundError, ObjectPermissionError)

    # Mocks response
    class Response:
        status_code = 200
        reason = 'reason'
        raised = False

        def raise_for_status(self):
            """Do nothing"""
            self.raised = True

    response = Response()

    # No error
    assert _handle_http_errors(response) is response

    # 403 error
    response.status_code = 403
    with pytest.raises(ObjectPermissionError):
        _handle_http_errors(response)

    # 404 error
    response.status_code = 404
    with pytest.raises(ObjectNotFoundError):
        _handle_http_errors(response)

    # Any error
    response.status_code = 500
    assert not response.raised
    _handle_http_errors(response)
    assert response.raised


def test_http_raw_io():
    """Tests pycosio.http.HTTPRawIO and _HTTPSystem"""
    from io import UnsupportedOperation
    from pycosio.storage.http import HTTPRawIO, _HTTPSystem
    import requests

    # Initializes some variables
    m_time = time.time()

    # Mocks requests

    class Response:
        """Fake response"""

        def __init__(self):
            self.status_code = 200

        reason = 'reason'
        headers = {'Accept-Ranges': 'bytes',
                   'Content-Length': '100',
                   'Last-Modified':
                       format_date_time(m_time)}

    class Session:
        """Fake Session"""

        def __init__(self, *_, **__):
            """Do nothing"""

        @staticmethod
        def request(method, url, headers=None, **_):
            """Check arguments and returns fake result"""
            assert url
            assert method in ('HEAD', 'GET')

            response = Response()

            if method == 'HEAD':
                return response

            try:
                response.content = parse_range(headers)
            except ValueError:
                response.status_code = 416

            return response

    requests_session = requests.Session
    requests.Session = Session

    # Tests
    try:
        http_object = HTTPRawIO('http://accelize.com')

        # Tests head
        check_head_methods(_HTTPSystem(), m_time)

        # Tests read
        check_raw_read_methods(http_object)

        # Test write
        with pytest.raises(io.UnsupportedOperation):
            HTTPRawIO('http://accelize.com', mode='w')

        # Test not seekable
        del Response.headers['Accept-Ranges']
        http_object = HTTPRawIO('http://accelize.com')
        assert not http_object.seekable()

        # Test not implemented features
        with pytest.raises(UnsupportedOperation):
            _HTTPSystem().make_dir('path')
        with pytest.raises(UnsupportedOperation):
            _HTTPSystem().remove('path')
        with pytest.raises(UnsupportedOperation):
            _HTTPSystem()._list_locators()
        with pytest.raises(UnsupportedOperation):
            _HTTPSystem()._list_objects(dict(), '', None)

    # Restore mocked functions
    finally:
        requests.Session = requests_session
