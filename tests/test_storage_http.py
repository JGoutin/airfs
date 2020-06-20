"""Test airfs.storage.http"""
import pytest

UNSUPPORTED_OPERATIONS = (
    "copy",
    "getmtime",
    "getctime",
    "getsize",
    "mkdir",
    "listdir",
    "remove",
    "symlink",
    "write",
)


def test_handle_http_errors():
    """Test airfs.http._handle_http_errors"""
    from airfs.storage.http import _handle_http_errors
    from airfs._core.exceptions import ObjectNotFoundError, ObjectPermissionError

    # Mocks response
    class Response:
        """Dummy response"""

        status_code = 200
        reason = "reason"
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


def test_mocked_storage():
    """Tests airfs.http with a mock"""
    import requests
    from requests.exceptions import HTTPError

    import airfs.storage.http
    from airfs.storage.http import HTTPRawIO, _HTTPSystem, HTTPBufferedIO

    from tests.test_storage import StorageTester
    from tests.storage_mock import ObjectStorageMock

    # Mocks client
    class HTTPException(Exception):
        """HTTP Exception

        Args:
            status_code (int): HTTP status
        """

        def __init__(self, status_code):
            self.status_code = status_code

    def raise_404():
        """Raise 404 error"""
        raise HTTPException(404)

    def raise_416():
        """Raise 416 error"""
        raise HTTPException(416)

    def raise_500():
        """Raise 500 error"""
        raise HTTPException(500)

    storage_mock = ObjectStorageMock(raise_404, raise_416, raise_500)

    class Response:
        """HTTP request response"""

        status_code = 200
        reason = "reason"

        def __init__(self, **attributes):
            for name, value in attributes.items():
                setattr(self, name, value)

        def raise_for_status(self):
            """Raise for status"""
            if self.status_code >= 300:
                raise HTTPError(self.reason, response=self)

    class Session:
        """Fake Session"""

        def __init__(self, *_, **__):
            """Do nothing"""

        @staticmethod
        def request(method, url, headers=None, **_):
            """Check arguments and returns fake result"""
            # Remove scheme
            try:
                url = url.split("//")[1]
            except IndexError:
                pass

            # Split path and locator
            locator, path = url.split("/", 1)

            # Perform requests
            try:
                if method == "HEAD":
                    return Response(headers=storage_mock.head_object(locator, path))
                elif method == "GET":
                    return Response(
                        content=storage_mock.get_object(locator, path, header=headers)
                    )
                else:
                    raise ValueError("Unknown method: " + method)

            # Return exception as response with status_code
            except HTTPException as exception:
                return Response(status_code=exception.status_code)

    requests_session = requests.Session
    airfs.storage.http._Session = Session

    # Tests
    try:
        # Init mocked system
        system = _HTTPSystem()
        storage_mock.attach_io_system(system)

        # Tests
        with StorageTester(
            system,
            HTTPRawIO,
            HTTPBufferedIO,
            storage_mock,
            unsupported_operations=UNSUPPORTED_OPERATIONS,
        ) as tester:

            # Common tests
            tester.test_common()

    # Restore mocked functions
    finally:
        airfs.storage.http._Session = requests_session
