"""Test airfs.storage.github"""

UNSUPPORTED_OPERATIONS = (
    "copy",
    "mkdir",
    "remove",
    "write",
    "shareable_url",
    "list_locator",
)


def test_mocked_storage():
    """Tests airfs.github with a mock"""
    # TODO: adapt mocked test for GitHub
    import pytest

    pytest.skip("WIP")

    from datetime import datetime

    from tests.test_storage import StorageTester
    from tests.storage_mock import ObjectStorageMock

    from airfs.storage.github import HTTPRawIO, _GithubSystem, HTTPBufferedIO
    import airfs.storage.github._client as _client

    # Mock

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

    class Response:
        """Mocked Response"""

        def __init__(self):
            self.headers = dict(Date=datetime.now().isoformat())
            self.status_code = 200
            self.content = None

        def json(self):
            """Mocked Json result"""
            return self.content

    def request(method, url, **_):
        """Mocked requests.request"""
        resp = Response()

        try:
            # API call
            if url.startswith(_client.GITHUB_API):
                locator, path = url.split(_client.GITHUB_API)[1].split("/", 1)
                headers = storage_mock.head_object(locator, path)
                resp.content = dict()

            else:
                # Raw Github call
                _, locator, path = url.split("://")[1].split("/", 2)
                resp.content = storage_mock.get_object(locator, path)

        # Return exception as response with status_code
        except HTTPException as exception:
            resp.status_code = exception.status_code

        print(method, url, resp.headers, resp.content, resp.status_code)  # TODO: remove
        return resp

    # Init mocked system
    system = _GithubSystem()
    system.client._request = request
    storage_mock = ObjectStorageMock(raise_404, raise_416, raise_500)
    storage_mock.attach_io_system(system)

    # Tests
    with StorageTester(
        system,
        HTTPRawIO,
        HTTPBufferedIO,
        storage_mock,
        unsupported_operations=UNSUPPORTED_OPERATIONS,
        path_prefix="repo_name/HEAD",
    ) as tester:

        # Common tests
        tester.test_common()
