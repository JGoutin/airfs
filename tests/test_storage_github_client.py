"""Test airfs.storage.github._client"""

import pytest


def test_client_api_headers():
    """Test airfs.storage.github._client.Client._api_headers"""
    import airfs.storage.github._client as _client

    # Test token in headers
    client = _client.Client()
    assert "Authorization" not in client._api_headers(), "Unauthenticated headers"

    client = _client.Client(token="123")
    assert client._api_headers()["Authorization"] == "token 123", "Authorization header"

    # Test headers update from previous request
    default_headers = client._api_headers()
    assert (
        client._api_headers(previous_headers={"Last-Modified": "01-02-03"})[
            "If-Modified-Since"
        ]
        == "01-02-03"
    ), "If-Modified-Since header"
    assert (
        client._api_headers(previous_headers={"ETag": "123"})["If-None-Match"] == "123"
    ), "If-None-Match header"
    assert client._api_headers() == default_headers, "Unmodified default headers"


def test_client_rate_limit():
    """Test rate limit with airfs.storage.github._client.Client"""
    import airfs.storage.github._client as _client

    # Mock
    class Response:
        """Mocked Response"""

        def __init__(self):
            self.headers = dict()
            self.json_content = None
            self.status_code = 200

        def json(self):
            """Mocked Json result"""
            return self.json_content

    class RateLimit:
        """Simulate rate limit"""

        remaining = 10

        @classmethod
        def request(cls, method, url, **_):
            """
            Mocked request.

            Args:
                method (str): Method.
                url (str): URL

            Returns:
                request.Response: Response.
            """
            resp = Response()

            # Rate limit API
            if "rate_limit" in url:
                resp.json_content = {
                    "resources": {"core": {"remaining": str(cls.remaining)}}
                }
                if cls.remaining <= 0:
                    # Rate limit reset
                    cls.remaining = 10

            # Any other API
            else:
                if cls.remaining > 0:
                    # Rate limit consumption
                    cls.remaining -= 1
                else:
                    # Rate limit reached
                    resp.status_code = 403
                resp.headers["X-RateLimit-Remaining"] = str(cls.remaining)

            return resp

    class Client(_client.Client):
        """Mocked Client"""

        def __init__(self, **kwargs):
            _client.Client.__init__(self, **kwargs)
            self._request = RateLimit.request

    # Test: Rate limit not reached
    client = Client()
    previous_remaining = RateLimit.remaining
    response = client.request(_client.GITHUB_API)
    assert int(response.headers["X-RateLimit-Remaining"]) == RateLimit.remaining
    assert response.status_code == 200
    assert RateLimit.remaining == previous_remaining - 1

    # Test: Rate limit reached, no wait
    RateLimit.remaining = 0
    client = Client(token="123", wait_rate_limit=False)
    with pytest.raises(_client.GithubRateLimitException):
        client.request(_client.GITHUB_API)

    # Test: Rate limit reached, wait
    RateLimit.remaining = 0
    client = Client(wait_retry_delay=0.001)
    assert not client._RATE_LIMIT_WARNED
    response = client.request(_client.GITHUB_API)
    assert client._RATE_LIMIT_WARNED
    assert response.status_code == 200


def test_client_get(tmpdir):
    """Test airfs.storage.github._client.Client.get"""
    from datetime import datetime, timedelta
    from requests import HTTPError
    import airfs.storage.github._client as _client
    import airfs._core.cache as cache

    # Mock
    path = "/test"
    path_paged = "/test_paged"
    max_pages = 1
    valid_link_header = True

    class Response:
        """Mocked Response"""

        counter = 0
        status_code = 200

        def __init__(self, params):
            try:
                page = int(params["page"]) + 1
            except (KeyError, TypeError):
                page = 2

            self.headers = dict(
                Counter=self.counter,
                Date=datetime.now().isoformat(),
            )
            if max_pages > 1:
                self.headers["Link"] = (
                    '<https://api.github.com/resource?page=%s>; rel="next"' % page
                )
                if valid_link_header:
                    self.headers["Link"] += (
                        ', <https://api.github.com/resource?page=%s>; rel="last"'
                        % max_pages
                    )
            self.json_content = None

        def json(self):
            """Mocked Json result"""
            return self.json_content

        def raise_for_status(self):
            """Raise for status"""
            if self.status_code >= 300:
                raise HTTPError("reason", response=self)

        @classmethod
        def request(cls, method, url, params=None, **_):
            """
            Mocked request.

            Args:
                method (str): Method.
                url (str): URL
                params (dict): Parameters.

            Returns:
                request.Response: Response.
            """
            resp = cls(params)
            cls.counter += 1

            if params is None:
                params = dict()
            content = dict(url=url, **params)
            if url.endswith(path_paged):
                params.setdefault("page", 1)
                content = [dict(url=url, **params)]

            resp.json_content = content
            return resp

    class Client(_client.Client):
        """Mocked Client"""

        def __init__(self, **kwargs):
            _client.Client.__init__(self, **kwargs)
            self._request = Response.request

    cache_dir = cache.CACHE_DIR
    cache.CACHE_DIR = str(tmpdir)
    cache_short_delta = _client._CACHE_SHORT_DELTA

    # Tests
    try:
        client = Client()

        # Get uncached
        response, headers = client.get(path)
        assert response["url"].endswith(path)
        assert headers["Counter"] == 0

        # Get from valid cache
        response, headers = client.get(path)
        assert headers["Counter"] == 0

        # Get but invalid cache
        _client._CACHE_SHORT_DELTA = timedelta(seconds=0)
        response, headers = client.get(path)
        assert headers["Counter"] == 1

        response, headers = client.get(path)
        assert headers["Counter"] == 2

        # Get always valid cache
        response, headers = client.get(path, never_expire=True)
        assert headers["Counter"] == 2

        # Get valid server side
        Response.status_code = 304
        response, headers = client.get(path)
        assert headers["Counter"] == 2

        # Get with params (Must use the cache for same path with no params)
        Response.status_code = 200
        response, headers = client.get(
            path, params=dict(key="value"), never_expire=True
        )
        assert response["key"] == "value"
        assert headers["Counter"] == 4

        # Get paged
        latest_page_read = 0
        max_pages = 5
        for index, response in enumerate(client.get_paged(path_paged)):
            latest_page_read = index + 1
            assert response["page"] == latest_page_read
        assert latest_page_read == max_pages

        # Get paged, single page
        latest_page_read = 0
        max_pages = 1
        for index, response in enumerate(client.get_paged(path_paged)):
            latest_page_read = index + 1
            assert response["page"] == latest_page_read
        assert latest_page_read == max_pages

        # Get paged with params
        latest_page_read = 0
        max_pages = 5
        for index, response in enumerate(
            client.get_paged(path_paged, params=dict(key="value"))
        ):
            latest_page_read = index + 1
            assert response["page"] == latest_page_read
        assert latest_page_read == max_pages

        # Invalid header
        valid_link_header = False
        with pytest.raises(RuntimeError):
            tuple(client.get_paged(path_paged))
        valid_link_header = True

    finally:
        cache.CACHE_DIR = cache_dir
        _client._CACHE_SHORT_DELTA = cache_short_delta
