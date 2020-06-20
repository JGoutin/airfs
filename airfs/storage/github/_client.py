"""GitHub API client
https://developer.github.com/v3/
"""
from datetime import datetime, timedelta
from json import dumps
from time import sleep
from urllib.parse import urlparse, parse_qs

from dateutil.parser import parse
from requests import Session

from airfs.storage.http import _handle_http_errors
from airfs._core.exceptions import (
    AirfsWarning,
    AirfsException,
    ObjectNotFoundError,
    ObjectPermissionError,
)
from airfs._core.cache import get_cache, set_cache, CACHE_SHORT_EXPIRY, NoCacheException


GITHUB_API = "https://api.github.com"

_CACHE_SHORT_DELTA = timedelta(seconds=CACHE_SHORT_EXPIRY)

_CODES_CONVERSION = {
    403: ObjectPermissionError,
    404: ObjectNotFoundError,
    # The API sometime returns this code when a commit hash is not found instead of
    # returning 404
    422: ObjectNotFoundError,
}


class GithubRateLimitException(AirfsException):
    """Exception if rate limit reached"""


class GithubRateLimitWarning(AirfsWarning):
    """Warning if rate limit reached and waiting"""


class Client:
    """
    GitHub REST API client.

    Args:
        token (str): GitHub API authentication token.
        wait_rate_limit (bool): If True, wait if API rate limit is reached, else raise
            "airfs.storage.github.GithubRateLimitException" exception.
        wait_warn (bool): If True and "wait_rate_limit" is True, warn using
            "airfs.storage.github.GithubRateLimitWarning" when waiting for rate limit
            reset for the first time.
        wait_retry_delay (int): Delay in seconds between two API get attempt when
            waiting for rate limit reset.
    """

    _RATE_LIMIT_WARNED = False

    __slots__ = (
        "_request",
        "_http_session",
        "_token",
        "_headers",
        "_wait_rate_limit",
        "_wait_warn",
        "_wait_retry_delay",
    )

    def __init__(
        self, token=None, wait_rate_limit=True, wait_warn=True, wait_retry_delay=60
    ):
        self._wait_rate_limit = wait_rate_limit
        self._wait_warn = wait_warn
        self._wait_retry_delay = wait_retry_delay
        self._headers = None
        self._token = token
        self._http_session = Session()
        self._request = self._http_session.request

    def _api_headers(self, previous_headers=None):
        """
        Return headers to use to make requests to GitHub API.

        Args:
            previous_headers (dict): Headers from a previous cached identical request.
                Used to perform a conditional request to check if data was updated
                without consume the rate limit.

        Returns:
            dict or None: API request headers.
        """
        # Create headers base with authentication if token provided
        if self._headers is None:
            auth_headers = {}
            token = self._token
            if token:
                auth_headers["Authorization"] = "token %s" % token
            self._headers = auth_headers

        # Add If-Modified-Since to perform API Conditional requests
        if previous_headers is not None:
            headers = self._headers.copy()
            for condition, key in (
                ("If-Modified-Since", "Last-Modified"),
                ("If-None-Match", "ETag"),
            ):
                try:
                    headers[condition] = previous_headers[key]
                except KeyError:
                    continue
            return headers

        return self._headers

    def request(self, path, method="GET", **kwargs):
        """
        Perform an HTTP request over the GitHub API. Also handle the case where the
        rate-limit is reached.

        Args:
            path (str): GitHub API path.
            method (str): HTTP method. Default to "GET".
            kwargs: requests.request keyword arguments.

        Returns:
            requests.Response: Response.
        """
        while True:
            response = self._request(method, GITHUB_API + path, **kwargs)

            # If API Rate limit is reached, wait until ready and retry
            if (
                response.status_code == 403
                and int(response.headers.get("X-RateLimit-Remaining", "-1")) == 0
            ):
                self._handle_rate_limit()
                continue

            return response

    def get(self, path, params=None, never_expire=False):
        """
        Get result from the GitHub API. Also handle caching of result to speed up
        futures requests and improve rate-limit consumption.

        Args:
            path (str): GitHub API path.
            params (dict): Request parameters.
            never_expire (bool): Indicate that the request result should never expire
                and can be cached indefinitely.

        Returns:
            tuple: result dict, headers dict.
        """
        # Retrieve request from cache
        cache_name = path
        if params:
            cache_name += dumps(params)
        try:
            result, headers = get_cache(cache_name)
        except NoCacheException:
            # No cached result
            result = headers = None

        else:
            # Return cached result directly if not expire
            if never_expire:
                return result, headers

            # Return cached result directly if very recent
            dt_date = parse(headers["Date"])
            if dt_date > datetime.now(dt_date.tzinfo) - _CACHE_SHORT_DELTA:
                return result, headers

        # Perform the request
        response = self.request(
            path, params=params, headers=self._api_headers(previous_headers=headers)
        )

        # If not an unchanged result, return previously cached result directly
        if response.status_code == 304:
            return result, headers

        # Else, return the new result and cache it for later
        _handle_http_errors(response, _CODES_CONVERSION)
        result = response.json()
        headers = dict(response.headers)
        set_cache(cache_name, [result, headers], long=True)
        return result, headers

    def get_paged(self, path, params=None):
        """
        Get a multiple paged result from the GitHub API.

        Args:
            path (str): GitHub API path.
            params (dict): Request parameters.

        Returns:
            generator of dict: results.
        """
        if params:
            params = params.copy()
        else:
            params = dict()

        max_page = 0
        page = 1
        while page <= max_page or not max_page:
            results, headers = self.get(path, params=params)
            for result in results:
                yield result

            page += 1
            params["page"] = page

            if max_page == 0:
                # Get last page from the "link" header value.
                try:
                    links = headers["link"]
                except KeyError:
                    # If not present, there is only one page.
                    break
                max_page = self._parse_link_header(links)

    @staticmethod
    def _parse_link_header(links):
        """
        Get number of the last page from the "Link" header.

        Args:
            links (str): "Links" header value.

        Returns:
            int: Number of the last page.
        """
        for link in links.split(","):
            url, rel = link.split(";", 1)
            if rel.strip() == 'rel="last"':
                return int(parse_qs(urlparse(url.strip("<>")).query)["page"][0])

    def _handle_rate_limit(self):
        """
        Wait until remaining rate limit is greater than 0, or raise exception.
        """
        while self._wait_rate_limit:
            # Warn user once per session
            if self._wait_warn and not Client._RATE_LIMIT_WARNED:
                from warnings import warn

                warn(self._rate_limit_reached(True), GithubRateLimitWarning)
                Client._RATE_LIMIT_WARNED |= True

            # Wait until rate limit API return remaining > 0
            sleep(self._wait_retry_delay)
            resp = self.request(
                "GET", GITHUB_API + "/rate_limit", headers=self._api_headers()
            )
            if int((resp.json())["resources"]["core"]["remaining"]):
                return

        raise GithubRateLimitException(self._rate_limit_reached())

    def _rate_limit_reached(self, waiting=False):
        """
        Rate limit message for exception or warning.

        Args:
            waiting (bool): True if waiting for reset.

        Returns:
            str: exception/Warning message
        """
        msg = ["GitHub rate limit reached."]
        if waiting:
            msg.append("Waiting for limit reset...")
        if "Authorization" not in self._headers:
            msg.append("Authenticate to GitHub to increase the limit.")
        return " ".join(msg)
