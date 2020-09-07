"""Test airfs.storage.github"""
import pytest

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
    pytest.xfail(
        "Unable to test using the generic test scenario due to "
        "fixed virtual filesystem tree."
    )


def test_github_storage():
    """Tests airfs.github specificities"""
    from airfs._core.storage_manager import _DEFAULTS

    try:
        assert _DEFAULTS["github"]["storage_parameters"]["token"]
    except (KeyError, AssertionError):
        pytest.skip("GitHub test with real API require a configured API token.")

    github_storage_scenario()


def test_github_mocked_storage():
    """Tests airfs.github specificities with a mock"""
    from collections import OrderedDict
    from datetime import datetime
    from requests import HTTPError
    import airfs._core.storage_manager as storage_manager

    # Mock API responses

    class Response:
        """Mocked Response"""

        def __init__(self, url, **kwargs):
            self.headers = dict(Date=datetime.now().isoformat())
            self.status_code = 200
            self.content = None
            self.url = url

        def json(self):
            """Mocked Json result"""
            return self.content

        def raise_for_status(self):
            """Mocked exception"""
            if self.status_code >= 400:
                raise HTTPError(
                    "Error %s on %s" % (self.status_code, self.url), response=self
                )

    def request(_, url, **kwargs):
        """Mocked requests.request"""
        return Response(url, **kwargs)

    mounted = storage_manager.MOUNTED
    storage_manager.MOUNTED = OrderedDict()
    try:
        storage = storage_manager.mount(storage="github", name="github_test")
        storage["github"]["system_cached"].client._request = request
        github_storage_scenario()
    finally:
        storage_manager.MOUNTED = mounted


def github_storage_scenario():
    """
    Test scenario. Called from both mocked and non-mocked tests.
    """
    # TODO: WIP
