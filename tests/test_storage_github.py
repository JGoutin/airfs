"""Test airfs.storage.github."""
import json
import pickle
from os.path import realpath, join

import requests
import pytest

UNSUPPORTED_OPERATIONS = (
    "copy",
    "mkdir",
    "remove",
    "write",
    "shareable_url",
    "list_locator",
)

#: Set to True and run tests to update mock test responses with real test responses
UPDATE_MOCK = False

#: Directory where are saved cached responses from GitHub API to use with mock
MOCK_DIR = realpath(join(__file__, "../resources/github_mock_responses"))


class MockResponse:
    """Mocked request.Response."""

    def __init__(self, url, headers, status_code, content, reason):
        """Init.

        Args:
            url: URL.
            headers: Headers.
            status_code: Status code.
            content: Content.
            reason: Reason.
        """
        self.headers = headers
        self.status_code = status_code
        self.content = content
        self.url = url
        self.reason = reason

    def json(self):
        """Mocked Json result."""
        return json.loads(self.content)

    @property
    def text(self):
        """Mocked Text result."""
        return self.content.decode()

    def raise_for_status(self):
        """Mocked exception."""
        if self.status_code >= 400:
            raise requests.HTTPError(
                f"{self.status_code} Error: {self.reason} for: {self.url}",
                response=self,
            )


def test_mocked_storage():
    """Tests airfs.github with a mock."""
    pytest.skip(
        "Unable to test using the generic test scenario due to "
        "fixed virtual filesystem tree."
    )


def test_github_storage(tmpdir):
    """Tests airfs.github specificities."""
    from airfs._core.storage_manager import _DEFAULTS

    try:
        assert _DEFAULTS["github"]["storage_parameters"]["token"]
    except (KeyError, AssertionError):
        pytest.skip("GitHub test with real API require a configured API token.")

    if UPDATE_MOCK:
        # Save all requests response to use them with mock
        from os import remove, listdir
        from airfs._core import cache
        from airfs._core.storage_manager import get_instance

        for file in listdir(MOCK_DIR):
            remove(join(MOCK_DIR, file))

        system = get_instance("https://github.com")
        request = system.client._request

        def request_save(method, url, *args, params=None, **kwargs):
            """Performs requests and save result."""
            resp = request(method, url, *args, params=params, **kwargs)
            resp_dict = dict(
                url=resp.url,
                headers=resp.headers,
                status_code=resp.status_code,
                content=resp.content,
                reason=resp.reason,
            )
            with open(
                join(MOCK_DIR, cache._hash_name(url + json.dumps(params or dict()))),
                "wb",
            ) as resp_cache:
                pickle.dump(resp_dict, resp_cache)
            return MockResponse(**resp_dict)

        cache_dir = cache.CACHE_DIR
        cache.CACHE_DIR = str(tmpdir.ensure_dir("cache"))

        system.client._request = request_save
        system.client.session.request = request_save

    try:
        github_storage_scenario()

    finally:
        if UPDATE_MOCK:
            system.client.session.request = request
            system.client._request = request
            cache.CACHE_DIR = cache_dir


def test_github_mocked_storage(tmpdir):
    """Tests airfs.github specificities with a mock."""
    if UPDATE_MOCK:
        pytest.skip("Mock is updating...")

    from collections import OrderedDict

    import airfs._core.storage_manager as storage_manager
    from airfs._core import cache

    cache_dir = cache.CACHE_DIR
    cache.CACHE_DIR = str(tmpdir.ensure_dir("cache"))

    mounted = storage_manager.MOUNTED
    storage_manager.MOUNTED = OrderedDict()

    def request_load(_, url, *__, params=None, **___):
        """Loads request result."""
        try:
            with open(
                join(MOCK_DIR, cache._hash_name(url + json.dumps(params or dict()))),
                "rb",
            ) as resp_cache:
                return MockResponse(**pickle.load(resp_cache))
        except FileNotFoundError:
            pytest.fail("Please, update mock responses (see UPDATE_MOCK)")

    try:
        # Loads requests responses from previously cached responses
        storage = storage_manager.mount(storage="github", name="github_test")
        client = storage["github"]["system_cached"].client

        client._request = request_load
        client.session.request = request_load

        # Tests
        github_storage_scenario()

    finally:
        storage_manager.MOUNTED = mounted
        cache.CACHE_DIR = cache_dir


def github_storage_scenario():
    """Test scenario. Called from both mocked and non-mocked tests."""
    exists_scenario()
    listdir_scenario()
    stat_scenario()
    symlink_scenario()
    get_scenario()


def listdir_scenario():
    """Tests listing."""
    from io import UnsupportedOperation
    import airfs

    # Users
    with pytest.raises(UnsupportedOperation):
        airfs.listdir("https://github.com/")

    # Repos
    assert "airfs" in airfs.listdir("https://github.com/jgoutin"), "List repos"

    assert sorted(airfs.listdir("https://github.com/jgoutin/airfs")) == [
        "HEAD",
        "archive",
        "blob",
        "branches",
        "commits",
        "refs",
        "releases",
        "tags",
        "tree",
    ], "List repo content"

    with pytest.raises(FileNotFoundError):
        airfs.listdir("https://github.com/jgoutin/not_exists")

    with pytest.raises(FileNotFoundError):
        airfs.listdir("https://github.com/jgoutin/airfs/not_exists")

    assert sorted(airfs.listdir("https://github.com/jgoutin/airfs/refs")) == [
        "heads",
        "tags",
    ], "List refs"

    with pytest.raises(FileNotFoundError):
        airfs.listdir("https://github.com/jgoutin/airfs/refs/not_exists")

    # HEAD
    assert "LICENSE" in airfs.listdir(
        "https://github.com/jgoutin/airfs/HEAD"
    ), "List HEAD"

    with pytest.raises(NotADirectoryError):
        airfs.listdir("https://github.com/jgoutin/airfs/HEAD/LICENSE")

    with pytest.raises(FileNotFoundError):
        airfs.listdir("https://github.com/jgoutin/airfs/HEAD/not_exists")

    assert "_core" in airfs.listdir(
        "https://github.com/jgoutin/airfs/HEAD/airfs"
    ), "List HEAD subdirectory"

    # Branches
    assert "master" in airfs.listdir(
        "https://github.com/jgoutin/airfs/branches"
    ), "List branches"

    assert "master" in airfs.listdir(
        "https://github.com/jgoutin/airfs/refs/heads"
    ), "List branches in refs"

    assert "LICENSE" in airfs.listdir(
        "https://github.com/jgoutin/airfs/branches/master"
    ), "List branch content"

    with pytest.raises(FileNotFoundError):
        airfs.listdir("https://github.com/jgoutin/airfs/branches/not_exists")

    # Commits
    commit_id = airfs.listdir("https://github.com/jgoutin/airfs/commits")[0]
    assert len(commit_id) == 40, "List commits"

    assert "LICENSE" in airfs.listdir(
        f"https://github.com/jgoutin/airfs/commits/{commit_id}"
    ), "List commit content"

    with pytest.raises(FileNotFoundError):
        airfs.listdir("https://github.com/jgoutin/airfs/commits/not_exists")

    # Tags
    assert "1.4.0" in airfs.listdir(
        "https://github.com/jgoutin/airfs/tags"
    ), "List tags"

    assert "1.4.0" in airfs.listdir(
        "https://github.com/jgoutin/airfs/refs/tags"
    ), "List tags in refs"

    assert "LICENSE" in airfs.listdir(
        "https://github.com/jgoutin/airfs/tags/1.4.0"
    ), "List tag content"

    with pytest.raises(FileNotFoundError):
        airfs.listdir("https://github.com/jgoutin/airfs/tags/not_exists")

    # Archives
    assert "1.4.0.tar.gz" in airfs.listdir(
        "https://github.com/jgoutin/airfs/archive"
    ), "List tar.gz archives"

    assert "1.4.0.zip" in airfs.listdir(
        "https://github.com/jgoutin/airfs/archive"
    ), "List zip archives"

    with pytest.raises(NotADirectoryError):
        airfs.listdir("https://github.com/jgoutin/airfs/archive/1.4.0.tar.gz")

    with pytest.raises(FileNotFoundError):
        airfs.listdir("https://github.com/jgoutin/airfs/archive/1.4.0.tar.xz")

    # Releases
    assert "latest" in airfs.listdir(
        "https://github.com/jgoutin/airfs/releases"
    ), "List releases"

    assert "1.4.0" in airfs.listdir(
        "https://github.com/jgoutin/airfs/releases/tag"
    ), "List release tags"

    assert sorted(
        airfs.listdir("https://github.com/jgoutin/airfs/releases/tag/1.4.0")
    ) == ["archive", "assets", "tree"], "List release content"

    with pytest.raises(FileNotFoundError):
        airfs.listdir("https://github.com/jgoutin/airfs/releases/tag/not_exists")

    assert sorted(
        airfs.listdir("https://github.com/jgoutin/airfs/releases/latest")
    ) == ["archive", "assets", "tree"], "List latest release content"

    assert sorted(
        airfs.listdir("https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive")
    ) == ["source_code.tar.gz", "source_code.zip"], "List release archive"

    with pytest.raises(FileNotFoundError):
        airfs.listdir(
            "https://github.com/jgoutin/airfs/releases/tag/not_exists/archive"
        )

    with pytest.raises(NotADirectoryError):
        airfs.listdir(
            "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/"
            "source_code.tar.gz"
        )

    assert sorted(
        airfs.listdir("https://github.com/jgoutin/airfs/releases/latest/archive")
    ) == ["source_code.tar.gz", "source_code.zip"], "List latest release archive"

    with pytest.raises(NotADirectoryError):
        airfs.listdir(
            "https://github.com/jgoutin/airfs/releases/latest/archive/"
            "source_code.tar.gz"
        )

    assert sorted(
        airfs.listdir("https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets")
    ) == ["airfs-1.4.0-py3-none-any.whl"], "List release assets"

    with pytest.raises(NotADirectoryError):
        airfs.listdir(
            "https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets/"
            "airfs-1.4.0-py3-none-any.whl"
        )

    with pytest.raises(FileNotFoundError):
        airfs.listdir(
            "https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets/not_exists"
        )


def symlink_scenario():
    """Tests symbolic links."""
    from io import UnsupportedOperation
    import airfs

    # Git tree
    assert airfs.islink("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")
    assert airfs.exists("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")
    assert airfs.lexists(
        "https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink"
    )
    assert not airfs.isdir(
        "https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink"
    )
    assert airfs.isfile("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")

    assert not airfs.islink("https://github.com/jgoutin/airfs/HEAD/LICENSE")
    assert not airfs.islink("https://github.com/jgoutin/airfs/HEAD/tests")

    assert (
        airfs.readlink("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")
        == "../../airfs/_core/exceptions.py"
    )
    assert (
        airfs.realpath("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")
        == "https://github.com/jgoutin/airfs/HEAD/airfs/_core/exceptions.py"
    )
    assert (
        airfs.realpath(
            "https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink_to_symlink"
        )
        == "https://github.com/jgoutin/airfs/HEAD/airfs/_core/exceptions.py"
    )
    with pytest.raises(UnsupportedOperation):
        airfs.symlink(
            "https://github.com/jgoutin/airfs/HEAD/airfs/_core/exceptions.py",
            "https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink_2",
        )

    with pytest.raises(OSError):
        airfs.readlink("https://github.com/jgoutin/airfs/HEAD/LICENSE")

    # HEAD
    assert airfs.islink("https://github.com/jgoutin/airfs/HEAD")
    assert (
        airfs.readlink("https://github.com/jgoutin/airfs/HEAD")
        == "https://github.com/jgoutin/airfs/branches/master"
    )
    assert airfs.realpath("https://github.com/jgoutin/airfs/HEAD").startswith(
        "https://github.com/jgoutin/airfs/commits/"
    )

    # Branches
    assert airfs.readlink(
        "https://github.com/jgoutin/airfs/branches/master"
    ).startswith("https://github.com/jgoutin/airfs/commits/")
    assert airfs.realpath(
        "https://github.com/jgoutin/airfs/branches/master"
    ).startswith("https://github.com/jgoutin/airfs/commits/")
    assert airfs.readlink(
        "https://github.com/jgoutin/airfs/refs/heads/master"
    ).startswith("https://github.com/jgoutin/airfs/commits/")
    assert airfs.readlink("https://github.com/jgoutin/airfs/blob/master").startswith(
        "https://github.com/jgoutin/airfs/commits/"
    )
    assert airfs.readlink("https://github.com/jgoutin/airfs/tree/master").startswith(
        "https://github.com/jgoutin/airfs/commits/"
    )

    # Tags
    assert airfs.readlink("https://github.com/jgoutin/airfs/tags/1.4.0").startswith(
        "https://github.com/jgoutin/airfs/commits/"
    )
    assert airfs.realpath("https://github.com/jgoutin/airfs/tags/1.4.0").startswith(
        "https://github.com/jgoutin/airfs/commits/"
    )
    assert airfs.readlink(
        "https://github.com/jgoutin/airfs/refs/tags/1.4.0"
    ).startswith("https://github.com/jgoutin/airfs/commits/")
    assert airfs.readlink("https://github.com/jgoutin/airfs/blob/1.4.0").startswith(
        "https://github.com/jgoutin/airfs/commits/"
    )
    assert airfs.readlink("https://github.com/jgoutin/airfs/tree/1.4.0").startswith(
        "https://github.com/jgoutin/airfs/commits/"
    )

    # Releases
    assert airfs.readlink(
        "https://github.com/jgoutin/airfs/releases/latest"
    ).startswith("https://github.com/jgoutin/airfs/releases/tag/")


def exists_scenario():
    """Tests exists, isdir, isfile."""
    import airfs

    # Root
    assert airfs.exists("https://github.com")
    assert airfs.isdir("https://github.com")
    assert not airfs.isfile("https://github.com")

    # User
    assert airfs.exists("https://github.com/jgoutin")
    assert airfs.isdir("https://github.com/jgoutin")
    assert not airfs.isfile("https://github.com/jgoutin")

    # Repos
    assert airfs.exists("https://github.com/jgoutin/airfs")
    assert airfs.isdir("https://github.com/jgoutin/airfs")
    assert not airfs.isfile("https://github.com/jgoutin/airfs")

    assert not airfs.exists("https://github.com/jgoutin/not_exists")
    assert not airfs.isdir("https://github.com/jgoutin/not_exists")
    assert not airfs.isfile("https://github.com/jgoutin/not_exists")

    assert not airfs.exists("https://github.com/jgoutin/airfs/refs/not_exists")
    assert not airfs.isdir("https://github.com/jgoutin/airfs/refs/not_exists")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/refs/not_exists")

    assert airfs.exists("https://raw.githubusercontent.com/jgoutin/airfs")
    assert airfs.isdir("https://raw.githubusercontent.com/jgoutin/airfs")
    assert not airfs.isfile("https://raw.githubusercontent.com/jgoutin/airfs")

    # HEAD
    assert airfs.exists("https://github.com/jgoutin/airfs/HEAD")
    assert airfs.isdir("https://github.com/jgoutin/airfs/HEAD")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/HEAD")

    assert airfs.exists("https://github.com/jgoutin/airfs/tree/HEAD")
    assert airfs.isdir("https://github.com/jgoutin/airfs/tree/HEAD")
    assert not airfs.isfile("https://github.com/jgoutin/tree/HEAD")

    assert airfs.exists("https://github.com/jgoutin/airfs/blob/HEAD")
    assert airfs.isdir("https://github.com/jgoutin/airfs/blob/HEAD")
    assert not airfs.isfile("https://github.com/jgoutin/blob/HEAD")

    assert airfs.exists("https://raw.githubusercontent.com/jgoutin/airfs/HEAD")
    assert airfs.isdir("https://raw.githubusercontent.com/jgoutin/airfs/HEAD")
    assert not airfs.isfile("https://raw.githubusercontent.com/jgoutin/airfs/HEAD")

    # Branches
    assert airfs.exists("https://github.com/jgoutin/airfs/branches")
    assert airfs.isdir("https://github.com/jgoutin/airfs/branches")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/branches")

    assert airfs.exists("https://github.com/jgoutin/airfs/branches/master")
    assert airfs.isdir("https://github.com/jgoutin/airfs/branches/master")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/branches/master")

    assert airfs.exists("https://github.com/jgoutin/airfs/refs/heads/master")
    assert airfs.isdir("https://github.com/jgoutin/airfs/refs/heads/master")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/refs/heads/master")

    assert airfs.exists("https://github.com/jgoutin/airfs/tree/master")
    assert airfs.isdir("https://github.com/jgoutin/airfs/tree/master")
    assert not airfs.isfile("https://github.com/jgoutin/tree/master")

    assert airfs.exists("https://github.com/jgoutin/airfs/blob/master")
    assert airfs.isdir("https://github.com/jgoutin/airfs/blob/master")
    assert not airfs.isfile("https://github.com/jgoutin/blob/master")

    assert airfs.exists("https://raw.githubusercontent.com/jgoutin/airfs/master")
    assert airfs.isdir("https://raw.githubusercontent.com/jgoutin/airfs/master")
    assert not airfs.isfile("https://raw.githubusercontent.com/jgoutin/airfs/master")

    assert not airfs.exists("https://github.com/jgoutin/airfs/branches/not_exists")
    assert not airfs.isdir("https://github.com/jgoutin/airfs/branches/not_exists")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/branches/not_exists")

    # Tags
    assert airfs.exists("https://github.com/jgoutin/airfs/tags")
    assert airfs.isdir("https://github.com/jgoutin/airfs/tags")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/tags")

    assert airfs.exists("https://github.com/jgoutin/airfs/tags/1.4.0")
    assert airfs.isdir("https://github.com/jgoutin/airfs/tags/1.4.0")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/tags/1.4.0")

    assert airfs.exists("https://github.com/jgoutin/airfs/refs/tags/1.4.0")
    assert airfs.isdir("https://github.com/jgoutin/airfs/refs/tags/1.4.0")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/refs/tags/1.4.0")

    assert airfs.exists("https://github.com/jgoutin/airfs/tree/1.4.0")
    assert airfs.isdir("https://github.com/jgoutin/airfs/tree/1.4.0")
    assert not airfs.isfile("https://github.com/jgoutin/tree/1.4.0")

    assert airfs.exists("https://github.com/jgoutin/airfs/blob/1.4.0")
    assert airfs.isdir("https://github.com/jgoutin/airfs/blob/1.4.0")
    assert not airfs.isfile("https://github.com/jgoutin/blob/1.4.0")

    assert airfs.exists("https://raw.githubusercontent.com/jgoutin/airfs/1.4.0")
    assert airfs.isdir("https://raw.githubusercontent.com/jgoutin/airfs/1.4.0")
    assert not airfs.isfile("https://raw.githubusercontent.com/jgoutin/airfs/1.4.0")

    assert not airfs.exists("https://github.com/jgoutin/airfs/tags/not_exists")
    assert not airfs.isdir("https://github.com/jgoutin/airfs/tags/not_exists")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/tags/not_exists")

    # Commits
    assert airfs.exists("https://github.com/jgoutin/airfs/commits")
    assert airfs.isdir("https://github.com/jgoutin/airfs/commits")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/commits")

    commit_id = airfs.listdir("https://github.com/jgoutin/airfs/commits")[0]
    assert airfs.exists(f"https://github.com/jgoutin/airfs/commits/{commit_id}")
    assert airfs.isdir(f"https://github.com/jgoutin/airfs/commits/{commit_id}")
    assert not airfs.isfile(f"https://github.com/jgoutin/airfs/commits/{commit_id}")

    assert airfs.exists(f"https://github.com/jgoutin/airfs/tree/{commit_id}")
    assert airfs.isdir(f"https://github.com/jgoutin/airfs/tree/{commit_id}")
    assert not airfs.isfile(f"https://github.com/jgoutin/tree/{commit_id}")

    assert airfs.exists(f"https://github.com/jgoutin/airfs/blob/{commit_id}")
    assert airfs.isdir(f"https://github.com/jgoutin/airfs/blob/{commit_id}")
    assert not airfs.isfile(f"https://github.com/jgoutin/blob/{commit_id}")

    assert airfs.exists(f"https://raw.githubusercontent.com/jgoutin/airfs/{commit_id}")
    assert airfs.isdir(f"https://raw.githubusercontent.com/jgoutin/airfs/{commit_id}")
    assert not airfs.isfile(
        f"https://raw.githubusercontent.com/jgoutin/airfs/{commit_id}"
    )

    assert not airfs.exists("https://github.com/jgoutin/airfs/commits/not_exists")
    assert not airfs.isdir("https://github.com/jgoutin/airfs/commits/not_exists")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/commits/not_exists")

    # Git Tree
    assert airfs.exists("https://github.com/jgoutin/airfs/HEAD/tests")
    assert airfs.isdir("https://github.com/jgoutin/airfs/HEAD/tests")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/HEAD/tests")

    assert airfs.exists("https://github.com/jgoutin/airfs/HEAD/LICENSE")
    assert not airfs.isdir("https://github.com/jgoutin/airfs/HEAD/LICENSE")
    assert airfs.isfile("https://github.com/jgoutin/airfs/HEAD/LICENSE")

    assert airfs.exists("https://raw.githubusercontent.com/jgoutin/airfs/HEAD/LICENSE")
    assert not airfs.isdir(
        "https://raw.githubusercontent.com/jgoutin/airfs/HEAD/LICENSE"
    )
    assert airfs.isfile("https://raw.githubusercontent.com/jgoutin/airfs/HEAD/LICENSE")

    assert not airfs.exists("https://github.com/jgoutin/airfs/HEAD/not_exists")
    assert not airfs.isdir("https://github.com/jgoutin/airfs/HEAD/not_exists")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/HEAD/not_exists")

    # Archives
    assert airfs.exists("https://github.com/jgoutin/airfs/archive")
    assert airfs.isdir("https://github.com/jgoutin/airfs/archive")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/archive")

    assert airfs.exists("https://github.com/jgoutin/airfs/archive/1.4.0.tar.gz")
    assert not airfs.isdir("https://github.com/jgoutin/airfs/archive/1.4.0.tar.gz")
    assert airfs.isfile("https://github.com/jgoutin/airfs/archive/1.4.0.tar.gz")

    assert not airfs.exists("https://github.com/jgoutin/airfs/archive/not_exists")
    assert not airfs.isdir("https://github.com/jgoutin/airfs/archive/not_exists")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/archive/not_exists")

    # Releases
    assert airfs.exists("https://github.com/jgoutin/airfs/releases")
    assert airfs.isdir("https://github.com/jgoutin/airfs/releases")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/releases")

    assert airfs.exists("https://github.com/jgoutin/airfs/releases/tag")
    assert airfs.isdir("https://github.com/jgoutin/airfs/releases/tag")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/releases/tag")

    assert airfs.exists("https://github.com/jgoutin/airfs/releases/tag/1.4.0")
    assert airfs.isdir("https://github.com/jgoutin/airfs/releases/tag/1.4.0")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/releases/tag/1.4.0")

    assert airfs.exists("https://github.com/jgoutin/airfs/releases/latest")
    assert airfs.isdir("https://github.com/jgoutin/airfs/releases/latest")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/releases/latest")

    assert airfs.exists("https://github.com/jgoutin/airfs/releases/latest/assets")
    assert airfs.isdir("https://github.com/jgoutin/airfs/releases/latest/assets")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/releases/latest/assets")

    assert airfs.exists("https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets")
    assert airfs.isdir("https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets")
    assert not airfs.isfile(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets"
    )

    assert airfs.exists(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets/"
        "airfs-1.4.0-py3-none-any.whl"
    )
    assert not airfs.isdir(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets/"
        "airfs-1.4.0-py3-none-any.whl"
    )
    assert airfs.isfile(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets/"
        "airfs-1.4.0-py3-none-any.whl"
    )

    assert airfs.exists("https://github.com/jgoutin/airfs/releases/latest/archive")
    assert airfs.isdir("https://github.com/jgoutin/airfs/releases/latest/archive")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/releases/latest/archive")

    assert airfs.exists("https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive")
    assert airfs.isdir("https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive")
    assert not airfs.isfile(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive"
    )

    assert airfs.exists(
        "https://github.com/jgoutin/airfs/releases/latest/archive/source_code.tar.gz"
    )
    assert not airfs.isdir(
        "https://github.com/jgoutin/airfs/releases/latest/archive/source_code.tar.gz"
    )
    assert airfs.isfile(
        "https://github.com/jgoutin/airfs/releases/latest/archive/source_code.tar.gz"
    )

    assert airfs.exists(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/source_code.tar.gz"
    )
    assert not airfs.isdir(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/source_code.tar.gz"
    )
    assert airfs.isfile(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/source_code.tar.gz"
    )


def stat_scenario():
    """Test stat."""
    import airfs
    from stat import S_IFDIR, S_IFREG, S_IFLNK

    file = S_IFREG + 0o644
    file_exec = S_IFREG + 0o755
    directory = S_IFDIR + 0o644
    link = S_IFLNK + 0o644

    # User
    stat = airfs.stat("https://github.com/jgoutin")
    assert stat.st_mode == directory
    assert stat.st_mtime > 0
    assert stat.st_ctime > 0

    # Repos
    stat = airfs.stat("https://github.com/jgoutin/airfs")
    assert stat.st_mode == directory
    assert stat.st_mtime > 0
    assert stat.st_ctime > 0

    stat = airfs.stat("https://github.com/jgoutin/airfs/refs")
    assert stat.st_mode == directory

    stat = airfs.stat("https://github.com/jgoutin/airfs/refs/heads")
    assert stat.st_mode == directory

    stat = airfs.stat("https://github.com/jgoutin/airfs/refs/tags")
    assert stat.st_mode == directory

    with pytest.raises(FileNotFoundError):
        airfs.stat("https://github.com/jgoutin/not_exists")

    with pytest.raises(FileNotFoundError):
        airfs.stat("https://github.com/jgoutin/airfs/refs/not_exists")

    # HEAD
    stat = airfs.lstat("https://github.com/jgoutin/airfs/HEAD")
    assert stat.st_mode == link
    assert stat.st_mtime > 0
    assert stat.sha  # noqa

    # Branches
    stat = airfs.stat("https://github.com/jgoutin/airfs/branches")
    assert stat.st_mode == directory

    stat = airfs.lstat("https://github.com/jgoutin/airfs/branches/master")
    assert stat.st_mode == link
    assert stat.st_mtime > 0
    assert stat.sha  # noqa

    stat = airfs.stat("https://github.com/jgoutin/airfs/branches/master")
    assert stat.st_mode == directory
    assert stat.st_mtime > 0
    assert stat.sha  # noqa

    stat = airfs.lstat("https://github.com/jgoutin/airfs/refs/heads/master")
    assert stat.st_mode == link
    assert stat.st_mtime > 0
    assert stat.sha  # noqa

    # Tags
    stat = airfs.stat("https://github.com/jgoutin/airfs/tags")
    assert stat.st_mode == directory

    stat = airfs.lstat("https://github.com/jgoutin/airfs/tags/1.4.0")
    assert stat.st_mode == link
    assert stat.st_mtime > 0
    assert stat.sha  # noqa

    stat = airfs.lstat("https://github.com/jgoutin/airfs/refs/tags/1.4.0")
    assert stat.st_mode == link
    assert stat.st_mtime > 0
    assert stat.sha  # noqa

    # Commits
    stat = airfs.stat("https://github.com/jgoutin/airfs/commits")
    assert stat.st_mode == directory

    commit_id = airfs.listdir("https://github.com/jgoutin/airfs/commits")[0]
    stat = airfs.stat(f"https://github.com/jgoutin/airfs/commits/{commit_id}")
    assert stat.st_mode == directory
    assert stat.st_mtime > 0
    assert stat.sha  # noqa

    # Git Tree
    stat = airfs.stat("https://github.com/jgoutin/airfs/HEAD/tests")
    assert stat.st_mode == directory
    assert stat.st_mtime > 0
    assert stat.st_size == 0
    assert stat.sha  # noqa

    stat = airfs.stat("https://github.com/jgoutin/airfs/HEAD/LICENSE")
    assert stat.st_mode == file
    assert stat.st_mtime > 0
    assert stat.st_size > 0
    assert stat.sha  # noqa

    stat = airfs.stat("https://github.com/jgoutin/airfs/HEAD/setup.py")
    assert stat.st_mode == file_exec
    assert stat.st_mtime > 0
    assert stat.st_size > 0
    assert stat.sha  # noqa

    symlink_stat = airfs.lstat(
        "https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink"
    )
    assert symlink_stat.st_mode == link
    assert symlink_stat.st_mtime > 0
    assert symlink_stat.st_size > 0
    assert symlink_stat.sha  # noqa

    stat = airfs.stat("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")
    assert stat.st_mode == file
    assert stat.st_mtime > 0
    assert stat.st_size > 0
    assert stat.st_size > symlink_stat.st_size
    assert stat.sha  # noqa

    with pytest.raises(FileNotFoundError):
        airfs.stat("https://github.com/jgoutin/airfs/HEAD/not_exists")

    # Releases
    stat = airfs.stat(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/source_code.tar.gz"
    )
    assert stat.st_mode == file
    assert stat.st_size > 0
    assert stat.st_mtime > 0
    assert stat.sha  # noqa

    stat = airfs.stat(
        "https://github.com/jgoutin/airfs/releases/latest/archive/source_code.tar.gz"
    )
    assert stat.st_mode == file
    assert stat.st_size > 0
    assert stat.st_mtime > 0
    assert stat.sha  # noqa

    stat = airfs.stat(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets/"
        "airfs-1.4.0-py3-none-any.whl"
    )
    assert stat.st_mode == file
    assert stat.st_size > 0
    assert stat.st_mtime > 0
    assert stat.sha  # noqa


def get_scenario():
    """Test get files."""
    import airfs
    from airfs.storage.github import GithubBufferedIO, GithubRawIO

    with airfs.open(
        "https://github.com/jgoutin/airfs/releases/latest/archive/source_code.tar.gz",
        buffering=0,
    ) as file:
        assert isinstance(file, GithubRawIO)
        assert file.read()

    with airfs.open(
        "https://github.com/jgoutin/airfs/releases/latest/archive/source_code.tar.gz"
    ) as file:
        assert isinstance(file, GithubBufferedIO)
        assert file.read()

    with airfs.open(
        (
            "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/"
            "source_code.tar.gz"
        ),
        buffering=0,
    ) as file:
        assert file.read()

    with airfs.open(
        (
            "https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets/"
            "airfs-1.4.0-py3-none-any.whl"
        ),
        buffering=0,
    ) as file:
        assert file.read()

    with airfs.open(
        "https://github.com/jgoutin/airfs/HEAD/LICENSE",
        buffering=0,
    ) as file:
        assert file.read()

    with airfs.open(
        "https://raw.githubusercontent.com/jgoutin/airfs/HEAD/LICENSE",
        buffering=0,
    ) as file:
        assert file.read()
