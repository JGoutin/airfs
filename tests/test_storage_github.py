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
    # TODO: Save responses from the real test to use them as responses for the mocked
    #       test (Simulate cached responses)
    pytest.skip()

    from collections import OrderedDict
    from datetime import datetime
    from requests import HTTPError
    import airfs._core.storage_manager as storage_manager

    # Mock API responses

    class Response:
        """Mocked Response"""

        def __init__(self, url, **_):
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
                    f"Error {self.status_code} on {self.url}", response=self
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
    listdir_scenario()
    exists_scenario()
    stat_scenario()
    symlink_scenario()


def listdir_scenario():
    """
    Tests listing
    """
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

    # TODO: Fix (Do not raise)
    # with pytest.raises(FileNotFoundError):
    #    airfs.listdir("https://github.com/jgoutin/airfs/HEAD/not_exists")

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
    ) == ["1.4.0.tar.gz", "1.4.0.zip"], "List release archive"

    with pytest.raises(FileNotFoundError):
        airfs.listdir(
            "https://github.com/jgoutin/airfs/releases/tag/not_exists/archive"
        )

    with pytest.raises(NotADirectoryError):
        airfs.listdir(
            "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/1.4.0.tar.gz"
        )

    assert sorted(
        airfs.listdir("https://github.com/jgoutin/airfs/releases/latest/archive")
    ) == ["latest.tar.gz", "latest.zip"], "List latest release archive"

    with pytest.raises(NotADirectoryError):
        airfs.listdir(
            "https://github.com/jgoutin/airfs/releases/latest/archive/latest.tar.gz"
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
    """
    Tests symbolic links
    """
    import airfs

    # Git tree
    assert airfs.islink("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")
    assert airfs.exists("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")
    # TODO: Fix, seen as dict
    # assert not airfs.isdir(
    #     "https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink"
    # )
    # TODO: Fix, "ref" not in spec
    # assert airfs.isfile("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")

    assert not airfs.islink("https://github.com/jgoutin/airfs/HEAD/LICENSE")
    assert not airfs.islink("https://github.com/jgoutin/airfs/HEAD/tests")

    # TODO: Fix, "ref" not in spec
    # assert (
    #     airfs.readlink("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")
    #     == "../../airfs/_core/exceptions.py"
    # )

    with pytest.raises(OSError):
        airfs.readlink("https://github.com/jgoutin/airfs/HEAD/LICENSE")

    # TODO: Test following when reading

    # Virtual tree
    # TODO: Check all

    assert airfs.islink("https://github.com/jgoutin/airfs/HEAD")

    assert (
        airfs.readlink("https://github.com/jgoutin/airfs/HEAD")
        == "github://jgoutin/airfs/branches/master"
    )


def exists_scenario():
    """
    Tests exists, isdir, isfile
    """
    import airfs

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

    assert not airfs.exists("https://github.com/jgoutin/refs/not_exists")
    assert not airfs.isdir("https://github.com/jgoutin/refs/not_exists")
    assert not airfs.isfile("https://github.com/jgoutin/refs/not_exists")

    # HEAD
    assert airfs.exists("https://github.com/jgoutin/airfs/HEAD")
    assert airfs.isdir("https://github.com/jgoutin/airfs/HEAD")
    assert not airfs.isfile("https://github.com/jgoutin/airfs/HEAD")

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
    # TODO: fix
    # assert airfs.exists("https://github.com/jgoutin/airfs/releases")
    # assert airfs.isdir("https://github.com/jgoutin/airfs/releases")
    # assert not airfs.isfile("https://github.com/jgoutin/airfs/releases")

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
        "https://github.com/jgoutin/airfs/releases/latest/archive/latest.tar.gz"
    )
    assert not airfs.isdir(
        "https://github.com/jgoutin/airfs/releases/latest/archive/latest.tar.gz"
    )
    assert airfs.isfile(
        "https://github.com/jgoutin/airfs/releases/latest/archive/latest.tar.gz"
    )

    assert airfs.exists(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/1.4.0.tar.gz"
    )
    assert not airfs.isdir(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/1.4.0.tar.gz"
    )
    assert airfs.isfile(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/1.4.0.tar.gz"
    )


def stat_scenario():
    """
    Test stat.
    """
    import airfs
    from stat import S_IFDIR, S_IFREG, S_IFLNK

    file = S_IFREG + 0o644
    file_exec = S_IFREG + 0o755
    directory = S_IFDIR + 0o644
    link = S_IFLNK + 0o644

    # TODO: Add not existing paths + Virtual dirs

    # User
    stat = airfs.stat("https://github.com/jgoutin")
    assert stat.st_mode == directory
    assert stat.st_mtime > 0
    assert stat.st_ctime > 0

    # Repos
    stat = airfs.stat("https://github.com/jgoutin/airfs")
    # TODO: Fix, seen as file
    # assert stat.st_mode == directory
    assert stat.st_mtime > 0
    assert stat.st_ctime > 0

    # HEAD
    stat = airfs.stat("https://github.com/jgoutin/airfs/HEAD")
    assert stat.st_mode == link
    assert stat.st_mtime > 0
    assert stat.sha

    # Branches
    stat = airfs.stat("https://github.com/jgoutin/airfs/branches/master")
    assert stat.st_mode == link
    assert stat.st_mtime > 0
    assert stat.sha

    stat = airfs.stat("https://github.com/jgoutin/airfs/refs/heads/master")
    assert stat.st_mode == link
    assert stat.st_mtime > 0
    assert stat.sha

    # Tags
    stat = airfs.stat("https://github.com/jgoutin/airfs/tags/1.4.0")
    assert stat.st_mode == link
    assert stat.st_mtime > 0
    assert stat.sha

    stat = airfs.stat("https://github.com/jgoutin/airfs/refs/tags/1.4.0")
    assert stat.st_mode == link
    assert stat.st_mtime > 0
    assert stat.sha

    # Commits
    commit_id = airfs.listdir("https://github.com/jgoutin/airfs/commits")[0]
    stat = airfs.stat(f"https://github.com/jgoutin/airfs/commits/{commit_id}")
    # TODO: Fix, seen as file
    # assert stat.st_mode == directory
    assert stat.st_mtime > 0
    assert stat.sha

    # Git Tree
    stat = airfs.stat("https://github.com/jgoutin/airfs/HEAD/tests")
    # TODO: Seen as S_IFREG if no trailing "/"
    # assert stat.st_mode == directory
    assert stat.st_mtime > 0
    assert stat.st_size == 0
    assert stat.sha

    stat = airfs.stat("https://github.com/jgoutin/airfs/HEAD/LICENSE")
    assert stat.st_mode == file
    assert stat.st_mtime > 0
    assert stat.st_size > 0
    assert stat.sha

    stat = airfs.stat("https://github.com/jgoutin/airfs/HEAD/setup.py")
    assert stat.st_mode == file_exec
    assert stat.st_mtime > 0
    assert stat.st_size > 0
    assert stat.sha

    symlink_stat = airfs.lstat(
        "https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink"
    )
    assert symlink_stat.st_mode == link
    assert symlink_stat.st_mtime > 0
    assert symlink_stat.st_size > 0
    assert symlink_stat.sha

    # TODO: Fix os functions to follow links
    # stat = airfs.stat("https://github.com/jgoutin/airfs/HEAD/tests/resources/symlink")
    # assert stat.st_mode == file
    # assert stat.st_mtime > 0
    # assert stat.st_size > 0
    # assert stat.st_size > symlink_stat.st_size
    # assert stat.sha

    # Releases
    stat = airfs.stat(
        "https://github.com/jgoutin/airfs/releases/tag/1.4.0/archive/1.4.0.tar.gz"
    )
    assert stat.st_mode == file
    assert stat.st_size > 0
    assert stat.st_mtime > 0
    assert stat.sha

    stat = airfs.stat(
        "https://github.com/jgoutin/airfs/releases/latest/archive/latest.tar.gz"
    )
    assert stat.st_mode == file
    assert stat.st_size > 0
    assert stat.st_mtime > 0
    assert stat.sha

    # TODO: Upload file
    # stat = airfs.stat(
    #     "https://github.com/jgoutin/airfs/releases/tag/1.4.0/assets/"
    #     "airfs-1.4.0-py3-none-any.whl"
    # )
    assert stat.st_mode == file
    assert stat.st_size > 0
    assert stat.st_mtime > 0
    assert stat.sha


def get_scenario():
    """
    Test get files.
    """
    # TODO:
