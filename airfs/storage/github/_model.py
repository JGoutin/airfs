"""Github as a filesystem model"""
from airfs._core.exceptions import ObjectNotImplementedError
from airfs.storage.github._model_archive import Archive
from airfs.storage.github._model_base import GithubObject
from airfs.storage.github._model_git import Branch, Commit, Tag
from airfs.storage.github._model_reference import DefaultBranch, Reference
from airfs.storage.github._model_release import LatestRelease, Release, ReleaseDownload


class Repo(GithubObject):
    """Git repository"""

    KEY = "repo"
    LIST = "/users/{owner}/repos"
    HEAD = "/repos/{owner}/{repo}"
    HEAD_KEYS = {
        "created_at",
        "updated_at",
        "pushed_at",
        "private",
        "forks_count",
        "open_issues_count",
        "stargazers_count",
        "subscribers_count",
        "watchers_count",
        "default_branch",
    }
    STRUCT = {
        "archive": Archive,
        "blob": Reference,
        "branches": Branch,
        "commits": Commit,
        "HEAD": DefaultBranch,
        "refs": {"heads": Branch, "tags": Tag},
        "releases": {
            "tag": Release,
            "latest": LatestRelease,
            "download": ReleaseDownload,
        },
        "tags": Tag,
        "tree": Reference,
    }


class Owner(GithubObject):
    """GitHub Owner (User or Organization)"""

    KEY = "owner"
    HEAD = "/users/{owner}"
    STRUCT = Repo
    HEAD_KEYS = {
        "created_at",
        "updated_at",
        "type",
        "public_repos",
        "public_gists",
        "followers",
        "following",
    }

    @classmethod
    def list(cls, client, spec, first_level=False):
        """
        List objects of this GitHub class matching the spec.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.
            first_level (bool): It True, returns only first level objects.

        Returns:
            generator of tuple: object name str, object header dict, has content bool
        """
        raise ObjectNotImplementedError(feature="Listing owners")


class Root(GithubObject):
    """GitHub Root"""

    STRUCT = Owner

    @classmethod
    def head_obj(cls, client, spec):
        """
        Head the object of this GitHub class matching the spec.

        Only return result directly from current object response as dict.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Returns:
            dict: Object headers.
        """
        return dict()
