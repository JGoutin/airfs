"""Git objects"""
from os.path import commonpath, dirname

from requests import request

from airfs._core.exceptions import ObjectNotASymlinkError, ObjectNotFoundError
from airfs.storage.github._model_base import GithubObject
from airfs.storage.http import _handle_http_errors


class Tree(GithubObject):
    """Git tree"""

    KEY = "path"
    LIST = "/repos/{owner}/{repo}/git/trees/{tree_sha}"
    HEAD_KEYS = {"mode", "size"}
    HEAD_FROM = {"sha", "pushed_at"}
    GET = "https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{path}"

    def __iter__(self):
        """
        Iterate over object header keys.

        Yields:
            str: keys
        """
        yield "mode"
        for key in self.HEAD_FROM:
            yield key
        if not self._is_dir():
            yield "size"

    def __len__(self):
        """
        Header length.

        Returns:
            int: Length
        """
        return len(self.HEAD_KEYS) + len(self.HEAD_FROM) - (1 if self._is_dir() else 0)

    def __getitem__(self, key):
        """
        Get a value from the object header.

        Args:
            key (str): Header key.

        Returns:
            object: Header value matching the key.
        """
        try:
            return self._headers[key]
        except KeyError:
            if key == "size" and not self._is_dir():
                raise

        if key in self.HEAD_KEYS:
            self._update_headers()

        else:
            spec = self._spec

            parent = spec["parent"]
            sha = parent.head(self._client, spec)["sha"]

            response = self._client.get(
                Commit.LIST.format(**spec), params=dict(path=spec["path"], sha=sha)
            )[0]
            commit_header = Commit.set_header(response[0])
            for from_key in self.HEAD_FROM:
                self._headers[from_key] = commit_header[from_key]

        return self._headers[key]

    @classmethod
    def head_obj(cls, client, spec):
        """
        Head the object of this Git tree matching the spec.

        Only return result directly from current object response as dict.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Returns:
            dict: Object headers.
        """
        path = spec["path"]
        parent_spec = spec.copy()
        parent_spec["path"] = dirname(path)

        for _, abspath, _, headers, _ in cls._list(
            client, parent_spec, "/" not in path
        ):
            if path == abspath:
                return cls.set_header(headers)

        raise ObjectNotFoundError(path=spec["full_path"])

    @classmethod
    def read_link(cls, client, spec):
        """
        Returns "read_link" result for the detected "_GithubObject" subclass.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Returns:
            str: Path.
        """
        if cls.head(client, spec)["mode"] != "120000":
            raise ObjectNotASymlinkError(path=spec["full_path"])
        response = request("GET", cls.GET.format(**spec))
        _handle_http_errors(response)
        return response.text

    @classmethod
    def list(cls, client, spec, first_level=False):
        """
        List objects of this GitHub class matching the spec.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.
            first_level (bool): It True, returns only first level objects.

        Yields:
            tuple: object name str, object header dict, has content bool
        """
        set_header = cls.set_header
        for relpath, abspath, spec, headers, isdir in cls._list(
            client, spec, first_level
        ):
            yield relpath, cls(client, spec, set_header(headers), abspath), isdir

    @classmethod
    def _list(cls, client, spec, first_level=False):
        """
        List tree using recursive then non recursive API. Yields raw results.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.
            first_level (bool): It True, returns only first level objects.

        Yields:
            tuple: Relative path, Absolute path, spec, headers, has content bool
        """
        if "tree_sha" not in spec:
            parent = spec["parent"] if spec["object"] == cls else spec["object"]
            spec["tree_sha"] = parent.head(client, spec)["tree_sha"]

        cwd = spec.get("path", "").rstrip("/")
        cwd_index = len(cwd)
        cwd_seen = not cwd
        if cwd_index:
            # Include the ending "/"
            cwd_index += 1

        response = client.get(
            cls.LIST.format(**spec),
            never_expire=True,
            params=dict(recursive=cwd or not first_level),
        )[0]

        truncated = response["truncated"]
        if truncated:
            seen = set()
            add_seen = seen.add
            last_tree = ""

        for headers in response["tree"]:
            abspath = headers["path"]

            if cwd:
                if commonpath((abspath, cwd)) != cwd:
                    continue

                relpath = abspath[cwd_index:]
            else:
                relpath = abspath

            isdir = headers["type"] == "tree"

            if not relpath:
                cls._raise_if_not_dir(isdir, spec)

                # Do not yield current working directory itself
                cwd_seen = True
                continue

            if truncated:
                add_seen(abspath)  # noqa
                if isdir:
                    last_tree = abspath

            yield relpath, abspath, spec, headers, False

        if truncated:
            last_tree = last_tree.split("/")  # noqa
            partial_trees = set(
                "/".join(last_tree[:index]) for index in range((len(last_tree)))
            )

            for relpath, abspath, spec, headers, _ in cls._list_non_recursive(
                client,
                spec,
                seen,  # noqa
                partial_trees,
                cwd,
                cwd_index,
                spec["tree_sha"],
                "",
                first_level,
            ):
                yield relpath, abspath, spec, headers, False

        if not cwd_seen:
            raise ObjectNotFoundError(path=spec["full_path"])

    @classmethod
    def _list_non_recursive(
        cls,
        client,
        spec,
        seen,
        partial_trees,
        cwd,
        cwd_index,
        tree_sha,
        tree_path,
        first_level,
    ):
        """
        List tree recursively using the non recursive API method.
        Yields raw results.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.
            seen (set): Absolute paths already seen to skip.
            partial_trees (set): Absolute paths of trees that may not be completely
                processed.
            cwd (str): Current working directory.
            cwd_index (int): Index used to get relative path.
            tree_sha (str): Tree SHA.
            tree_path (str): Tree path.
            first_level (bool): It True, returns only first level objects. Else, returns
                the full tree.

        Yields:
            tuple: Relative path, Absolute path, spec, headers, has content bool
        """
        tree_spec = spec.copy()
        tree_spec["tree_sha"] = tree_sha

        for headers in client.get(cls.LIST.format(**tree_spec), never_expire=True)[0][
            "tree"
        ]:
            if tree_path:
                abspath = "/".join((tree_path, headers["path"]))
            else:
                abspath = headers["path"]

            if cwd:
                cwd_common = commonpath((abspath, cwd))
                if not cwd_common:
                    continue

                in_cwd = cwd_common == cwd
                relpath = abspath[cwd_index:]

            else:
                in_cwd = True
                relpath = abspath

            isdir = headers["type"] == "tree"

            cls._raise_if_not_dir(not relpath and not isdir, spec)

            not_seen = abspath not in seen
            if in_cwd and relpath and not_seen:
                yield relpath, abspath, spec, headers, isdir

            if (
                isdir
                and (not in_cwd or not first_level)
                and (not_seen or abspath in partial_trees)
            ):
                for entry in cls._list_non_recursive(
                    client,
                    spec,
                    seen,
                    partial_trees,
                    cwd,
                    cwd_index,
                    headers["sha"],
                    abspath,
                    first_level,
                ):
                    yield entry

    def _is_dir(self):
        """
        Check if path is a directory using the Git "mode".

        Returns:
            bool: True if directory.
        """
        return self["mode"] == "040000"


class Branch(GithubObject):
    """Git branch"""

    KEY = "branch"
    REF = True
    LIST = "/repos/{owner}/{repo}/branches"
    HEAD = "/repos/{owner}/{repo}/branches/{branch}"
    HEAD_EXTRA = (
        ("pushed_at", ("commit", "commit", "committer", "date")),
        ("sha", ("commit", "sha")),
        ("tree_sha", ("commit", "commit", "tree", "sha")),
    )
    STRUCT = Tree
    SYMLINK = "https://github.com/{owner}/{repo}/commits/{sha}"


class Commit(GithubObject):
    """Git commit"""

    KEY = "sha"
    REF = True
    LIST = "/repos/{owner}/{repo}/commits"
    LIST_KEY = "sha"
    HEAD = "/repos/{owner}/{repo}/commits/{sha}"
    HEAD_KEYS = {"sha"}
    HEAD_EXTRA = (
        ("pushed_at", ("commit", "committer", "date")),
        ("tree_sha", ("commit", "tree", "sha")),
    )
    STRUCT = Tree

    @classmethod
    def head_obj(cls, client, spec):
        """
        Head the object of this commit matching the spec.

        Only return result directly from current object response as dict.

        Args:
            client (airfs.storage.github._api.ApiV3): Client.
            spec (dict): Item spec.

        Returns:
            dict: Object headers.
        """
        return cls.set_header(client.get(cls.HEAD.format(**spec), never_expire=True)[0])


class Tag(GithubObject):
    """Git tag"""

    KEY = "tag"
    REF = True
    LIST = "/repos/{owner}/{repo}/tags"
    HEAD = "/repos/{owner}/{repo}/git/ref/tags/{tag}"
    HEAD_EXTRA = (
        ("sha", ("object", "sha")),
        ("sha", ("commit", "sha")),
    )
    HEAD_FROM = {"pushed_at": Commit, "tree_sha": Commit}
    STRUCT = Tree
    SYMLINK = "https://github.com/{owner}/{repo}/commits/{sha}"
