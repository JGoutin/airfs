"""GitHub"""
from collections import deque as _deque
from os.path import (
    isabs as _isabs,
    realpath as _realpath,
    join as _join,
    basename as _basename,
    normpath as _normpath,
)
from re import compile as _compile

from airfs.io import SystemBase as _SystemBase

from airfs._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectNotADirectoryError as _ObjectNotADirectoryError,
)
from airfs.storage.github._model import Root as _Root
from airfs.storage.github._model_git import Tree as _Tree
from airfs.storage.github._client import (
    Client as _Client,
    GithubRateLimitException,
    GithubRateLimitWarning,
)

# TODO: Use specific RAW and Buffered objects
from airfs.storage.http import HTTPRawIO, HTTPBufferedIO


__all__ = [
    "GithubRateLimitException",
    "GithubRateLimitWarning",
    "HTTPRawIO",
    "HTTPBufferedIO",
]


class _GithubSystem(_SystemBase):
    """
    GitHub system.

    Args:
        storage_parameters (dict): "github.MainClass.Github" keyword arguments.
    """

    _SIZE_KEYS = (
        "size",
        "Content-Length",
    )
    _CTIME_KEYS = ("created_at",)
    _MTIME_KEYS = ("pushed_at", "updated_at", "published_at")

    # Keys to retrieve from parents for virtual directories
    _VIRTUAL_KEYS = set(_CTIME_KEYS)  # type: ignore
    _VIRTUAL_KEYS.update(_MTIME_KEYS)  # type: ignore

    __slots__ = ()

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """
        return (
            "github://",
            _compile(r"^https?://github\.com"),
            _compile(r"^https?://raw\.githubusercontent\.com"),
        )

    def _get_client(self):
        """
        GitHub client

        Returns:
            airfs.storage.github._client.Client: client
        """
        return _Client(**self._storage_parameters)

    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """
        # TODO: URL rewrite https://raw.githubusercontent.com/:owner/:repo/:ref/:path
        keys = _deque(self.relpath(path).rstrip("/").split("/"))
        spec = dict(keys=keys, full_path=path)
        model = _Root
        while keys:
            model = model.next_model(spec)
        del spec["keys"]
        return spec

    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        # TODO: return a subset of header like "list" when heading a "dict"
        return client_kwargs["object"].head(self.client, client_kwargs)

    def _list_objects(self, client_kwargs, path, max_results, first_level):
        """
        Lists objects.

        args:
            client_kwargs (dict): Client arguments.
            path (str): Path to list.
            max_results (int): The maximum results that should return the method.
            first_level (bool): It True, may only first level objects.

        Yields:
            tuple: object path str, object header dict, has content bool
        """
        content = client_kwargs["content"]
        if isinstance(content, dict):
            # "Virtual" directory only represented by a dict and not a real GitHub
            # object
            parent_header = self._head(client_kwargs)
            header = {
                key: parent_header[key]
                for key in (parent_header.keys() & self._VIRTUAL_KEYS)
            }
            for key in content:
                yield key, header, True

        elif content is not None:
            # GitHub object
            for item in content.list(
                self.client, client_kwargs, first_level=first_level
            ):
                yield item

        else:
            raise _ObjectNotADirectoryError()

    def islink(self, path=None, client_kwargs=None, header=None):
        """
        Returns True if o²bject is a symbolic link.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            bool: True if object is Symlink.
        """
        if client_kwargs is None:
            client_kwargs = self.get_client_kwargs(path)
        obj_cls = client_kwargs["object"]

        # Check if islink from git file mode.
        if obj_cls == _Tree:
            try:
                return self.head(path, client_kwargs, header)["mode"] == "120000"
            except _ObjectNotFoundError:
                return False

        # Virtual symlinks
        return obj_cls.SYMLINK is not None

    def isdir(
        self, path=None, client_kwargs=None, virtual_dir=True, assume_exists=None
    ):
        """
        Return True if path is an existing directory.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.
            virtual_dir (bool): If True, checks if directory exists virtually if an
                object path if not exists as a specific object.
            assume_exists (bool or None): This value define the value to return in the
                case there is no enough permission to determinate the existing status of
                the file. If set to None, the permission exception is reraised
                (Default behavior). if set to True or False, return this value.

        Returns:
            bool: True if directory exists.
        """
        if client_kwargs is None:
            client_kwargs = self.get_client_kwargs(path)
        obj_cls = client_kwargs["object"]

        # Virtual directory
        if obj_cls.STRUCT is not None:
            return True

        # Git object
        elif obj_cls == _Tree:
            return self._has_git_mode("040")

        # Not a directory
        return False

    def isfile(self, path=None, client_kwargs=None, assume_exists=None):
        """
        Return True if path is an existing regular file.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.
            assume_exists (bool or None): This value define the value to return in the
                case there is no enough permission to determinate the existing status of
                the file. If set to None, the permission exception is reraised
                (Default behavior). if set to True or False, return this value.

        Returns:
            bool: True if file exists.
        """
        if client_kwargs is None:
            client_kwargs = self.get_client_kwargs(path)
        obj_cls = client_kwargs["object"]

        # Virtual directory
        if obj_cls.STRUCT is not None:
            return False

        # Git object
        elif obj_cls == _Tree:
            return self._has_git_mode("100")

        # Archive or asset
        return self.exists(path, client_kwargs, assume_exists)

    def _has_git_mode(self, mode_start, path=None, client_kwargs=None):
        """
        Check if the Git object has the specified Git mode.
        Follow symlinks if any.

        Args:
            mode_start (str): Returns True if the mode starts with this string.
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.

        Returns:
            bool: True if excepted mode or non existing object.
        """
        try:
            header = self.head(path, client_kwargs)
        except _ObjectNotFoundError:
            return False
        mode = header["mode"]

        # Git directory
        if mode.startswith(mode_start):
            return True

        # Git symlink, requires to check its target
        elif mode == "120000":
            target = self.read_link(path, client_kwargs, recursive=True)
            try:
                target_mode = self.head(target)["mode"]
            except _ObjectNotFoundError:
                return False
            return target_mode.startswith(mode_start)

    def _getmode(self, path=None, client_kwargs=None, header=None):
        """
        Get object mode permission bits in Unix format.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            int: Group ID.
        """
        client_kwargs = self.get_client_kwargs(path)
        obj_cls = client_kwargs["object"]

        # Virtual directory
        if obj_cls == _Tree:
            st_mode = self.head(path, client_kwargs, header)["mode"]
            return int(st_mode[-3:], 8) or 0o644
        return 0o644

    def read_link(self, path=None, client_kwargs=None, header=None, recursive=False):
        """
        Return the path linked by the symbolic link.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.
            recursive (bool): Follow links chains until end and return absolute path of
            the final destination.

        Returns:
            str: Path.
        """
        client = self._get_client()
        if client_kwargs is None:
            client_kwargs = self.get_client_kwargs(path)

        target = client_kwargs["object"].read_link(client, client_kwargs)

        # If not recursive return absolute or relative target
        if not recursive:
            return target

        # If target is an absolute local path, continue to follow locally
        elif _isabs(target):
            return _realpath(target)

        # Get absolute target path and related spec
        parent_path = client_kwargs["path"]
        target_path = _normpath(_join(_basename(parent_path), target))
        if target_path.startswith(".."):
            # Target is outside the Git repository
            return target_path

        spec = client_kwargs.copy()
        base_url = client_kwargs["full_path"][-len(parent_path) :]
        full_path = spec["full_path"] = base_url + target_path
        spec["path"] = target_path

        # If target is a link, continue to follow
        if self.islink(full_path, spec):
            return self.read_link(full_path, spec, recursive=True)

        # Return the absolute destination path
        return full_path
