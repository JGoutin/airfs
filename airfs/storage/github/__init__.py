"""GitHub"""
from collections import deque as _deque
from re import compile as _compile

from airfs._core.io_base import memoizedmethod as _memoizedmethod
from airfs.io import SystemBase as _SystemBase

from airfs._core.exceptions import (
    ObjectNotFoundError as _ObjectNotFoundError,
    ObjectNotADirectoryError as _ObjectNotADirectoryError,
    ObjectPermissionError as _ObjectPermissionError,
)
from airfs.storage.github._model import Root as _Root
from airfs.storage.github._model_git import Tree as _Tree
from airfs.storage.github._client import (
    Client as _Client,
    GithubRateLimitException,
    GithubRateLimitWarning,
)
from airfs.storage.http import (
    HTTPRawIO as _HTTPRawIO,
    HTTPBufferedIO as _HTTPBufferedIO,
)


__all__ = [
    "GithubRateLimitException",
    "GithubRateLimitWarning",
    "GithubRawIO",
    "GithubBufferedIO",
]

_RAW_GITHUB = _compile(r"^https?://raw\.githubusercontent\.com")


class _GithubSystem(_SystemBase):
    """
    GitHub system.

    Args:
        storage_parameters (dict): "github.MainClass.Github" keyword arguments.
    """

    SUPPORTS_SYMLINKS = True

    _SIZE_KEYS = (
        "size",
        "Content-Length",
    )
    _CTIME_KEYS = ("created_at",)
    _MTIME_KEYS = ("pushed_at", "updated_at", "published_at")

    #: Keys to retrieve from parents for virtual directories
    _VIRTUAL_KEYS = set(_CTIME_KEYS)  # type: ignore
    _VIRTUAL_KEYS.update(_MTIME_KEYS)  # type: ignore

    __slots__ = ()

    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """
        return ("github://", _compile(r"^https?://github\.com"), _RAW_GITHUB)

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
        raw_match = _RAW_GITHUB.match(path)
        if raw_match:
            # "raw.githubusercontent.com" case
            keys = path[raw_match.end() + 1 :].rstrip("/").split("/", 3)
            if len(keys) > 2:
                keys.insert(2, "tree")
        else:
            keys = self.relpath(path).rstrip("/").split("/")
        keys = _deque(key for key in keys if key)
        spec = dict(keys=keys, full_path=path, object=_Root)
        model = _Root
        while keys:
            model = model.next_model(self.client, spec)
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
        if isinstance(client_kwargs["object"], dict):
            return dict()
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
            parent_header = self._head(client_kwargs)
            header = {
                key: parent_header[key]
                for key in (parent_header.keys() & self._VIRTUAL_KEYS)
            }
            for key in content:
                yield key, header, True

        elif content is not None:
            for item in content.list(
                self.client, client_kwargs, first_level=first_level
            ):
                yield item

        else:
            raise _ObjectNotADirectoryError(path=client_kwargs["full_path"])

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

        if obj_cls == _Tree:
            try:
                return self.head(path, client_kwargs, header)["mode"] == "120000"
            except _ObjectNotFoundError:
                return False

        return hasattr(obj_cls, "SYMLINK") and obj_cls.SYMLINK is not None

    def _is(
        self,
        path,
        client_kwargs,
        assume_exists,
        mode_start,
        default,
        header,
        follow_symlinks,
    ):
        """
        Return True if path is exists and if of specified kind.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.
            assume_exists (bool or None): This value define the value to return in the
                case there is no enough permission to determinate the existing status of
                the file. If set to None, the permission exception is reraised
                (Default behavior). if set to True or False, return this value.
            mode_start (str): Returns True if the mode starts with this string.
            default (bool): Default answer for the specified kind.
            header (dict): Object header.
            follow_symlinks (bool): Follow symlinks.

        Returns:
            bool: True if exists and specified kind.
        """
        try:
            if client_kwargs is None:
                client_kwargs = self.get_client_kwargs(path)
            obj_cls = client_kwargs["object"]

            if (
                isinstance(obj_cls, dict)
                or obj_cls.STRUCT is not None
                or "owner" not in client_kwargs
            ):
                answer = not default

            elif obj_cls == _Tree:
                return self._has_git_mode(
                    mode_start, path, client_kwargs, header, follow_symlinks
                )

            else:
                answer = default

            if answer:
                return self.exists(
                    path, client_kwargs, assume_exists, header, follow_symlinks
                )
            return False

        except _ObjectNotFoundError:
            return False

        except _ObjectPermissionError:
            if assume_exists is None:
                raise
            return assume_exists

    def isdir(
        self,
        path=None,
        client_kwargs=None,
        virtual_dir=True,
        assume_exists=None,
        header=None,
        follow_symlinks=None,
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
            header (dict): Object header.
            follow_symlinks (bool): Follow symlinks.

        Returns:
            bool: True if directory exists.
        """
        return self._is(
            path, client_kwargs, assume_exists, "040", False, header, follow_symlinks
        )

    def isfile(
        self,
        path=None,
        client_kwargs=None,
        assume_exists=None,
        header=None,
        follow_symlinks=None,
    ):
        """
        Return True if path is an existing regular file.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.
            assume_exists (bool or None): This value define the value to return in the
                case there is no enough permission to determinate the existing status of
                the file. If set to None, the permission exception is reraised
                (Default behavior). if set to True or False, return this value.
            header (dict): Object header.
            follow_symlinks (bool): Follow symlinks.

        Returns:
            bool: True if file exists.
        """
        return self._is(
            path, client_kwargs, assume_exists, "100", True, header, follow_symlinks
        )

    def _has_git_mode(self, mode_start, path, client_kwargs, header, follow_symlinks):
        """
        Check if the Git object has the specified Git mode.
        Follow symlinks if any.

        Args:
            mode_start (str): Returns True if the mode starts with this string.
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.
            follow_symlinks (bool): Follow symlinks.

        Returns:
            bool: True if excepted mode or non existing object.
        """
        path, client_kwargs, header = self.resolve(
            path, client_kwargs, header, follow_symlinks
        )
        if self.head(path, client_kwargs, header)["mode"].startswith(mode_start):
            return True

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

        if obj_cls == _Tree:
            st_mode = self.head(path, client_kwargs, header)["mode"]
            return int(st_mode[-3:], 8) or 0o644
        return 0o644

    def read_link(self, path=None, client_kwargs=None, header=None):
        """
        Return the path linked by the symbolic link.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            str: Path.
        """
        if client_kwargs is None:
            client_kwargs = self.get_client_kwargs(path)
        return client_kwargs["object"].read_link(self.client, client_kwargs)


class GithubRawIO(_HTTPRawIO):
    """Binary GitHub Object I/O

    Args:
        name (path-like object): URL to the file which will be opened.
        mode (str): The mode can be 'r' for reading (default)
    """

    _SYSTEM_CLASS = _GithubSystem
    __DEFAULT_CLASS = True

    @property
    @_memoizedmethod
    def name(self):
        """
        Name

        Returns:
            str: Name
        """
        return self._client_kwargs["object"].get_url(
            self._system.client, self._client_kwargs
        )

    @property
    @_memoizedmethod
    def _client(self):
        """
        Returns client instance.

        Returns:
            client
        """
        return self._system.client.session


class GithubBufferedIO(_HTTPBufferedIO):
    """Buffered GitHub Object I/O

    Args:
        name (path-like object): URL to the file which will be opened.
        mode (str): The mode can be 'r' for reading.
        buffer_size (int): The size of buffer.
        max_buffers (int): The maximum number of buffers to preload in read mode or
            awaiting flush in write mode. 0 for no limit.
        max_workers (int): The maximum number of threads that can be used to execute
            the given calls.
    """

    _RAW_CLASS = GithubRawIO
    __DEFAULT_CLASS = True
