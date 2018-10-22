# coding=utf-8
"""Cloud storage abstract System"""
from abc import abstractmethod
from email.utils import parsedate
from io import UnsupportedOperation
from time import mktime

from pycosio._core.compat import ABC, Pattern
from pycosio._core.exceptions import ObjectNotFoundError


class SystemBase(ABC):
    """
    Cloud storage system handler.

    This class subclasses are not intended to be public and are
    implementation details.

    Args:
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
        roots (tuple): Tuple of roots to force use.
    """

    def __init__(self, storage_parameters=None, unsecure=False, roots=None):
        # Save storage parameters
        self._storage_parameters = storage_parameters or dict()
        self._unsecure = unsecure

        # Initialize client
        self._client = None

        # Initialize roots
        if roots:
            self._roots = roots
        else:
            self._roots = self._get_roots()

    @property
    def client(self):
        """
        Storage client

        Returns:
            client
        """
        if self._client is None:
            self._client = self._get_client()
        return self._client

    def copy(self, src, dst):
        """
        Copy object of the same storage.

        Args:
            src (str): Path or URL.
            dst (str): Path or URL.
        """
        raise UnsupportedOperation

    def exists(self, path=None, client_kwargs=None):
        """
        Return True if path refers to an existing path.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.

        Returns:
            bool: True if exists.
        """
        try:
            self.head(path, client_kwargs)
        except ObjectNotFoundError:
            return False
        return True

    @abstractmethod
    def _get_client(self):
        """
        Storage client

        Returns:
            client
        """

    @abstractmethod
    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """

    def getmtime(self, path=None, client_kwargs=None, header=None):
        """
        Return the time of last access of path.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            float: The number of seconds since the epoch
                (see the time module).
        """
        return self._getmtime_from_header(
            self.head(path, client_kwargs, header))

    @staticmethod
    def _getmtime_from_header(header):
        """
        Return the time from header

        Args:
            header (dict): Object header.

        Returns:
            float: The number of seconds since the epoch
        """
        # By default, assumes that information are in a standard HTTP header
        for key in ('Last-Modified', 'last-modified'):
            try:
                return mktime(parsedate(header[key]))
            except KeyError:
                continue
        else:
            raise TypeError('Modification time not available')

    @abstractmethod
    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """

    def getsize(self, path=None, client_kwargs=None, header=None):
        """
        Return the size, in bytes, of path.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            int: Size in bytes.
        """
        return self._getsize_from_header(self.head(path, client_kwargs, header))

    @staticmethod
    def _getsize_from_header(header):
        """
        Return the size from header

        Args:
            header (dict): Object header.

        Returns:
            int: Size in bytes.
        """
        # By default, assumes that information are in a standard HTTP header
        for key in ('Content-Length', 'content-length'):
            try:
                return int(header[key])
            except KeyError:
                continue
        else:
            raise TypeError('Size not available')

    def isdir(self, path=None, client_kwargs=None):
        """
        Return True if path is an existing directory.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.

        Returns:
            bool: True if directory exists.
        """
        relative = self.relpath(path)
        if not relative:
            # Root always exists and is a directory
            return True

        if path[-1] == '/' or self.is_locator(relative, relative=True):
            return self.exists(path=path, client_kwargs=client_kwargs)
        return False

    def isfile(self, path=None, client_kwargs=None):
        """
        Return True if path is an existing regular file.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.

        Returns:
            bool: True if file exists.
        """
        relative = self.relpath(path)
        if not relative:
            # Root always exists and is a directory
            return False

        if path[-1] != '/' and not self.is_locator(path, relative=True):
            return self.exists(path=path, client_kwargs=client_kwargs)
        return False

    @property
    def storage_parameters(self):
        """
        Storage parameters

        Returns:
            dict: Storage parameters
        """
        return self._storage_parameters

    @abstractmethod
    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """

    def head(self, path=None, client_kwargs=None, header=None):
        """
        Returns object HTTP header.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            dict: HTTP header.
        """
        if header:
            return header
        elif client_kwargs is None:
            client_kwargs = self.get_client_kwargs(path)
        return self._head(client_kwargs)

    @property
    def roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str: URL roots
        """
        return self._roots

    @roots.setter
    def roots(self, roots):
        """
        Set URL roots for this storage.

        Args:
            roots (tuple of str): URL roots
        """
        self._roots = roots

    def relpath(self, path):
        """
        Get path relative to storage.

        args:
            path (str): Absolute path or URL.

        Returns:
            str: relative path.
        """
        for root in self.roots:
            try:
                if isinstance(root, Pattern):
                    relative = root.split(path, maxsplit=1)[1]
                else:
                    relative = path.split(root, 1)[1]
                # Strip "/" only at path start. "/" is used to known if
                # path is a directory on some cloud storage.
                return relative.lstrip('/')
            except IndexError:
                continue
        return path

    def is_locator(self, path, relative=False):
        """
        Returns True if path refer to a locator.

        Depending the storage, locator may be a bucket or container name,
        a hostname, ...

        args:
            path (str): path or URL.
            relative (bool): Path is relative to current root.

        Returns:
            bool: True if locator.
        """
        if not relative:
            path = self.relpath(path)
        # Bucket is the main directory
        return path and '/' not in path.rstrip('/')

    def split_locator(self, path):
        """
        Split the path into a pair (locator, path).

        args:
            path (str): Absolute path or URL.

        Returns:
            tuple of str: locator, path.
        """
        relative = self.relpath(path)
        try:
            locator, tail = relative.split('/', 1)
        except ValueError:
            locator = relative
            tail = ''
        return locator, tail

    def make_dir(self, path, relative=False):
        """
        Make a directory.

        Args:
            path (str): Path or URL.
            relative (bool): Path is relative to current root.
        """
        if not relative:
            path = self.relpath(path)
        self._make_dir(self.get_client_kwargs(self.ensure_dir_path(
            path, relative=True)))

    @abstractmethod
    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """

    def ensure_dir_path(self, path, relative=False):
        """
        Ensure the path is a dir path.

        Should end with '/' except for schemes and locators.

        Args:
            path (str): Path or URL.
            relative (bool): Path is relative to current root.

        Returns:
            path: dir path
        """
        if not relative:
            rel_path = self.relpath(path)
        else:
            rel_path = path

        # Locator
        if self.is_locator(rel_path, relative=True):
            path = path.rstrip('/')

        # Directory
        elif rel_path:
            path = path.rstrip('/') + '/'
        # else: root
        return path
