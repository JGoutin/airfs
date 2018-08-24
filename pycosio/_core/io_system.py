# coding=utf-8
"""Cloud storage abstract System"""
from abc import abstractmethod
from email.utils import parsedate
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
        prefixes (tuple): Tuple of prefixes to force use.
    """

    def __init__(self, storage_parameters=None, unsecure=False, prefixes=None):
        # Save storage parameters
        self._storage_parameters = storage_parameters or dict()
        self._unsecure = unsecure

        # Initialize client
        self._client = None

        # Initialize prefixes
        if prefixes:
            self._prefixes = prefixes
        else:
            self._prefixes = self._get_prefixes()

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

    @abstractmethod
    def _get_prefixes(self):
        """
        Return URL prefixes for this storage.

        Returns:
            tuple of str or re.Pattern: URL prefixes
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

    def isfile(self, path=None, client_kwargs=None):
        """
        Return True if path is an existing regular file.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.

        Returns:
            bool: True if file exists.
        """
        try:
            self.head(path, client_kwargs)
        except ObjectNotFoundError:
            return False
        return True

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
        # This is not an abstract method because this may not
        # be used every time

    def head(self, path=None, client_kwargs=None, header=None):
        """
        Returns object HTTP header.

        Args:
            path (str): File path or URL.
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
    def prefixes(self):
        """
        Return URL prefixes for this storage.

        Returns:
            tuple of str: URL prefixes
        """
        return self._prefixes

    @prefixes.setter
    def prefixes(self, prefixes):
        """
        Set URL prefixes for this storage.

        Args:
            prefixes (tuple of str): URL prefixes
        """
        self._prefixes = prefixes

    def relpath(self, path):
        """
        Get path relative to storage.

        args:
            path (str): Absolute path or URL.

        Returns:
            str: relative path.
        """
        for prefix in self.prefixes:
            try:
                if isinstance(prefix, Pattern):
                    relative = prefix.split(path, maxsplit=1)[1]
                else:
                    relative = path.split(prefix, 1)[1]
                return relative.strip(r'\/')
            except IndexError:
                continue
        return path
