# coding=utf-8
"""Cloud storage abstract System"""
from abc import abstractmethod
from email.utils import parsedate
from time import mktime

from pycosio._core.compat import ABC
from pycosio._core.exceptions import ObjectNotFoundError


class SystemBase(ABC):
    """
    Cloud storage system handler.

    Args:
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
    """

    def __init__(self, storage_parameters=None):
        # Save storage parameters
        self._storage_parameters = storage_parameters or dict()

        # Initialize client
        self._client = self._get_client()

        # Initialize prefixes, get cached values if available
        prefixes = self._storage_parameters.pop(
            'storage.prefixes', None)
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
        return mktime(parsedate({
            key.lower(): value
            for key, value in header.items()}['last-modified']))

    @abstractmethod
    def _get_prefixes(self):
        """
        Return URL prefixes for this storage.

        Returns:
            tuple of str: URL prefixes
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
        return self._getsize_from_header(
            self.head(path, client_kwargs, header))

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
        return int({
            key.lower(): value
            for key, value in header.items()}['content-length'])

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

    def listdir(self, path='.'):
        """
        Return a list containing the names of the entries in
        the directory given by path

        Args:
            path (str): File path or URL.

        Returns:
            list of str: Directory content.
        """
        return self._listdir(self.get_client_kwargs(path))

    def _listdir(self, client_kwargs):
        """
        Return a list containing the names of the entries in
        the directory given by path

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            list of str: Directory content.
        """
        # By default, assumes that directory is not listable
        raise OSError("Can't list directory")

    @property
    def prefixes(self):
        """
        Return URL prefixes for this storage.

        Returns:
            tuple of str: URL prefixes
        """
        return self._prefixes

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
                return path.split(prefix)[1]
            except IndexError:
                continue
        return path
