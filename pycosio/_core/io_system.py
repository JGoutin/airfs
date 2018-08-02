# coding=utf-8
"""Cloud storage abstract System"""
from abc import abstractmethod
from email.utils import parsedate
from time import mktime

from pycosio._core.compat import ABC
from pycosio._core.utilities import memoizedmethod


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

        # Cache for values
        self._cache = {}

    @abstractmethod
    def client_kwargs(self, path):
        """
        Get base keyword arguments for client for a
        specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """

    @abstractmethod
    def _get_client(self):
        """
        Storage client

        Returns:
            client
        """

    @memoizedmethod
    def get_client(self):
        """
        Storage client

        Returns:
            client
        """
        return self._get_client()

    def getmtime_client(self, **client_kwargs):
        """
        Return the time of last access of path.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            float: The number of seconds since the epoch
                (see the time module).

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        # By default, assumes that information are in a standard HTTP header
        return mktime(parsedate({
            key.lower(): value
            for key, value in self.head(**client_kwargs).items()}['last-modified']))

    def getmtime(self, path):
        """
        Return the time of last access of path.

        Args:
            path (path-like object): File path or URL.

        Returns:
            float: The number of seconds since the epoch
                (see the time module).

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        return self.getmtime_client(**self.client_kwargs(path))

    @property
    @memoizedmethod
    def prefixes(self):
        """
        Return URL prefixes for this storage.

        Returns:
            tuple of str: URL prefixes
        """
        return self._get_prefixes()

    @abstractmethod
    def _get_prefixes(self):
        """
        Return URL prefixes for this storage.

        Returns:
            tuple of str: URL prefixes
        """

    def getsize(self, path):
        """
        Return the size, in bytes, of path.

        Args:
            path (str): File path or URL.

        Returns:
            int: Size in bytes.

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        return self.getsize_client(**self.client_kwargs(path))

    def getsize_client(self, **client_kwargs):
        """
        Return the size, in bytes, of path.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            int: Size in bytes.

        Raises:
             OSError: if the file does not exist or is inaccessible.
        """
        # By default, assumes that information are in a standard HTTP header
        return int({
            key.lower(): value
            for key, value in self.head(**client_kwargs).items()}['content-length'])

    def get_storage_parameters(self):
        """
        Storage parameters

        Returns:
            dict: Storage parameters
        """
        return self._storage_parameters

    def head(self, **client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """
        # This is not an abstract method because this may not
        # be used every time

    def listdir(self, path='.'):
        """
        Return a list containing the names of the entries in
        the directory given by path

        Args:
            path (path-like object): File path or URL.

        Returns:
            list of str: Directory content.
        """
        return self.listdir_client(**self.client_kwargs(path))

    def listdir_client(self, **client_kwargs):
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
