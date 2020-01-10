# coding=utf-8
"""Cloud object compatibles standard library 'os' equivalent functions"""
import os
from os import scandir as os_scandir, fsdecode, fsencode
from os.path import dirname
from stat import S_ISLNK, S_ISDIR

from airfs._core.compat import fspath
from airfs._core.storage_manager import get_instance
from airfs._core.functions_core import equivalent_to, is_storage
from airfs._core.exceptions import (
    ObjectExistsError, ObjectNotFoundError, handle_os_exceptions,
    ObjectPermissionError)
from airfs._core.io_base import memoizedmethod


@equivalent_to(os.listdir)
def listdir(path='.'):
    """
    Return a list containing the names of the entries in the directory given by
    path.

    Equivalent to "os.listdir".

    Args:
        path (path-like object): Path or URL.

    Returns:
        list of str: Entries names.
    """
    return [name.rstrip('/') for name, _ in
            get_instance(path).list_objects(path, first_level=True)]


@equivalent_to(os.makedirs)
def makedirs(name, mode=0o777, exist_ok=False):
    """
    Super-mkdir; create a leaf directory and all intermediate ones.
    Works like mkdir, except that any intermediate path segment
    (not just the rightmost) will be created if it does not exist.

    Equivalent to "os.makedirs".

    Args:
        name (path-like object): Path or URL.
        mode (int): The mode parameter is passed to os.mkdir();
            see the os.mkdir() description for how it is interpreted.
            Not supported on cloud storage objects.
        exist_ok (bool): Don't raises error if target directory already
            exists.

    Raises:
        FileExistsError: if exist_ok is False and if the target directory
            already exists.
    """
    system = get_instance(name)

    # Checks if directory not already exists
    if not exist_ok and system.isdir(system.ensure_dir_path(name)):
        raise ObjectExistsError("File exists: '%s'" % name)

    # Create directory
    system.make_dir(name)


@equivalent_to(os.mkdir)
def mkdir(path, mode=0o777, dir_fd=None):
    """
    Create a directory named path with numeric mode mode.

    Equivalent to "os.mkdir".

    Args:
        path (path-like object): Path or URL.
        mode (int): The mode parameter is passed to os.mkdir();
            see the os.mkdir() description for how it is interpreted.
            Not supported on cloud storage objects.
        dir_fd: directory descriptors;
            see the os.remove() description for how it is interpreted.
            Not supported on cloud storage objects.

    Raises:
        FileExistsError : Directory already exists.
        FileNotFoundError: Parent directory not exists.
    """
    system = get_instance(path)
    relative = system.relpath(path)

    # Checks if parent directory exists
    parent_dir = dirname(relative.rstrip('/'))
    if parent_dir:
        parent = path.rsplit(relative, 1)[0] + parent_dir + '/'
        if not system.isdir(parent):
            raise ObjectNotFoundError(
                "No such file or directory: '%s'" % parent)

    # Checks if directory not already exists
    if system.isdir(system.ensure_dir_path(path)):
        raise ObjectExistsError("File exists: '%s'" % path)

    # Create directory
    system.make_dir(relative, relative=True)


@equivalent_to(os.remove)
def remove(path, dir_fd=None):
    """
    Remove a file.

    Equivalent to "os.remove" and "os.unlink".

    Args:
        path (path-like object): Path or URL.
        dir_fd: directory descriptors;
            see the os.remove() description for how it is interpreted.
            Not supported on cloud storage objects.
    """
    system = get_instance(path)

    # Only support files
    if system.is_locator(path) or path[-1] == '/':
        raise IsADirectoryError("Is a directory: '%s'" % path)

    # Remove
    system.remove(path)


# "os.unlink" is alias of "os.remove"
unlink = remove


@equivalent_to(os.rmdir)
def rmdir(path, dir_fd=None):
    """
    Remove a directory.

    Equivalent to "os.rmdir".

    Args:
        path (path-like object): Path or URL.
        dir_fd: directory descriptors;
            see the os.rmdir() description for how it is interpreted.
            Not supported on cloud storage objects.
    """
    system = get_instance(path)
    system.remove(system.ensure_dir_path(path))


@equivalent_to(os.lstat)
def lstat(path, dir_fd=None):
    """
    Get the status of a file or a file descriptor.
    Perform the equivalent of a "lstat()" system call on the given path.

    Equivalent to "os.lstat".

    On cloud object, may return extra storage specific attributes in
    "os.stat_result".

    Args:
        path (path-like object): Path or URL.
        dir_fd: directory descriptors;
            see the os.rmdir() description for how it is interpreted.
            Not supported on cloud storage objects.

    Returns:
        os.stat_result: stat result.
    """
    return get_instance(path).stat(path)


@equivalent_to(os.stat)
def stat(path, dir_fd=None, follow_symlinks=True):
    """
    Get the status of a file or a file descriptor.
    Perform the equivalent of a "stat()" system call on the given path.

    Equivalent to "os.stat".

    On cloud object, may return extra storage specific attributes in
    "os.stat_result".

    Args:
        path (path-like object): Path or URL.
        dir_fd: directory descriptors;
            see the os.rmdir() description for how it is interpreted.
            Not supported on cloud storage objects.
        follow_symlinks (bool): Follow symlinks.
            Not supported on cloud storage objects.

    Returns:
        os.stat_result: stat result.
    """
    return get_instance(path).stat(path)


class DirEntry:
    """
    Object yielded by scandir() to expose the file path and other file
    attributes of a directory entry.

    Equivalent to "os.DirEntry".

    Not intended to be instantiated directly.
    """
    __slots__ = ('_cache', '_system', '_name', '_header', '_path',
                 '_bytes_path')

    def __init__(self, scandir_path, system, name, header, bytes_path):
        """
        Should only be instantiated by "scandir".

        Args:
            scandir_path (str): scandir path argument.
            system (airfs._core.io_system.SystemBase subclass):
                Storage system.
            name (str): Name of the object relative to "scandir_path".
            header (dict): Object header
            bytes_path (bool): True if path must be returned as bytes.
        """
        self._cache = dict()
        self._system = system
        self._name = name
        self._header = header
        self._path = ''.join((
            scandir_path if scandir_path[-1] == '/' else (scandir_path + '/'),
            name))
        self._bytes_path = bytes_path

    @memoizedmethod
    def __str__(self):
        return "<DirEntry '%s'>" % self._name.rstrip('/')

    __repr__ = __str__

    @property
    @memoizedmethod
    def _client_kwargs(self):
        """
        Get base keyword arguments for client

        Returns:
            dict: keyword arguments
        """
        return self._system.get_client_kwargs(self._path)

    @property
    @memoizedmethod
    def name(self):
        """
        The entry’s base filename, relative to the scandir() path argument.

        Returns:
            str: name.
        """
        name = self._name.rstrip('/')
        if self._bytes_path:
            name = fsencode(name)
        return name

    @property
    @memoizedmethod
    def path(self):
        """
        The entry’s full path name:
        equivalent to os.path.join(scandir_path, entry.name)
        where scandir_path is the scandir() path argument.

        The path is only absolute if the scandir() path argument was absolute.

        Returns:
            str: name.
        """
        path = self._path.rstrip('/')
        if self._bytes_path:
            path = fsencode(path)
        return path

    @memoizedmethod
    def inode(self):
        """
        Return the inode number of the entry.

        The result is cached on the os.DirEntry object.

        Returns:
            int: inode.
        """
        return self.stat().st_ino

    @memoizedmethod
    def is_dir(self, follow_symlinks=True):
        """
        Return True if this entry is a directory or a symbolic link pointing to
        a directory; return False if the entry is or points to any other kind
        of file, or if it doesn’t exist anymore.

        The result is cached on the os.DirEntry object.

        Args:
            follow_symlinks (bool): Follow symlinks.
                Not supported on cloud storage objects.

        Returns:
            bool: True if directory exists.
        """
        try:
            return (self._system.isdir(
                path=self._path, client_kwargs=self._client_kwargs,
                virtual_dir=False) or

                # Some directories only exists virtually in object path and
                # don't have headers.
                bool(S_ISDIR(self.stat().st_mode)))

        except ObjectPermissionError:
            # The directory was listed, but unable to head it or access to its
            # content
            return True

    @memoizedmethod
    def is_file(self, follow_symlinks=True):
        """
        Return True if this entry is a file or a symbolic link pointing to a
        file; return False if the entry is or points to a directory or other
        non-file entry, or if it doesn’t exist anymore.

        The result is cached on the os.DirEntry object.

        Args:
            follow_symlinks (bool): Follow symlinks.
                Not supported on cloud storage objects.

        Returns:
            bool: True if directory exists.
        """
        return self._system.isfile(
            path=self._path, client_kwargs=self._client_kwargs)

    @memoizedmethod
    def is_symlink(self):
        """
        Return True if this entry is a symbolic link

        The result is cached on the os.DirEntry object.

        Returns:
            bool: True if symbolic link.
        """
        return bool(S_ISLNK(self.stat().st_mode))

    @memoizedmethod
    def stat(self, follow_symlinks=True):
        """
        Return a stat_result object for this entry.

        The result is cached on the os.DirEntry object.

        Args:
            follow_symlinks (bool): Follow symlinks.
                Not supported on cloud storage objects.

        Returns:
            os.stat_result: Stat result object
        """
        return self._system.stat(
            path=self._path, client_kwargs=self._client_kwargs,
            header=self._header)


DirEntry.__module__ = 'airfs'


def scandir(path='.'):
    """
    Return an iterator of os.DirEntry objects corresponding to the entries in
    the directory given by path. The entries are yielded in arbitrary order,
    and the special entries '.' and '..' are not included.

    Equivalent to "os.scandir".

    Args:
        path (path-like object): Path or URL.
             If path is of type bytes (directly or indirectly through the
             PathLike interface), the type of the name and path attributes
             of each os.DirEntry will be bytes; in all other circumstances,
             they will be of type str.

    Returns:
        Generator of os.DirEntry: Entries information.
    """
    # Handles path-like objects
    scandir_path = fsdecode(path).replace('\\', '/')

    if not is_storage(scandir_path):
        return os_scandir(scandir_path)

    return _scandir_generator(
        is_bytes=isinstance(fspath(path), (bytes, bytearray)),
        scandir_path=scandir_path, system=get_instance(scandir_path))


def _scandir_generator(is_bytes, scandir_path, system):
    """
    scandir generator

    Args:
        is_bytes (bool): True if DirEntry must handle path as bytes.
        scandir_path (str): Path.
        system (airfs._core.io_system.SystemBase subclass):
            Storage system.

    Yields:
        DirEntry: Directory entries
    """
    with handle_os_exceptions():
        for name, header in system.list_objects(scandir_path, first_level=True):
            yield DirEntry(
                scandir_path=scandir_path, system=system, name=name,
                header=header, bytes_path=is_bytes)
