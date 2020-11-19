"""Cloud object compatibles standard library 'os' equivalent functions"""
import os
from os import scandir as os_scandir, fsdecode, fsencode, fspath
from os.path import dirname
from stat import S_ISLNK, S_ISDIR

from airfs._core.storage_manager import get_instance
from airfs._core.functions_core import (
    equivalent_to,
    is_storage,
    raises_on_dir_fd,
    format_and_is_storage,
)
from airfs._core.exceptions import (
    ObjectExistsError,
    ObjectNotFoundError,
    handle_os_exceptions,
    ObjectPermissionError,
    ObjectIsADirectoryError,
    ObjectSameFileError,
    ObjectNotImplementedError,
)
from airfs._core.io_base import memoizedmethod


@equivalent_to(os.listdir)
def listdir(path="."):
    """
    Return a list containing the names of the entries in the directory given by path.
    Follow symlinks if any.

    Equivalent to "os.listdir".

    .. versionadded:: 1.2.0

    Args:
        path (path-like object): Path or URL.

    Returns:
        list of str: Entries names.
    """
    system = get_instance(path)
    path = system.resolve(path, follow_symlinks=True)[0]
    return [name.rstrip("/") for name, _ in system.list_objects(path, first_level=True)]


@equivalent_to(os.makedirs)
def makedirs(name, mode=0o777, exist_ok=False):
    """
    Super-mkdir; create a leaf directory and all intermediate ones.
    Works like mkdir, except that any intermediate path segment (not just the rightmost)
    will be created if it does not exist.

    Equivalent to "os.makedirs".

    .. versionadded:: 1.1.0

    Args:
        name (path-like object): Path or URL.
        mode (int): The mode parameter is passed to os.mkdir();
            see the os.mkdir() description for how it is interpreted.
            Not supported on storage objects.
        exist_ok (bool): Don't raises error if target directory already exists.

    Raises:
        FileExistsError: if exist_ok is False and if the target directory already
            exists.
    """
    system = get_instance(name)

    if not exist_ok and system.isdir(system.ensure_dir_path(name)):
        raise ObjectExistsError(path=name)

    system.make_dir(name)


@equivalent_to(os.mkdir)
def mkdir(path, mode=0o777, *, dir_fd=None):
    """
    Create a directory named path with numeric mode mode.

    Equivalent to "os.mkdir".

    .. versionadded:: 1.1.0

    Args:
        path (path-like object): Path or URL.
        mode (int): The mode parameter is passed to os.mkdir();
            see the os.mkdir() description for how it is interpreted.
            Not supported on storage objects.
        dir_fd (int): directory descriptors;
            see the os.remove() description for how it is interpreted.
            Not supported on storage objects.

    Raises:
        FileExistsError : Directory already exists.
        FileNotFoundError: Parent directory not exists.
    """
    raises_on_dir_fd(dir_fd)
    system = get_instance(path)
    relative = system.relpath(path)

    parent_dir = dirname(relative.rstrip("/"))
    if parent_dir:
        parent = "{}{}/".format(path.rsplit(relative, 1)[0], parent_dir)
        if not system.isdir(parent):
            raise ObjectNotFoundError(path=parent)

    if system.isdir(system.ensure_dir_path(path)):
        raise ObjectExistsError(path=path)

    system.make_dir(relative, relative=True)


@equivalent_to(os.readlink, keep_path_type=True)
def readlink(path, *, dir_fd=None):
    """
    Return a string representing the path to which the symbolic link points.
    The result may be either an absolute or relative pathname; if it is relative, it may
    be converted to an absolute pathname using
    os.path.join(os.path.dirname(path), result).

    If the path is a string object (directly or indirectly through a PathLike
    interface), the result will also be a string object, and the call may raise a
    UnicodeDecodeError. If the path is a bytes object (direct or indirectly), the result
    will be a bytes object.

    Equivalent to "os.readlink".

    .. versionadded:: 1.5.0

    Args:
        path (path-like object): Path or URL.
        dir_fd (int): directory descriptors;
            see the os.readlink() description for how it is interpreted.
            Not supported on storage objects.
    """
    raises_on_dir_fd(dir_fd)
    return get_instance(path).read_link(path)


@equivalent_to(os.remove)
def remove(path, *, dir_fd=None):
    """
    Remove a file.

    Equivalent to "os.remove" and "os.unlink".

    .. versionadded:: 1.2.0

    Args:
        path (path-like object): Path or URL.
        dir_fd (int): directory descriptors;
            see the os.remove() description for how it is interpreted.
            Not supported on storage objects.
    """
    raises_on_dir_fd(dir_fd)
    system = get_instance(path)

    if system.is_locator(path) or path[-1] == "/":
        raise ObjectIsADirectoryError(path=path)

    system.remove(path)


unlink = remove


@equivalent_to(os.rmdir)
def rmdir(path, *, dir_fd=None):
    """
    Remove a directory.

    Equivalent to "os.rmdir".

    .. versionadded:: 1.2.0

    Args:
        path (path-like object): Path or URL.
        dir_fd (int): directory descriptors;
            see the os.rmdir() description for how it is interpreted.
            Not supported on storage objects.
    """
    raises_on_dir_fd(dir_fd)
    system = get_instance(path)
    system.remove(system.ensure_dir_path(path))


@equivalent_to(os.lstat)
def lstat(path, *, dir_fd=None):
    """
    Get the status of a file or a file descriptor.
    Perform the equivalent of a "lstat()" system call on the given path.

    Equivalent to "os.lstat".

    On storage object, may return extra storage specific attributes in "os.stat_result".

    .. versionadded:: 1.2.0

    Args:
        path (path-like object): Path or URL.
        dir_fd (int): directory descriptors;
            see the os.rmdir() description for how it is interpreted.
            Not supported on storage objects.

    Returns:
        os.stat_result: stat result.
    """
    raises_on_dir_fd(dir_fd)
    return get_instance(path).stat(path)


@equivalent_to(os.stat)
def stat(path, *, dir_fd=None, follow_symlinks=True):
    """
    Get the status of a file or a file descriptor.
    Perform the equivalent of a "stat()" system call on the given path.

    Equivalent to "os.stat".

    On storage object, may return extra storage specific attributes in "os.stat_result".

    .. versionadded:: 1.2.0

    Args:
        path (path-like object): Path or URL.
        dir_fd (int): directory descriptors;
            see the os.rmdir() description for how it is interpreted.
            Not supported on storage objects.
        follow_symlinks (bool): Follow symlinks.

    Returns:
        os.stat_result: stat result.
    """
    raises_on_dir_fd(dir_fd)
    return get_instance(path).stat(path, follow_symlinks=follow_symlinks)


class DirEntry:
    """
    Object yielded by scandir() to expose the file path and other file attributes of a
    directory entry.

    Equivalent to "os.DirEntry".

    Not intended to be instantiated directly.

    .. versionadded:: 1.2.0
    """

    __slots__ = ("_cache", "_system", "_name", "_header", "_path", "_bytes_path")

    def __init__(self, scandir_path, system, name, header, bytes_path):
        """
        Should only be instantiated by "scandir".

        Args:
            scandir_path (str): scandir path argument.
            system (airfs._core.io_system.SystemBase subclass): Storage system.
            name (str): Name of the object relative to "scandir_path".
            header (dict): Object header
            bytes_path (bool): True if path must be returned as bytes.
        """
        self._cache = dict()
        self._system = system
        self._name = name
        self._header = header
        self._path = "".join(
            (scandir_path if scandir_path[-1] == "/" else (scandir_path + "/"), name)
        )
        self._bytes_path = bytes_path

    @memoizedmethod
    def __str__(self):
        return f"<DirEntry '{self._name.rstrip('/')}'>"

    __repr__ = __str__

    @property  # type: ignore
    @memoizedmethod
    def _client_kwargs(self):
        """
        Get base keyword arguments for client

        Returns:
            dict: keyword arguments
        """
        return self._system.get_client_kwargs(self._path)

    @property  # type: ignore
    @memoizedmethod
    def name(self):
        """
        The entry’s base filename, relative to the scandir() path argument.

        Returns:
            str: name.
        """
        name = self._name.rstrip("/")
        if self._bytes_path:
            name = fsencode(name)
        return name

    @property  # type: ignore
    @memoizedmethod
    def path(self):
        """
        The entry’s full path name:
        equivalent to os.path.join(scandir_path, entry.name) where scandir_path is the
        scandir() path argument.

        The path is only absolute if the scandir() path argument was absolute.

        Returns:
            str: name.
        """
        path = self._path.rstrip("/")
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
    def is_dir(self, *, follow_symlinks=True):
        """
        Return True if this entry is a directory or a symbolic link pointing to a
        directory; return False if the entry is or points to any other kind of file, or
        if it doesn’t exist anymore.

        The result is cached on the os.DirEntry object.

        Args:
            follow_symlinks (bool): Follow symlinks.

        Returns:
            bool: True if directory exists.
        """
        with handle_os_exceptions():
            try:
                return self._system.isdir(
                    path=self._path,
                    client_kwargs=self._client_kwargs,
                    virtual_dir=False,
                    follow_symlinks=follow_symlinks,
                ) or bool(
                    # Some directories only exists virtually in object path and don't
                    # have headers.
                    S_ISDIR(self.stat().st_mode)
                )

            except ObjectPermissionError:
                return True

    @memoizedmethod
    def is_file(self, *, follow_symlinks=True):
        """
        Return True if this entry is a file or a symbolic link pointing to a file;
        return False if the entry is or points to a directory or other non-file entry,
        or if it doesn’t exist anymore.

        The result is cached on the os.DirEntry object.

        Args:
            follow_symlinks (bool): Follow symlinks.

        Returns:
            bool: True if directory exists.
        """
        with handle_os_exceptions():
            return self._system.isfile(
                path=self._path,
                client_kwargs=self._client_kwargs,
                follow_symlinks=follow_symlinks,
            )

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
    def stat(self, *, follow_symlinks=True):
        """
        Return a stat_result object for this entry.

        The result is cached on the os.DirEntry object.

        Args:
            follow_symlinks (bool): Follow symlinks.

        Returns:
            os.stat_result: Stat result object
        """
        with handle_os_exceptions():
            return self._system.stat(
                path=self._path,
                client_kwargs=self._client_kwargs,
                header=self._header,
                follow_symlinks=follow_symlinks,
            )


DirEntry.__module__ = "airfs"


def scandir(path="."):
    """
    Return an iterator of os.DirEntry objects corresponding to the entries in the
    directory given by path. The entries are yielded in arbitrary order, and the special
    entries '.' and '..' are not included.

    Equivalent to "os.scandir".

    .. versionadded:: 1.2.0

    Args:
        path (path-like object): Path or URL.
             If path is of type bytes (directly or indirectly through the PathLike
             interface), the type of the name and path attributes of each os.DirEntry
             will be bytes; in all other circumstances, they will be of type str.

    Returns:
        Generator of os.DirEntry: Entries information.
    """
    scandir_path = fsdecode(path).replace("\\", "/")

    if not is_storage(scandir_path):
        return os_scandir(scandir_path)

    system = get_instance(scandir_path)
    return _scandir_generator(
        is_bytes=isinstance(fspath(path), (bytes, bytearray)),
        scandir_path=system.resolve(scandir_path, follow_symlinks=True)[0],
        system=system,
    )


def _scandir_generator(is_bytes, scandir_path, system):
    """
    scandir generator

    Args:
        is_bytes (bool): True if DirEntry must handle path as bytes.
        scandir_path (str): Path.
        system (airfs._core.io_system.SystemBase subclass): Storage system.

    Yields:
        DirEntry: Directory entries
    """
    with handle_os_exceptions():
        for name, header in system.list_objects(scandir_path, first_level=True):
            yield DirEntry(
                scandir_path=scandir_path,
                system=system,
                name=name,
                header=header,
                bytes_path=is_bytes,
            )


def symlink(src, dst, target_is_directory=False, *, dir_fd=None):
    """
    Create a symbolic link pointing to src named dst.

    Equivalent to "os.symlink".

    .. versionadded:: 1.5.0

    Args:
        src (path-like object): Path or URL to the symbolic link.
        dst (path-like object): Path or URL to the target.
        target_is_directory (bool): On Windows, define if symlink represents either a
            file or a directory.
            Not supported on storage objects and non-Windows platforms.
        dir_fd (int): directory descriptors;
            see the os.symlink() description for how it is interpreted.
            Not supported on storage objects.
    """
    src, src_is_storage = format_and_is_storage(src)
    dst, dst_is_storage = format_and_is_storage(dst)

    if not src_is_storage and not dst_is_storage:
        return os.symlink(
            src, dst, target_is_directory=target_is_directory, dir_fd=dir_fd
        )

    with handle_os_exceptions():
        if not src_is_storage or not dst_is_storage:
            ObjectNotImplementedError("Cross storage symlinks are not supported")

        raises_on_dir_fd(dir_fd)
        system_src = get_instance(src)
        system_dst = get_instance(dst)

        if system_src is not system_dst:
            ObjectNotImplementedError("Cross storage symlinks are not supported")

        elif system_src.relpath(src) == system_dst.relpath(dst):
            raise ObjectSameFileError(path1=src, path2=dst)

        return get_instance(src).symlink(src, dst)
