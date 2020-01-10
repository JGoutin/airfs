# coding=utf-8
"""Cloud object compatibles standard library 'os.path' equivalent functions"""
import os
from os.path import relpath as os_path_relpath, samefile as os_path_samefile

from airfs._core.storage_manager import get_instance
from airfs._core.functions_core import equivalent_to, format_and_is_storage
from airfs._core.exceptions import handle_os_exceptions


@equivalent_to(os.path.exists)
def exists(path):
    """
    Return True if path refers to an existing path.

    Equivalent to "os.path.exists".

    Args:
        path (path-like object): Path or URL.

    Returns:
        bool: True if path exists.
    """
    return get_instance(path).exists(path)


@equivalent_to(os.path.getsize)
def getsize(path):
    """
    Return the size, in bytes, of path.

    Equivalent to "os.path.getsize".

    Args:
        path (path-like object): File path or URL.

    Returns:
        int: Size in bytes.

    Raises:
         OSError: if the file does not exist or is inaccessible.
         io.UnsupportedOperation: Information not available for this path.
    """
    return get_instance(path).getsize(path)


@equivalent_to(os.path.getctime)
def getctime(path):
    """
    Return the creation time of path.

    Equivalent to "os.path.getctime".

    Args:
        path (path-like object): File path or URL.

    Returns:
        float: The number of seconds since the epoch
            (see the time module).

    Raises:
         OSError: if the file does not exist or is inaccessible.
         io.UnsupportedOperation: Information not available for this path.
    """
    return get_instance(path).getctime(path)


@equivalent_to(os.path.getmtime)
def getmtime(path):
    """
    Return the time of last access of path.

    Equivalent to "os.path.getmtime".

    Args:
        path (path-like object): File path or URL.

    Returns:
        float: The number of seconds since the epoch
            (see the time module).

    Raises:
         OSError: if the file does not exist or is inaccessible.
         io.UnsupportedOperation: Information not available for this path.
    """
    return get_instance(path).getmtime(path)


@equivalent_to(os.path.isabs)
def isabs(path):
    """
    Return True if path is an absolute pathname.

    Equivalent to "os.path.isabs".

    Args:
        path (path-like object): Path or URL.

    Returns:
        bool: True if path is absolute.
    """
    # If detected as storage path, it is an absolute path.
    return True


@equivalent_to(os.path.isdir)
def isdir(path):
    """
    Return True if path is an existing directory.

    Equivalent to "os.path.isdir".

    Args:
        path (path-like object): Path or URL.

    Returns:
        bool: True if directory exists.
    """
    system = get_instance(path)

    # User may use directory path without trailing '/'
    # like on standard file systems
    return system.isdir(system.ensure_dir_path(path))


@equivalent_to(os.path.isfile)
def isfile(path):
    """
    Return True if path is an existing regular file.

    Equivalent to "os.path.isfile".

    Args:
        path (path-like object): Path or URL.

    Returns:
        bool: True if file exists.
    """
    return get_instance(path).isfile(path)


@equivalent_to(os.path.islink)
def islink(path):
    """
    Return True if path is an existing symlink.

    Equivalent to "os.path.islink".

    Args:
        path (path-like object): Path or URL.

    Returns:
        bool: True if symlink.
    """
    return get_instance(path).islink(path)


@equivalent_to(os.path.ismount)
def ismount(path):
    """
    Return True if pathname path is a mount point.

    Equivalent to "os.path.ismount".

    Args:
        path (path-like object): Path or URL.

    Returns:
        bool: True if path is a mount point.
    """
    return True if not get_instance(path).relpath(path) else False


@equivalent_to(os.path.relpath)
def relpath(path, start=None):
    """
    Return a relative file path to path either from the
    current directory or from an optional start directory.

    For storage objects, "path" and "start" are relative to
    storage root.

    "/" are not stripped on storage objects path. The ending slash is required
    on some storage to signify that target is a directory.

    Equivalent to "os.path.relpath".

    Args:
        path (path-like object): Path or URL.
        start (path-like object): Relative from this optional directory.
            Default to "os.curdir" for local files.

    Returns:
        str: Relative path.
    """
    relative = get_instance(path).relpath(path)
    if start:
        # Storage relative path
        # Replaces "\" by "/" for Windows.
        return os_path_relpath(relative, start=start).replace('\\', '/')
    return relative


def samefile(path1, path2):
    """
    Return True if both pathname arguments refer to the same file or directory.

    Equivalent to "os.path.samefile".

    Args:
        path1 (path-like object): Path or URL.
        path2 (path-like object): Path or URL.

    Returns:
        bool: True if same file or directory.
    """
    # Handles path-like objects and checks if storage
    path1, path1_is_storage = format_and_is_storage(path1)
    path2, path2_is_storage = format_and_is_storage(path2)

    # Local files: Redirects to "os.path.samefile"
    if not path1_is_storage and not path2_is_storage:
        return os_path_samefile(path1, path2)

    # One path is local, the other storage
    if not path1_is_storage or not path2_is_storage:
        return False

    with handle_os_exceptions():
        # Paths don't use same storage
        system = get_instance(path1)
        if system is not get_instance(path2):
            return False

        # Relative path are different
        elif system.relpath(path1) != system.relpath(path2):
            return False

    # Same files
    return True


@equivalent_to(os.path.splitdrive)
def splitdrive(path):
    """
    Split the path into a pair (drive, tail) where drive is either a
    mount point or the empty string. On systems which do not use drive
    specifications, drive will always be the empty string.

    In all cases, drive + tail will be the same as path.

    Equivalent to "os.path.splitdrive".

    Args:
        path (path-like object): Path or URL.

    Returns:
        tuple of str: drive, tail.
    """
    relative = get_instance(path).relpath(path)
    drive = path.rsplit(relative, 1)[0]
    if drive and not drive[-2:] == '//':
        # Keep "/" tail side
        relative = '/' + relative
        drive = drive.rstrip('/')
    return drive, relative
