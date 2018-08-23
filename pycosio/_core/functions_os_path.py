# coding=utf-8
"""Cloud object compatibles standard library 'os.path' equivalent functions"""
import os
from os.path import relpath as os_path_relpath

from pycosio._core.storage_manager import get_instance
from pycosio._core.functions_core import equivalent_to


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
    """
    return get_instance(path).getsize(path)


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
    """
    return get_instance(path).getmtime(path)


@equivalent_to(os.path.isfile)
def isfile(path):
    """
    Return True if path is an existing regular file.

    Equivalent to "os.path.isfile".

    Args:
        path (path-like object): File path or URL.

    Returns:
        bool: True if file exists.
    """
    return get_instance(path).isfile(path)


@equivalent_to(os.path.relpath)
def relpath(path, start=None):
    """
    Return a relative filepath to path either from the
    current directory or from an optional start directory.

    For storage objects, "path" and "start" are relative to
    storage root.

    Equivalent to "os.path.relpath".

    Args:
        path (path-like object): File path or URL.
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
