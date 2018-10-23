# coding=utf-8
"""Cloud object compatibles standard library 'os.path' equivalent functions"""
import os
from os.path import dirname

from pycosio._core.compat import (
    makedirs as os_makedirs, remove as os_remove, rmdir as os_rmdir,
    is_a_directory_error, mkdir as os_mkdir)
from pycosio._core.storage_manager import get_instance
from pycosio._core.functions_core import equivalent_to
from pycosio._core.exceptions import ObjectExistsError, ObjectNotFoundError


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


@equivalent_to(os_makedirs)
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
            Not support on cloud objects.
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


@equivalent_to(os_mkdir)
def mkdir(path, mode=0o777, dir_fd=None):
    """
    Create a directory named path with numeric mode mode.

    Equivalent to "os.mkdir".

    Args:
        path (path-like object): Path or URL.
        mode (int): The mode parameter is passed to os.mkdir();
            see the os.mkdir() description for how it is interpreted.
            Not support on cloud objects.
        dir_fd: directory descriptors;
            see the os.remove() description for how it is interpreted.
            Not support on cloud objects.

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


@equivalent_to(os_remove)
def remove(path, dir_fd=None):
    """
    Remove a file.

    Equivalent to "os.remove" and "os.unlink".

    Args:
        path (path-like object): Path or URL.
        dir_fd: directory descriptors;
            see the os.remove() description for how it is interpreted.
            Not support on cloud objects.
    """
    system = get_instance(path)

    # Only support files
    if system.is_locator(path) or path[-1] == '/':
        raise is_a_directory_error("Is a directory: '%s'" % path)

    # Remove
    system.remove(path)


# "os.unlink" is alias of "os.remove"
unlink = remove


@equivalent_to(os_rmdir)
def rmdir(path, dir_fd=None):
    """
    Remove a directory.

    Equivalent to "os.rmdir".

    Args:
        path (path-like object): Path or URL.
        dir_fd: directory descriptors;
            see the os.rmdir() description for how it is interpreted.
            Not support on cloud objects.
    """
    system = get_instance(path)
    system.remove(system.ensure_dir_path(path))
