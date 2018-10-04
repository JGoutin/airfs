# coding=utf-8
"""Cloud object compatibles standard library 'os.path' equivalent functions"""
import os
from os.path import dirname

from pycosio._core.compat import makedirs as _os_makedirs
from pycosio._core.storage_manager import get_instance
from pycosio._core.functions_core import equivalent_to
from pycosio._core.functions_os_path import isdir
from pycosio._core.exceptions import ObjectExistsError, ObjectNotFoundError


@equivalent_to(_os_makedirs)
def makedirs(name, mode=0o777, exist_ok=False):
    """
    Super-mkdir; create a leaf directory and all intermediate ones.
    Works like mkdir, except that any intermediate path segment
    (not just the rightmost) will be created if it does not exist.

    Equivalent to "os.makedirs".

    "mode" is currently not yet supported on cloud storage.

    Args:
        name (path-like object): Path or URL.
        mode (int): The mode parameter is passed to os.mkdir();
            see the os.mkdir() description for how it is interpreted.
        exist_ok (bool): Don't raises error if target directory already
            exists.

    Raises:
        FileExistsError: if exist_ok is False and if the target directory
            already exists.
    """
    # TODO: mode

    # Checks if directory not already exists
    if not exist_ok and isdir(name):
        raise ObjectExistsError("File exists: '%s'" % name)

    # Create directory
    get_instance(name).make_dir(name)


@equivalent_to(os.mkdir)
def mkdir(name, mode=0o777):
    """
    Create a directory named path with numeric mode mode.

    Equivalent to "os.mkdir".

    "mode" is currently not yet supported on cloud storage.

    Args:
        name (path-like object): Path or URL.
        mode (int): The mode parameter is passed to os.mkdir();
            see the os.mkdir() description for how it is interpreted.

    Raises:
        FileExistsError : Directory already exists.
        FileNotFoundError: Parent directory not exists.
    """
    # TODO: mode

    # Checks if parent directory exists
    parent = dirname(name)
    if not isdir(parent):
        raise ObjectNotFoundError("No such file or directory: '%s'" % parent)

    # Checks if directory not already exists
    if not isdir:
        raise ObjectExistsError("File exists: '%s'" % name)

    # Create directory
    get_instance(name).make_dir(name)
