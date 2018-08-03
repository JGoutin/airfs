# coding=utf-8
"""Cloud object compatibles standard library equivalent functions"""
from contextlib import contextmanager as _contextmanager
from functools import wraps as _wraps
from io import open as _open, TextIOWrapper as _TextIOWrapper
from os import listdir as _listdir
from os.path import (
    isdir as _isdir, basename as _basename, join as _join,
    getmtime as _getmtime, getsize as _getsize, relpath as _relpath,
    isfile as _isfile)
from shutil import copy as _copy, copyfileobj as _copyfileobj

from pycosio._core.compat import fsdecode as _fsdecode
from pycosio._core.storage_manager import (
    get_instance as _get_instance, is_storage as _is_storage)
from pycosio._core.utilities import handle_os_exceptions as _handle_os_exceptions


def _equivalent_to(std_function):
    """
    Decorate a cloud object compatible function
    to provides fall back to standard function if
    use on local files.

    Args:
        std_function (function): standard function to
            used with local files.

    Returns:
        function: new function
    """
    def decorate(cos_function):
        """Decorator argument handler"""

        @_wraps(cos_function)
        def decorated(path, *args, **kwargs):
            """Decorated function"""

            # Handles path-like objects
            path = _fsdecode(path)

            # Storage object: Handle with Cloud object storage
            # function
            if _is_storage(path):
                with _handle_os_exceptions():
                    return cos_function(path, *args, **kwargs)

            # Local file: Redirect to standard function
            return std_function(path, *args, **kwargs)
        return decorated
    return decorate


def copy(src, dst):
    """
    Copies a source file to a destination file or directory.

    Equivalent to "shutil.copy".

    Args:
        src (path-like object): Source file.
        dst (path-like object): Destination file or directory.
    """
    # Handles path-like objects
    src = _fsdecode(src)
    dst = _fsdecode(dst)

    # Local files: Redirects to "shutil.copy"
    dst_is_storage = _is_storage(dst)
    if not _is_storage(src) and not dst_is_storage:
        return _copy(src, dst)

    # If destination si local directory, defines
    # output file
    if dst_is_storage:
        if _isdir(dst):
            dst = _join(dst, _basename(src))

    # At least one storage object: copies streams
    with open(src, 'r') as fsrc:
        with open(dst, 'w') as fdst:
            _copyfileobj(fsrc, fdst)


@_equivalent_to(_getsize)
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
    return _get_instance(cls='system', name=path).getsize(path)


@_equivalent_to(_getmtime)
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
    return _get_instance(cls='system', name=path).getmtime(path)


@_equivalent_to(_isfile)
def isfile(path):
    """
    Return True if path is an existing regular file.

    Equivalent to "os.path.isfile".

    Args:
        path (path-like object): File path or URL.

    Returns:
        bool: True if file exists.
    """
    return _get_instance(cls='system', name=path).isfile(path)


@_equivalent_to(_listdir)
def listdir(path='.'):
    """
    Return a list containing the names of the entries in
    the directory given by path

    Equivalent to "os.listdir".

    Args:
        path (path-like object): File path or URL.

    Returns:
        list of str: Directory content.
    """
    return _get_instance(cls='system', name=path).listdir(path)


@_contextmanager
def open(file, mode='r', buffering=-1, encoding=None, errors=None,
         newline=None, storage=None, storage_parameters=None, **kwargs):
    """
    Open file and return a corresponding file object.

    Equivalent to "io.open" or builtin "open".

    Args:
        file (path-like object): File path or URL.
        mode (str): mode in which the file is opened (default to 'rb').
            see "io.open" for all possible modes. Note that all modes may
            not be supported by all kind of file and storage.
        buffering (int): Set the buffering policy.
            -1 to use default behavior,
            0 to switch buffering off,
            1 to select line buffering (only usable in text mode),
            and an integer > 1 to indicate the size in bytes of a
            fixed-size chunk buffer.
            See "io.open" for more information.
        encoding (str): The name of the encoding used to
            decode or encode the file. This should only be used in text mode.
            See "io.open" for more information.
        errors (str):  Specifies how encoding and decoding errors
            are to be handled.
            This should only be used in text mode.
            See "io.open" for more information.
        newline (str): Controls how universal newlines mode works.
            This should only be used in text mode.
            See "io.open" for more information.
        storage (str): Storage name.
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        kwargs: Other arguments to pass to opened object.
            Note that theses arguments may not be compatible with
            all kind of file and storage.

    Returns:
        file-like object: opened file.

    Raises:
        OSError: If the file cannot be opened.
    """
    # Handles path-like objects
    file = _fsdecode(file)

    # Storage object
    if _is_storage(file, storage):
        with _get_instance(
                cls='raw' if buffering == 0 else 'buffered', name=file,
                storage=storage, storage_parameters=storage_parameters,
                **kwargs) as stream:

            # Text mode
            if "t" in mode:
                text_stream = _TextIOWrapper(
                    stream, encoding=encoding, errors=errors, newline=newline)
                yield text_stream
                text_stream.flush()

            # Binary mode
            else:
                yield stream

    # Local file: Redirect to "io.open"
    else:
        with _open(file, mode=mode, buffering=buffering, encoding=encoding,
                   errors=errors, newline=newline, **kwargs) as stream:
            yield stream


@_equivalent_to(_relpath)
def relpath(path, start=None):
    """
    Return a relative filepath to path either from the
    current directory or from an optional start directory.

    For storage objects, "path" and "start" are relative to
    storage root.

    Equivalent to "os.path.relpath".

    Args:
        path (path-like object): File path or URL.
        start(path-like object): Relative from this optional directory.
            Default to "os.curdir" for local files.

    Returns:
        str: Relative path.
    """
    relative = _get_instance(cls='system', name=path).relpath(path)
    if start:
        relative = _relpath(relative, start=start).replace('\\', '/')
    return relative
