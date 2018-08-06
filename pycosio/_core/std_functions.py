# coding=utf-8
"""Cloud object compatibles standard library equivalent functions"""
from contextlib import contextmanager
from functools import wraps
from io import open as io_open, TextIOWrapper
import os
from os.path import isdir, basename, join, relpath as os_path_relpath
from shutil import copy as shutil_copy, copyfileobj

from pycosio._core.compat import fsdecode
from pycosio._core.storage_manager import get_instance, is_storage
from pycosio._core.exceptions import handle_os_exceptions

# TODO: add following functions:
# - listdir
# - remove
# - scandir
# - walk


def equivalent_to(std_function):
    """
    Decorate a cloud object compatible function
    to provides fall back to standard function if
    used on local files.

    Args:
        std_function (function): standard function to
            used with local files.

    Returns:
        function: new function
    """
    def decorate(cos_function):
        """Decorator argument handler"""

        @wraps(cos_function)
        def decorated(path, *args, **kwargs):
            """Decorated function"""

            # Handles path-like objects
            path = fsdecode(path)

            # Storage object: Handle with Cloud object storage
            # function
            if is_storage(path):
                with handle_os_exceptions():
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
    src = fsdecode(src)
    dst = fsdecode(dst)

    # Local files: Redirects to "shutil.copy"
    dst_is_storage = is_storage(dst)
    if not is_storage(src) and not dst_is_storage:
        return shutil_copy(src, dst)

    # If destination si local directory, defines
    # output file
    if dst_is_storage:
        if isdir(dst):
            dst = join(dst, basename(src))

    # At least one storage object: copies streams
    with cos_open(src, 'r') as fsrc:
        with cos_open(dst, 'w') as fdst:
            copyfileobj(fsrc, fdst)


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


@equivalent_to(os.listdir)
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
    return get_instance(path).listdir(path)


@contextmanager
def cos_open(file, mode='r', buffering=-1, encoding=None, errors=None,
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
    file = fsdecode(file)

    # Storage object
    if is_storage(file, storage):
        with get_instance(
                name=file, cls='raw' if buffering == 0 else 'buffered',
                storage=storage, storage_parameters=storage_parameters,
                **kwargs) as stream:

            # Text mode
            if "t" in mode:
                text_stream = TextIOWrapper(
                    stream, encoding=encoding, errors=errors, newline=newline)
                yield text_stream
                text_stream.flush()

            # Binary mode
            else:
                yield stream

    # Local file: Redirect to "io.open"
    else:
        with io_open(file, mode=mode, buffering=buffering, encoding=encoding,
                     errors=errors, newline=newline, **kwargs) as stream:
            yield stream


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
