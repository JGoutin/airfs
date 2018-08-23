"""Cloud object compatibles standard library 'shutil' equivalent functions"""
from os.path import join, basename, isdir
from shutil import copy as shutil_copy, copyfileobj

from pycosio._core.functions_io import cos_open
from pycosio._core.functions_core import format_and_is_storage


def copy(src, dst):
    """
    Copies a source file to a destination file or directory.

    Equivalent to "shutil.copy".

    Source and destination can also be binary opened file-like objects.

    Args:
        src (path-like object or file-like object): Source file.
        dst (path-like object or file-like object):
            Destination file or directory.
    """
    # Handles path-like objects and checks if storage
    src, src_is_storage = format_and_is_storage(src)
    dst, dst_is_storage = format_and_is_storage(dst)

    # Local files: Redirects to "shutil.copy"
    if not src_is_storage and not dst_is_storage:
        return shutil_copy(src, dst)

    # If destination si local directory, defines
    # output file
    if not dst_is_storage:
        if isdir(dst):
            dst = join(dst, basename(src))

    # At least one storage object: copies streams
    with cos_open(src, 'rb') as fsrc:
        with cos_open(dst, 'wb') as fdst:

            # Get stream buffer size
            for stream in (fdst, fsrc):
                try:
                    buffer_size = getattr(stream, '_buffer_size')
                    break
                except AttributeError:
                    continue
            else:
                buffer_size = 16384

            # Read and write
            copyfileobj(fsrc, fdst, buffer_size)
