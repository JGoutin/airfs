# coding=utf-8
"""Cloud object compatibles standard library 'shutil' equivalent functions"""
from io import UnsupportedOperation
from os.path import join, basename, dirname
from shutil import (
    copy as shutil_copy, copyfileobj, copyfile as shutil_copyfile,
    SameFileError)

from airfs._core.compat import COPY_BUFSIZE
from airfs._core.functions_io import cos_open
from airfs._core.functions_os_path import isdir
from airfs._core.functions_core import format_and_is_storage
from airfs._core.exceptions import ObjectException, handle_os_exceptions
from airfs._core.storage_manager import get_instance


def _copy(src, dst, src_is_storage, dst_is_storage):
    """
    Copies file from source to destination

    Args:
        src (str or file-like object): Source file.
        dst (str or file-like object): Destination file.
        src_is_storage (bool): Source is storage.
        dst_is_storage (bool): Destination is storage.
    """
    with handle_os_exceptions():
        # If both storage: Tries to perform same storage direct copy
        if src_is_storage and dst_is_storage:
            system_src = get_instance(src)
            system_dst = get_instance(dst)

            # Same storage copy
            if system_src is system_dst:

                # Checks if same file
                if system_src.relpath(src) == system_dst.relpath(dst):
                    raise SameFileError(
                        "'%s' and '%s' are the same file" % (src, dst))

                # Tries to copy
                try:
                    return system_dst.copy(src, dst)
                except (UnsupportedOperation, ObjectException):
                    pass

            # Copy from compatible storage using "copy_from_<src_storage>" or
            # "copy_to_<src_storage>" method if any
            for caller, called, method in (
                    (system_dst, system_src, 'copy_from_%s'),
                    (system_src, system_dst, 'copy_to_%s')):
                if hasattr(caller, method % called.storage):
                    try:
                        return getattr(caller, method % called.storage)(
                            src, dst, called)
                    except (UnsupportedOperation, ObjectException):
                        continue

        # At least one storage object: copies streams
        with cos_open(src, 'rb') as fsrc:
            with cos_open(dst, 'wb') as fdst:

                # Get stream buffer size
                for stream in (fsrc, fdst):
                    try:
                        buffer_size = getattr(stream, '_buffer_size')
                        break
                    except AttributeError:
                        continue
                else:
                    buffer_size = COPY_BUFSIZE

                # Read and write
                copyfileobj(fsrc, fdst, buffer_size)


def copy(src, dst):
    """
    Copies a source file to a destination file or directory.

    Equivalent to "shutil.copy".

    Source and destination can also be binary opened file-like objects.

    Args:
        src (path-like object or file-like object): Source file.
        dst (path-like object or file-like object):
            Destination file or directory.

    Raises:
         IOError: Destination directory not found.
    """
    # Handles path-like objects and checks if storage
    src, src_is_storage = format_and_is_storage(src)
    dst, dst_is_storage = format_and_is_storage(dst)

    # Local files: Redirects to "shutil.copy"
    if not src_is_storage and not dst_is_storage:
        return shutil_copy(src, dst)

    # Checks destination
    if not hasattr(dst, 'read'):
        try:
            # If destination is directory: defines an output file inside it
            if isdir(dst):
                dst = join(dst, basename(src))

            # Checks if destination dir exists
            elif not isdir(dirname(dst)):
                raise FileNotFoundError(
                    "No such file or directory: '%s'" % dst)

        except PermissionError:
            # Unable to check target directory due to missing read access,
            # but do not raise to allow to write if possible
            print('PermissionError reached')
            pass

    # Performs copy
    _copy(src, dst, src_is_storage, dst_is_storage)


def copyfile(src, dst, follow_symlinks=True):
    """
    Copies a source file to a destination file.

    Equivalent to "shutil.copyfile".

    Source and destination can also be binary opened file-like objects.

    Args:
        src (path-like object or file-like object): Source file.
        dst (path-like object or file-like object): Destination file.
        follow_symlinks (bool): Follow symlinks.
            Not supported on cloud storage objects.

    Raises:
         IOError: Destination directory not found.
    """
    # Handles path-like objects and checks if storage
    src, src_is_storage = format_and_is_storage(src)
    dst, dst_is_storage = format_and_is_storage(dst)

    # Local files: Redirects to "shutil.copyfile"
    if not src_is_storage and not dst_is_storage:
        return shutil_copyfile(src, dst, follow_symlinks=follow_symlinks)

    # Checks destination
    try:
        if not hasattr(dst, 'read') and not isdir(dirname(dst)):
            raise FileNotFoundError(
                "No such file or directory: '%s'" % dst)

    except PermissionError:
        # Unable to check target directory due to missing read access, but
        # do not raise to allow to write if possible
        pass

    # Performs copy
    _copy(src, dst, src_is_storage, dst_is_storage)
