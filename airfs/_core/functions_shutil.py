"""Cloud object compatibles standard library 'shutil' equivalent functions"""
from io import UnsupportedOperation
from os.path import join, basename, dirname
from shutil import (
    copy as shutil_copy,
    copyfileobj,
    copyfile as shutil_copyfile,
    SameFileError,
)

from airfs._core.compat import COPY_BUFSIZE
from airfs._core.functions_io import cos_open
from airfs._core.functions_os_path import isdir
from airfs._core.functions_core import format_and_is_storage
from airfs._core.exceptions import AirfsException, handle_os_exceptions
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
        if src_is_storage and dst_is_storage:
            system_src = get_instance(src)
            system_dst = get_instance(dst)

            if system_src is system_dst:

                if system_src.relpath(src) == system_dst.relpath(dst):
                    raise SameFileError(f"'{src}' and '{dst}' are the same file")

                try:
                    return system_dst.copy(src, dst)
                except (UnsupportedOperation, AirfsException):
                    pass

            for caller, called, method in (
                (system_dst, system_src, "copy_from_%s"),
                (system_src, system_dst, "copy_to_%s"),
            ):
                if hasattr(caller, method % called.storage):
                    try:
                        return getattr(caller, method % called.storage)(
                            src, dst, called
                        )
                    except (UnsupportedOperation, AirfsException):
                        continue

        _copy_stream(dst, src)


def _copy_stream(dst, src):
    """
    Copy files by streaming content from source to destination.

    Args:
        src (str or file-like object): Source file.
        dst (str or file-like object): Destination file.
    """
    with cos_open(src, "rb") as fsrc:
        with cos_open(dst, "wb") as fdst:
            for stream in (fsrc, fdst):
                try:
                    buffer_size = getattr(stream, "_buffer_size")
                    break
                except AttributeError:
                    continue
            else:
                buffer_size = COPY_BUFSIZE
            copyfileobj(fsrc, fdst, buffer_size)


def copy(src, dst, *, follow_symlinks=True):
    """
    Copies a source file to a destination file or directory.

    Equivalent to "shutil.copy".

    Source and destination can also be binary opened file-like objects.

    .. versionadded:: 1.0.0

    Args:
        src (path-like object or file-like object): Source file.
        dst (path-like object or file-like object): Destination file or directory.
        follow_symlinks (bool): If True, follow symlinks.
            Not supported on storage objects.

    Raises:
         IOError: Destination directory not found.
    """
    src, src_is_storage = format_and_is_storage(src)
    dst, dst_is_storage = format_and_is_storage(dst)

    if not src_is_storage and not dst_is_storage:
        return shutil_copy(src, dst, follow_symlinks=follow_symlinks)

    if not hasattr(dst, "read"):
        try:
            if isdir(dst):
                dst = join(dst, basename(src))

            elif not isdir(dirname(dst)):
                raise FileNotFoundError(f"No such file or directory: '{dst}'")

        except PermissionError:
            # Unable to check target directory due to missing read access, but do not
            # raise to allow to write if possible
            pass

    _copy(src, dst, src_is_storage, dst_is_storage)


def copyfile(src, dst, *, follow_symlinks=True):
    """
    Copies a source file to a destination file.

    Equivalent to "shutil.copyfile".

    Source and destination can also be binary opened file-like objects.

    .. versionadded:: 1.2.0

    Args:
        src (path-like object or file-like object): Source file.
        dst (path-like object or file-like object): Destination file.
        follow_symlinks (bool): Follow symlinks.
            Not supported on storage objects.

    Raises:
         IOError: Destination directory not found.
    """
    src, src_is_storage = format_and_is_storage(src)
    dst, dst_is_storage = format_and_is_storage(dst)

    if not src_is_storage and not dst_is_storage:
        return shutil_copyfile(src, dst, follow_symlinks=follow_symlinks)

    try:
        if not hasattr(dst, "read") and not isdir(dirname(dst)):
            raise FileNotFoundError(f"No such file or directory: '{dst}'")

    except PermissionError:
        # Unable to check target directory due to missing read access, but do not raise
        # to allow to write if possible
        pass

    _copy(src, dst, src_is_storage, dst_is_storage)
