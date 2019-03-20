# coding=utf-8
"""Python old versions compatibility"""
import abc as _abc
import concurrent.futures as _futures
import re as _re
import os as _os
import shutil as _shutil
from sys import version_info as _py


def _deprecation_warning():
    """
    Warn user about deprecation of this Python version in next Pycosio
    version.
    """
    import warnings
    warnings.warn(
        "Next Pycosio version will not support Python %d.%d." % (
            _py[0], _py[1]), DeprecationWarning, stacklevel=2)


# Python 2 compatibility
if _py[0] == 2:

    # Missing .timestamp() method of "datetime.datetime"
    import time as _time

    def to_timestamp(dt):
        """Return POSIX timestamp as float"""
        return _time.mktime(dt.timetuple()) + dt.microsecond / 1e6

    # Missing "os.fsdecode" / "os.fsencode"
    def fsdecode(filename):
        """Return filename unchanged"""
        return filename

    def fsencode(filename):
        """Return filename unchanged"""
        return filename

    # Mission "exists_ok" in "os.makedirs"
    def makedirs(name, mode=0o777, exist_ok=False):
        """
        Super-mkdir; create a leaf directory and all intermediate ones.
        Works like mkdir, except that any intermediate path segment
        (not just the rightmost) will be created if it does not exist.

        Args:
            name (str): Path
            mode (int): The mode parameter is passed to os.mkdir();
                see the os.mkdir() description for how it is interpreted.
            exist_ok (bool): Don't raises error if target directory already
                exists.

        Raises:
            OSError: if exist_ok is False and if the target directory already
                exists.
        """
        try:
            _os.makedirs(name, mode)
        except OSError:
            if not exist_ok or not _os.path.isdir(name):
                raise

    # Missing "follow_symlinks" in "copyfile"

    def _check_follow_symlinks(follow_symlinks):
        """Checks follow_symlinks value

        Args:
            follow_symlinks: Must be True.
        """
        if follow_symlinks is not True:
            raise TypeError('"follow_symlinks" not supported on Python 2')

    def copyfile(src, dst, follow_symlinks=True):
        """
        Copies a source file to a destination file.

        Args:
            src (str): Source file.
            dst (str): Destination file.
            follow_symlinks (bool): Ignored.
        """
        _check_follow_symlinks(follow_symlinks)
        _shutil.copyfile(src, dst)

    # Missing "dir_fd" in "os" functions

    def _check_dir_fd(dir_fd):
        """Checks dir_fd value

        Args:
            dir_fd: Must be None.
        """
        if dir_fd is not None:
            raise TypeError('"dir_fd" not supported on Python 2')

    def mkdir(path, mode=0o777, dir_fd=None):
        """
        Create a directory named path.

        Args:
            path (str): Path.
            mode (int): Mode.
            dir_fd: Ignored.
        """
        _check_dir_fd(dir_fd)
        _os.mkdir(path, mode)

    def remove(path, dir_fd=None):
        """
        Remove a file.

        Args:
            path (str): Path.
            dir_fd: Ignored.
        """
        _check_dir_fd(dir_fd)
        _os.remove(path)

    def rmdir(path, dir_fd=None):
        """
        Remove a directory.

        Args:
            path (str): Path.
            dir_fd: Ignored.
        """
        _check_dir_fd(dir_fd)
        _os.rmdir(path)

    def stat(path, dir_fd=None, follow_symlinks=True):
        """
        Get the status of a file.

        Args:
            path (str): Path.
            dir_fd: Ignored.
            follow_symlinks: Ignored
        """
        _check_dir_fd(dir_fd)
        _check_follow_symlinks(follow_symlinks)
        return _os.stat(path)

    def lstat(path, dir_fd=None):
        """
        Get the status of a file.

        Args:
            path (str): Path.
            dir_fd: Ignored.
        """
        _check_dir_fd(dir_fd)
        return _os.lstat(path)

    # Missing "abc.ABC"
    ABC = _abc.ABCMeta('ABC', (object,), {})

    # Missing exceptions
    file_not_found_error = OSError
    permission_error = OSError
    file_exits_error = OSError
    same_file_error = OSError
    is_a_directory_error = OSError

else:
    def to_timestamp(dt):
        """Return POSIX timestamp as float"""
        return dt.timestamp()

    fsdecode = _os.fsdecode
    fsencode = _os.fsencode
    makedirs = _os.makedirs
    mkdir = _os.mkdir
    remove = _os.remove
    rmdir = _os.rmdir
    stat = _os.stat
    lstat = _os.lstat
    copyfile = _shutil.copyfile
    ABC = _abc.ABC
    file_not_found_error = FileNotFoundError
    permission_error = PermissionError
    file_exits_error = FileExistsError
    same_file_error = _shutil.SameFileError
    is_a_directory_error = IsADirectoryError


# Python 2 Windows compatibility
try:
    from os.path import samefile
except ImportError:

    # Missing "os.path.samefile"

    def samefile(*_, **__):
        """Checks if same files."""
        raise NotImplementedError(
            '"os.path.samefile" not available on Windows with Python 2.')

# Python 3.4 compatibility
if _py[0] == 3 and _py[1] == 4:

    _deprecation_warning()

    # "max_workers" as keyword argument for ThreadPoolExecutor
    class ThreadPoolExecutor(_futures.ThreadPoolExecutor):
        """concurrent.futures.ThreadPoolExecutor"""
        def __init__(self, max_workers=None, **kwargs):
            """Initializes a new ThreadPoolExecutor instance.

            Args:
                max_workers: The maximum number of threads that can be used to
                    execute the given calls.
            """
            if max_workers is None:
                # Use this number because ThreadPoolExecutor is often
                # used to overlap I/O instead of CPU work.
                max_workers = (_os.cpu_count() or 1) * 5
            _futures.ThreadPoolExecutor.__init__(self, max_workers, **kwargs)

else:
    ThreadPoolExecutor = _futures.ThreadPoolExecutor

# Python < 3.5 compatibility
if _py[0] < 3 or (_py[0] == 3 and _py[1] < 5):

    # Missing "os.scandir"
    from scandir import scandir, walk

else:
    from os import scandir, walk

# Python < 3.6 compatibility
if _py[0] < 3 or (_py[0] == 3 and _py[1] < 6):

    # Missing "os.fspath"
    def fspath(filename):
        """Return filename unchanged"""
        return filename

else:
    fspath = _os.fspath

# Python < 3.7 compatibility
if _py[0] < 3 or (_py[0] == 3 and _py[1] < 7):

    # Missing "re.Pattern"
    Pattern = type(_re.compile(''))

else:
    Pattern = _re.Pattern

# Python < 3.8 compatibility
if _py[0] < 3 or (_py[0] == 3 and _py[1] < 8):

    # "shutil.COPY_BUFSIZE" backport
    COPY_BUFSIZE = 1024 * 1024 if _os.name == 'nt' else 64 * 1024

    # Missing "dirs_exist_ok" in "shutil.copytree" function
    def copytree(
            src, dst, symlinks=False, ignore=None, copy_function=_shutil.copy2,
            ignore_dangling_symlinks=False, dirs_exist_ok=False):
        """
        Recursively copy an entire directory tree rooted at src to a directory
        named dst and return the destination directory

        Args:
            src (str): Source directory.
            dst (str): Destination directory.
            symlinks (bool): Copy symbolic links as symbolic links.
            ignore (callable): Function used to filter files to copy.
            copy_function (callable): Copy function to use to copy files.
            ignore_dangling_symlinks (bool): Ignore symbolic links that point
                nowhere.
            dirs_exist_ok (bool): Ignored.
        """
        if dirs_exist_ok is not False:
            raise TypeError('"dirs_exist_ok" not supported on Python < 3.8')
        return _shutil.copytree(
            src, dst, symlinks=symlinks, ignore=ignore,
            copy_function=copy_function,
            ignore_dangling_symlinks=ignore_dangling_symlinks)

else:
    COPY_BUFSIZE = _shutil.COPY_BUFSIZE
    copytree = _shutil.copytree
