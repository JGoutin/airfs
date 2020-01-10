# coding=utf-8
"""Python old versions compatibility"""
import re as _re
import os as _os
import shutil as _shutil
from sys import version_info as _py

# Raise import error on incompatible versions
if _py[0] < 3 or (_py[0] == 3 and _py[1] < 5):
    raise ImportError('airfs require Python 3.5 or more.')


# Warn about future incompatibles versions
def _deprecation_warning():
    """
    Warn user about deprecation of this Python version in next airfs
    version.
    """
    import warnings
    warnings.warn(
        "Next airfs version will not support Python %d.%d." % (
            _py[0], _py[1]), DeprecationWarning, stacklevel=2)


# Python < 3.6 compatibility
if _py[0] == 3 and _py[1] < 6:

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
