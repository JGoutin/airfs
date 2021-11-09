"""Python old versions compatibility"""
import re as _re
import os as _os
import shutil as _shutil
from sys import version_info as _py

__all__ = [
    "getgid",
    "getuid",
    "contents",
    "copytree",
    "COPY_BUFSIZE",
    "Pattern",
]

if _py[0] < 3 or (_py[0] == 3 and _py[1] < 6):  # pragma: no cover
    raise ImportError("airfs require Python 3.6 or more.")


def _deprecation_warning():  # pragma: no cover
    """
    Warn user about deprecation of this Python version in next airfs version.
    """
    import warnings

    warnings.warn(
        f"Next airfs version will not support Python {_py[0]}.{_py[1]}.",
        DeprecationWarning,
        stacklevel=2,
    )


# Python 3.6 deprecation warning
if _py[0] == 3 and _py[1] == 6:  # pragma: no cover
    _deprecation_warning()


# Python < 3.7 compatibility
if _py[0] == 3 and _py[1] < 7:

    # Missing "re.Pattern"
    Pattern = type(_re.compile(""))

    # Missing importlib.resources
    from importlib_resources import contents  # type: ignore

else:
    Pattern = _re.Pattern
    from importlib.resources import contents

# Python < 3.8 compatibility
if _py[0] == 3 and _py[1] < 8:

    # "shutil.COPY_BUFSIZE" backport
    COPY_BUFSIZE = 1024 * 1024 if _os.name == "nt" else 64 * 1024

    # Missing "dirs_exist_ok" in "shutil.copytree" function
    def copytree(
        src,
        dst,
        symlinks=False,
        ignore=None,
        copy_function=_shutil.copy2,
        ignore_dangling_symlinks=False,
        dirs_exist_ok=False,
    ):
        """
        Recursively copy an entire directory tree rooted at src to a directory named
        dst and return the destination directory

        Args:
            src (str): Source directory.
            dst (str): Destination directory.
            symlinks (bool): Copy symbolic links as symbolic links.
            ignore (callable): Function used to filter files to copy.
            copy_function (callable): Copy function to use to copy files.
            ignore_dangling_symlinks (bool): Ignore symbolic links that point nowhere.
            dirs_exist_ok (bool): Ignored.
        """
        if dirs_exist_ok is not False:
            raise NotImplementedError('"dirs_exist_ok" not supported on Python < 3.8')
        return _shutil.copytree(
            src,
            dst,
            symlinks=symlinks,
            ignore=ignore,
            copy_function=copy_function,
            ignore_dangling_symlinks=ignore_dangling_symlinks,
        )


else:
    COPY_BUFSIZE = _shutil.COPY_BUFSIZE  # type: ignore
    copytree = _shutil.copytree


# Python < 3.10 compatibility
if _py[0] == 3 and _py[1] < 10:

    # Missing "strict" in "os.path.realpath" function
    def realpath(path, *, strict=False):
        """
        Return the canonical path of the specified filename, eliminating any symbolic
        links encountered in the path.

        Args:
            path (path-like object): Path.
            strict (bool): If a path doesn’t exist or a symlink loop is encountered,
                and strict is True, OSError is raised. If strict is False,
                the path is resolved as far as possible and any remainder is appended
                without checking whether it exists.

        Returns:
            str: Absolute path.
        """
        if strict is not False:
            raise NotImplementedError('"strict" not supported on Python < 3.10')
        return _os.path.realpath(path)


else:
    realpath = _os.path.realpath

# Windows compatibility

try:
    from os import getgid, getuid
except ImportError:

    def getuid():  # type: ignore
        """
        Get user or group ID.

        Returns:
            int: ID
        """
        return 0

    getgid = getuid
