"""Python older versions compatibility."""
import os as _os
from sys import version_info as _py

__all__ = ["getgid", "getuid", "realpath"]


# Python < 3.10 compatibility
if _py[0] == 3 and _py[1] < 10:
    # Missing "strict" in "os.path.realpath" function
    def realpath(path, *, strict=False):
        """Return the canonical path of the specified filename.

        Args:
            path (path-like object): Path.
            strict (bool): If a path doesn't exist or a symlink loop is encountered,
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
        """Get user or group ID.

        Returns:
            int: ID
        """
        return 0

    getgid = getuid
