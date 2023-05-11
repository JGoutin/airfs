"""Python "Remote File Systems" library."""

# Shadowing "open" built-in name is done to provide "airfs.open" function
from airfs._core.functions_io import cos_open as open  # noqa: F401
from airfs._core.functions_os import (  # noqa: F401
    listdir,
    lstat,
    makedirs,
    mkdir,
    readlink,
    remove,
    rmdir,
    scandir,
    stat,
    symlink,
    unlink,
)
from airfs._core.functions_os_path import (  # noqa: F401
    exists,
    getctime,
    getmtime,
    getsize,
    isabs,
    isdir,
    isfile,
    islink,
    ismount,
    lexists,
    realpath,
    relpath,
    samefile,
    splitdrive,
)
from airfs._core.functions_shutil import copy, copyfile  # noqa: F401
from airfs._core.functions_extra import shareable_url  # noqa: F401
from airfs._core.storage_manager import mount  # noqa: F401
from airfs._core.exceptions import (  # noqa: F401
    AirfsException,
    AirfsWarning,
    MountException,
)

__all__ = list(
    sorted(
        (
            # Standard library "io"
            "open",
            # Standard library "os"
            "listdir",
            "lstat",
            "makedirs",
            "mkdir",
            "readlink",
            "remove",
            "rmdir",
            "scandir",
            "stat",
            "symlink",
            "unlink",
            # Standard library "os.path"
            "exists",
            "getctime",
            "getmtime",
            "getsize",
            "isabs",
            "isdir",
            "isfile",
            "islink",
            "ismount",
            "lexists",
            "realpath",
            "relpath",
            "samefile",
            "splitdrive",
            # Standard library "shutil"
            "copy",
            "copyfile",
            # airfs
            "shareable_url",
            "mount",
            "AirfsException",
            "AirfsWarning",
            "MountException",
        )
    )
)

for _name in __all__:
    locals()[_name].__module__ = __name__
locals()["open"].__qualname__ = "open"
locals()["open"].__name__ = "open"
del _name
