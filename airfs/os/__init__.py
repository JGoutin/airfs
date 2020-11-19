# type: ignore
"""Standard library "os" equivalents"""

from os import *  # noqa
from airfs.os import path  # noqa
from airfs._core.functions_os import (  # noqa
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
