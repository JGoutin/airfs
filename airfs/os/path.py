# type: ignore
"""Standard library "os.path" equivalents"""

from os.path import *  # noqa
from airfs._core.functions_os_path import (  # noqa
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
