"""Standard library "os" equivalents"""

from os import *  # noqa
from airfs.os import path  # noqa
from airfs._core.functions_os import (  # noqa
    listdir, lstat, makedirs, mkdir, remove, rmdir, scandir, stat, unlink)

import os as _src_module
__all__ = _src_module.__all__
del _src_module
