"""Standard library "shutil" equivalents"""

from shutil import *  # noqa
from airfs._core.functions_shutil import copy, copyfile  # noqa

import shutil as _src_module

__all__ = _src_module.__all__
del _src_module
