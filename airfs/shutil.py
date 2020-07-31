# type: ignore
"""Standard library "shutil" equivalents"""

from shutil import *  # noqa
from airfs._core.functions_shutil import copy, copyfile  # noqa
from airfs._core.compat import COPY_BUFSIZE  # noqa
