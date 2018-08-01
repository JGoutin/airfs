# coding=utf-8
"""Python Cloud Object Storage I/O"""

# TODO: OSError for function depending on seekable, writable, ...
# TODO: Check full exception, and IO interface behavior
# TODO: Text IO wrapper
# TODO: Auto sub class selection (open)

__version__ = '1.0.0a1'

import pycosio.io
from pycosio._core.os_functions import (
    open, copy, getmtime, getsize, listdir)
