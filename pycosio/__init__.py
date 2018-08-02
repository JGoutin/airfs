# coding=utf-8
"""Python Cloud Object Storage I/O"""

__version__ = '1.0.0a1'

import pycosio.io
from pycosio._core.std_functions import (
    open, copy, getmtime, getsize, listdir)
from pycosio._core.storage_manager import register
