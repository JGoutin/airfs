# coding=utf-8
"""Python Cloud Object Storage I/O"""

__version__ = '1.0.0a1'

# Adds names to public interface
# Shadowing "open" built-in name is done to provides "pycosio.open" function
from pycosio._core.std_functions import (
    cos_open as open, copy, getmtime, getsize, isfile, listdir, relpath)
from pycosio._core.storage_manager import register

__all__ = [
    # Standard functions
    'open', 'copy', 'getmtime', 'getsize', 'listdir', 'relpath', 'isfile',

    # Utilities
    'register']

# Makes cleaner namespace
for _name in __all__:
    try:
        locals()[_name].__module__ = __name__
    except (AttributeError, KeyError):
        continue
locals()['open'].__qualname__ = 'open'
locals()['open'].__name__ = 'open'
del _name
