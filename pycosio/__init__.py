# coding=utf-8
"""Python Cloud Object Storage I/O"""

__version__ = '1.0.0'

# Adds names to public interface
# Shadowing "open" built-in name is done to provides "pycosio.open" function
from pycosio._core.functions_io import cos_open as open
from pycosio._core.functions_os_path import getmtime, getsize, isfile, relpath
from pycosio._core.functions_shutil import copy
from pycosio._core.storage_manager import mount

__all__ = list(sorted((
    # Standard library "io"
    'open',

    # Standard library "os.path"
    'getmtime', 'getsize', 'isfile', 'relpath',

    # Standard library "shutil"
    'copy',

    # Pycosio
    'mount',)))

# Makes cleaner namespace
for _name in __all__:
    locals()[_name].__module__ = __name__
locals()['open'].__qualname__ = 'open'
locals()['open'].__name__ = 'open'
del _name
