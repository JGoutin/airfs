# coding=utf-8
"""Cloud storage abstract IO classes"""

# Add abstract classes to public interface.
from pycosio._core.io_raw import ObjectRawIOBase
from pycosio._core.io_buffered import ObjectBufferedIOBase
from pycosio._core.io_system import SystemBase

__all__ = ['ObjectRawIOBase', 'ObjectBufferedIOBase', 'SystemBase']

# Makes cleaner namespace
for _name in __all__:
    try:
        locals()[_name].__module__ = __name__
    except (AttributeError, KeyError):
        continue
del _name
