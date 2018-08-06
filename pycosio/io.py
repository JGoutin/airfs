# coding=utf-8
"""Cloud storage abstract IO classes

Theses abstract classes are used as base to implement storage specific IO
classes"""

# Add abstract classes to public interface.
from pycosio._core.io_raw import ObjectRawIOBase
from pycosio._core.io_buffered import ObjectBufferedIOBase
from pycosio._core.io_system import SystemBase

__all__ = ['ObjectRawIOBase', 'ObjectBufferedIOBase', 'SystemBase']

# Makes cleaner namespace
for _name in __all__:
    locals()[_name].__module__ = __name__
del _name
