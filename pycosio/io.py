# coding=utf-8
"""Cloud storage abstract IO classes

Theses abstract classes are used as base to implement storage specific IO
classes"""

# Add abstract classes to public interface.
from pycosio._core.io_base_raw import ObjectRawIOBase
from pycosio._core.io_base_buffered import ObjectBufferedIOBase
from pycosio._core.io_base_system import SystemBase

# Add advanced abstract classes to public interface
from pycosio._core.io_random_write import (
    ObjectRawIORandomWriteBase, ObjectBufferedIORandomWriteBase)
from pycosio._core.io_file_system import FileSystemBase

__all__ = ['ObjectRawIOBase', 'ObjectBufferedIOBase', 'SystemBase',
           'ObjectRawIORandomWriteBase', 'ObjectBufferedIORandomWriteBase',
           'FileSystemBase']

# Makes cleaner namespace
for _name in __all__:
    locals()[_name].__module__ = __name__
del _name
