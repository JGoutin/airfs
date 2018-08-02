# coding=utf-8
"""Cloud storage abstract IO classes"""

# Add abstract classes to public interface.
__ALL__ = ['ObjectRawIOBase', 'ObjectBufferedIOBase', 'SystemBase']

from pycosio._core.io_raw import ObjectRawIOBase
from pycosio._core.io_buffered import ObjectBufferedIOBase
from pycosio._core.io_system import SystemBase
