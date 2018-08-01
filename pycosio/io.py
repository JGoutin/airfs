# coding=utf-8
"""Cloud storage abstract IO classes"""

# Add abstract classes to public interface.
__ALL__ = ['ObjectRawIOBase', 'ObjectBufferedIOBase']

from pycosio._core.io_raw import ObjectRawIOBase
from pycosio._core.io_buffered import ObjectBufferedIOBase
