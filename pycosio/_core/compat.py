# coding=utf-8
"""Python old versions compatibility"""
import abc as _abc
import concurrent.futures as _futures
import re as _re
import os as _os
from sys import version_info as _py

# Python 2 compatibility
if _py[0] == 2:

    # Missing .timestamp() method of "datetime.datetime"
    import time as _time


    def to_timestamp(dt):
        """Return POSIX timestamp as float"""
        return _time.mktime(dt.timetuple()) + dt.microsecond / 1e6


    # Missing "os.fsdecode"
    def fsdecode(filename):
        """Return filename unchanged"""
        return filename


    # Missing "abc.ABC"
    ABC = _abc.ABCMeta('ABC', (object,), {})

    # Missing exceptions
    file_not_found_error = OSError
    permission_error = OSError
    file_exits_error = OSError

else:
    def to_timestamp(dt):
        """Return POSIX timestamp as float"""
        return dt.timestamp()


    fsdecode = _os.fsdecode
    ABC = _abc.ABC
    file_not_found_error = FileNotFoundError
    permission_error = PermissionError
    file_exits_error = FileExistsError


# Python 3.4 compatibility
if _py[0] == 3 and _py[1] == 4:

    # "max_workers" as keyword argument for ThreadPoolExecutor
    class ThreadPoolExecutor(_futures.ThreadPoolExecutor):
        def __init__(self, max_workers=None, **kwargs):
            """Initializes a new ThreadPoolExecutor instance.

            Args:
                max_workers: The maximum number of threads that can be used to
                    execute the given calls.
            """
            if max_workers is None:
                # Use this number because ThreadPoolExecutor is often
                # used to overlap I/O instead of CPU work.
                max_workers = (_os.cpu_count() or 1) * 5
            _futures.ThreadPoolExecutor.__init__(self, max_workers, **kwargs)

else:
    ThreadPoolExecutor = _futures.ThreadPoolExecutor

# Python <= 3.6 compatibility
if _py[0] < 3 or (_py[0] == 3 and _py[1] <= 6):
    # Missing re.Pattern
    Pattern = type(_re.compile(''))

else:
    Pattern = _re.Pattern
