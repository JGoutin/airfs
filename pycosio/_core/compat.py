# coding=utf-8
"""Python old versions compatibility"""

import concurrent.futures as _futures
import os as _os
from sys import version_info as _py

# Python 2 compatibility
if _py[0] == 2:

    # Missing .timestamp() method of "datetime.datetime"
    import time as _time

    def to_timestamp(dt):
        """Return POSIX timestamp as float"""
        return _time.mktime(dt.timetuple()) + dt.microsecond / 1e6

    def fsdecode(filename):
        """Return filename unchanged"""
        return filename

else:
    # Current Python
    def to_timestamp(dt):
        """Return POSIX timestamp as float"""
        return dt.timestamp()

    fsdecode = _os.fsdecode


# Python 3.4 compatibility
if _py[0] == 3 and _py[1] == 4:

    # "max_workers" as keyword argument for ThreadPoolExecutor
    from os import cpu_count as _cpu_count

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
                max_workers = (_cpu_count() or 1) * 5
            _futures.ThreadPoolExecutor.__init__(self, max_workers, **kwargs)

else:
    # Current Python
    ThreadPoolExecutor = _futures.ThreadPoolExecutor
