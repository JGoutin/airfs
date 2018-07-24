# coding=utf-8
"""Python old versions compatibility"""

import concurrent.futures as _futures
from sys import version_info

# Python 2 compatibility
if version_info[0] == 2:

    # Missing .timestamp() method of "datetime.datetime"
    import time as _time

    def to_timestamp(dt):
        """Return POSIX timestamp as float"""
        return _time.mktime(dt.timetuple()) + dt.microsecond / 1e6

else:
    # Current Python
    def to_timestamp(dt):
        """Return POSIX timestamp as float"""
        return dt.timestamp()


# Python 3.4 compatibility
if version_info[0] == 3 and version_info[1] == 4:

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
