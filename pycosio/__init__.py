# coding=utf-8
"""Python Cloud Object Storage I/O"""

# TODO: OSError for function depending on seekable, writable, ...
# TODO: Check full exception, and IO interface behavior
# TODO: Text IO wrapper
# TODO: Auto sub class selection (open)

__version__ = '1.0.0a1'

# Generic functions to implement


def open():
    """"""


def copy():
    """"""


def getsize(path):
    """
    Return the size, in bytes, of path.

    Returns:
        int: Size in bytes.

    Raises:
         OSError: if the file does not exist or is inaccessible.
    """


def getmtime(path):
    """
    Return the time of last access of path.

    Returns:
        float: The number of seconds since the epoch
            (see the time module).

    Raises:
         OSError: if the file does not exist or is inaccessible.
    """
