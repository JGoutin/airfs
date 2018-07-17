import io as _io
import os as _os


def open():
    """"""


def copy():
    """"""


def getsize():
    """"""


class RawStorageIO(_io.RawIOBase):
    """Raw binary storage I/O"""

    def __init__(self, name, mode='w+'):
        _io.RawIOBase.__init__(self)
        self._name = name
        self._mode = mode  # r,w,x,+
        self._seek = 0
        self._size = 0

    def flush(self):
        """"""

    @property
    def mode(self):
        """"""
        return self._mode

    @property
    def name(self):
        """"""
        return self._name

    def readall(self):
        """"""

    def readable(self):
        """"""
        return 'b' in self._mode or '+' in self._mode

    def readinto(self, b):
        """"""

    def seek(self, offset, whence=_os.SEEK_SET):
        """"""
        if whence == _os.SEEK_SET:
            self._seek = offset
        elif whence == _os.SEEK_CUR:
            self._seek += offset
        elif whence == _os.SEEK_END:
            self._seek = offset + self._size

    @staticmethod
    def seekable():
        """"""
        return True

    def tell(self):
        """"""
        return self._seek

    def writable(self):
        """"""
        return 'w' in self._mode

    def write(self, b):
        """"""

# getbuffer, getvalue


class BufferedStorageIO(_io.BufferedIOBase):
    """Buffered binary storage I/O"""
