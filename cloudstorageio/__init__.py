import io as _io


def open():
    """"""


def copy():
    """"""


def getsize():
    """"""


class RawStorageIO(_io.FileIO):
    """Raw binary storage I/O"""

    def readall(self):
        """"""

    def readinto(self, b):
        """"""

    def seek(self, offset, whence=0):
        """"""

    def write(self, b):
        """"""


class BufferedStorageIO(_io.BufferedIOBase):
    """Buffered binary storage I/O"""
