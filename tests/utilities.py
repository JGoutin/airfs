# coding=utf-8
"""Utilities common to all tests"""

import pytest

BYTE = b'0'
SIZE = 100


def parse_range(header):
    """Parse HTTP range

    Args:
        header (dict): header.

    Returns
        bytes: Content

    Raises:
        ValueError: EOF
    """

    data_range = (header or dict()).get('Range')
    if data_range is None:
        # Return full object
        content = BYTE * SIZE

    else:
        # Return object part
        data_range = data_range.split('=')[1]
        start, end = data_range.split('-')
        start = int(start)
        try:
            end = int(end) + 1
        except ValueError:
            end = SIZE

        if start >= SIZE:
            # EOF reached
            raise ValueError

        if end > SIZE:
            end = SIZE
        content = BYTE * (end - start)

    return content


def check_head_methods(io_object, m_time, size=SIZE):
    """
    Tests head methods.

    args:
        io_object (pycosio.io_base.ObjectIOBase subclass):
            Object to test
    """
    assert io_object.getmtime() == pytest.approx(m_time, 1)
    assert io_object.getsize() == size


def check_raw_read_methods(io_object):
    """
    Tests read methods.

    args:
        io_object (pycosio.io_base.ObjectIOBase subclass):
            Object to test
    """

    # Tests _read_all
    assert io_object.readall() == SIZE * BYTE
    assert io_object.tell() == SIZE

    assert io_object.seek(10) == 10
    assert io_object.readall() == (SIZE - 10) * BYTE
    assert io_object.tell() == SIZE

    # Tests _read_range
    assert io_object.seek(0) == 0
    buffer = bytearray(40)
    assert io_object.readinto(buffer) == 40
    assert bytes(buffer) == 40 * BYTE
    assert io_object.tell() == 40

    buffer = bytearray(40)
    assert io_object.readinto(buffer) == 40
    assert bytes(buffer) == 40 * BYTE
    assert io_object.tell() == 80

    buffer = bytearray(40)
    assert io_object.readinto(buffer) == 20
    assert bytes(buffer) == 20 * BYTE + b'\x00' * 20
    assert io_object.tell() == SIZE

    buffer = bytearray(40)
    assert io_object.readinto(buffer) == 0
    assert bytes(buffer) == b'\x00' * 40
    assert io_object.tell() == SIZE
