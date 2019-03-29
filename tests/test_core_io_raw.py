# coding=utf-8
"""Test pycosio._core.io_raw"""


def test_object_raw_base_io_http_range():
    """Tests pycosio._core.io_raw.ObjectRawIOBase._http_range"""
    from pycosio._core.io_base_raw import ObjectRawIOBase
    assert ObjectRawIOBase._http_range(10, 50) == 'bytes=10-49'
    assert ObjectRawIOBase._http_range(10) == 'bytes=10-'
