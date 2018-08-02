# coding=utf-8
"""Test pycosio._core.io_base"""

import pytest


def test_object_base_io():
    """Tests pycosio._core.io_base.ObjectIOBase"""
    from pycosio._core.io_base import ObjectIOBase

    name = 'name'

    # Tests mode
    object_io = ObjectIOBase(name, mode='r')
    assert object_io.name == name
    assert object_io.mode == 'r'
    assert object_io.readable()
    assert object_io.seekable()
    assert not object_io.writable()

    object_io = ObjectIOBase(name, mode='w')
    assert object_io.mode == 'w'
    assert not object_io.readable()
    assert object_io.seekable()
    assert object_io.writable()

    object_io = ObjectIOBase(name, mode='a')
    assert object_io.mode == 'a'
    assert not object_io.readable()
    assert object_io.seekable()
    assert object_io.writable()

    with pytest.raises(ValueError):
        ObjectIOBase(name, mode='z')
