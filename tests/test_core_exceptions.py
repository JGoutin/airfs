# coding=utf-8
"""Test pycosio._core.exceptions"""

import pytest


def test_handle_os_exceptions():
    """Tests pycosio._core.exceptions.handle_os_exceptions"""
    from pycosio._core.exceptions import (
        handle_os_exceptions, ObjectNotFoundError, ObjectPermissionError)

    with pytest.raises(FileNotFoundError):
        with handle_os_exceptions():
            raise ObjectNotFoundError('error')

    with pytest.raises(PermissionError):
        with handle_os_exceptions():
            raise ObjectPermissionError('error')
