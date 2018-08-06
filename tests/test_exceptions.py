# coding=utf-8
"""Test pycosio._core.exceptions"""

import pytest


def test_handle_os_exceptions():
    """Tests pycosio._core.exceptions.handle_os_exceptions"""
    from pycosio._core.exceptions import (
        handle_os_exceptions, ObjectNotFoundError, ObjectPermissionError)
    from pycosio._core.compat import (
        file_not_found_error, permission_error)

    with pytest.raises(file_not_found_error):
        with handle_os_exceptions():
            raise ObjectNotFoundError('error')

    with pytest.raises(permission_error):
        with handle_os_exceptions():
            raise ObjectPermissionError('error')
