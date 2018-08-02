# coding=utf-8
"""Test pycosio._core.io_system"""
import time
from wsgiref.handlers import format_date_time

import pytest

from tests.utilities import SIZE, check_head_methods


def test_system_base():
    """Tests pycosio._core.io_system.SystemBase"""
    from pycosio._core.io_system import SystemBase

    # Mocks subclass
    m_time = time.time()
    client_kwargs = {'arg1': 1, 'arg2': 2}
    client = 'client'
    prefixes = 'prefix://', '://',
    storage_parameters = {'arg3': 3, 'arg4': 4}

    class DummySystem(SystemBase):
        """Dummy System"""

        def client_kwargs(self, path):
            """Checks arguments and returns fake result"""
            assert path
            return client_kwargs

        def _get_client(self):
            """Returns fake result"""
            return client

        def _get_prefixes(self):
            """Returns fake result"""
            return prefixes

        @staticmethod
        def head(**kwargs):
            """Checks arguments and returns fake result"""
            assert client_kwargs == kwargs
            return {'Content-Length': str(SIZE),
                    'Last-Modified': format_date_time(m_time)}

    system = DummySystem(storage_parameters=storage_parameters)

    # Tests basic methods
    assert client == system.get_client()
    client = 'client2'
    assert client != system.get_client()

    assert prefixes == system.prefixes
    prefixes = 'prefixes',
    assert client != system.prefixes

    assert storage_parameters == system.get_storage_parameters()

    # Tests head
    check_head_methods(system, m_time)

    # Tests listdir
    with pytest.raises(OSError):
        system.listdir()

    # Tests relpath
    assert system.relpath('scheme://path') == 'path'
    assert system.relpath('path') == 'path'
