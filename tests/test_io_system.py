# coding=utf-8
"""Test pycosio._core.io_system"""
import time
import re
from wsgiref.handlers import format_date_time

import pytest

from tests.utilities import SIZE, check_head_methods


def test_system_base():
    """Tests pycosio._core.io_system.SystemBase"""
    from pycosio._core.io_system import SystemBase
    from pycosio._core.exceptions import ObjectNotFoundError

    # Mocks subclass
    m_time = time.time()
    dummy_client_kwargs = {'arg1': 1, 'arg2': 2}
    client = 'client'
    roots = re.compile('root2://'), 'root://', '://',
    storage_parameters = {'arg3': 3, 'arg4': 4}
    raise_not_exists_exception = False
    header = {'Content-Length': str(SIZE),
              'Last-Modified': format_date_time(m_time)}

    class DummySystem(SystemBase):
        """Dummy System"""

        def get_client_kwargs(self, path):
            """Checks arguments and returns fake result"""
            assert path
            dummy_client_kwargs['path'] = path
            return dummy_client_kwargs

        def _get_client(self):
            """Returns fake result"""
            return client

        def _get_roots(self):
            """Returns fake result"""
            return roots

        def _head(self, client_kwargs):
            """Checks arguments and returns fake result"""
            assert client_kwargs == dummy_client_kwargs
            if raise_not_exists_exception:
                raise ObjectNotFoundError
            return header

        def _make_dir(self, client_kwargs):
            """Checks arguments"""
            path = client_kwargs['path']
            assert '/' not in path or path[-1] == '/'
            assert client_kwargs == dummy_client_kwargs

    system = DummySystem(storage_parameters=storage_parameters)

    # Tests basic methods
    assert client == system.client

    assert roots == system.roots
    roots = 'roots',
    assert client != system.roots

    assert storage_parameters == system.storage_parameters

    # Tests head
    check_head_methods(system, m_time)

    # Tests relpath
    assert system.relpath('scheme://path') == 'path'
    assert system.relpath('path') == 'path'

    # Tests locator
    assert system.is_locator('scheme://locator')
    assert not system.is_locator('scheme://locator/path')

    assert system.split_locator('scheme://locator') == ('locator', '')
    assert system.split_locator('scheme://locator/path') == ('locator', 'path')

    # Tests exists, isdir, isfile
    assert system.exists('root://path')
    raise_not_exists_exception = True
    assert not system.exists('root://path')
    raise_not_exists_exception = False

    assert system.isfile('root://locator/path')
    assert not system.isfile('root://locator/path/')
    assert not system.isfile('root://')

    assert system.isdir('root://locator/path/')
    assert not system.isdir('root://locator/path')
    assert system.isdir('root://locator')
    assert system.isdir('root://')

    # Tests make dir
    system.make_dir('root://locator')
    system.make_dir('root://locator/path')
    system.make_dir('root://locator/path/')

    # Test empty header
    header = {}
    with pytest.raises(TypeError):
        system.getmtime('path')
    with pytest.raises(TypeError):
        system.getsize('path')
