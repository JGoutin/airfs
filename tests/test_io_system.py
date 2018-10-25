# coding=utf-8
"""Test pycosio._core.io_system"""
from itertools import product
import time
import re
from wsgiref.handlers import format_date_time

import pytest

from tests.utilities import SIZE, check_head_methods


def test_system_base():
    """Tests pycosio._core.io_system.SystemBase"""
    from pycosio._core.io_system import SystemBase
    from pycosio._core.exceptions import (
        ObjectNotFoundError, ObjectPermissionError)
    from io import UnsupportedOperation
    from stat import S_ISDIR, S_ISREG

    # Mocks subclass
    m_time = time.time()
    dummy_client_kwargs = {'arg1': 1, 'arg2': 2}
    client = 'client'
    roots = re.compile('root2://'), 'root://', '://',
    storage_parameters = {'arg3': 3, 'arg4': 4}
    raise_not_exists_exception = False
    header = {'Content-Length': str(SIZE),
              'Last-Modified': format_date_time(m_time),
              'ETag': '123456'}
    locators = ('locator', 'locator_no_access')
    objects = ('dir1/object1', 'dir1/object2', 'dir1/object3',
               'dir1/dir2/object4', 'dir1/dir2/object5', 'dir1')

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

        def _list_objects(self, client_kwargs, *_, **__):
            """Checks arguments and returns fake result"""
            assert client_kwargs == dummy_client_kwargs

            path = client_kwargs['path'].strip('/')
            if path == 'locator_no_access':
                raise ObjectPermissionError
            elif path in ('locator', 'locator/dir1'):
                for obj in objects:
                    yield obj, header.copy()
            else:
                raise StopIteration

        def _list_locators(self):
            """Returns fake result"""
            for locator in locators:
                yield locator, header.copy()

        def _head(self, client_kwargs):
            """Checks arguments and returns fake result"""
            assert client_kwargs == dummy_client_kwargs
            if raise_not_exists_exception:
                raise ObjectNotFoundError
            return header.copy()

        def _make_dir(self, client_kwargs):
            """Checks arguments"""
            path = client_kwargs['path']
            assert '/' not in path or path[-1] == '/'
            assert client_kwargs == dummy_client_kwargs

        def _remove(self, client_kwargs):
            """Checks arguments"""
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

    with pytest.raises(UnsupportedOperation):
        system.getctime('path')

    header['Last-Modified'] = m_time
    assert system.getmtime('path') == m_time
    header['Last-Modified'] = format_date_time(m_time)

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

    raise_not_exists_exception = True
    assert not system.isdir('root://locator/path/')
    assert system.isdir('root://locator/dir1/')
    assert not system.isdir('root://locator/dir1/', virtual_dir=False)
    raise_not_exists_exception = False

    # Test ensure_dir_path
    assert system.ensure_dir_path(
        'root://locator/path') == 'root://locator/path/'
    assert system.ensure_dir_path(
        'root://locator/path/') == 'root://locator/path/'
    assert system.ensure_dir_path('root://locator') == 'root://locator'
    assert system.ensure_dir_path('root://locator/') == 'root://locator'
    assert system.ensure_dir_path('root://') == 'root://'

    # Tests make dir
    system.make_dir('root://locator')
    system.make_dir('root://locator/path')
    system.make_dir('root://locator/path/')
    system.make_dir('locator', relative=True)
    system.make_dir('locator/path', relative=True)
    system.make_dir('locator/path/', relative=True)

    # Test empty header
    header_old = header
    header = {}
    with pytest.raises(UnsupportedOperation):
        system.getmtime('path')
    with pytest.raises(UnsupportedOperation):
        system.getsize('path')
    header = header_old

    # Test default unsupported
    with pytest.raises(UnsupportedOperation):
        system.copy('path1', 'path2')

    # Tests remove
    system.remove('root://locator')
    system.remove('root://locator/path')
    system.remove('root://locator/path/')
    system.remove('locator', relative=True)
    system.remove('locator/path', relative=True)
    system.remove('locator/path/', relative=True)

    # Tests stat
    header['Content-Length'] = '0'
    stat_result = system.stat('root://locator/path/')
    assert S_ISDIR(stat_result.st_mode)

    header['Content-Length'] = str(SIZE)
    stat_result = system.stat('root://locator/path')
    assert S_ISREG(stat_result.st_mode)
    assert stat_result.st_ino == 0
    assert stat_result.st_dev == 0
    assert stat_result.st_nlink == 0
    assert stat_result.st_uid == 0
    assert stat_result.st_gid == 0
    assert stat_result.st_size == SIZE
    assert stat_result.st_atime == 0
    assert stat_result.st_mtime == pytest.approx(m_time, 1)
    assert stat_result.st_ctime == 0
    assert stat_result.st_etag == header['ETag']

    # Tests list_objects
    assert list(system.list_objects(path='root://', first_level=True)) == [
        (locator, header) for locator in locators]

    excepted = (
        [(locators[0], header)] +
        [('/'.join((locators[0], obj)), header) for obj in objects] +
        [(locators[1], header)])
    assert list(system.list_objects(path='root://')) == excepted

    assert list(system.list_objects(path='root://locator')) == [
        (obj, header) for obj in objects]

    assert list(system.list_objects(path='locator', relative=True)) == [
        (obj, header) for obj in objects]

    excepted = (
        [(obj, header) for obj in ('object1', 'object2', 'object3')] +
        [('dir2/', dict())])
    assert list(system.list_objects(
        path='root://locator/dir1', first_level=True)) == excepted
