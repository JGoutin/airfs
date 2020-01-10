# coding=utf-8
"""Test airfs._core.storage_manager"""
import re

import pytest


def test_mount():
    """Tests airfs._core.storage_manager.mount and get_instance"""
    from airfs._core.storage_manager import (
        mount, MOUNTED, get_instance, _compare_root)
    from airfs.storage.http import (
        HTTPRawIO, _HTTPSystem, HTTPBufferedIO)
    import requests

    # Get HTTP as storage to mount
    roots = _HTTPSystem().roots
    storage_parameters = {'arg1': 1, 'arg2': 2}
    storage_parameters_2 = {'arg1': 1, 'arg2': 3}
    expected_info = dict(
        raw=HTTPRawIO, system=_HTTPSystem,
        buffered=HTTPBufferedIO,
        system_parameters={'storage_parameters': storage_parameters})
    https = 'https://path'
    http = 'http://path'

    # Mock requests
    class Response:
        """Fake response"""
        status_code = 200
        headers = {'Accept-Ranges': 'bytes',
                   'Content-Length': '100'}

    class Session:
        """Fake Session"""

        def __init__(self, *_, **__):
            """Do nothing"""

        @staticmethod
        def request(*_, **__):
            """Returns fake result"""
            return Response()

    requests_session = requests.Session
    requests.Session = Session

    # Tests mount:
    try:
        for mount_kwargs in (
                # With storage
                dict(storage='http',
                     storage_parameters=storage_parameters),
                # With name
                dict(name=http,
                     storage_parameters=storage_parameters),
                # With both
                dict(storage='http', name=http,
                     storage_parameters=storage_parameters),
                # Lazy registration
                None):

            # Unmount if already mounted
            for root in roots:
                try:
                    del MOUNTED[root]
                except KeyError:
                    continue

            # Add dummy storage to mounted
            MOUNTED['aaaa'] = {}
            MOUNTED['zzzz'] = {}
            MOUNTED[re.compile('bbbbbb')] = {}

            # mount
            if mount_kwargs:
                # Using mount function
                mount(**mount_kwargs)
            else:
                # Using lazy registration
                get_instance(name=http,
                             storage_parameters=storage_parameters)

            # Check registration
            for root in roots:
                # Test infos
                for key, value in expected_info.items():
                    assert MOUNTED[root][key] == value

                # Tests cached system
                assert isinstance(
                    MOUNTED[root]['system_cached'], _HTTPSystem)

                # Tests get_instance cached system
                assert get_instance(
                    name=root) is MOUNTED[root]['system_cached']

                assert get_instance(
                    storage_parameters=storage_parameters,
                    name=root) is MOUNTED[root]['system_cached']

                assert get_instance(
                    storage_parameters=storage_parameters_2,
                    name=root) is not MOUNTED[root]['system_cached']

                # Test get_instance other classes with cached system
                raw = get_instance(name=https, cls='raw')
                assert isinstance(raw, HTTPRawIO)
                assert raw._system is MOUNTED[root]['system_cached']

                buffered = get_instance(name=http, cls='buffered')
                assert isinstance(buffered, HTTPBufferedIO)
                assert buffered._raw._system is MOUNTED[root]['system_cached']

                buffered = get_instance(
                    name=http, cls='buffered',
                    storage_parameters=storage_parameters_2)
                assert isinstance(buffered, HTTPBufferedIO)
                assert (buffered._raw._system is not
                        MOUNTED[root]['system_cached'])

            # Test mount order
            assert tuple(MOUNTED) == tuple(reversed(sorted(
                MOUNTED, key=_compare_root)))

            # Cleanup
            del MOUNTED['aaaa']
            del MOUNTED['zzzz']
            for root in roots:
                del MOUNTED[root]

        # Tests extra root
        extra = 'extra_http://'
        mount(storage='http', extra_root=extra,
              storage_parameters=storage_parameters),
        assert MOUNTED[extra] == MOUNTED[roots[0]]

        for root in roots:
            del MOUNTED[root]
        del MOUNTED[extra]

        # Tests not as arguments to define storage
        with pytest.raises(ValueError):
            mount(name='path')

    # Restore mocked functions
    finally:
        requests.Session = requests_session
