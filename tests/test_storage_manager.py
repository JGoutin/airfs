# coding=utf-8
"""Test pycosio._core.storage_manager"""


def test_is_storage():
    """Tests pycosio._core.storage_manager.is_storage"""
    from pycosio._core.storage_manager import is_storage

    # Remote paths
    assert is_storage('', storage='storage')
    assert is_storage('http://path')

    # Local paths
    assert not is_storage('path')
    assert not is_storage('file://path')


def test_mount():
    """Tests pycosio._core.storage_manager.mount and get_instance"""
    from pycosio._core.storage_manager import mount, MOUNTED, get_instance
    from pycosio.storage.http import (
        HTTPRawIO, _HTTPSystem, HTTPBufferedIO)
    import requests

    # Get HTTP as storage to mount
    prefixes = _HTTPSystem().prefixes
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
            for prefix in prefixes:
                try:
                    del MOUNTED[prefix]
                except KeyError:
                    continue

            # Add dummy storage to mounted
            MOUNTED['aaaa'] = {}
            MOUNTED['zzzz'] = {}

            # mount
            if mount_kwargs:
                # Using mount function
                mount(**mount_kwargs)
            else:
                # Using lazy registration
                get_instance(name=http,
                             storage_parameters=storage_parameters)

            # Check registration
            for prefix in prefixes:
                # Test infos
                for key, value in expected_info.items():
                    assert MOUNTED[prefix][key] == value

                # Tests cached system
                assert isinstance(
                    MOUNTED[prefix]['system_cached'], _HTTPSystem)

                # Tests get_instance cached system
                assert get_instance(
                    name=prefix) is MOUNTED[prefix]['system_cached']

                assert get_instance(
                    storage_parameters=storage_parameters,
                    name=prefix) is MOUNTED[prefix]['system_cached']

                assert get_instance(
                    storage_parameters=storage_parameters_2,
                    name=prefix) is not MOUNTED[prefix]['system_cached']

                # Test get_instance other classes with cached system
                raw = get_instance(name=https, cls='raw')
                assert isinstance(raw, HTTPRawIO)
                assert raw._system is MOUNTED[prefix]['system_cached']

                buffered = get_instance(name=http, cls='buffered')
                assert isinstance(buffered, HTTPBufferedIO)
                assert buffered._raw._system is MOUNTED[prefix]['system_cached']

            # Test mount order
            assert tuple(MOUNTED) == tuple(reversed(sorted(MOUNTED)))

            # Cleanup
            del MOUNTED['aaaa']
            del MOUNTED['zzzz']
            for prefix in prefixes:
                del MOUNTED[prefix]

        # Tests extra prefix
        extra = 'extra_http://'
        mount(storage='http', extra_url_prefix=extra,
              storage_parameters=storage_parameters),
        assert MOUNTED[extra] == MOUNTED[prefixes[0]]

        for prefix in prefixes:
            del MOUNTED[prefix]
        del MOUNTED[extra]

    # Restore mocked functions
    finally:
        requests.Session = requests_session
