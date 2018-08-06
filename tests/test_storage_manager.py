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


def test_register():
    """Tests pycosio._core.storage_manager.register and get_instance"""
    from pycosio._core.storage_manager import register, STORAGE, get_instance
    from pycosio.storage.http import (
        HTTPRawIO, _HTTPSystem, HTTPBufferedIO)
    import requests

    # Get HTTP as storage to register
    prefixes = _HTTPSystem().prefixes
    storage_parameters = {'arg1': 1, 'arg2': 2}
    expected_info = dict(
        raw=HTTPRawIO, system=_HTTPSystem,
        buffered=HTTPBufferedIO,
        storage_parameters=storage_parameters)
    https = 'https://pass'
    http = 'http://pass'

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

    # Tests register:
    try:
        for register_kwargs in (
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

            # Unregister if already registered
            for prefix in prefixes:
                try:
                    del STORAGE[prefix]
                except KeyError:
                    continue

            # Add dummy storage to registered
            STORAGE['aaaa'] = {}
            STORAGE['zzzz'] = {}

            # Register
            if register_kwargs:
                # Using register function
                register(**register_kwargs)
            else:
                # Using lazy registration
                get_instance(name=http,
                             storage_parameters=storage_parameters)

            # Check registration
            for prefix in prefixes:
                # Test infos
                for key, value in expected_info.items():
                    assert STORAGE[prefix][key] == value

                # Tests cached system
                assert isinstance(
                    STORAGE[prefix]['system_cached'], _HTTPSystem)

                # Tests get_instance cached system
                assert get_instance(
                    name=prefix) is STORAGE[prefix]['system_cached']

                # Test get_instance other classes with cached system
                raw = get_instance(name=https, cls='raw')
                assert isinstance(raw, HTTPRawIO)
                assert raw._system is STORAGE[prefix]['system_cached']

                buffered = get_instance(name=http, cls='buffered')
                assert isinstance(buffered, HTTPBufferedIO)
                assert buffered._system is STORAGE[prefix]['system_cached']

            # Test register order
            assert tuple(STORAGE) == tuple(reversed(sorted(STORAGE)))

            # Cleanup
            del STORAGE['aaaa']
            del STORAGE['zzzz']
            for prefix in prefixes:
                del STORAGE[prefix]

    # Restore mocked functions
    finally:
        requests.Session = requests_session
