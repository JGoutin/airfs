"""Test airfs._core.storage_manager."""
import re

import pytest


def test_mount():
    """Tests airfs._core.storage_manager.mount and get_instance."""
    from airfs._core.storage_manager import mount, MOUNTED, get_instance, _root_sort_key
    import airfs.storage.http
    from airfs.storage.http import HTTPRawIO, _HTTPSystem, HTTPBufferedIO
    from airfs import MountException
    import requests

    # Get HTTP as storage to mount
    roots = _HTTPSystem().roots
    storage_parameters = {"arg1": 1, "arg2": 2}
    storage_parameters_2 = {"arg1": 1, "arg2": 3}
    expected_info = dict(
        raw=HTTPRawIO,
        system=_HTTPSystem,
        buffered=HTTPBufferedIO,
        system_parameters={"storage_parameters": storage_parameters},
    )
    https = "https://path"
    http = "http://path"

    # Mock requests
    class Response:
        """Fake response."""

        status_code = 200
        headers = {"Accept-Ranges": "bytes", "Content-Length": "100"}

    class Session:
        """Fake Session."""

        def __init__(self, *_, **__):
            """Do nothing."""

        @staticmethod
        def request(*_, **__):
            """Returns fake result."""
            return Response()

    requests_session = requests.Session
    airfs.storage.http._Session = Session

    # Tests mount:
    try:
        for mount_kwargs in (
            # With storage
            dict(storage="http", storage_parameters=storage_parameters),
            # With name
            dict(name=http, storage_parameters=storage_parameters),
            # With both
            dict(storage="http", name=http, storage_parameters=storage_parameters),
            # Lazy registration
            None,
        ):
            # Unmount if already mounted
            for root in roots:
                try:
                    del MOUNTED[root]
                except KeyError:
                    continue

            # Add dummy storage to mounted
            MOUNTED["aaaa"] = {}
            MOUNTED["zzzz"] = {}
            MOUNTED[re.compile("bbbbbb")] = {}

            # mount
            if mount_kwargs:
                # Using mount function
                mount(**mount_kwargs)
            else:
                # Using lazy registration
                get_instance(name=http, storage_parameters=storage_parameters)

            # Check registration
            for root in roots:
                # Test infos
                for key, value in expected_info.items():
                    assert MOUNTED[root][key] == value

                # Tests cached system
                assert isinstance(MOUNTED[root]["system_cached"], _HTTPSystem)

                # Tests get_instance cached system
                assert get_instance(name=root) is MOUNTED[root]["system_cached"]

                assert (
                    get_instance(storage_parameters=storage_parameters, name=root)
                    is MOUNTED[root]["system_cached"]
                )

                assert (
                    get_instance(storage_parameters=storage_parameters_2, name=root)
                    is not MOUNTED[root]["system_cached"]
                )

                # Test get_instance other classes with cached system
                raw = get_instance(name=https, cls="raw")
                assert isinstance(raw, HTTPRawIO)
                assert raw._system is MOUNTED[root]["system_cached"]

                buffered = get_instance(name=http, cls="buffered")
                assert isinstance(buffered, HTTPBufferedIO)
                assert buffered._raw._system is MOUNTED[root]["system_cached"]

                buffered = get_instance(
                    name=http, cls="buffered", storage_parameters=storage_parameters_2
                )
                assert isinstance(buffered, HTTPBufferedIO)
                assert buffered._raw._system is not MOUNTED[root]["system_cached"]

            # Test mount order
            assert tuple(MOUNTED) == tuple(
                reversed(sorted(MOUNTED, key=_root_sort_key))
            )

            # Cleanup
            del MOUNTED["aaaa"]
            del MOUNTED["zzzz"]
            for root in roots:
                del MOUNTED[root]

        # Tests extra root
        extra = "extra_http://"
        mount(storage="http", extra_root=extra, storage_parameters=storage_parameters),
        assert MOUNTED[extra] == MOUNTED[roots[0]]

        for root in roots:
            del MOUNTED[root]
        del MOUNTED[extra]

        # Tests not as arguments to define storage
        with pytest.raises(MountException):
            mount(name="path")

    # Restore mocked functions
    finally:
        airfs.storage.http._Session = requests_session


def test_find_storage():
    """Test storage name inferance from url."""
    from re import compile
    from uuid import uuid4
    from airfs._core.storage_manager import _find_storage as find_storage
    import airfs._core.storage_manager as storage_manager

    storage_manager_automount = storage_manager.AUTOMOUNT
    domain = uuid4()
    storage_manager.AUTOMOUNT = dict(to_mount=(compile(r"https?://%s\.com" % domain),))

    try:
        # Storage as scheme
        assert find_storage("storage://dir/file") == "storage"

        # Not yet mounted known storage root starting by the HTTP scheme
        assert find_storage(f"http://{domain}.com/dir/file") == "to_mount"
        assert find_storage(f"https://{domain}.com/dir/file") == "to_mount"

        # Fall back on HTTP on any unknown URL storage
        assert find_storage(f"http://{uuid4()}.com/dir/file") == "http"
        assert find_storage(f"https://{uuid4()}.com/dir/file") == "http"

    finally:
        storage_manager.AUTOMOUNT = storage_manager_automount


def test_import_storage_errors():
    """Test errors on storage import."""
    from airfs import MountException
    from airfs._core.storage_manager import _import_storage_module
    from tests_storage_package import init_test_storage

    init_test_storage()

    # Not existing storage
    with pytest.raises(MountException):
        _import_storage_module("not_exists")

    # Missing dependency
    with pytest.raises(ImportError):
        _import_storage_module("storage_with_error")
