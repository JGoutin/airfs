"""Test airfs._core.io_system."""
import time
import re
from wsgiref.handlers import format_date_time

import pytest


def test_system_base():
    """Tests airfs._core.io_system.SystemBase."""
    from airfs._core.io_base_system import SystemBase
    from airfs._core.exceptions import (
        ObjectNotFoundError,
        ObjectPermissionError,
        ObjectUnsupportedOperation,
    )
    from airfs._core.compat import getuid, getgid
    from stat import S_ISDIR, S_ISREG, S_ISLNK

    # Mocks subclass
    size = 100
    m_time = time.time()
    dummy_client_kwargs = {"arg1": 1, "arg2": 2}
    client = "client"
    roots = (
        re.compile("root2://"),
        "root://",
        "://",
    )
    storage_parameters = {"arg3": 3, "arg4": 4}
    raise_not_exists_exception = False
    object_header = {
        "Content-Length": str(size),
        "Last-Modified": format_date_time(m_time),
        "ETag": "123456",
    }
    locators = ("locator", "locator_no_access")
    objects = (
        "dir1/object1",
        "dir1/object2",
        "dir1/object3",
        "dir1/dir2/object4",
        "dir1/dir2/object5",
        "dir1",
    )

    class DummySystem(SystemBase):
        """Dummy System."""

        def get_client_kwargs(self, path):
            """Checks arguments and returns fake result."""
            assert path
            dummy_client_kwargs["path"] = path
            return dummy_client_kwargs

        def _get_client(self):
            """Returns fake result."""
            return client

        def _get_roots(self):
            """Returns fake result."""
            return roots

        def _list_objects(self, client_kwargs, *_, **__):
            """Checks arguments and returns fake result."""
            assert client_kwargs == dummy_client_kwargs

            path = client_kwargs["path"].strip("/")
            if path == "locator_no_access":
                raise ObjectPermissionError
            elif path in ("locator", "locator/dir1"):
                for obj in objects:
                    yield obj, object_header.copy(), False
            else:
                return

        def _list_locators(self, *_, **__):
            """Returns fake result."""
            for locator in locators:
                yield locator, object_header.copy(), True

        def _head(self, client_kwargs):
            """Checks arguments and returns fake result."""
            assert client_kwargs == dummy_client_kwargs
            if raise_not_exists_exception:
                raise ObjectNotFoundError
            return object_header.copy()

        def _make_dir(self, client_kwargs):
            """Checks arguments."""
            path = client_kwargs["path"]
            assert "/" not in path or path[-1] == "/"
            assert client_kwargs == dummy_client_kwargs

        def _remove(self, client_kwargs):
            """Checks arguments."""
            assert client_kwargs == dummy_client_kwargs

    system = DummySystem(storage_parameters=storage_parameters)

    # Tests basic methods
    assert client == system.client

    assert roots == system.roots
    roots = ("roots",)
    assert client != system.roots

    assert storage_parameters == system.storage_parameters

    # Tests head
    assert system.getmtime("path") == pytest.approx(m_time, 1)
    assert system.getsize("path") == size

    with pytest.raises(ObjectUnsupportedOperation):
        system.getctime("path")

    object_header["Last-Modified"] = m_time
    assert system.getmtime("path") == m_time
    object_header["Last-Modified"] = format_date_time(m_time)

    # Tests "relpath"
    assert system.relpath("scheme://path") == "path"
    assert system.relpath("path") == "path"

    # Tests "is_abs"
    assert system.is_abs("root://path")
    assert system.is_abs("root2://path")
    assert not system.is_abs("path")

    # Tests locator
    assert system.is_locator("scheme://locator")
    assert not system.is_locator("scheme://locator/path")

    assert system.split_locator("scheme://locator") == ("locator", "")
    assert system.split_locator("scheme://locator/path") == ("locator", "path")

    # Tests "exists", "isdir", "isfile"
    assert system.exists("root://path")
    raise_not_exists_exception = True
    assert not system.exists("root://path")
    raise_not_exists_exception = False

    assert system.isfile("root://locator/path")
    assert not system.isfile("root://locator/path/")
    assert not system.isfile("root://")

    assert system.isdir("root://locator/path/")
    assert not system.isdir("root://locator/path")
    assert system.isdir("root://locator")
    assert system.isdir("root://")

    raise_not_exists_exception = True
    assert not system.isdir("root://locator/path/")
    assert system.isdir("root://locator/dir1/")
    assert not system.isdir("root://locator/dir1/", virtual_dir=False)
    raise_not_exists_exception = False

    # Test ensure_dir_path
    assert system.ensure_dir_path("root://locator/path") == "root://locator/path/"
    assert system.ensure_dir_path("root://locator/path/") == "root://locator/path/"
    assert system.ensure_dir_path("root://locator") == "root://locator"
    assert system.ensure_dir_path("root://locator/") == "root://locator"
    assert system.ensure_dir_path("root://") == "root://"

    # Tests make dir
    system.make_dir("root://locator")
    system.make_dir("root://locator/path")
    system.make_dir("root://locator/path/")
    system.make_dir("locator", relative=True)
    system.make_dir("locator/path", relative=True)
    system.make_dir("locator/path/", relative=True)

    # Test empty header
    header_old = object_header
    object_header = {}
    with pytest.raises(ObjectUnsupportedOperation):
        system.getmtime("path")
    with pytest.raises(ObjectUnsupportedOperation):
        system.getsize("path")
    object_header = header_old

    # Test default unsupported
    with pytest.raises(ObjectUnsupportedOperation):
        system.copy("path1", "path2")

    # Tests remove
    system.remove("root://locator")
    system.remove("root://locator/path")
    system.remove("root://locator/path/")
    system.remove("locator", relative=True)
    system.remove("locator/path", relative=True)
    system.remove("locator/path/", relative=True)

    # Tests stat
    object_header["Content-Length"] = "0"
    stat_result = system.stat("root://locator/path/")
    assert S_ISDIR(stat_result.st_mode)

    object_header["Content-Length"] = str(size)
    stat_result = system.stat("root://locator/path")
    assert S_ISREG(stat_result.st_mode)
    assert stat_result.st_ino == 0
    assert stat_result.st_dev == 0
    assert stat_result.st_nlink == 0
    assert stat_result.st_uid == getuid()
    assert stat_result.st_gid == getgid()
    assert stat_result.st_size == size
    assert stat_result.st_atime == 0
    assert stat_result.st_mtime == pytest.approx(m_time, 1)
    assert stat_result.st_ctime == 0
    assert stat_result.etag == object_header["ETag"]

    def islink(header=None, **_):
        """Checks arguments and returns fake result."""
        assert header is not None
        return True

    system_islink = system.islink
    system.islink = islink
    try:
        stat_result = system.stat("root://locator/path")
        assert S_ISLNK(stat_result.st_mode)
    finally:
        system.islink = system_islink

    # Tests list_objects
    assert list(system.list_objects(path="root://", first_level=True)) == [
        (locator + "/", object_header) for locator in locators
    ]

    with pytest.raises(ObjectPermissionError):
        list(system.list_objects(path="root://"))

    assert list(system.list_objects(path="root://locator")) == [
        (obj, object_header) for obj in objects
    ]

    assert list(system.list_objects(path="locator", relative=True)) == [
        (obj, object_header) for obj in objects
    ]
