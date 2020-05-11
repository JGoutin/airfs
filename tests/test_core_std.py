"""Test Standard library modules replacements"""


def test_os():
    """The "os" module."""
    import os
    import airfs.os
    from airfs._core.functions_os import makedirs

    assert airfs.os.__all__ == os.__all__
    assert airfs.os.makedirs is makedirs
    assert airfs.os.fork is os.fork


def test_os_path():
    """The "os.path" module."""
    import os.path
    import airfs.os.path
    from airfs._core.functions_os_path import relpath

    assert airfs.os.path.__all__ == os.path.__all__
    assert airfs.os.path.relpath is relpath
    assert airfs.os.path.join is os.path.join


def test_shutil():
    """The "shutil" module."""
    import shutil
    import airfs.shutil
    from airfs._core.functions_shutil import copy

    assert airfs.shutil.__all__ == shutil.__all__
    assert airfs.shutil.copy is copy
    assert airfs.shutil.copyfileobj is shutil.copyfileobj
