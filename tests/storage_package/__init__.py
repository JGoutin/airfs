# coding=utf-8
"""Test specific storage modules."""


def init_test_storage():
    """
    Initialize test storage.
    """
    from pycosio._core.storage_manager import STORAGE_PACKAGE
    if not __package__ in STORAGE_PACKAGE:
        STORAGE_PACKAGE.append(__package__)
