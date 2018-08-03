# coding=utf-8
"""Pycosio internal exceptions"""


class ObjectException(Exception):
    """Pycosio base exception"""


class ObjectNotFoundError(ObjectException):
    """Object not found"""


class ObjectPermissionError(ObjectException):
    """PermissionError"""
