"""Storage only extra functions"""
from io import UnsupportedOperation
from os import fsdecode
from airfs._core.storage_manager import get_instance
from airfs._core.functions_core import is_storage
from airfs._core.exceptions import handle_os_exceptions


def shareable_url(path, expires_in=3600):
    """
    Get a shareable public URL for the specified path of an existing object.

    Only available for some storage and not for local paths.

    Args:
        path (str): Path or URL.
        expires_in (int): Expiration in seconds. Default to 1 hour.
    """
    # Handles path-like objects
    path_str = fsdecode(path).replace("\\", "/")

    # Not available for local path
    if not is_storage(path_str):
        raise UnsupportedOperation("shareable_url")

    with handle_os_exceptions():
        return get_instance(path).shareable_url(path_str, expires_in)
