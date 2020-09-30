"""Storage only extra functions"""
from os import fsdecode
from airfs._core.storage_manager import get_instance
from airfs._core.functions_core import is_storage
from airfs._core.exceptions import handle_os_exceptions, ObjectUnsupportedOperation


def shareable_url(path, expires_in=3600):
    """
    Get a shareable public URL for the specified path of an existing object.

    Only available for some storage and not for local paths.

    .. versionadded:: 1.5.0

    Args:
        path (str): Path or URL.
        expires_in (int): Expiration in seconds. Default to 1 hour.
    """
    with handle_os_exceptions():
        path_str = fsdecode(path).replace("\\", "/")

        if not is_storage(path_str):
            raise ObjectUnsupportedOperation("shareable_url")

        return get_instance(path).shareable_url(path_str, expires_in)
