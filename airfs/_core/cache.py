"""
A simple cache system to store requests results and improve performance.

Cache modes:
    Short cache:
        Short cache have a short expiration delay and will be discarded once this delay
        is reached.
    Long cache:
        Long cache have a far greater expiration delay that is reset on access.
        This is useful to store data that will not change.
"""
from gzip import open as open_archive
from hashlib import blake2b
from json import load, dump
from os import listdir, utime, remove, makedirs, chmod
from os.path import join, getmtime
from time import time
from airfs._core.config import CACHE_DIR


class NoCacheException(Exception):
    """No cache available"""


#: Long cache default expiry
CACHE_LONG_EXPIRY = 172800

#: Short cache default expiry
CACHE_SHORT_EXPIRY = 60

#: To initialize cache directories only once
_CACHE_INITIALIZED = False


def _hash_name(name):
    """
    Convert name to hashed name.

    Args:
        name (str): name.

    Returns:
        str: Hashed name.
    """
    return blake2b(name.encode(), digest_size=32).hexdigest()


def clear_cache():
    """
    Clear expired cache files.
    """
    expiry = _get_expiry()
    for cached_name in listdir(CACHE_DIR):
        path = join(CACHE_DIR, cached_name)
        if getmtime(path) < expiry[cached_name[-1]]:
            remove(path)
            continue


def _get_expiry():
    """
    Get expiry timestamps.

    Returns:
        dict: Expiry for both short and long modes.
    """
    current_time = time()
    return {
        "s": current_time - CACHE_SHORT_EXPIRY,
        "l": current_time - CACHE_LONG_EXPIRY,
    }


def get_cache(name):
    """
    Get an object from disk cache.

    Args:
        name (str): Cache name.

    Returns:
        dict or list or None: object, None if object is not cached.
    """
    expiry = _get_expiry()
    hashed_name = _hash_name(name)

    for mode in ("s", "l"):
        path = join(CACHE_DIR, hashed_name + mode)

        try:
            timestamp = getmtime(path)
        except FileNotFoundError:
            continue

        if timestamp < expiry[mode]:
            remove(path)
            continue

        if mode == "l":
            # In long cache mode, reset expiry delay
            utime(path)

        with open_archive(path, "rt") as file:
            return load(file)

    raise NoCacheException()


def set_cache(name, obj, long=False):
    """
    Add an object to disk cache.

    Args:
        name (str): Cache name.
        obj (dict or list): Object to cache.
        long (bool): If true, enable "long cache".
    """
    path = join(CACHE_DIR, _hash_name(name) + ("l" if long else "s"))

    global _CACHE_INITIALIZED
    if not _CACHE_INITIALIZED:
        makedirs(CACHE_DIR, exist_ok=True)
        chmod(CACHE_DIR, 0o700)
        _CACHE_INITIALIZED = True

    with open_archive(path, "wt") as file:
        dump(obj, file)
