"""Application directories"""
from json import load
import os
from os import getenv
from os.path import join, expandvars, expanduser


def _init_paths():
    """
    Initialize application directories.

    Returns:
        tuple of str: Configuration directory, cache directory
    """
    if os.name == "nt":
        config_dir = join(expandvars("%APPDATA%"), "airfs")
        cache_dir = join(expandvars("%LOCALAPPDATA%"), r"airfs\cache")

    elif os.getuid() != 0:
        config_dir = join(getenv("XDG_CONFIG_HOME", expanduser("~/.config")), "airfs")
        cache_dir = join(getenv("XDG_CACHE_HOME", expanduser("~/.cache")), "airfs")

    else:
        config_dir = "/etc/airfs"
        cache_dir = "/var/cache/airfs"

    return config_dir, cache_dir


CONFIG_DIR, CACHE_DIR = _init_paths()
CONFIG_FILE = join(CONFIG_DIR, "config.json")


def read_config():
    """
    Read the configuration.

    Returns:
        dict or None: Configuration. None if no configuration.
    """
    try:
        with open(CONFIG_FILE, "rt") as config_file:
            return load(config_file)

    except FileNotFoundError:
        return
