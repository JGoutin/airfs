"""Airfs configuration"""
from json import dump as _dump
from os import chmod
from airfs._core.config import CONFIG_FILE as _CONFIG_FILE, read_config as _read_config


def get_mount(storage, config_name=None):
    """
    Get the mount configuration.

    .. versionadded:: 1.5.0

    Args:
        storage (str): Storage name.
        config_name (str): If specified, load the configuration as a specific storage
            configuration. "See airfs.config.set_mount".

    Returns:
        dict or None: Storage configuration, None if not configured.
    """
    if config_name:
        storage = f"{storage}.{config_name}"
    try:
        return _read_config()[storage]
    except (KeyError, TypeError):
        return None


def set_mount(
    storage, config_name=None, storage_parameters=None, unsecure=None, extra_root=None
):
    """
    Set a mount configuration. Most arguments are identical to "airfs.mount".

    .. versionadded:: 1.5.0

    Args:
        storage (str): Storage name.
        config_name (str): If specified, save the configuration as a specific storage
            configuration. This allow to save multiple configurations for a same
            "storage".
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure. Default to False.
        extra_root (str): Extra root that can be used in replacement of root in path.
            This can be used to provides support for shorter URLS.
            Example: with root "https://www.my_storage.com/user" and extra_root
            "mystorage://" it is possible to access object using
            "mystorage://container/object" instead of
            "https://www.my_storage.com/user/container/object".
    """
    if config_name:
        storage = f"{storage}.{config_name}"

    config = _read_config() or dict()
    config[storage] = {
        key: value
        for key, value in dict(
            unsecure=unsecure,
            extra_root=extra_root,
            storage_parameters=storage_parameters,
        ).items()
        if value
    }

    with open(_CONFIG_FILE, "wt") as config_file:
        _dump(config, config_file)
    chmod(_CONFIG_FILE, 0o600)
