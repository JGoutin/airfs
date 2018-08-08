# coding=utf-8
"""Handle storage classes"""
from collections import OrderedDict
from importlib import import_module
from threading import RLock

from pycosio._core.io_raw import ObjectRawIOBase
from pycosio._core.io_buffered import ObjectBufferedIOBase
from pycosio._core.io_system import SystemBase

STORAGE = OrderedDict()
_STORAGE_LOCK = RLock()
_BASE_CLASSES = {
    'raw': ObjectRawIOBase, 'buffered': ObjectBufferedIOBase,
    'system': SystemBase}


def is_storage(url, storage=None):
    """
    Check if file is a local file or a storage file.

    File is considered local if:
        - URL is a local path.
        - URL starts by "file://"
        - a "storage" is provided.

    Args:
        url (str): file path or URL
        storage (str): Storage name.

    Returns:
        bool: return True if file is local.
    """
    if storage:
        return True
    split_url = url.split('://', 1)
    if len(split_url) == 2 and split_url[0].lower() != 'file':
        return True
    return False


def get_instance(name, cls='system', storage=None,
                 storage_parameters=None, *args, **kwargs):
    """
    Get a cloud object storage instance.

    Args:
        name (str): File name, path or URL.
        cls (str): Type of class to instantiate.
            'raw', 'buffered' or 'system'.
        storage (str): Storage name.
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        args, kwargs: Instance arguments

    Returns:
        pycosio._core.io_base.ObjectIOBase subclass: Instance
    """
    # Gets storage information
    with _STORAGE_LOCK:
        for prefix in STORAGE:
            if name.startswith(prefix):
                info = STORAGE[prefix]
                break

        # If not found, tries to register before getting
        else:
            info = register(storage=storage, name=name,
                            storage_parameters=storage_parameters)

    # Returns cached system instance
    if cls == 'system':
        return info['system_cached']

    # Passes cached system instance and instantiates class
    if not storage_parameters:
        storage_parameters = info['storage_parameters'] or dict()
    storage_parameters['pycosio.system_cached'] = info['system_cached']
    return info[cls](storage_parameters=storage_parameters,
                     name=name, *args, **kwargs)


def register(storage=None, name='', storage_parameters=None,
             extra_url_prefix=None):
    """
    Register a new storage.

    Args:
        storage (str): Storage name.
        name (str): File URL. If storage is not specified,
            URL scheme will be used as storage value.
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        extra_url_prefix (str): Extra URL prefix that can be used in
            replacement of root URL in path. This can be used to
            provides support for shorter URLS.
            Example: with root URL "https://www.mycloud.com/user"
            and extra_url_prefix "mycloud://" it is possible to access object
            using "mycloud://container/object" instead of
            "https://www.mycloud.com/user/container/object".

    Returns:
        dict of class: Subclasses
    """
    # Tries to infer storage from name
    if storage is None:
        if '://' in name:
            storage = name.split('://', 1)[0].lower()

    # Saves get_storage_parameters
    storage_info = dict(storage_parameters=storage_parameters)

    # Finds module containing target subclass
    module = import_module('pycosio.storage.%s' % storage)

    # Finds storage subclass
    classes_items = tuple(_BASE_CLASSES.items())
    for member_name in dir(module):
        member = getattr(module, member_name)
        for cls_name, cls in classes_items:
            try:
                if issubclass(member, cls) and member is not cls:
                    storage_info[cls_name] = member
            except TypeError:
                continue

    # Caches a system instance
    storage_info['system_cached'] = storage_info['system'](storage_parameters)

    # Gets prefixes
    prefixes = storage_info['system_cached'].prefixes

    # Adds extra URL prefix
    if extra_url_prefix:
        prefixes = list(prefixes)
        prefixes.append(extra_url_prefix)

    # Registers
    with _STORAGE_LOCK:
        for prefix in prefixes:
            STORAGE[prefix] = storage_info

        # Reorder to have correct lookup
        items = OrderedDict((key, STORAGE[key])
                            for key in reversed(sorted(STORAGE)))
        STORAGE.clear()
        STORAGE.update(items)

    return storage_info
