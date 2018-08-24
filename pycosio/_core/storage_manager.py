# coding=utf-8
"""Handle storage classes"""
from collections import OrderedDict
from importlib import import_module
from threading import RLock

from pycosio._core.io_raw import ObjectRawIOBase
from pycosio._core.io_buffered import ObjectBufferedIOBase
from pycosio._core.io_system import SystemBase
from pycosio._core.compat import Pattern

MOUNTED = OrderedDict()
_MOUNT_LOCK = RLock()
_BASE_CLASSES = {
    'raw': ObjectRawIOBase, 'buffered': ObjectBufferedIOBase,
    'system': SystemBase}


def get_instance(name, cls='system', storage=None, storage_parameters=None,
                 unsecure=None, *args, **kwargs):
    """
    Get a cloud object storage instance.

    Args:
        name (str): File name, path or URL.
        cls (str): Type of class to instantiate.
            'raw', 'buffered' or 'system'.
        storage (str): Storage name.
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
            Default to False.
        args, kwargs: Instance arguments

    Returns:
        pycosio._core.io_base.ObjectIOBase subclass: Instance
    """
    system_parameters = _system_parameters(
        unsecure=unsecure, storage_parameters=storage_parameters)

    # Gets storage information
    with _MOUNT_LOCK:
        for prefix in MOUNTED:
            if ((isinstance(prefix, Pattern) and prefix.match(name)) or
                    (not isinstance(prefix, Pattern) and
                     name.startswith(prefix))):
                info = MOUNTED[prefix]

                # Get stored storage parameters
                stored_parameters = info.get('system_parameters') or dict()
                if not system_parameters:
                    same_parameters = True
                    system_parameters = stored_parameters
                elif system_parameters == stored_parameters:
                    same_parameters = True
                else:
                    same_parameters = False
                    # Copy not specified parameters from default
                    system_parameters.update({
                        key: value for key, value in stored_parameters.items()
                        if key not in system_parameters})
                break

        # If not found, tries to mount before getting
        else:
            info = mount(storage=storage, name=name, **system_parameters)
            same_parameters = True

    # Returns system class
    if cls == 'system':
        if same_parameters:
            return info['system_cached']
        else:
            return info['system'](
                prefixes=info['prefixes'], **system_parameters)

    # Returns other classes
    if same_parameters:
        if 'storage_parameters' not in system_parameters:
            system_parameters['storage_parameters'] = dict()
        system_parameters['storage_parameters'][
            'pycosio.system_cached'] = info['system_cached']
    kwargs.update(system_parameters)
    return info[cls](name=name, *args, **kwargs)


def mount(storage=None, name='', storage_parameters=None,
          unsecure=None, extra_url_prefix=None):
    """
    Mount a new storage.

    Args:
        storage (str): Storage name.
        name (str): File URL. If storage is not specified,
            URL scheme will be used as storage value.
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        unsecure (bool): If True, disables TLS/SSL to improves
            transfer performance. But makes connection unsecure.
            Default to False.
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
            # Alias HTTPS to HTTP
            storage = 'http' if storage == 'https' else storage

    # Saves get_storage_parameters
    system_parameters = _system_parameters(
        unsecure=unsecure, storage_parameters=storage_parameters)
    storage_info = dict(system_parameters=system_parameters)

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
    storage_info['system_cached'] = storage_info['system'](**system_parameters)

    # Gets prefixes
    prefixes = storage_info['system_cached'].prefixes

    # Adds extra URL prefix
    if extra_url_prefix:
        prefixes = list(prefixes)
        prefixes.append(extra_url_prefix)
        prefixes = tuple(prefixes)
    storage_info['system_cached'].prefixes = storage_info['prefixes'] = prefixes

    # Mounts
    with _MOUNT_LOCK:
        for prefix in prefixes:
            MOUNTED[prefix] = storage_info

        # Reorder to have correct lookup
        items = OrderedDict(
            (key, MOUNTED[key]) for key in reversed(
                sorted(MOUNTED, key=_compare_prefix)))
        MOUNTED.clear()
        MOUNTED.update(items)

    return storage_info


def _system_parameters(**kwargs):
    """
    Returns system keyword arguments removing Nones.

    Args:
        kwargs: system keyword arguments.

    Returns:
        dict: system keyword arguments.
    """
    return {key: value for key, value in kwargs.items()
            if (value is not None or value == {})}


def _compare_prefix(prefix):
    """
    Allow prefix comparison.

    Args:
        prefix (str or re.Pattern): Prefix.

    Returns:
        str: Comparable prefix string.
    """
    try:
        return prefix.pattern
    except AttributeError:
        return prefix
