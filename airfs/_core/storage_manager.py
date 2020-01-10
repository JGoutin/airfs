# coding=utf-8
"""Handle storage classes"""
from collections import OrderedDict
from importlib import import_module
from threading import RLock

from airfs._core.io_base_raw import ObjectRawIOBase
from airfs._core.io_base_buffered import ObjectBufferedIOBase
from airfs._core.io_base_system import SystemBase
from airfs._core.compat import Pattern

# Packages where to search for storage
STORAGE_PACKAGE = ['airfs.storage']

# Mounted storage
MOUNTED = OrderedDict()
_MOUNT_LOCK = RLock()

# List Base classes, and advanced base classes that are not abstract.
_BASE_CLASSES = {
    'raw': ObjectRawIOBase,
    'buffered': ObjectBufferedIOBase,
    'system': SystemBase}

# Use this flag on subclass to make this class the default class for a
# specific storage (Useful when a storage provides multiple class):
# __DEFAULT_CLASS = True


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
        airfs._core.io_base.ObjectIOBase subclass: Instance
    """
    system_parameters = _system_parameters(
        unsecure=unsecure, storage_parameters=storage_parameters)

    # Gets storage information
    with _MOUNT_LOCK:
        for root in MOUNTED:
            if ((isinstance(root, Pattern) and root.match(name)) or
                    (not isinstance(root, Pattern) and
                     name.startswith(root))):
                info = MOUNTED[root]

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
            mount_info = mount(
                storage=storage, name=name, **system_parameters)
            info = mount_info[tuple(mount_info)[0]]
            same_parameters = True

    # Returns system class
    if cls == 'system':
        if same_parameters:
            return info['system_cached']
        else:
            return info['system'](
                roots=info['roots'], **system_parameters)

    # Returns other classes
    if same_parameters:
        if 'storage_parameters' not in system_parameters:
            system_parameters['storage_parameters'] = dict()
        system_parameters['storage_parameters'][
            'airfs.system_cached'] = info['system_cached']

    kwargs.update(system_parameters)
    return info[cls](name=name, *args, **kwargs)


def mount(storage=None, name='', storage_parameters=None,
          unsecure=None, extra_root=None):
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
        extra_root (str): Extra root that can be used in
            replacement of root in path. This can be used to
            provides support for shorter URLS.
            Example: with root "https://www.mycloud.com/user"
            and extra_root "mycloud://" it is possible to access object
            using "mycloud://container/object" instead of
            "https://www.mycloud.com/user/container/object".

    Returns:
        dict: keys are mounted storage, values are dicts of storage information.
    """
    # Tries to infer storage from name
    if storage is None:
        if '://' in name:
            storage = name.split('://', 1)[0].lower()
            # Alias HTTPS to HTTP
            storage = 'http' if storage == 'https' else storage
        else:
            raise ValueError(
                'No storage specified and unable to infer it from file name.')

    # Saves get_storage_parameters
    system_parameters = _system_parameters(
        unsecure=unsecure, storage_parameters=storage_parameters)
    storage_info = dict(storage=storage, system_parameters=system_parameters)

    # Finds module containing target subclass
    for package in STORAGE_PACKAGE:
        try:
            module = import_module('%s.%s' % (package, storage))
            break
        except ImportError:
            continue
    else:
        raise ImportError('No storage named "%s" found' % storage)

    # Case module is a mount redirection to mount multiple storage at once
    if hasattr(module, 'MOUNT_REDIRECT'):
        if extra_root:
            raise ValueError(
                ("Can't define extra_root with %s. "
                 "%s can't have a common root.") % (
                    storage, ', '.join(extra_root)))
        result = dict()
        for storage in getattr(module, 'MOUNT_REDIRECT'):
            result[storage] = mount(
                storage=storage, storage_parameters=storage_parameters,
                unsecure=unsecure)
        return result

    # Finds storage subclass
    classes_items = tuple(_BASE_CLASSES.items())
    for member_name in dir(module):
        member = getattr(module, member_name)
        for cls_name, cls in classes_items:

            # Skip if not subclass of the target class
            try:
                if not issubclass(member, cls) or member is cls:
                    continue
            except TypeError:
                continue

            # The class may have been flag as default or not-default
            default_flag = '_%s__DEFAULT_CLASS' % member.__name__.strip('_')
            try:
                is_default = getattr(member, default_flag)
            except AttributeError:
                is_default = None

            # Skip if explicitly flagged as non default
            if is_default is False:
                continue

            # Skip if is an abstract class not explicitly flagged as default
            elif is_default is not True and member.__abstractmethods__:
                continue

            # Is the default class
            storage_info[cls_name] = member
            break

    # Caches a system instance
    storage_info['system_cached'] = storage_info['system'](**system_parameters)

    # Gets roots
    roots = storage_info['system_cached'].roots

    # Adds extra root
    if extra_root:
        roots = list(roots)
        roots.append(extra_root)
        roots = tuple(roots)
    storage_info['system_cached'].roots = storage_info['roots'] = roots

    # Mounts
    with _MOUNT_LOCK:
        for root in roots:
            MOUNTED[root] = storage_info

        # Reorder to have correct lookup
        items = OrderedDict(
            (key, MOUNTED[key]) for key in reversed(
                sorted(MOUNTED, key=_compare_root)))
        MOUNTED.clear()
        MOUNTED.update(items)

    return {storage: storage_info}


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


def _compare_root(root):
    """
    Allow root comparison.

    Args:
        root (str or re.Pattern): Root.

    Returns:
        str: Comparable root string.
    """
    try:
        return root.pattern
    except AttributeError:
        return root
