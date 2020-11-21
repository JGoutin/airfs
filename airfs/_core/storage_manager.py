"""Handle storage classes"""
from collections import OrderedDict
from importlib import import_module
from importlib.util import find_spec
from threading import RLock

from airfs._core.io_base_raw import ObjectRawIOBase
from airfs._core.io_base_buffered import ObjectBufferedIOBase
from airfs._core.io_base_system import SystemBase
from airfs._core.config import read_config
from airfs._core.compat import Pattern
from airfs._core.exceptions import MountException

#: Packages where to search for storage
STORAGE_PACKAGE = ["airfs.storage"]

#: Mounted storage
MOUNTED = OrderedDict()
_MOUNT_LOCK = RLock()

#: List Base classes, and advanced base classes that are not abstract.
_BASE_CLASSES = {
    "raw": ObjectRawIOBase,
    "buffered": ObjectBufferedIOBase,
    "system": SystemBase,
}

# Use this flag on subclass to make this class the default class for a specific storage
# (Useful when a storage provides multiple class):
# __DEFAULT_CLASS = True


def _automount():
    """
    Initialize AUTOMOUNT variable with roots patterns that may be lazily automounted.

    The target storage must allow to be mounted with a default configuration.

    Returns:
        dict: storage names as keys, List of roots patterns as values.
    """
    import airfs._automount as package
    from airfs._core.compat import contents
    from importlib import import_module
    from os.path import splitext
    from sys import modules

    package_name = package.__name__
    automount = dict()
    for file in contents(package_name):
        if file.endswith(".py") and not file.startswith("_"):
            storage = splitext(file)[0]

            module_name = f"{package_name}.{storage}"
            module = import_module(module_name)
            automount[storage] = module.ROOTS
            del modules[module_name]
    del modules[package_name]
    return automount


#: Storage to automount
AUTOMOUNT = _automount()
_AUTOMOUNT_LOCK = RLock()
del _automount

#: Default configuration from users
_DEFAULTS = dict()


def _user_mount():
    """
    Mount user configured storages.
    """
    config = read_config()
    if config is not None:
        for storage, system_parameters in read_config().items():
            if "." in storage:
                # User specific storage: mounted immediately
                mount(storage.split(".", 1)[0], **system_parameters)

            else:
                # Default storage: Mounted lazily
                _DEFAULTS[storage] = system_parameters


_user_mount()


def get_instance(
    name,
    cls="system",
    storage=None,
    storage_parameters=None,
    unsecure=None,
    *args,
    **kwargs,
):
    """
    Get a storage instance.

    Args:
        name (str): File name, path or URL.
        cls (str): Type of class to instantiate. 'raw', 'buffered' or 'system'.
        storage (str): Storage name.
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure. Default to False.
        args, kwargs: Instance arguments

    Returns:
        airfs._core.io_base.ObjectIOBase subclass: Instance
    """
    system_parameters = _system_parameters(
        unsecure=unsecure, storage_parameters=storage_parameters
    )

    info, system_parameters, unchanged = _get_storage_info(
        name, storage, system_parameters
    )

    if cls == "system":
        if unchanged:
            return info["system_cached"]
        else:
            return info["system"](roots=info["roots"], **system_parameters)

    if unchanged:
        if "storage_parameters" not in system_parameters:
            system_parameters["storage_parameters"] = dict()
        system_parameters["storage_parameters"]["airfs.system_cached"] = info[
            "system_cached"
        ]

    kwargs.update(system_parameters)
    return info[cls](name=name, *args, **kwargs)


def _get_storage_info(name, storage, system_parameters):
    """
    Get mounted storage information. Mount storage if required.

    Args:
        name (str): File name, path or URL.
        storage (str): Storage name.
        system_parameters (dict): Storage system parameters.

    Returns:
        tuple: storage information, storage system parameters, flag that is True if
            storage system parameters are unchanged.
    """
    with _MOUNT_LOCK:
        for root in MOUNTED:
            if _match_root(root, name):
                info = MOUNTED[root]

                stored_parameters = info.get("system_parameters") or dict()
                if not system_parameters:
                    unchanged = True
                    system_parameters = stored_parameters
                elif system_parameters == stored_parameters:
                    unchanged = True
                else:
                    unchanged = False
                    system_parameters.update(
                        {
                            key: value
                            for key, value in stored_parameters.items()
                            if key not in system_parameters
                        }
                    )
                break

        else:
            mount_info = mount(storage=storage, name=name, **system_parameters)
            info = mount_info[tuple(mount_info)[0]]
            unchanged = True

    return info, system_parameters, unchanged


def mount(
    storage=None, name="", storage_parameters=None, unsecure=None, extra_root=None
):
    """
    Mount a new storage.

    .. versionadded:: 1.0.0

    Args:
        storage (str): Storage name.
        name (str): File URL. If storage is not specified, it will be infered from this
            name.
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

    Returns:
        dict: keys are mounted storage, values are dicts of storage information.
    """
    if storage is None:
        storage = _find_storage(name)

    storage_parameters = _get_default(storage, "storage_parameters", storage_parameters)
    unsecure = _get_default(storage, "unsecure", unsecure)
    extra_root = _get_default(storage, "extra_root", extra_root)

    system_parameters = _system_parameters(
        unsecure=unsecure, storage_parameters=storage_parameters
    )
    storage_info = dict(storage=storage, system_parameters=system_parameters)

    module = _import_storage_module(storage)
    if hasattr(module, "MOUNT_REDIRECT"):
        if extra_root:
            raise MountException(
                f"Can't define extra_root with {storage}. "
                f"{extra_root} can't have a common root."
            )
        result = dict()
        for storage in getattr(module, "MOUNT_REDIRECT"):
            result[storage] = mount(
                storage=storage,
                storage_parameters=storage_parameters,
                unsecure=unsecure,
            )
        return result

    _find_storage_classes(module, storage_info)

    storage_info["system_cached"] = storage_info["system"](**system_parameters)

    _storage_roots(storage_info, extra_root)
    _updates_mounts(storage, storage_info)

    return {storage: storage_info}


def _find_storage_classes(module, storage_info):
    """
    Update storage information with storage sub-classes.

    Args:
        module (module): Storage Python module.
        storage_info (dict): Storage information.
    """
    classes_items = tuple(_BASE_CLASSES.items())
    found_default = {cls_name: False for cls_name in _BASE_CLASSES}
    for member_name in dir(module):
        member = getattr(module, member_name)
        for cls_name, cls in classes_items:
            if found_default[cls_name]:
                continue
            try:
                if not issubclass(member, cls) or member is cls:
                    continue
            except TypeError:
                continue

            default_flag = f"_{member.__name__.strip('_')}__DEFAULT_CLASS"
            try:
                is_default = getattr(member, default_flag)
            except AttributeError:
                pass
            else:
                if is_default:
                    found_default[cls_name] = True
                elif is_default is False:
                    continue

            if member.__abstractmethods__:
                continue

            storage_info[cls_name] = member
            break


def _updates_mounts(storage, storage_info):
    """
    Update mount information.

    Args:
        storage (str): Storage name.
        storage_info (dict): Storage information.
    """
    with _MOUNT_LOCK:
        for root in storage_info["roots"]:
            MOUNTED[root] = storage_info

        items = OrderedDict(
            (key, MOUNTED[key]) for key in reversed(sorted(MOUNTED, key=_root_sort_key))
        )
        MOUNTED.clear()
        MOUNTED.update(items)

    with _AUTOMOUNT_LOCK:
        try:
            del AUTOMOUNT[storage]
        except KeyError:
            pass


def _storage_roots(storage_info, extra_root):
    """
    Update storage information with storage roots.

    Args:
        storage_info (dict): Storage information.
        extra_root (str): Extra root that can be used in replacement of root in path.

    Returns:
        list: Roots.
    """
    roots = storage_info["system_cached"].roots
    if extra_root:
        roots = list(roots)
        roots.append(extra_root)
        roots = tuple(roots)
    storage_info["system_cached"].roots = storage_info["roots"] = roots
    return roots


def _import_storage_module(storage):
    """
    Import the Python module of the specified storage.

    Args:
        storage (str): storage name.

    Returns:
        Storage Python module.
    """
    for package in STORAGE_PACKAGE:
        module_name = f"{package}.{storage}"
        try:
            return import_module(module_name)
        except ImportError:
            if find_spec(module_name) is not None:
                raise
    raise MountException(f'No storage named "{storage}" found')


def _find_storage(name):
    """
    Find the storage from the file name or URL.

    Args:
        name (str): File URL or path.

    Returns:
        str: storage name.
    """
    candidate = None

    try:
        scheme, _ = name.split("://", 1)
    except ValueError:
        pass
    else:
        if scheme not in ("http", "https"):
            return scheme

        candidate = "http"

    with _AUTOMOUNT_LOCK:
        for storage, roots in AUTOMOUNT.items():
            if any(_match_root(root, name) for root in roots):
                candidate = storage
                break

    if candidate:
        return candidate

    raise MountException("No storage specified and unable to infer it from file name.")


def _system_parameters(**kwargs):
    """
    Returns system keyword arguments removing Nones.

    Args:
        kwargs: system keyword arguments.

    Returns:
        dict: system keyword arguments.
    """
    return {
        key: value
        for key, value in kwargs.items()
        if (value is not None or value == {})
    }


def _root_sort_key(root):
    """
    Allow root comparison when sorting.

    Args:
        root (str or re.Pattern): Root.

    Returns:
        str: Comparable root string.
    """
    try:
        return root.pattern
    except AttributeError:
        return root


def _match_root(root, name):
    """

    Args:
        root (str or re.Pattern): Root.
        name (str): File URL or path.

    Returns:
        bool: True if match.
    """
    return (isinstance(root, Pattern) and root.match(name)) or (
        not isinstance(root, Pattern) and name.startswith(root)
    )


def _get_default(storage, key, value):
    """
    Get default if value is not specified.

    Args:
        storage (str): Storage name.
        key (str): Parameter key.
        value: Parameter value.

    Returns:
        value: Parameter value.
    """
    if value is None:
        try:
            return _DEFAULTS[storage][key]
        except KeyError:
            return
    return value
