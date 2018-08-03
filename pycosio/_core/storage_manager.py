# coding=utf-8
"""Handle storage classes"""
from collections import OrderedDict
from importlib import import_module
from threading import RLock

from pycosio._core.io_raw import ObjectRawIOBase
from pycosio._core.io_buffered import ObjectBufferedIOBase
from pycosio._core.io_system import SystemBase


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
    if len(split_url) == 2 or split_url[0].lower() != 'file':
        return True
    return False


class StorageHook:
    """Hook of available storage

    Storage are by default lazy instantiated on needs
    """

    _BASE_CLASSES = {'raw': ObjectRawIOBase,
                     'buffered': ObjectBufferedIOBase,
                     'system': SystemBase}

    def __init__(self):
        self._items = OrderedDict()
        self._lock = RLock()

    def get_info(self, name='', storage=None, storage_parameters=None):
        """
        Get a cloud object storage information.

        Args:
            name (str): File name, path or URL.
            storage (str): Storage name.
            storage_parameters (dict): Storage configuration parameters.
                Generally, client configuration and credentials.

        Returns:
            dict: storage information.
        """
        # Get subclass from registered
        name_lower = name.lower()
        with self._lock:
            for prefix in self._items:
                if name_lower.startswith(prefix):
                    return self._items[prefix]

            # If not found, try to register before getting
            return self.register(
                storage=storage, name=name,
                storage_parameters=storage_parameters)

    def get_instance(self, cls='buffered', name='', storage=None,
                     storage_parameters=None, *args, **kwargs):
        """
        Get a cloud object storage instance.

        Args:
            cls (str): Type of class to instantiate.
                'raw', 'buffered' or 'system'.
            name (str): File name, path or URL.
            storage (str): Storage name.
            storage_parameters (dict): Storage configuration parameters.
                Generally, client configuration and credentials.
            args, kwargs: Instance arguments

        Returns:
            pycosio._core.io_base.ObjectIOBase subclass: Instance
        """
        # Gets storage information
        info = self.get_info(name, storage, storage_parameters)
        if not storage_parameters:
            storage_parameters = info['get_storage_parameters']

        # Store prefixes
        storage_parameters['storage.prefixes'] = info['prefixes']

        # Instantiates class
        return info[cls](
            storage_parameters=storage_parameters,
            name=name,
            *args, **kwargs)

    def register(self, storage=None, name='', storage_parameters=None):
        """
        Register a new storage.

        Args:
            storage (str): Storage name.
            name (str): File URL. If storage is not specified,
                URL scheme will be used as storage value.
            storage_parameters (dict): Storage configuration parameters.
                Generally, client configuration and credentials.

        Returns:
            dict of class: Subclasses
        """
        # Try to infer storage from name
        if storage is None:
            if '://' in name:
                storage = name.split('://', 1)[0].lower()

        # Save get_storage_parameters
        storage_info = dict(storage_parameters=storage_parameters)

        # Finds module containing target subclass
        module = import_module('pycosio.storage.%s' % storage)

        # Finds storage subclass
        classes_items = tuple(self._BASE_CLASSES.items())
        for member_name in dir(module):
            member = getattr(module, member_name)
            for cls_name, cls in classes_items:
                if issubclass(member, cls):
                    storage_info[cls_name] = member

        # Get prefixes
        # "_get_prefix" method is protected at package level
        # and should be used elsewhere
        storage_info['prefixes'] = storage_info[
            'system'](storage_parameters).prefixes

        # Register
        with self._lock:
            items = self._items
            for prefix in storage_info['prefixes']:
                items[prefix.lower()] = storage_info

            # Reorder to have correct lookup
            self._items = OrderedDict(
                (key, items[key])
                for key in reversed(sorted(items)))

        return storage_info


# Create hook
STORAGE = StorageHook()

# Functions shortcuts
get_info = STORAGE.get_info
get_instance = STORAGE.get_instance
register = STORAGE.register
