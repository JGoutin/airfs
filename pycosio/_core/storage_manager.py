# coding=utf-8
"""Handle storage classes"""
from collections import OrderedDict
from importlib import import_module
from threading import RLock

from pycosio._core.io_raw import ObjectRawIOBase
from pycosio._core.io_buffered import ObjectBufferedIOBase


class StorageHook:
    """Hook of available storage

    Storage are by default lazy instantiated on needs
    """

    _BASE_CLASSES = {'raw': ObjectRawIOBase,
                     'buffered': ObjectBufferedIOBase}

    def __init__(self):
        self._classes = OrderedDict()
        self._lock = RLock()

    def get_subclass(self, io_type='buffered', name='', storage=None,
                     **storage_kwargs):
        """
        Get a cloud object storage subclass

        Args:
            io_type (str): 'raw' or 'buffered'.
            name (str): File name, path or URL.
            storage (str): Storage name.
            storage_kwargs: Storage specific key arguments.

        Returns:
            pycosio._core.io_base.ObjectIOBase subclass: Instance
        """
        # Get subclass from registered
        name_lower = name.lower()
        with self._lock:
            for prefix in self._classes:
                if name_lower.startswith(prefix):
                    return self._classes[prefix][io_type]

            # If not found, try to register and returns subclass
            return self.register(
                storage=storage, name=name, **storage_kwargs)[io_type]

    def register(self, storage=None, name='', **storage_kwargs):
        """
        Register a new storage.

        Args:
            storage (str): Storage name.
            name (str): File name, path or URL.
            storage_kwargs: Storage specific key arguments.

        Returns:
            dict of class: Subclasses
        """
        # Try to infer storage from name
        if storage is None:
            if '://' in name:
                storage = name.split('://')[0].lower()

        # Finds module containing target subclass
        module_name = 'pycosio.storages.%s' % storage
        module = import_module(module_name)

        # Finds storage subclass
        storage_classes = dict()
        classes_items = tuple(self._BASE_CLASSES.items())
        for member_name in dir(module):
            member = getattr(module, member_name)
            for cls_name, cls in classes_items:
                if issubclass(member, cls):
                    storage_classes[cls_name] = member

        # Get prefixes
        # "_get_prefix" method is protected at package level
        # and should be used elsewhere
        prefixes = storage_classes['raw']._get_prefix(**storage_kwargs)

        # Register
        with self._lock:
            classes = self._classes
            for prefix in prefixes:
                classes[prefix.lower()] = storage_classes

            # Reorder to avoid collision
            self._classes = OrderedDict(
                (key, classes[key])
                for key in reversed(sorted(classes)))

        return storage_classes


# Create hook
STORAGE = StorageHook()
