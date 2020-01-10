# coding=utf-8
"""File System using true file hierarchy"""
from abc import abstractmethod

from airfs._core.io_base_system import SystemBase


class FileSystemBase(SystemBase):
    """
    Cloud storage system handler with true file hierarchy.
    """

    def list_objects(self, path='', relative=False, first_level=False,
                     max_request_entries=None):
        """
        List objects.

        Args:
            path (str): Path or URL.
            relative (bool): Path is relative to current root.
            first_level (bool): It True, returns only first level objects.
                Else, returns full tree.
            max_request_entries (int): If specified, maximum entries returned
                by request.

        Returns:
            generator of tuple: object name str, object header dict
        """
        entries = 0
        next_values = []
        max_request_entries_arg = None

        if not relative:
            path = self.relpath(path)

        # From root
        if not path:
            objects = self._list_locators()

        # Sub directory
        else:
            objects = self._list_objects(
                self.get_client_kwargs(path), max_request_entries)

        # Yield file hierarchy
        for obj in objects:
            # Generate first level objects entries
            try:
                name, header, is_directory = obj
            except ValueError:
                # Locators
                name, header = obj
                is_directory = True

            # Start to generate subdirectories content
            if is_directory and not first_level:
                name = next_path = name.rstrip('/') + '/'

                if path:
                    next_path = '/'.join((path.rstrip('/'), name))

                if max_request_entries is not None:
                    max_request_entries_arg = max_request_entries - entries

                next_values.append((
                    name, self._generate_async(self.list_objects(
                        next_path, relative=True,
                        max_request_entries=max_request_entries_arg))))

            entries += 1
            yield name, header
            if entries == max_request_entries:
                return

        for next_name, generator in next_values:
            # Generate other levels objects entries
            for name, header in generator:

                entries += 1
                yield '/'.join((next_name.rstrip('/'), name)), header
                if entries == max_request_entries:
                    return

    @abstractmethod
    def _list_objects(self, client_kwargs, max_request_entries):
        """
        Lists objects. Like "SystemBase._list_objects" but also return a bool
        that is True if the returned entry is a directory.

        args:
            client_kwargs (dict): Client arguments.
            max_request_entries (int): If specified, maximum entries returned
                by request.

        Returns:
            generator of tuple: object name str, object header dict,
            directory bool
        """
