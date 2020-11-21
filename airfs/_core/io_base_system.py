"""Cloud storage abstract System"""
from abc import abstractmethod, ABC
from collections import OrderedDict, namedtuple
from re import compile
from stat import S_IFDIR, S_IFREG, S_IFLNK
from posixpath import join, normpath, dirname
from dateutil.parser import parse

from airfs._core.io_base import WorkerPoolBase
from airfs._core.compat import Pattern, getgid, getuid
from airfs._core.exceptions import (
    ObjectNotFoundError,
    ObjectPermissionError,
    ObjectNotImplementedError,
    ObjectUnsupportedOperation,
)
from airfs._core.functions_core import SeatsCounter


class SystemBase(ABC, WorkerPoolBase):
    """
    Cloud storage system handler.

    This class subclasses are not intended to be public and are implementation details.

    This base system is for Object storage that does not handles files with a true
    hierarchy like file systems. Directories are virtual with this kind of storage.

    Args:
        storage_parameters (dict): Storage configuration parameters.
            Generally, client configuration and credentials.
        unsecure (bool): If True, disables TLS/SSL to improves transfer performance.
            But makes connection unsecure.
        roots (tuple): Tuple of roots to force use.
    """

    __slots__ = (
        "_storage_parameters",
        "_unsecure",
        "_storage",
        "_client",
        "_cache",
        "_roots",
    )

    #: If True, storage support symlinks
    SUPPORTS_SYMLINKS = False

    # By default, assumes that information are in a standard HTTP header
    _SIZE_KEYS = ("Content-Length",)
    _CTIME_KEYS = ()
    _MTIME_KEYS = ("Last-Modified",)

    _CHAR_FILTER = compile(r"[^a-z0-9_]*")

    def __init__(self, storage_parameters=None, unsecure=False, roots=None, **_):
        WorkerPoolBase.__init__(self)

        if storage_parameters:
            storage_parameters = storage_parameters.copy()
            for key in tuple(storage_parameters):
                if key.startswith("airfs."):
                    del storage_parameters[key]
        else:
            storage_parameters = dict()

        self._storage_parameters = storage_parameters
        self._unsecure = unsecure
        self._storage = self.__module__.rsplit(".", 1)[1]

        self._client = None

        self._cache = {}

        if roots:
            self._roots = roots
        else:
            self._roots = self._get_roots()

    @property
    def storage(self):
        """
        Storage name

        Returns:
            str: Storage
        """
        return self._storage

    @property
    def client(self):
        """
        Storage client

        Returns:
            client
        """
        if self._client is None:
            self._client = self._get_client()
        return self._client

    def copy(self, src, dst, other_system=None):
        """
        Copy object of the same storage.

        Args:
            src (str): Path or URL.
            dst (str): Path or URL.
            other_system (airfs._core.io_system.SystemBase subclass):
                Other storage system. May be required for some storage.
        """
        # This method is intended to copy objects to and from a same storage

        # It is possible to define methods to copy from a different storage by creating
        # a "copy_from_<src_storage>" method for the target storage and, vice versa, to
        # copy to a different storage by creating a "copy_to_<dst_storage>" method.

        # Theses methods must have the same signature as "copy".
        # "other_system" is optional and will be:
        # - The destination storage system with "copy_to_<src_storage>" method.
        # - The source storage system with "copy_from_<src_storage>" method.
        # - None elsewhere.

        # Note that if no "copy_from"/'copy_to" methods are defined, copy are performed
        # over the current machine with "shutil.copyfileobj".
        raise ObjectUnsupportedOperation

    def exists(
        self,
        path=None,
        client_kwargs=None,
        assume_exists=None,
        header=None,
        follow_symlinks=None,
    ):
        """
        Return True if path refers to an existing path.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.
            assume_exists (bool or None): This value define the value to return in the
                case there is no enough permission to determinate the existing status of
                the file. If set to None, the permission exception is reraised
                (Default behavior). if set to True or False, return this value.
            header (dict): Object header.
            follow_symlinks (bool): Follow symlinks.

        Returns:
            bool: True if exists.
        """
        try:
            path, client_kwargs, header = self.resolve(
                path, client_kwargs, header, follow_symlinks
            )
            self.head(path, client_kwargs, header)
        except ObjectNotFoundError:
            return False
        except ObjectPermissionError:
            if assume_exists is None:
                raise
            return assume_exists
        return True

    @abstractmethod
    def _get_client(self):
        """
        Storage client

        Returns:
            client
        """

    @abstractmethod
    def get_client_kwargs(self, path):
        """
        Get base keyword arguments for client for a specific path.

        Args:
            path (str): Absolute path or URL.

        Returns:
            dict: client args
        """

    def getctime(self, path=None, client_kwargs=None, header=None):
        """
        Return the creation time of path.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            float: The number of seconds since the epoch (see the time module).
        """
        return self._getctime_from_header(self.head(path, client_kwargs, header))

    def _getctime_from_header(self, header):
        """
        Return the time from header

        Args:
            header (dict): Object header.

        Returns:
            float: The number of seconds since the epoch
        """
        return self._get_time(header, self._CTIME_KEYS, "getctime")

    def getmtime(self, path=None, client_kwargs=None, header=None):
        """
        Return the time of last access of path.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            float: The number of seconds since the epoch (see the time module).
        """
        return self._getmtime_from_header(self.head(path, client_kwargs, header))

    def _getmtime_from_header(self, header):
        """
        Return the time from header

        Args:
            header (dict): Object header.

        Returns:
            float: The number of seconds since the epoch
        """
        return self._get_time(header, self._MTIME_KEYS, "getmtime")

    @staticmethod
    def _get_time(header, keys, name):
        """
        Get time from header

        Args:
            header (dict): Object header.
            keys (tuple of str): Header keys.
            name (str): Method name.

        Returns:
            float: The number of seconds since the epoch
        """
        for key in keys:
            try:
                date_value = header[key]
            except KeyError:
                continue
            try:
                return parse(date_value).timestamp()
            except TypeError:
                return float(date_value)
        raise ObjectUnsupportedOperation(name)

    @abstractmethod
    def _get_roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str or re.Pattern: URL roots
        """

    def getsize(self, path=None, client_kwargs=None, header=None):
        """
        Return the size, in bytes, of path.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            int: Size in bytes.
        """
        return self._getsize_from_header(self.head(path, client_kwargs, header))

    def _getsize_from_header(self, header):
        """
        Return the size from header

        Args:
            header (dict): Object header.

        Returns:
            int: Size in bytes.
        """
        for key in self._SIZE_KEYS:
            try:
                return int(header[key])
            except KeyError:
                continue
        else:
            raise ObjectUnsupportedOperation("getsize")

    def isdir(
        self,
        path=None,
        client_kwargs=None,
        virtual_dir=True,
        assume_exists=None,
        header=None,
        follow_symlinks=None,
    ):
        """
        Return True if path is an existing directory.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.
            virtual_dir (bool): If True, checks if directory exists virtually if an
                object path if not exists as a specific object.
            assume_exists (bool or None): This value define the value to return in the
                case there is no enough permission to determinate the existing status of
                the file. If set to None, the permission exception is reraised
                (Default behavior). if set to True or False, return this value.
            header (dict): Object header.
            follow_symlinks (bool): Follow symlinks.

        Returns:
            bool: True if directory exists.
        """
        relative = self.relpath(path)
        if not relative:
            # Root always exists and is a directory
            return True

        if path[-1] == "/" or self.is_locator(relative, relative=True):
            exists = self.exists(
                path, client_kwargs, assume_exists, header, follow_symlinks
            )
            if exists:
                return True

            elif virtual_dir:
                try:
                    next(self.list_objects(relative, relative=True, max_results=1))
                    return True
                except (StopIteration, ObjectNotFoundError, ObjectUnsupportedOperation):
                    return False
        return False

    def isfile(
        self,
        path=None,
        client_kwargs=None,
        assume_exists=None,
        header=None,
        follow_symlinks=None,
    ):
        """
        Return True if path is an existing regular file.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.
            assume_exists (bool or None): This value define the value to return in the
                case there is no enough permission to determinate the existing status of
                the file. If set to None, the permission exception is reraised
                (Default behavior). if set to True or False, return this value.
            header (dict): Object header.
            follow_symlinks (bool): Follow symlinks.

        Returns:
            bool: True if file exists.
        """
        relative = self.relpath(path)
        if not relative:
            # Root always exists and is a directory
            return False

        if path[-1] != "/" and not self.is_locator(path, relative=True):
            return self.exists(
                path, client_kwargs, assume_exists, header, follow_symlinks
            )
        return False

    @property
    def storage_parameters(self):
        """
        Storage parameters

        Returns:
            dict: Storage parameters
        """
        return self._storage_parameters

    @abstractmethod
    def _head(self, client_kwargs):
        """
        Returns object HTTP header.

        Args:
            client_kwargs (dict): Client arguments.

        Returns:
            dict: HTTP header.
        """

    def head(self, path=None, client_kwargs=None, header=None):
        """
        Returns object HTTP header.

        Args:
            path (str): Path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            dict: HTTP header.
        """
        if header is not None:
            return header
        elif client_kwargs is None:
            client_kwargs = self.get_client_kwargs(path)
        return self._head(client_kwargs)

    @property
    def roots(self):
        """
        Return URL roots for this storage.

        Returns:
            tuple of str: URL roots
        """
        return self._roots

    @roots.setter
    def roots(self, roots):
        """
        Set URL roots for this storage.

        Args:
            roots (tuple of str): URL roots
        """
        self._roots = roots

    def relpath(self, path):
        """
        Get path relative to storage.

        args:
            path (str): Absolute path or URL.

        Returns:
            str: relative path.
        """
        for root in self.roots:
            if isinstance(root, Pattern):
                match = root.match(path)
                if not match:
                    continue
                root = match.group(0)

            try:
                relative = path.split(root, 1)[1]
                return relative.lstrip("/")
            except IndexError:
                continue

        return path

    def is_abs(self, path):
        """
        Return True if path is absolute in this storage.

        args:
            path (str): Path or URL.

        Returns:
            bool: True if absolute path.
        """
        for root in self.roots:
            if isinstance(root, Pattern):
                if root.match(path):
                    return True
            elif path.startswith(root):
                return True
        return False

    def is_locator(self, path, relative=False):
        """
        Returns True if path refer to a locator.

        Depending the storage, locator may be a bucket or container name, a hostname,...

        args:
            path (str): path or URL.
            relative (bool): Path is relative to current root.

        Returns:
            bool: True if locator.
        """
        if not relative:
            path = self.relpath(path)
        return path and "/" not in path.rstrip("/")

    def split_locator(self, path):
        """
        Split the path into a pair (locator, path).

        args:
            path (str): Absolute path or URL.

        Returns:
            tuple of str: locator, path.
        """
        relative = self.relpath(path)
        try:
            locator, tail = relative.split("/", 1)
        except ValueError:
            locator = relative
            tail = ""
        return locator, tail

    def make_dir(self, path, relative=False):
        """
        Make a directory.

        Args:
            path (str): Path or URL.
            relative (bool): Path is relative to current root.
        """
        if not relative:
            path = self.relpath(path)
        self._make_dir(
            self.get_client_kwargs(self.ensure_dir_path(path, relative=True))
        )

    def _make_dir(self, client_kwargs):
        """
        Make a directory.

        args:
            client_kwargs (dict): Client arguments.
        """
        raise ObjectUnsupportedOperation("mkdir")

    def remove(self, path, relative=False):
        """
        Remove an object.

        Args:
            path (str): Path or URL.
            relative (bool): Path is relative to current root.
        """
        if not relative:
            path = self.relpath(path)
        self._remove(self.get_client_kwargs(path))

    def _remove(self, client_kwargs):
        """
        Remove an object.

        args:
            client_kwargs (dict): Client arguments.
        """
        raise ObjectUnsupportedOperation("remove")

    def ensure_dir_path(self, path, relative=False):
        """
        Ensure the path is a dir path.

        Should end with '/' except for schemes and locators.

        Args:
            path (str): Path or URL.
            relative (bool): Path is relative to current root.

        Returns:
            path: dir path
        """
        if not relative:
            rel_path = self.relpath(path)
        else:
            rel_path = path

        if self.is_locator(rel_path, relative=True):
            path = path.rstrip("/")

        elif rel_path:
            path = path.rstrip("/") + "/"

        return path

    def list_objects(
        self, path="", relative=False, first_level=False, max_results=None
    ):
        """
        List objects.

        Returns object path (relative to input "path") and object headers.

        Args:
            path (str): Path or URL.
            relative (bool): Path is relative to current root.
            first_level (bool): It True, returns only first level objects.
                Else, returns full tree.
            max_results (int): If specified, the maximum result count returned.

        Yields:
            tuple: object path str, object header dict
        """
        seats = SeatsCounter(max_results)

        if not relative:
            path = self.relpath(path)

        if path == "":
            generator = self._list_locators(max_results)
        else:
            generator = self._list_objects(
                self.get_client_kwargs(path), path, max_results, first_level
            )

        if first_level:
            generator = self._list_first_level_only(generator)
        else:
            generator = self._list_all_levels(generator, path, seats)

        take_seat = seats.take_seat
        for item in generator:
            yield item
            take_seat()
            if seats.full:
                return

    def _list_all_levels(self, generator, path, seats):
        """
        Recursively yields all level entries.

        Args:
            generator (iterable of tuple): path str, header dict, directory bool
            path (str): Path being listed.
            seats (airfs._core.functions_core.SeatsCounter): Seats counter.

        Yields:
            tuple: object path str, object header dict
        """
        dirs = list()
        add_dir = dirs.append
        for obj_path, header, is_dir in generator:

            if not obj_path:
                # Do not yield itself
                continue

            if is_dir:
                add_dir(obj_path)
                obj_path = obj_path.rstrip("/") + "/"

            yield obj_path, header

        if dirs:
            path = path.rstrip("/")
            for sub_path in dirs:
                if path:
                    full_path = "/".join((path, sub_path))
                else:
                    full_path = sub_path

                max_results = seats.seats_left
                if max_results:
                    # Add an extra seat to ensure the good count when yielding itself
                    max_results += 1

                for obj_path, header in self._list_all_levels(
                    self._list_objects(
                        self.get_client_kwargs(full_path),
                        full_path,
                        max_results,
                        False,
                    ),
                    full_path,
                    seats,
                ):
                    yield "/".join((sub_path.rstrip("/"), obj_path)), header

    @staticmethod
    def _list_first_level_only(generator):
        """
        Yield the first level entries only.

        Args:
            generator (iterable of tuple): path str, header dict, has content bool

        Yields:
            tuple: object path str, object header dict
        """
        dirs = set()
        virtual_dirs = set()
        add_virtual_dir = virtual_dirs.add
        add_dir = dirs.add
        for obj_path, header, is_dir in generator:
            obj_path = obj_path.rstrip("/")
            try:
                obj_path, _ = obj_path.split("/", 1)

            except ValueError:

                if is_dir:
                    add_dir(obj_path)
                    obj_path += "/"
                yield obj_path, header

            else:
                add_virtual_dir(obj_path)

        for obj_path in virtual_dirs - dirs:
            yield obj_path + "/", dict()

    def _list_locators(self, max_results):
        """
        Lists locators.

        args:
            max_results (int): The maximum results that should return the method.

        Yields:
            tuple: locator name str, locator header dict, has content bool
        """
        # Implementation note: See "_list_objects" method.
        raise ObjectUnsupportedOperation("listdir")

    def _list_objects(self, client_kwargs, path, max_results, first_level):
        """
        Lists objects.

        args:
            client_kwargs (dict): Client arguments.
            path (str): Path to list.
            max_results (int): The maximum results that should return the method.
                None if no limit.
            first_level (bool): It True, may only first level objects.

        Yields:
            tuple: object path str, object header dict, has content bool
        """
        # Implementation note:
        #
        # Should return a tuple of the following values
        # - The object path (relative to the "path" argument)
        # - The object headers
        # - The "had content" bool that must be True if the object has sub-content that
        #   should be listed recursively by the function. For instance, it should be
        #   False for files, True for directories that are list without there content
        #   and False for directories that are list with their content.
        #
        # Returning only first level entries with "first_level" or only the maximum
        # entries with "max_results" are optional, these parameters are mainly
        # intended to help to reduce result size from requests against the storage and
        # improve the performance.
        raise ObjectUnsupportedOperation("listdir")

    def islink(self, path=None, client_kwargs=None, header=None):
        """
        Returns True if object is a symbolic link.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            bool: True if object is Symlink.
        """
        return False

    def _getuid(self, path=None, client_kwargs=None, header=None):
        """
        Get object user ID.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            int: User ID.
        """
        # Default to current process UID
        return getuid()

    def _getgid(self, path=None, client_kwargs=None, header=None):
        """
        Get object group ID.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            int: Group ID.
        """
        # Default to current process GID
        return getgid()

    def _getmode(self, path=None, client_kwargs=None, header=None):
        """
        Get object permission mode in Unix format.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            int: Group ID.
        """
        # Default to an arbitrary common value
        return 0o644

    def stat(self, path=None, client_kwargs=None, header=None, follow_symlinks=None):
        """
        Get the status of an object.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.
            follow_symlinks (bool): Follow symlinks.

        Returns:
            namedtuple: Stat result object. Follow the "os.stat_result" specification
                and may contain storage dependent extra entries.
        """
        path, client_kwargs, header = self.resolve(
            path, client_kwargs, header, follow_symlinks
        )
        stat = OrderedDict(
            (
                ("st_mode", self._getmode(path, client_kwargs, header)),
                ("st_ino", 0),
                ("st_dev", 0),
                ("st_nlink", 0),
                ("st_uid", self._getuid()),
                ("st_gid", self._getgid()),
                ("st_size", 0),
                ("st_atime", 0),
                ("st_mtime", 0),
                ("st_ctime", 0),
                ("st_atime_ns", 0),
                ("st_mtime_ns", 0),
                ("st_ctime_ns", 0),
            )
        )

        header = self.head(path, client_kwargs, header)
        try:
            stat["st_size"] = int(self._getsize_from_header(header))
        except ObjectUnsupportedOperation:
            pass

        for st_time, st_time_ns, method in (
            ("st_mtime", "st_mtime_ns", self._getmtime_from_header),
            ("st_ctime", "st_ctime_ns", self._getctime_from_header),
        ):
            try:
                time_value = method(header)
            except ObjectUnsupportedOperation:
                continue
            stat[st_time] = int(time_value)
            stat[st_time_ns] = int(time_value * 1000000000)

        if self.islink(path=path, header=header):
            stat["st_mode"] += S_IFLNK
        elif self.isdir(path=path, client_kwargs=client_kwargs, header=header):
            stat["st_mode"] += S_IFDIR
        else:
            stat["st_mode"] += S_IFREG

        sub = self._CHAR_FILTER.sub
        for key, value in tuple(header.items()):
            stat[sub("", key.lower().replace("-", "_"))] = value

        stat_result = namedtuple("stat_result", tuple(stat))
        stat_result.__name__ = "os.stat_result"
        stat_result.__module__ = "airfs"
        return stat_result(**stat)

    def read_link(self, path=None, client_kwargs=None, header=None):
        """
        Return the path linked by the symbolic link.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            str: Path.
        """
        raise ObjectUnsupportedOperation("symlink")

    def symlink(self, target, path=None, client_kwargs=None):
        """
        Creates a symbolic link to target.

        Args:
            target (str): Target path or URL.
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
        """
        raise ObjectUnsupportedOperation("symlink")

    def resolve(self, path=None, client_kwargs=None, header=None, follow_symlinks=None):
        """
        Follow symlinks and return input arguments updated for target.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.
            follow_symlinks (bool): If True, follow symlink if any.
                If False, return input directly if symlink are supported by storage,
                else raise NotImplementedError. If None, same as False but returns
                input instead of raising exception.

        Returns:
            tuple: path, client_kwargs, headers of the target.

        Raises:
            ObjectNotImplementedError: follow_symlink is False on storage that do not
                support symlink.
        """
        if not self.SUPPORTS_SYMLINKS and follow_symlinks is False:
            raise ObjectNotImplementedError(feature="follow_symlink=False")
        elif not follow_symlinks or not self.SUPPORTS_SYMLINKS:
            return path, client_kwargs, header
        return self._resolve(path, client_kwargs, header)

    def _resolve(self, path=None, client_kwargs=None, header=None):
        """
        Resolve core function.

        Args:
            path (str): File path or URL.
            client_kwargs (dict): Client arguments.
            header (dict): Object header.

        Returns:
            tuple: path, client_kwargs, headers of the target.
        """
        is_link = self.islink(path, client_kwargs, header)

        if is_link:
            target = self.read_link(path, client_kwargs, header)
            if not self.is_abs(target):
                rel_path = self.relpath(path)
                target = path[: -len(rel_path)] + normpath(
                    join(dirname(rel_path), target)
                )
            return self._resolve(target)

        if not is_link and self.exists(path, client_kwargs, header=header):
            return path, client_kwargs, header

        try:
            parent, name = path.rstrip("/").rsplit("/", 1)
        except ValueError:
            return path, client_kwargs, header

        parent_target = self._resolve(parent + "/")[0]
        path = "/".join((parent_target, name)) + ("/" if path.endswith("/") else "")
        return path, None, None

    def shareable_url(self, path, expires_in):
        """
        Get a shareable URL for the specified path.

        Args:
            path (str): Path or URL.
            expires_in (int): Expiration in seconds.

        Returns:
            str: Shareable URL.
        """
        return self._shareable_url(
            self.get_client_kwargs(self.relpath(path)), expires_in
        )

    def _shareable_url(self, client_kwargs, expires_in):
        """
        Get a shareable URL for the specified path.

        Args:
            client_kwargs (dict): Client arguments.
            expires_in (int): Expiration in seconds.

        Returns:
            str: Shareable URL.
        """
        raise ObjectNotImplementedError("shareable_url")
