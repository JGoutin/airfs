"""Cloud storage abstract IO Base class"""
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from io import IOBase, UnsupportedOperation
from itertools import chain
from os import fsdecode
from threading import Lock


class ObjectIOBase(IOBase):
    """
    Base class to handle storage object.

    Args:
        name (path-like object): URL or path to the file which will be opened.
        mode (str): The mode can be 'r', 'w', 'a', 'x' for reading (default),
            writing, appending or creation.
    """

    __slots__ = (
        "_name",
        "_mode",
        "_seek",
        "_seek_lock",
        "_cache",
        "_closed",
        "_writable",
        "_readable",
        "_seekable",
    )

    def __init__(self, name, mode="r"):
        IOBase.__init__(self)

        self._name = fsdecode(name)
        self._mode = mode
        self._seek = 0
        self._seek_lock = Lock()
        self._cache = {}
        self._closed = False
        self._writable = False
        self._readable = False
        self._seekable = True

        if "w" in mode or "a" in mode or "x" in mode:
            self._writable = True

        elif "r" in mode:
            self._readable = True

        else:
            raise ValueError(f'Invalid mode "{mode}"')

    def __str__(self):
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__} "
            f"name='{self._name}' mode='{self._mode}'>"
        )

    __repr__ = __str__

    @property
    def mode(self):
        """
        The mode.

        Returns:
            str: Mode.
        """
        return self._mode

    @property
    def name(self):
        """
        The file name.

        Returns:
            str: Name.
        """
        return self._name

    def readable(self):
        """
        Return True if the stream can be read from.
        If False, read() will raise OSError.

        Returns:
            bool: Supports reading.
        """
        return self._readable

    def seekable(self):
        """
        Return True if the stream supports random access.
        If False, seek(), tell() and truncate() will raise OSError.

        Returns:
            bool: Supports random access.
        """
        return self._seekable

    def tell(self):
        """
        Return the current stream position.

        Returns:
            int: Stream position.
        """
        if not self._seekable:
            raise UnsupportedOperation("tell")

        with self._seek_lock:
            return self._seek

    def writable(self):
        """
        Return True if the stream supports writing.
        If False, write() and truncate() will raise OSError.

        Returns:
            bool: Supports writing.
        """
        return self._writable


def memoizedmethod(method):
    """
    Decorator that caches method result.

    Args:
        method (function): Method

    Returns:
        function: Memoized method.

    Notes:
        Target method class needs as "_cache" attribute (dict).

        It is the case of "ObjectIOBase" and all its subclasses.
    """
    method_name = method.__name__

    @wraps(method)
    def patched(self, *args, **kwargs):
        """Patched method"""
        try:
            return self._cache[method_name]

        except KeyError:
            result = self._cache[method_name] = method(self, *args, **kwargs)
            return result

    return patched


class WorkerPoolBase:
    """
    Base class that handle a worker pool.

    Args:
        max_workers (int): Maximum number of workers.
    """

    def __init__(self, max_workers=None):
        self._workers_count = max_workers

    @property  # type: ignore
    @memoizedmethod
    def _workers(self):
        """Executor pool

        Returns:
            concurrent.futures.Executor: Executor pool"""
        return ThreadPoolExecutor(max_workers=self._workers_count)

    def _generate_async(self, generator):
        """
        Return the previous generator object after having run the first element
        evaluation as a background task.

        Args:
            generator (iterable): A generator function.

        Returns:
            iterable: The generator function with first element evaluated in background.
        """
        first_value_future = self._workers.submit(next, generator)

        def get_first_element(future=first_value_future):
            """
            Get first element value from future.

            Args:
                future (concurrent.futures._base.Future): First value future.

            Returns:
                Evaluated value
            """
            try:
                yield future.result()
            except StopIteration:
                return

        return chain(get_first_element(), generator)
