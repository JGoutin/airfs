import abc
from abc import ABC, abstractmethod
from airfs._core.io_base import WorkerPoolBase
from typing import Any, Optional, Tuple

class SystemBase(ABC, WorkerPoolBase, metaclass=abc.ABCMeta):
    _SIZE_KEYS: Tuple[str, ...]
    _CTIME_KEYS: Tuple[str, ...]
    _MTIME_KEYS: Tuple[str, ...]
    def __init__(
        self,
        storage_parameters: Optional[Any] = ...,
        unsecure: bool = ...,
        roots: Optional[Any] = ...,
        **_: Any
    ) -> None: ...
    @property
    def storage(self): ...
    @property
    def client(self): ...
    def copy(self, src: Any, dst: Any, other_system: Optional[Any] = ...) -> None: ...
    def exists(
        self,
        path: Optional[Any] = ...,
        client_kwargs: Optional[Any] = ...,
        assume_exists: Optional[Any] = ...,
    ): ...
    @abstractmethod
    def get_client_kwargs(self, path: Any) -> Any: ...
    def getctime(
        self,
        path: Optional[Any] = ...,
        client_kwargs: Optional[Any] = ...,
        header: Optional[Any] = ...,
    ): ...
    def getmtime(
        self,
        path: Optional[Any] = ...,
        client_kwargs: Optional[Any] = ...,
        header: Optional[Any] = ...,
    ): ...
    def getsize(
        self,
        path: Optional[Any] = ...,
        client_kwargs: Optional[Any] = ...,
        header: Optional[Any] = ...,
    ): ...
    def isdir(
        self,
        path: Optional[Any] = ...,
        client_kwargs: Optional[Any] = ...,
        virtual_dir: bool = ...,
        assume_exists: Optional[Any] = ...,
    ): ...
    def isfile(
        self,
        path: Optional[Any] = ...,
        client_kwargs: Optional[Any] = ...,
        assume_exists: Optional[Any] = ...,
    ): ...
    @property
    def storage_parameters(self): ...
    def head(
        self,
        path: Optional[Any] = ...,
        client_kwargs: Optional[Any] = ...,
        header: Optional[Any] = ...,
    ): ...
    @property
    def roots(self): ...
    @roots.setter
    def roots(self, roots: Any) -> None: ...
    def relpath(self, path: Any): ...
    def is_locator(self, path: Any, relative: bool = ...): ...
    def split_locator(self, path: Any): ...
    def make_dir(self, path: Any, relative: bool = ...) -> None: ...
    def remove(self, path: Any, relative: bool = ...) -> None: ...
    def ensure_dir_path(self, path: Any, relative: bool = ...): ...
    def list_objects(
        self,
        path: str = ...,
        relative: bool = ...,
        first_level: bool = ...,
        max_results: Optional[Any] = ...,
    ) -> None: ...
    def islink(
        self,
        path: Optional[Any] = ...,
        client_kwargs: Optional[Any] = ...,
        header: Optional[Any] = ...,
    ): ...
    def stat(
        self,
        path: Optional[Any] = ...,
        client_kwargs: Optional[Any] = ...,
        header: Optional[Any] = ...,
    ): ...
    def read_link(
        self,
        path: Optional[Any] = ...,
        client_kwargs: Optional[Any] = ...,
        header: Optional[Any] = ...,
        recursive: bool = ...,
    ) -> None: ...
    def shareable_url(self, path: Any, expires_in: Any) -> None: ...
