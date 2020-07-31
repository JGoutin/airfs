from typing import Any, Dict, Optional

STORAGE_PACKAGE: Any
MOUNTED: Dict[Any, Dict[str, Any]]
AUTOMOUNT: Any

def get_instance(
    name: Any,
    cls: str = ...,
    storage: Optional[Any] = ...,
    storage_parameters: Optional[Any] = ...,
    unsecure: Optional[Any] = ...,
    *args: Any,
    **kwargs: Any
): ...
def mount(
    storage: Optional[Any] = ...,
    name: str = ...,
    storage_parameters: Optional[Any] = ...,
    unsecure: Optional[Any] = ...,
    extra_root: Optional[Any] = ...,
): ...
