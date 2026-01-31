# source: libcoreworker.pxi
from types import TracebackType
from typing import Union

class ProfileEvent:
    """Cython wrapper class of C++ `ray::core::worker::ProfileEvent`."""

    def set_extra_data(self, extra_data: Union[str, bytes] ) -> None: ...

    def __enter__(self) -> None: ...

    def __exit__(self, type: type[Exception], value: Exception, tb: TracebackType): ...
