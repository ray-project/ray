from pickle import HIGHEST_PROTOCOL, PickleError, PicklingError, UnpicklingError
from pickle import _load as load
from pickle import _loads as loads
from pickle import _Pickler
from pickle import _Unpickler as Unpickler

__all__ = [
    "PickleError",
    "PicklingError",
    "UnpicklingError",
    "Pickler",
    "Unpickler",
    "load",
    "loads",
    "HIGHEST_PROTOCOL",
]


class Pickler(_Pickler):
    def __init__(self, file, protocol=None, *, fix_imports=True, buffer_callback=None):
        super().__init__(
            file, protocol, fix_imports=fix_imports, buffer_callback=buffer_callback
        )
        # avoid being overrided by cloudpickle
        self.dispatch = _Pickler.dispatch.copy()
