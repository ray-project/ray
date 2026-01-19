"""Expose datasources and datasinks in ray.data._internal.datasource.

This module dynamically imports each submodule in the package and re-exports
names listed in the submodule's __all__ attribute.
"""

from importlib import import_module
from pkgutil import iter_modules

__all__ = []

for _loader, _module_name, _is_pkg in iter_modules(__path__):
    if _module_name.startswith("_"):
        continue
    _module = import_module(f"{__name__}.{_module_name}")
    _exported_names = getattr(_module, "__all__", [])
    for _name in _exported_names:
        globals()[_name] = getattr(_module, _name)
    __all__.extend(_exported_names)

__all__.sort()
