"""Selection of the sandbox backend that ``SandboxRuntime`` runs on.

gVisor is the default. Another backend is chosen by name, through the
``backend`` argument of ``SandboxRuntime`` or the ``RAY_SANDBOX_BACKEND``
environment variable, where a name is one of:

* a name passed to :func:`register_backend`, such as the built-in ``gvisor``;
* a name in the ``ray.sandbox_backends`` entry point group of an installed
  package (looked up once per name and process: a package installed later in
  the process isn't found);
* an import path ``package.module:ClassName``.

The backend is built as ``factory(image_manager=image_manager)``. A factory
whose ``needs_image_manager`` attribute is False (``BaseSandboxBackend``
subclasses inherit True) gets ``image_manager=None``, so that the runtime does
not build an ``ImageManager`` for a backend that pulls images elsewhere.
"""

import importlib
import os
import threading
from typing import Any, Callable, Dict, List, Optional

RAY_SANDBOX_BACKEND_ENV = "RAY_SANDBOX_BACKEND"
DEFAULT_BACKEND = "gvisor"
ENTRY_POINT_GROUP = "ray.sandbox_backends"

_lock = threading.Lock()
_factories: Dict[str, Callable[..., Any]] = {}
# Entry point lookups, cached: scanning the installed distributions is slow.
_entry_point_cache: Dict[str, Optional[Callable[..., Any]]] = {}


def _gvisor_factory(image_manager: Optional[Any] = None) -> Any:
    from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend

    return GVisorSandboxBackend(image_manager=image_manager)


def register_backend(name: str, factory: Callable[..., Any]) -> None:
    """Make a backend selectable by name.

    Args:
        name: Name to select the backend with, case-insensitive.
        factory: Callable taking ``image_manager`` as a keyword argument
            and returning a ``BaseSandboxBackend``, such as the class.
    """
    with _lock:
        _factories[name.strip().lower()] = factory


def registered_backends() -> List[str]:
    """The names given to :func:`register_backend`, lower case."""
    with _lock:
        return sorted(_factories)


def load_import_path(path: str) -> Any:
    """Return the object that a ``package.module:attr.sub`` path names."""
    module_name, _, attr = path.partition(":")
    if not module_name or not attr:
        raise ValueError(f"Invalid sandbox backend import path {path!r}")
    obj: Any = importlib.import_module(module_name)
    for part in attr.split("."):
        obj = getattr(obj, part)
    return obj


def _load_entry_point(name: str) -> Optional[Callable[..., Any]]:
    with _lock:
        if name in _entry_point_cache:
            return _entry_point_cache[name]
    from importlib.metadata import entry_points

    factory = None
    for ep in entry_points(group=ENTRY_POINT_GROUP):
        if ep.name.lower() == name:
            factory = ep.load()
            break
    with _lock:
        _entry_point_cache[name] = factory
    return factory


def resolve_backend_factory(name: Optional[str] = None) -> Callable[..., Any]:
    """Return the factory of the named backend.

    Args:
        name: Backend name; None reads ``RAY_SANDBOX_BACKEND`` and falls back
            to ``gvisor``.

    Returns:
        A callable that builds the backend.
    """
    if name is None:
        name = os.environ.get(RAY_SANDBOX_BACKEND_ENV, "") or DEFAULT_BACKEND
    name = name.strip()
    if ":" in name:
        return load_import_path(name)
    key = name.lower()
    with _lock:
        factory = _factories.get(key)
    if factory is None:
        factory = _load_entry_point(key)
    if factory is None:
        with _lock:
            known = sorted(_factories)
        raise ValueError(
            f"Unknown sandbox backend {name!r}: not one of {known}, not in the "
            f"{ENTRY_POINT_GROUP!r} entry point group, and not a "
            "'module:Class' import path"
        )
    return factory


def needs_image_manager(factory: Callable[..., Any]) -> bool:
    """Whether ``factory`` builds a backend that uses an ``ImageManager``."""
    return bool(getattr(factory, "needs_image_manager", True))


def create_backend(name: Optional[str] = None, image_manager: Optional[Any] = None):
    """Build the named backend (see :func:`resolve_backend_factory`).

    ``image_manager`` is passed only to a factory that needs one.
    """
    factory = resolve_backend_factory(name)
    if not needs_image_manager(factory):
        image_manager = None
    return factory(image_manager=image_manager)


register_backend(DEFAULT_BACKEND, _gvisor_factory)
