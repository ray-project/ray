from typing import Dict, Type

from .collective_group.base_collective_group import BaseGroup
from ray.util.annotations import PublicAPI


class BackendRegistry:
    _instance = None
    _map: Dict[str, Type[BaseGroup]]

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(BackendRegistry, cls).__new__(cls)
            cls._instance._map = {}
        return cls._instance

    def put(self, name: str, group_cls: Type[BaseGroup]) -> None:
        if not issubclass(group_cls, BaseGroup):
            raise TypeError(f"{group_cls} is not a subclass of BaseGroup")
        if name.upper() in self._map:
            raise ValueError(f"Backend {name.upper()} already registered")
        self._map[name.upper()] = group_cls

    def get(self, name: str) -> Type[BaseGroup]:
        name = name.upper()
        if name not in self._map:
            raise ValueError(f"Backend {name} not registered")
        return self._map[name]

    def is_registered(self, name: str) -> bool:
        """Check if a backend is registered (regardless of availability)."""
        return name.upper() in self._map

    def check(self, name: str) -> bool:
        """Check if a backend is both registered and available."""
        try:
            cls = self.get(name)
            return cls.check_backend_availability()
        except (ValueError, AttributeError):
            return False


_global_registry = BackendRegistry()


@PublicAPI(stability="alpha")
def register_collective_backend(name: str, group_cls: Type[BaseGroup]):
    """Register a custom collective backend with Ray.

    This function registers a custom backend class that can be used for
    collective operations. The backend must be a subclass of
    :class:`~ray.util.collective.collective_group.base_collective_group.BaseGroup`
    and implement all required collective operations.

    Important: The backend must be registered on both the driver and all
    actors before creating collective groups. This is because each process
    (driver and each actor) needs to know about your backend class to
    instantiate it.

    Args:
        name: The name of the backend (e.g., "MY_BACKEND"). This will be
            automatically added to the Backend enum as Backend.MY_BACKEND.
        group_cls: The backend class, which must be a subclass of
            :class:`~ray.util.collective.collective_group.base_collective_group.BaseGroup`.

    Example:
        >>> import ray
        >>> from ray.util.collective import create_collective_group, init_collective_group
        >>> from ray.util.collective.backend_registry import register_collective_backend
        >>> from ray.util.collective.collective_group.base_collective_group import BaseGroup
        >>>
        >>> class MyCustomBackend(BaseGroup):
        ...     def __init__(self, world_size, rank, group_name):
        ...         super().__init__(world_size, rank, group_name)
        ...     @classmethod
        ...     def backend(cls):
        ...         return "MY_BACKEND"
        ...     @classmethod
        ...     def check_backend_availability(cls) -> bool:
        ...         return True
        ...     def allreduce(self, tensor, allreduce_options=None):
        ...         pass
        ...     def broadcast(self, tensor, broadcast_options=None):
        ...         pass
        ...     def barrier(self, barrier_options=None):
        ...         pass
        >>>
        >>> # Register on the driver
        >>> register_collective_backend("MY_BACKEND", MyCustomBackend)
        >>>
        >>> ray.init()
        >>>
        >>> @ray.remote
        ... class Worker:
        ...     def __init__(self, rank):
        ...         self.rank = rank
        ...     def setup(self, world_size):
        ...         # IMPORTANT: Register on each worker too
        ...         register_collective_backend("MY_BACKEND", MyCustomBackend)
        ...         init_collective_group(
        ...             world_size=world_size,
        ...             rank=self.rank,
        ...             backend="MY_BACKEND",
        ...             group_name="default",
        ...         )
        >>>
        >>> actors = [Worker.remote(rank=i) for i in range(2)]
        >>> create_collective_group(
        ...     actors=actors,
        ...     world_size=2,
        ...     ranks=[0, 1],
        ...     backend="MY_BACKEND",
        ...     group_name="default",
        ... )
        >>> ray.get([a.setup.remote(2) for a in actors])
    """
    _global_registry.put(name, group_cls)
    from . import types

    upper_name = name.upper()
    if not hasattr(types.Backend, upper_name):
        setattr(types.Backend, upper_name, upper_name)
