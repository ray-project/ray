from typing import Dict, Type

from .collective_group.base_collective_group import BaseGroup


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
            raise ValueError(f"Backend {name} already registered")
        self._map[name.upper()] = group_cls

    def get(self, name: str) -> Type[BaseGroup]:
        name = name.upper()
        if name not in self._map:
            raise ValueError(f"Backend {name} not registered")
        return self._map[name]

    def check(self, name: str) -> bool:
        try:
            cls = self.get(name)
            return cls.check_backend_availability()
        except (ValueError, AttributeError):
            return False

    def list_backends(self) -> list:
        return list(self._map.keys())


_global_registry = BackendRegistry()


def register_collective_backend(name: str, group_cls: Type[BaseGroup]) -> None:
    _global_registry.put(name, group_cls)


def get_backend_registry() -> BackendRegistry:
    return _global_registry
