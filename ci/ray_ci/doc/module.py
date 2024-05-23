import importlib
import inspect
from types import ModuleType
from typing import Set


class Module:
    def __init__(self, module: str):
        self._module = importlib.import_module(module)
        self._visited = set()
        self._class_apis = set()
        self._function_apis = set()

    def walk(self) -> None:
        self._walk(self._module)

    def get_class_apis(self) -> Set:
        self.walk()
        return self._class_apis

    def get_function_apis(self) -> Set:
        self.walk()
        return self._function_apis

    def _walk(self, module: ModuleType) -> None:
        """
        Depth-first search through the module and its children to find annotated classes
        and functions.
        """
        if module in self._visited:
            return
        self._visited.add(module)

        if not self._is_valid_child(module):
            return

        for child in dir(module):
            attribute = getattr(module, child)

            if inspect.ismodule(attribute):
                self._walk(attribute)
            if inspect.isclass(attribute):
                if self._is_api(attribute):
                    self._class_apis.add(self._fullname(attribute))
                self._walk(attribute)
            if inspect.isfunction(attribute):
                if self._is_api(attribute):
                    self._function_apis.add(self._fullname(attribute))

        return

    def _fullname(self, module: ModuleType) -> str:
        return f"{module.__module__}.{module.__qualname__}"

    def _is_valid_child(self, module: ModuleType) -> bool:
        """
        This module is a valid child of the top level module if its module name
        starts with the top level module name.
        """
        return inspect.getmodule(module).__name__.startswith(self._module.__name__)

    def _is_api(self, module: ModuleType) -> bool:
        return self._is_valid_child(module) and hasattr(module, "_attribute")
