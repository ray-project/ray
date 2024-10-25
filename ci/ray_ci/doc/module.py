import importlib
import inspect
from types import ModuleType
from typing import List

from ci.ray_ci.doc.api import API, AnnotationType, CodeType


class Module:
    """
    Module class represents the top level module to walk through and find annotated
    APIs.
    """

    def __init__(self, module: str):
        self._module = importlib.import_module(module)
        self._visited = set()
        self._apis = []

    def walk(self) -> None:
        self._walk(self._module)

    def get_apis(self) -> List[API]:
        self.walk()
        return self._apis

    def _walk(self, module: ModuleType) -> None:
        """
        Depth-first search through the module and its children to find annotated classes
        and functions.
        """
        if module.__hash__ in self._visited:
            return
        self._visited.add(module.__hash__)

        if not self._is_valid_child(module):
            return

        for child in dir(module):
            attribute = getattr(module, child)

            if inspect.ismodule(attribute):
                self._walk(attribute)
            if inspect.isclass(attribute):
                if self._is_api(attribute):
                    self._apis.append(
                        API(
                            name=self._fullname(attribute),
                            annotation_type=self._get_annotation_type(attribute),
                            code_type=CodeType.CLASS,
                        )
                    )
                self._walk(attribute)
            if inspect.isfunction(attribute):
                if self._is_api(attribute):
                    self._apis.append(
                        API(
                            name=self._fullname(attribute),
                            annotation_type=self._get_annotation_type(attribute),
                            code_type=CodeType.FUNCTION,
                        )
                    )

        return

    def _fullname(self, module: ModuleType) -> str:
        return f"{module.__module__}.{module.__qualname__}"

    def _is_valid_child(self, module: ModuleType) -> bool:
        """
        This module is a valid child of the top level module if it is the top level
        module itself, or its module name starts with the top level module name.
        """
        module = inspect.getmodule(module)
        if not hasattr(module, "__name__"):
            return False
        return module.__name__.startswith(self._module.__name__)

    def _is_api(self, module: ModuleType) -> bool:
        return self._is_valid_child(module) and hasattr(module, "_annotated")

    def _get_annotation_type(self, module: ModuleType) -> AnnotationType:
        return AnnotationType(module._annotated_type.value)
