import importlib
import logging
from typing import List

logger = logging.getLogger(__name__)


def import_attr(full_path: str):
    """Given a full import path to a module attr, return the imported attr.

    For example, the following are equivalent:
        MyClass = import_attr("module.submodule:MyClass")
        MyClass = import_attr("module.submodule.MyClass")
        from module.submodule import MyClass

    Returns:
        Imported attr
    """
    if full_path is None:
        raise TypeError("import path cannot be None")

    if ":" in full_path:
        if full_path.count(":") > 1:
            raise ValueError(
                f'Got invalid import path "{full_path}". An '
                "import path may have at most one colon."
            )
        module_name, attr_name = full_path.split(":")
    else:
        last_period_idx = full_path.rfind(".")
        module_name = full_path[:last_period_idx]
        attr_name = full_path[last_period_idx + 1 :]

    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def try_import_each_module(module_names_to_import: List[str]) -> None:
    """
    Make a best-effort attempt to import each named Python module.
    This is used by the Python default_worker.py to preload modules.
    """
    for module_to_preload in module_names_to_import:
        try:
            importlib.import_module(module_to_preload)
        except ImportError:
            logger.exception(f'Failed to preload the module "{module_to_preload}"')
