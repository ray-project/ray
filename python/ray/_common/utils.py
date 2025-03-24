import importlib


def import_attr(full_path: str, *, reload_module: bool = False):
    """Given a full import path to a module attr, return the imported attr.

    If `reload_module` is set, the module will be reloaded using `importlib.reload`.

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
    if reload_module:
        importlib.reload(module)
    return getattr(module, attr_name)
