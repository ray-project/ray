import importlib


def load_function_or_class(path):
    """Load a function or class at runtime given a full path.

    Example of the path: mypkg.mysubpkg.myclass
    """
    class_data = path.split(".")
    if len(class_data) < 2:
        raise ValueError("You need to pass a valid path like mymodule.provider_class")
    module_path = ".".join(class_data[:-1])
    fn_or_class_str = class_data[-1]
    module = importlib.import_module(module_path)
    return getattr(module, fn_or_class_str)
