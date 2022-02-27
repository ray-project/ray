import inspect


def is_cython(obj):
    """Check if an object is a Cython function or method"""

    # TODO(suo): We could split these into two functions, one for Cython
    # functions and another for Cython methods.
    # TODO(suo): There doesn't appear to be a Cython function 'type' we can
    # check against via isinstance. Please correct me if I'm wrong.
    def check_cython(x):
        return type(x).__name__ == "cython_function_or_method"

    # Check if function or method, respectively
    return check_cython(obj) or (
        hasattr(obj, "__func__") and check_cython(obj.__func__)
    )


def is_function_or_method(obj):
    """Check if an object is a function or method.

    Args:
        obj: The Python object in question.

    Returns:
        True if the object is an function or method.
    """
    return inspect.isfunction(obj) or inspect.ismethod(obj) or is_cython(obj)


def is_class_method(f):
    """Returns whether the given method is a class_method."""
    return hasattr(f, "__self__") and f.__self__ is not None


def is_static_method(cls, f_name):
    """Returns whether the class has a static method with the given name.

    Args:
        cls: The Python class (i.e. object of type `type`) to
            search for the method in.
        f_name: The name of the method to look up in this class
            and check whether or not it is static.
    """
    for cls in inspect.getmro(cls):
        if f_name in cls.__dict__:
            return isinstance(cls.__dict__[f_name], staticmethod)
    return False
