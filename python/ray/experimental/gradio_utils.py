def type_to_string(_type: type) -> str:
    """Gets the string representation of a type.

    THe original type can be derived from the returned string representation through
    pydoc.locate().
    """
    if _type.__module__ == "typing":
        return f"{_type.__module__}.{_type._name}"
    elif _type.__module__ == "builtins":
        return _type.__name__
    else:
        return f"{_type.__module__}.{_type.__name__}"
