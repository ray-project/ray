def type_to_string(_type):
    if _type.__module__ == 'typing':
        return str(_type)
    else:
        return f"{_type.__module__}.{_type.__name__}"
