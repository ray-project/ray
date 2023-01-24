import inspect
from typing import Callable, Dict, Sequence, Union

from ray import ObjectRef


def _inspect_func_object_references(base_obj):
    assert inspect.isfunction(base_obj)
    closure = inspect.getclosurevars(base_obj)
    found = False
    if closure.globals:
        print(
            f"Detected {len(closure.globals)} global variables. "
            "Checking for object references..."
        )
        for name, obj in closure.globals.items():
            found = found or isinstance(obj, ObjectRef)
            if found:
                print(f"Found an object ref: {name}={obj}")
                break

    if closure.nonlocals:
        print(
            f"Detected {len(closure.nonlocals)} nonlocal variables. "
            "Checking for object refs..."
        )
        for name, obj in closure.nonlocals.items():
            found = found or isinstance(obj, ObjectRef)
            if found:
                print(f"Found an object ref: {name}={obj}")
                break
    return found


def contains_object_refs(base_obj: Union[Dict, Sequence, Callable]) -> bool:
    if isinstance(base_obj, dict):
        return any(isinstance(v, ObjectRef) for v in base_obj.values())
    elif isinstance(base_obj, (list, tuple)):
        return any(isinstance(v, ObjectRef) for v in base_obj)
    elif inspect.isfunction(base_obj):
        return _inspect_func_object_references(base_obj)
    else:
        raise NotImplementedError(
            f"Checking for object references in type {type(base_obj)} "
            "is not implemented."
        )
