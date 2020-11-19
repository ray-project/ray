"""A utility for debugging serialization issues."""

import inspect
import ray.cloudpickle as cp
from contextlib import contextmanager


@contextmanager
def _indent(printer):
    printer.level += 1
    yield
    printer.level -= 1


class _Printer:
    def __init__(self):
        self.level = 0

    def indent(self):
        return _indent(self)

    def print(self, msg):
        indent = "    " * self.level
        print(indent + msg)


_printer = _Printer()


def _inspect_func_serialization(base_obj, depth, failure_set):
    assert inspect.isfunction(base_obj)
    closure = inspect.getclosurevars(base_obj)
    found = False
    if closure.globals:
        _printer.print(f"Detected {len(closure.globals)} global variables. "
                       "Checking serializability...")

        with _printer.indent():
            for name, obj in closure.globals.items():
                serializable, _ = inspect_serializability(
                    obj, name=name, depth=depth - 1, _failure_set=failure_set)
                found = found or not serializable
                if found:
                    break

    if closure.nonlocals:
        _printer.print(
            f"Detected {len(closure.nonlocals)} nonlocal variables. "
            "Checking serializability...")
        with _printer.indent():
            for name, obj in closure.nonlocals.items():
                serializable, _ = inspect_serializability(
                    obj, name=name, depth=depth - 1, _failure_set=failure_set)
                found = found or not serializable
                if found:
                    break
    if not found:
        _printer.print(
            f"WARNING: Did not find non-serializable object in {base_obj}. "
            "This may be an oversight.")
    return found


def _inspect_type_serialization(base_obj, depth, failure_set):
    assert not inspect.isfunction(base_obj)
    functions = inspect.getmembers(base_obj, predicate=inspect.isfunction)
    found = False
    with _printer.indent():
        for name, obj in functions:
            serializable, _ = inspect_serializability(
                obj, name=name, depth=depth - 1, _failure_set=failure_set)
            found = found or not serializable
            if found:
                break

    with _printer.indent():
        members = inspect.getmembers(base_obj)
        for name, obj in members:
            if name.startswith("__") and name.endswith(
                    "__") or inspect.isbuiltin(obj):
                continue
            serializable, _ = inspect_serializability(
                obj, name=name, depth=depth - 1, _failure_set=failure_set)
            found = found or not serializable
            if found:
                break
    if not found:
        _printer.print(
            f"WARNING: Did not find non-serializable object in {base_obj}. "
            "This may be an oversight.")
    return found


def inspect_serializability(base_obj, name=None, depth=3, _failure_set=None):
    """Identifies what objects are preventing serialization.

    Args:
        base_obj: Object to be serialized.
        name (str): Optional name of string.
        depth (int): Depth of the scope stack to walk through. Defaults to 3.

    Returns:
        True if serializable.
        set: Set of objects that cloudpickle is unable to serialize.
    """
    top_level = False
    found = False
    if _failure_set is None:
        top_level = True
        _failure_set = set()

    if name is None:
        name = str(base_obj)
    _printer.print(f"Serializing {base_obj}[name='{name}']...")
    try:
        cp.dumps(base_obj)
        _printer.print("...serialization succeeded!")
        return True, _failure_set
    except Exception as e:
        _printer.print(f"...serialization FAILED: {e}")
        found = True
        try:
            _failure_set.add((name, base_obj))
        except Exception:
            pass

    if depth <= 0:
        return False, _failure_set

    if inspect.isfunction(base_obj):
        _inspect_func_serialization(
            base_obj, depth=depth, failure_set=_failure_set)
    else:
        _inspect_type_serialization(
            base_obj, depth=depth, failure_set=_failure_set)

    if top_level:
        print("Result:\n")
        if not _failure_set:
            print("  Nothing failed the diagnostic test, though "
                  "serialization did not succeed. Feel free to raise an "
                  "issue on github.")
        else:
            print(
                f"  Variable(s) {_failure_set} found to be non-serializable. "
                "\n\nConsider either removing the instantiation/imports "
                "of these objects or moving the "
                "instantiation into the scope of the function/class. ")
    return not found, _failure_set
