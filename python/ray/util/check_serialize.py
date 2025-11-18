"""A utility for debugging serialization issues."""
import inspect
from contextlib import contextmanager
from typing import Any, Optional, Set, Tuple

import colorama

# Import ray first to use the bundled colorama
import ray  # noqa: F401
import ray.cloudpickle as cp
from ray.util.annotations import DeveloperAPI


@contextmanager
def _indent(printer):
    printer.level += 1
    yield
    printer.level -= 1


class _Printer:
    def __init__(self, print_file):
        self.level = 0
        self.print_file = print_file

    def indent(self):
        return _indent(self)

    def print(self, msg):
        indent = "    " * self.level
        print(indent + msg, file=self.print_file)


@DeveloperAPI
class FailureTuple:
    """Represents the serialization 'frame'.

    Attributes:
        obj: The object that fails serialization.
        name: The variable name of the object.
        parent: The object that references the `obj`.
    """

    def __init__(self, obj: Any, name: str, parent: Any):
        self.obj = obj
        self.name = name
        self.parent = parent

    def __repr__(self):
        return f"FailTuple({self.name} [obj={self.obj}, parent={self.parent}])"


def _inspect_func_serialization(base_obj, depth, parent, failure_set, printer):
    """Adds the first-found non-serializable element to the failure_set."""
    assert inspect.isfunction(base_obj)
    closure = inspect.getclosurevars(base_obj)
    found = False
    if closure.globals:
        printer.print(
            f"Detected {len(closure.globals)} global variables. "
            "Checking serializability..."
        )

        with printer.indent():
            for name, obj in closure.globals.items():
                serializable, _ = _inspect_serializability(
                    obj,
                    name=name,
                    depth=depth - 1,
                    parent=parent,
                    failure_set=failure_set,
                    printer=printer,
                )
                found = found or not serializable
                if found:
                    break

    if closure.nonlocals:
        printer.print(
            f"Detected {len(closure.nonlocals)} nonlocal variables. "
            "Checking serializability..."
        )
        with printer.indent():
            for name, obj in closure.nonlocals.items():
                serializable, _ = _inspect_serializability(
                    obj,
                    name=name,
                    depth=depth - 1,
                    parent=parent,
                    failure_set=failure_set,
                    printer=printer,
                )
                found = found or not serializable
                if found:
                    break
    if not found:
        printer.print(
            f"WARNING: Did not find non-serializable object in {base_obj}. "
            "This may be an oversight."
        )
    return found


def _inspect_generic_serialization(base_obj, depth, parent, failure_set, printer):
    """Adds the first-found non-serializable element to the failure_set."""
    assert not inspect.isfunction(base_obj)
    functions = inspect.getmembers(base_obj, predicate=inspect.isfunction)
    found = False
    with printer.indent():
        for name, obj in functions:
            serializable, _ = _inspect_serializability(
                obj,
                name=name,
                depth=depth - 1,
                parent=parent,
                failure_set=failure_set,
                printer=printer,
            )
            found = found or not serializable
            if found:
                break

    with printer.indent():
        members = inspect.getmembers(base_obj)
        for name, obj in members:
            if name.startswith("__") and name.endswith("__") or inspect.isbuiltin(obj):
                continue
            serializable, _ = _inspect_serializability(
                obj,
                name=name,
                depth=depth - 1,
                parent=parent,
                failure_set=failure_set,
                printer=printer,
            )
            found = found or not serializable
            if found:
                break
    if not found:
        printer.print(
            f"WARNING: Did not find non-serializable object in {base_obj}. "
            "This may be an oversight."
        )
    return found


@DeveloperAPI
def inspect_serializability(
    base_obj: Any,
    name: Optional[str] = None,
    depth: int = 3,
    print_file: Optional[Any] = None,
) -> Tuple[bool, Set[FailureTuple]]:
    """Identifies what objects are preventing serialization.

    Args:
        base_obj: Object to be serialized.
        name: Optional name of string.
        depth: Depth of the scope stack to walk through. Defaults to 3.
        print_file: file argument that will be passed to print().

    Returns:
        bool: True if serializable.
        set[FailureTuple]: Set of unserializable objects.

    .. versionadded:: 1.1.0

    """
    printer = _Printer(print_file)
    return _inspect_serializability(base_obj, name, depth, None, None, printer)


def _inspect_serializability(
    base_obj, name, depth, parent, failure_set, printer
) -> Tuple[bool, Set[FailureTuple]]:
    colorama.init()
    top_level = False
    declaration = ""
    found = False
    if failure_set is None:
        top_level = True
        failure_set = set()
        declaration = f"Checking Serializability of {base_obj}"
        printer.print("=" * min(len(declaration), 80))
        printer.print(declaration)
        printer.print("=" * min(len(declaration), 80))

        if name is None:
            name = str(base_obj)
    else:
        printer.print(f"Serializing '{name}' {base_obj}...")
    try:
        cp.dumps(base_obj)
        return True, failure_set
    except Exception as e:
        printer.print(
            f"{colorama.Fore.RED}!!! FAIL{colorama.Fore.RESET} " f"serialization: {e}"
        )
        found = True
        try:
            if depth == 0:
                failure_set.add(FailureTuple(base_obj, name, parent))
        # Some objects may not be hashable, so we skip adding this to the set.
        except Exception:
            pass

    if depth <= 0:
        return False, failure_set

    # TODO: we only differentiate between 'function' and 'object'
    # but we should do a better job of diving into something
    # more specific like a Type, Object, etc.
    if inspect.isfunction(base_obj):
        _inspect_func_serialization(
            base_obj,
            depth=depth,
            parent=base_obj,
            failure_set=failure_set,
            printer=printer,
        )
    else:
        _inspect_generic_serialization(
            base_obj,
            depth=depth,
            parent=base_obj,
            failure_set=failure_set,
            printer=printer,
        )

    if not failure_set:
        failure_set.add(FailureTuple(base_obj, name, parent))

    if top_level:
        printer.print("=" * min(len(declaration), 80))
        if not failure_set:
            printer.print(
                "Nothing failed the inspect_serialization test, though "
                "serialization did not succeed."
            )
        else:
            fail_vars = (
                f"\n\n\t{colorama.Style.BRIGHT}"
                + "\n".join(str(k) for k in failure_set)
                + f"{colorama.Style.RESET_ALL}\n\n"
            )
            printer.print(
                f"Variable: {fail_vars}was found to be non-serializable. "
                "There may be multiple other undetected variables that were "
                "non-serializable. "
            )
            printer.print(
                "Consider either removing the "
                "instantiation/imports of these variables or moving the "
                "instantiation into the scope of the function/class. "
            )
        printer.print("=" * min(len(declaration), 80))
        printer.print(
            "Check https://docs.ray.io/en/master/ray-core/objects/serialization.html#troubleshooting for more information."  # noqa
        )
        printer.print(
            "If you have any suggestions on how to improve "
            "this error message, please reach out to the "
            "Ray developers on github.com/ray-project/ray/issues/"
        )
        printer.print("=" * min(len(declaration), 80))
    return not found, failure_set
