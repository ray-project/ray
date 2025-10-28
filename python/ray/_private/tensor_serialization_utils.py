import warnings
from typing import Any, Callable, Optional, Set


class TensorObjRestoreWarning(UserWarning):
    """Warning issued when a tensor-nested object is reconstructed without calling __init__."""

    pass


warnings.filterwarnings("once", category=TensorObjRestoreWarning)

_ZERO_COPY_MAKER_KEY = "_ray_zc_k_"
_ZERO_COPY_MAKER_VALUE = "_ray_zc_v_"
_ZERO_COPY_DATA = "_ray_zc_d_"
_ZERO_COPY_PLACEMENT = "_ray_zc_pl_"


def _is_restorable(obj) -> bool:
    """
    Heuristic to determine if an object is likely safe to restore
    via object.__new__ + __dict__ assignment.
    """
    typ = type(obj)

    # Skip built-in or well-known non-user types
    if typ.__module__ in ("builtins", "torch", "numpy", "collections", "typing"):
        return False

    # Must have __dict__
    if not hasattr(obj, "__dict__"):
        return False

    # Must NOT have meaningful __slots__
    if hasattr(typ, "__slots__"):
        slots = typ.__slots__
        if slots is not None and slots != ():
            return False

    # Skip if it's a type that looks like a function/callable
    if callable(obj) or isinstance(obj, type):
        return False

    return True


def _is_namedtuple_instance(obj) -> bool:
    """Check if obj is an instance of a namedtuple."""
    typ = type(obj)
    # namedtuple instances are tuple subclasses with _fields attribute
    return (
        isinstance(obj, tuple)
        and hasattr(typ, "_fields")
        and hasattr(typ, "_make")
        and hasattr(typ, "_asdict")
    )


def _walk_and_transform(
    obj: Any,
    convert_node: Callable[[Any], Any],
    _visited: Optional[Set[int]] = None,
) -> Any:
    """
    Recursively traverses obj and applies convert_node to each node.
    Stops recursing into a node if convert_node returns a new object.
    Handles circular references and supports dict/list/tuple/custom objects.
    """
    if _visited is None:
        _visited = set()

    obj_id = id(obj)
    if obj_id in _visited:
        return obj

    converted = convert_node(obj)
    if converted is not obj:
        return converted

    should_recurse = isinstance(obj, (dict, list, tuple)) or _is_restorable(obj)

    if should_recurse:
        _visited.add(obj_id)

    if isinstance(obj, dict):
        new_dict = {
            k: _walk_and_transform(v, convert_node, _visited) for k, v in obj.items()
        }
        orig_type = type(obj)
        if orig_type is dict:
            return new_dict

        try:
            if orig_type.__module__ == "collections" or (
                orig_type.__module__ == "collections.abc"
                and orig_type.__name__ in ("Counter", "OrderedDict", "defaultdict")
            ):
                if orig_type.__name__ == "defaultdict":
                    factory = getattr(obj, "default_factory", None)
                    return orig_type(factory, new_dict)
                else:
                    return orig_type(new_dict)
        except Exception:
            pass

        return new_dict

    elif isinstance(obj, (list, tuple)) and not _is_namedtuple_instance(obj):
        converted_items = [
            _walk_and_transform(item, convert_node, _visited) for item in obj
        ]
        return type(obj)(converted_items)

    elif _is_restorable(obj):
        typ = type(obj)
        # Warn if __init__ is non-trivial
        if typ.__init__ is not object.__init__:
            warnings.warn(
                f"Reconstructing instance of {typ.__module__}.{typ.__qualname__} "
                f"without calling __init__; object state may be inconsistent.",
                TensorObjRestoreWarning,
                stacklevel=3,
            )

        try:
            new_obj = object.__new__(typ)
            new_obj.__dict__ = {
                k: _walk_and_transform(v, convert_node, _visited)
                for k, v in obj.__dict__.items()
            }
            return new_obj
        except Exception:
            return obj

    else:
        # Return primitives or unsupported types as-is.
        return obj


def _to_numpy_node(node):
    """
    Convert a single tensor node to a marked NumPy-compatible dict.
    Non-tensor nodes are returned unchanged.
    """
    try:
        import torch
    except ImportError:
        return node
    if isinstance(node, torch.Tensor):
        return {
            _ZERO_COPY_MAKER_KEY: _ZERO_COPY_MAKER_VALUE,
            _ZERO_COPY_DATA: node.detach().cpu().contiguous().numpy(),
            _ZERO_COPY_PLACEMENT: str(node.device),
        }
    return node


def serialize_tensor_to_numpy(obj: Any) -> Any:
    """
    Recursively converts tensors in a nested object to NumPy arrays.
    Supports nested dicts, lists, tuples, and custom objects (via `__dict__`).
    Non-tensor values (e.g., str, int, np.ndarray) are returned unchanged.
    """
    return _walk_and_transform(obj, _to_numpy_node)


def _to_tensor_node(node):
    """
    Convert a marked NumPy dict back to a torch.Tensor on the original device.
    Non-marked nodes are returned unchanged.
    """
    if not (
        isinstance(node, dict)
        and node.get(_ZERO_COPY_MAKER_KEY) == _ZERO_COPY_MAKER_VALUE
    ):
        return node

    try:
        import torch

        np_array = node[_ZERO_COPY_DATA]
        device_str = node.get(_ZERO_COPY_PLACEMENT, "cpu")
        tensor = torch.from_numpy(np_array)
        dev = torch.device(device_str)

        target_device = torch.device("cpu")

        if dev.type == "cpu":
            target_device = dev

        elif dev.type == "cuda":
            if torch.cuda.is_available():
                if dev.index is None:
                    target_device = torch.device("cuda")
                elif dev.index < torch.cuda.device_count():
                    target_device = dev
                else:
                    target_device = torch.device("cuda")

        return tensor.to(device=target_device)

    except (ImportError, RuntimeError, TypeError, ValueError):
        return node[_ZERO_COPY_DATA]


def deserialize_tensor_from_numpy(obj: Any) -> Any:
    """
    Recursively restores tensors in a nested object from marked NumPy arrays.
    Supports nested dicts, lists, tuples, and custom objects (via `__dict__`).
    Non-marked values (e.g., str, int, raw np.ndarray) are returned unchanged.
    """
    return _walk_and_transform(obj, _to_tensor_node)
