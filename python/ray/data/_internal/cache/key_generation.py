"""Cache key generation for Ray Data caching.

Generates stable, unique cache keys from logical plans using SHA-256 hashing.
"""

import hashlib
import json
from typing import Any, Dict

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan

CACHE_KEY_HASH_LENGTH = 16


def make_cache_key(logical_plan: LogicalPlan, operation_name: str, **params) -> str:
    """Create a stable, unique cache key from logical plan and parameters."""
    try:
        plan_dict = _serialize_logical_plan(logical_plan)
        cache_dict = {
            "operation": operation_name,
            "plan": plan_dict,
            "params": _serialize_params(params),
            "context": _serialize_relevant_context(logical_plan),
        }
        key_input = json.dumps(cache_dict, sort_keys=True, default=str)
        hash_obj = hashlib.sha256(key_input.encode("utf-8"))
        hash_hex = hash_obj.hexdigest()[:CACHE_KEY_HASH_LENGTH]
        return f"{operation_name}_{hash_hex}"
    except Exception:
        try:
            fallback_input = f"{operation_name}|{str(logical_plan.dag)}|{str(params)}"
            fallback_hash = hashlib.sha256(fallback_input.encode("utf-8")).hexdigest()
            return f"{operation_name}_{fallback_hash[:CACHE_KEY_HASH_LENGTH]}"
        except Exception:
            try:
                last_resort = f"{operation_name}|{str(logical_plan)}"
                fallback_hash = hashlib.sha256(last_resort.encode("utf-8")).hexdigest()
                return f"{operation_name}_{fallback_hash[:CACHE_KEY_HASH_LENGTH]}"
            except Exception:
                return f"{operation_name}_fallback_error"


def _serialize_logical_plan(logical_plan: LogicalPlan) -> Dict[str, Any]:
    """Serialize logical plan to a stable dictionary representation."""
    return {
        "dag": _serialize_operator(logical_plan.dag),
        "context": _serialize_relevant_context(logical_plan),
    }


def _serialize_operator(op: LogicalOperator) -> Dict[str, Any]:
    """Recursively serialize a logical operator and its dependencies."""
    op_dict = {
        "name": op.name,
        "class": op.__class__.__name__,
        "inputs": [_serialize_operator(input_op) for input_op in op.input_dependencies],
    }
    op_dict.update(_get_operator_params(op))
    return op_dict


def _get_operator_params(op: LogicalOperator) -> Dict[str, Any]:
    """Extract serializable parameters from a logical operator."""
    params = {}

    if hasattr(op, "_num_outputs") and op._num_outputs is not None:
        params["num_outputs"] = op._num_outputs

    if hasattr(op, "_fn") and op._fn is not None:
        params["fn_name"] = _get_function_signature(op._fn)

    if hasattr(op, "_fn_args") and op._fn_args is not None:
        params["fn_args"] = _serialize_params({"args": op._fn_args})["args"]

    if hasattr(op, "_fn_kwargs") and op._fn_kwargs is not None:
        params["fn_kwargs"] = _serialize_params(op._fn_kwargs)

    if hasattr(op, "_left_key_columns"):
        params["left_key_columns"] = op._left_key_columns
    if hasattr(op, "_right_key_columns"):
        params["right_key_columns"] = op._right_key_columns
    if hasattr(op, "_join_type"):
        params["join_type"] = str(op._join_type)

    if hasattr(op, "_limit"):
        params["limit"] = op._limit

    if hasattr(op, "_sort_key"):
        params["sort_key"] = op._sort_key
    if hasattr(op, "_descending"):
        params["descending"] = op._descending

    if hasattr(op, "_columns"):
        params["columns"] = op._columns

    if hasattr(op, "_compute") and op._compute is not None:
        params["compute_strategy"] = op._compute.__class__.__name__

    if hasattr(op, "_ray_remote_args") and op._ray_remote_args:
        params["ray_remote_args"] = _serialize_params(op._ray_remote_args)

    return params


def _get_function_signature(fn: Any) -> str:
    """Get a stable signature for a function or callable."""
    try:
        if hasattr(fn, "__name__"):
            return fn.__name__
        elif hasattr(fn, "__class__"):
            return fn.__class__.__name__
        else:
            return str(type(fn))
    except Exception:
        return "unknown_function"


def _serialize_params(params: Dict[str, Any], _depth: int = 0) -> Dict[str, Any]:
    """Serialize parameters to a stable, hashable representation."""
    if _depth > 10:
        return {"<max_depth_exceeded>": True}

    serialized: Dict[str, Any] = {}

    for key, value in params.items():
        try:
            if value is None or isinstance(value, (bool, int, float, str)):
                serialized[key] = value
            elif isinstance(value, (list, tuple)):
                if len(value) > 100:
                    serialized[key] = f"<list_length_{len(value)}>"
                else:
                    serialized[key] = [_serialize_value(v, _depth + 1) for v in value]
            elif isinstance(value, dict):
                if len(value) > 50:
                    serialized[key] = f"<dict_length_{len(value)}>"
                else:
                    serialized[key] = _serialize_params(value, _depth + 1)
            else:
                str_repr = str(value)
                if len(str_repr) > 1000:
                    serialized[key] = f"<{type(value).__name__}_length_{len(str_repr)}>"
                else:
                    serialized[key] = str_repr
        except Exception:
            serialized[key] = f"<{type(value).__name__}>"

    return serialized


def _serialize_value(value: Any, _depth: int = 0) -> Any:
    """Serialize a single value to a stable representation."""
    if _depth > 10:
        return "<max_depth_exceeded>"

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    elif isinstance(value, (list, tuple)):
        if len(value) > 100:
            return f"<{type(value).__name__}_length_{len(value)}>"
        return [_serialize_value(v, _depth + 1) for v in value]
    elif isinstance(value, dict):
        if len(value) > 50:
            return f"<dict_length_{len(value)}>"
        return _serialize_params(value, _depth + 1)
    else:
        try:
            str_repr = str(value)
            if len(str_repr) > 1000:
                return f"<{type(value).__name__}_length_{len(str_repr)}>"
            return str_repr
        except Exception:
            return f"<{type(value).__name__}>"


def _serialize_relevant_context(logical_plan: LogicalPlan) -> Dict[str, Any]:
    """Serialize context settings that affect caching results."""
    context = logical_plan.context
    relevant_settings = {}

    if hasattr(context, "target_max_block_size"):
        relevant_settings["target_max_block_size"] = context.target_max_block_size

    if hasattr(context, "use_polars_sort"):
        relevant_settings["use_polars_sort"] = context.use_polars_sort

    if hasattr(context, "use_push_based_shuffle"):
        relevant_settings["use_push_based_shuffle"] = context.use_push_based_shuffle

    return relevant_settings
