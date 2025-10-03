"""
Robust cache key generation for Ray Data caching.

Generates stable, unique cache keys that include the full logical plan structure,
operator parameters, and operation-specific parameters.
"""

import hashlib
import json
from typing import Any, Dict

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan

# Configuration constants

# Number of characters to use from SHA-256 hash for cache keys.
# 16 characters provides excellent collision resistance while keeping keys readable.
CACHE_KEY_HASH_LENGTH = 16

# Simple fallback hash modulo for error cases.
# Used when serialization fails, provides reasonable distribution.
FALLBACK_HASH_MODULO = 10000000


def make_cache_key(logical_plan: LogicalPlan, operation_name: str, **params) -> str:
    """Create a stable, unique cache key from logical plan and parameters.

    The cache key includes:
    1. Full DAG structure with operator names and relationships
    2. Operator parameters (functions, arguments, configurations)
    3. Operation-specific parameters (e.g., column names, limits)
    4. Data context settings that affect results

    Args:
        logical_plan: The logical plan representing the dataset
        operation_name: Name of the operation being cached
        **params: Additional parameters specific to the operation

    Returns:
        A stable cache key string
    """
    try:
        # Build comprehensive plan representation
        plan_dict = _serialize_logical_plan(logical_plan)

        # Add operation-specific parameters
        cache_dict = {
            "operation": operation_name,
            "plan": plan_dict,
            "params": _serialize_params(params),
            "context": _serialize_relevant_context(logical_plan),
        }

        # Create stable JSON representation
        key_input = json.dumps(cache_dict, sort_keys=True, default=str)

        # Hash for fixed-length key
        hash_obj = hashlib.sha256(key_input.encode("utf-8"))
        hash_hex = hash_obj.hexdigest()[:CACHE_KEY_HASH_LENGTH]

        return f"{operation_name}_{hash_hex}"

    except Exception:
        # Fallback to simple hash
        try:
            fallback_input = f"{operation_name}|{str(logical_plan.dag)}|{str(params)}"
            return f"{operation_name}_{hash(fallback_input) % FALLBACK_HASH_MODULO:07d}"
        except Exception:
            # Last resort fallback
            return (
                f"{operation_name}_{hash(str(logical_plan)) % FALLBACK_HASH_MODULO:07d}"
            )


def _serialize_logical_plan(logical_plan: LogicalPlan) -> Dict[str, Any]:
    """Serialize logical plan to a stable dictionary representation."""
    return {
        "dag": _serialize_operator(logical_plan.dag),
        "context_id": id(logical_plan.context),  # Context identity for uniqueness
    }


def _serialize_operator(op: LogicalOperator) -> Dict[str, Any]:
    """Recursively serialize a logical operator and its dependencies."""
    op_dict = {
        "name": op.name,
        "class": op.__class__.__name__,
        "inputs": [_serialize_operator(input_op) for input_op in op.input_dependencies],
    }

    # Add operator-specific parameters
    op_dict.update(_get_operator_params(op))

    return op_dict


def _get_operator_params(op: LogicalOperator) -> Dict[str, Any]:
    """Extract serializable parameters from a logical operator."""
    params = {}

    # Common parameters for all operators
    if hasattr(op, "_num_outputs") and op._num_outputs is not None:
        params["num_outputs"] = op._num_outputs

    # UDF-based operators (MapBatches, Filter, etc.)
    if hasattr(op, "_fn") and op._fn is not None:
        params["fn_name"] = _get_function_signature(op._fn)

    if hasattr(op, "_fn_args") and op._fn_args is not None:
        params["fn_args"] = _serialize_params({"args": op._fn_args})["args"]

    if hasattr(op, "_fn_kwargs") and op._fn_kwargs is not None:
        params["fn_kwargs"] = _serialize_params(op._fn_kwargs)

    # Join operators
    if hasattr(op, "_left_key_columns"):
        params["left_key_columns"] = op._left_key_columns
    if hasattr(op, "_right_key_columns"):
        params["right_key_columns"] = op._right_key_columns
    if hasattr(op, "_join_type"):
        params["join_type"] = str(op._join_type)

    # Limit operators
    if hasattr(op, "_limit"):
        params["limit"] = op._limit

    # Sort operators
    if hasattr(op, "_sort_key"):
        params["sort_key"] = op._sort_key
    if hasattr(op, "_descending"):
        params["descending"] = op._descending

    # Project operators (select_columns)
    if hasattr(op, "_columns"):
        params["columns"] = op._columns

    # Compute strategy
    if hasattr(op, "_compute") and op._compute is not None:
        params["compute_strategy"] = op._compute.__class__.__name__

    # Ray remote args
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
    """Serialize parameters to a stable, hashable representation.

    Args:
        params: Parameters to serialize
        _depth: Current recursion depth (internal parameter)

    Returns:
        Serialized parameters dictionary
    """
    # Prevent infinite recursion
    if _depth > 10:
        return {"<max_depth_exceeded>": True}

    serialized: Dict[str, Any] = {}

    for key, value in params.items():
        try:
            # Handle common types
            if value is None or isinstance(value, (bool, int, float, str)):
                serialized[key] = value
            elif isinstance(value, (list, tuple)):
                # Limit list size to prevent huge serializations
                if len(value) > 100:
                    serialized[key] = f"<list_length_{len(value)}>"
                else:
                    serialized[key] = [_serialize_value(v, _depth + 1) for v in value]
            elif isinstance(value, dict):
                # Limit dict size to prevent huge serializations
                if len(value) > 50:
                    serialized[key] = f"<dict_length_{len(value)}>"
                else:
                    serialized[key] = _serialize_params(value, _depth + 1)
            else:
                # For complex objects, use string representation with length limit
                str_repr = str(value)
                if len(str_repr) > 1000:
                    serialized[key] = f"<{type(value).__name__}_length_{len(str_repr)}>"
                else:
                    serialized[key] = str_repr
        except Exception:
            # Fallback for problematic values
            serialized[key] = f"<{type(value).__name__}>"

    return serialized


def _serialize_value(value: Any, _depth: int = 0) -> Any:
    """Serialize a single value to a stable representation.

    Args:
        value: Value to serialize
        _depth: Current recursion depth (internal parameter)

    Returns:
        Serialized value
    """
    # Prevent infinite recursion
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

    # Only include settings that affect computation results
    if hasattr(context, "target_max_block_size"):
        relevant_settings["target_max_block_size"] = context.target_max_block_size

    if hasattr(context, "use_polars_sort"):
        relevant_settings["use_polars_sort"] = context.use_polars_sort

    if hasattr(context, "use_push_based_shuffle"):
        relevant_settings["use_push_based_shuffle"] = context.use_push_based_shuffle

    return relevant_settings
