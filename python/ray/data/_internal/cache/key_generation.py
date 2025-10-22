"""
Robust cache key generation for Ray Data caching.

This module generates stable, unique cache keys from logical plans. Cache keys
must be:
1. **Stable**: Same logical plan → same key (content-based, not memory address)
2. **Unique**: Different logical plans → different keys
3. **Deterministic**: Same inputs → same key across process restarts
4. **Complete**: Include all factors that affect results

Cache Key Components:
    - Operation name (e.g., "count", "schema")
    - Full DAG structure (all operators and their relationships)
    - Operator parameters (functions, arguments, configurations)
    - Operation-specific parameters (e.g., column names, limits)
    - Data context settings (settings that affect computation results)

Key Generation Strategy:
    1. Serialize logical plan to stable dict representation
    2. Include operation name and parameters
    3. Hash with SHA-256 for fixed-length key
    4. Use first 16 characters of hash (excellent collision resistance)

Example Cache Keys:
    - count_a3f2b1c4d5e6f7g8  (count operation)
    - schema_1234567890abcdef (schema operation)
    - sum_98765fedcba43210_on=price (sum with parameter)

Why SHA-256 instead of Python hash():
    Python's hash() is randomized for security (PEP 456), so the same input produces
    different hashes across process restarts. SHA-256 is deterministic.
    https://docs.python.org/3/library/hashlib.html
"""

# https://docs.python.org/3/library/hashlib.html
# Use SHA-256 for deterministic hashing. Python's built-in hash() is randomized
# for security (PEP 456), making it unsuitable for cache keys that need stability
# across process restarts.
import hashlib
import json
from typing import Any, Dict

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan

# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================

# Number of characters to use from SHA-256 hash for cache keys.
# 16 characters (64 bits) provides excellent collision resistance:
# - Probability of collision with 1M keys: ~10^-12 (1 in a trillion)
# - Keeps keys short and readable in logs
CACHE_KEY_HASH_LENGTH = 16


# =============================================================================
# MAIN CACHE KEY GENERATION
# =============================================================================


def make_cache_key(logical_plan: LogicalPlan, operation_name: str, **params) -> str:
    """Create a stable, unique cache key from logical plan and parameters.

    This is the main entry point for cache key generation. It creates a
    deterministic key that uniquely identifies a dataset operation.

    The cache key includes:
    1. Full DAG structure with operator names and relationships
    2. Operator parameters (functions, arguments, configurations)
    3. Operation-specific parameters (e.g., column names, limits)
    4. Data context settings that affect results

    Args:
        logical_plan: The logical plan representing the dataset
        operation_name: Name of the operation being cached (e.g., "count")
        **params: Additional parameters specific to the operation

    Returns:
        A stable cache key string (format: "{operation}_{hash}")

    Examples:
        >>> # Same logical plan always produces same key
        >>> import ray
        >>> ds1 = ray.data.range(100)
        >>> ds2 = ray.data.range(100)
        >>> key1 = make_cache_key(ds1._logical_plan, "count")
        >>> key2 = make_cache_key(ds2._logical_plan, "count")
        >>> assert key1 == key2  # Content-based, not memory address

        >>> # Different operations produce different keys
        >>> key_count = make_cache_key(ds1._logical_plan, "count")
        >>> key_schema = make_cache_key(ds1._logical_plan, "schema")
        >>> assert key_count != key_schema

        >>> # Parameters are included in key
        >>> key_sum_price = make_cache_key(ds1._logical_plan, "sum", on="price")
        >>> key_sum_quantity = make_cache_key(ds1._logical_plan, "sum", on="quantity")
        >>> assert key_sum_price != key_sum_quantity

    Note:
        Uses SHA-256 for deterministic hashing (unlike Python's randomized hash()).
    """
    try:
        # ---------------------------------------------------------------------
        # Build comprehensive cache key representation
        # ---------------------------------------------------------------------
        # Serialize the logical plan to a stable dict
        plan_dict = _serialize_logical_plan(logical_plan)

        # Combine all components into cache key dict
        cache_dict = {
            "operation": operation_name,  # What operation (count, schema, etc.)
            "plan": plan_dict,  # Full DAG structure and parameters
            "params": _serialize_params(params),  # Operation-specific parameters
            "context": _serialize_relevant_context(logical_plan),  # Context settings
        }

        # ---------------------------------------------------------------------
        # Create stable JSON representation
        # ---------------------------------------------------------------------
        # sort_keys=True ensures key order is deterministic
        # default=str handles non-JSON-serializable types
        key_input = json.dumps(cache_dict, sort_keys=True, default=str)

        # ---------------------------------------------------------------------
        # Hash for fixed-length key
        # ---------------------------------------------------------------------
        # Use SHA-256 (deterministic) instead of hash() (randomized)
        hash_obj = hashlib.sha256(key_input.encode("utf-8"))
        hash_hex = hash_obj.hexdigest()[:CACHE_KEY_HASH_LENGTH]

        return f"{operation_name}_{hash_hex}"

    except Exception:
        # ---------------------------------------------------------------------
        # Fallback 1: Try simpler serialization
        # ---------------------------------------------------------------------
        try:
            fallback_input = f"{operation_name}|{str(logical_plan.dag)}|{str(params)}"
            fallback_hash = hashlib.sha256(fallback_input.encode("utf-8")).hexdigest()
            return f"{operation_name}_{fallback_hash[:CACHE_KEY_HASH_LENGTH]}"
        except Exception:
            # -----------------------------------------------------------------
            # Fallback 2: Even simpler serialization
            # -----------------------------------------------------------------
            try:
                last_resort = f"{operation_name}|{str(logical_plan)}"
                fallback_hash = hashlib.sha256(last_resort.encode("utf-8")).hexdigest()
                return f"{operation_name}_{fallback_hash[:CACHE_KEY_HASH_LENGTH]}"
            except Exception:
                # -------------------------------------------------------------
                # Fallback 3: Constant error key
                # -------------------------------------------------------------
                # If even encoding fails, use a constant fallback
                # This ensures we always return something, though caching
                # won't work correctly for this case
                return f"{operation_name}_fallback_error"


# =============================================================================
# LOGICAL PLAN SERIALIZATION
# =============================================================================


def _serialize_logical_plan(logical_plan: LogicalPlan) -> Dict[str, Any]:
    """Serialize logical plan to a stable dictionary representation.

    This extracts both the DAG structure and relevant context settings.

    Args:
        logical_plan: The logical plan to serialize

    Returns:
        Dictionary with serialized plan
    """
    return {
        "dag": _serialize_operator(logical_plan.dag),  # Full DAG structure
        "context": _serialize_relevant_context(logical_plan),  # Context settings
    }


def _serialize_operator(op: LogicalOperator) -> Dict[str, Any]:
    """Recursively serialize a logical operator and its dependencies.

    This creates a nested dict representation of the operator DAG, including:
    - Operator name and type
    - Operator-specific parameters
    - Input dependencies (recursively serialized)

    Args:
        op: The logical operator to serialize

    Returns:
        Dictionary with serialized operator

    Example Structure:
        {
            "name": "MapBatches",
            "class": "MapBatches",
            "fn_name": "transform_func",
            "inputs": [
                {
                    "name": "Read",
                    "class": "Read",
                    "inputs": []
                }
            ]
        }
    """
    # Basic operator identification
    op_dict = {
        "name": op.name,  # Operator name (e.g., "MapBatches", "Filter")
        "class": op.__class__.__name__,  # Class name for additional uniqueness
        "inputs": [  # Recursively serialize input operators
            _serialize_operator(input_op) for input_op in op.input_dependencies
        ],
    }

    # Add operator-specific parameters (functions, args, config)
    op_dict.update(_get_operator_params(op))

    return op_dict


def _get_operator_params(op: LogicalOperator) -> Dict[str, Any]:
    """Extract serializable parameters from a logical operator.

    This function knows about the common parameters used by different operator
    types and extracts them for inclusion in the cache key.

    Args:
        op: The logical operator

    Returns:
        Dictionary of serializable parameters

    Extracted Parameters:
        - UDF-based ops: function name, args, kwargs
        - Join ops: key columns, join type
        - Limit ops: limit value
        - Sort ops: sort key, descending flag
        - Project ops: column list
        - Compute strategy, ray remote args
    """
    params = {}

    # -------------------------------------------------------------------------
    # Common parameters for all operators
    # -------------------------------------------------------------------------
    if hasattr(op, "_num_outputs") and op._num_outputs is not None:
        params["num_outputs"] = op._num_outputs

    # -------------------------------------------------------------------------
    # UDF-based operators (MapBatches, Filter, FlatMap, etc.)
    # -------------------------------------------------------------------------
    # These operators apply user-defined functions to data
    if hasattr(op, "_fn") and op._fn is not None:
        params["fn_name"] = _get_function_signature(op._fn)

    if hasattr(op, "_fn_args") and op._fn_args is not None:
        params["fn_args"] = _serialize_params({"args": op._fn_args})["args"]

    if hasattr(op, "_fn_kwargs") and op._fn_kwargs is not None:
        params["fn_kwargs"] = _serialize_params(op._fn_kwargs)

    # -------------------------------------------------------------------------
    # Join operators
    # -------------------------------------------------------------------------
    if hasattr(op, "_left_key_columns"):
        params["left_key_columns"] = op._left_key_columns
    if hasattr(op, "_right_key_columns"):
        params["right_key_columns"] = op._right_key_columns
    if hasattr(op, "_join_type"):
        params["join_type"] = str(op._join_type)

    # -------------------------------------------------------------------------
    # Limit operators
    # -------------------------------------------------------------------------
    if hasattr(op, "_limit"):
        params["limit"] = op._limit

    # -------------------------------------------------------------------------
    # Sort operators
    # -------------------------------------------------------------------------
    if hasattr(op, "_sort_key"):
        params["sort_key"] = op._sort_key
    if hasattr(op, "_descending"):
        params["descending"] = op._descending

    # -------------------------------------------------------------------------
    # Project operators (select_columns, drop_columns)
    # -------------------------------------------------------------------------
    if hasattr(op, "_columns"):
        params["columns"] = op._columns

    # -------------------------------------------------------------------------
    # Compute strategy
    # -------------------------------------------------------------------------
    if hasattr(op, "_compute") and op._compute is not None:
        params["compute_strategy"] = op._compute.__class__.__name__

    # -------------------------------------------------------------------------
    # Ray remote args (num_cpus, num_gpus, etc.)
    # -------------------------------------------------------------------------
    if hasattr(op, "_ray_remote_args") and op._ray_remote_args:
        params["ray_remote_args"] = _serialize_params(op._ray_remote_args)

    return params


def _get_function_signature(fn: Any) -> str:
    """Get a stable signature for a function or callable.

    This tries to extract a meaningful name for the function that's stable
    across runs (not a memory address).

    Args:
        fn: Function or callable object

    Returns:
        Stable function signature string

    Examples:
        >>> def my_func(x): return x * 2
        >>> _get_function_signature(my_func)
        'my_func'

        >>> _get_function_signature(lambda x: x * 2)
        '<lambda>'

        >>> class MyCallable:
        ...     def __call__(self, x): return x * 2
        >>> _get_function_signature(MyCallable())
        'MyCallable'
    """
    try:
        # Try __name__ first (regular functions)
        if hasattr(fn, "__name__"):
            return fn.__name__
        # Try class name (callable classes)
        elif hasattr(fn, "__class__"):
            return fn.__class__.__name__
        # Fall back to type name
        else:
            return str(type(fn))
    except Exception:
        # If all else fails, return a constant
        return "unknown_function"


# =============================================================================
# PARAMETER SERIALIZATION
# =============================================================================


def _serialize_params(params: Dict[str, Any], _depth: int = 0) -> Dict[str, Any]:
    """Serialize parameters to a stable, hashable representation.

    This function recursively serializes parameter dictionaries, handling
    common types and limiting recursion depth to prevent infinite loops.

    Args:
        params: Parameters dictionary to serialize
        _depth: Current recursion depth (internal parameter)

    Returns:
        Serialized parameters dictionary

    Handles:
        - Primitives (None, bool, int, float, str) → as-is
        - Lists/tuples → recursively serialize (with length limit)
        - Dicts → recursively serialize (with size limit)
        - Complex objects → string representation (with length limit)
    """
    # Prevent infinite recursion (max depth = 10 levels)
    if _depth > 10:
        return {"<max_depth_exceeded>": True}

    serialized: Dict[str, Any] = {}

    for key, value in params.items():
        try:
            # -----------------------------------------------------------------
            # Handle primitives (pass through as-is)
            # -----------------------------------------------------------------
            if value is None or isinstance(value, (bool, int, float, str)):
                serialized[key] = value

            # -----------------------------------------------------------------
            # Handle lists and tuples (recursively serialize)
            # -----------------------------------------------------------------
            elif isinstance(value, (list, tuple)):
                # Limit list size to prevent huge serializations
                if len(value) > 100:
                    serialized[key] = f"<list_length_{len(value)}>"
                else:
                    serialized[key] = [_serialize_value(v, _depth + 1) for v in value]

            # -----------------------------------------------------------------
            # Handle dictionaries (recursively serialize)
            # -----------------------------------------------------------------
            elif isinstance(value, dict):
                # Limit dict size to prevent huge serializations
                if len(value) > 50:
                    serialized[key] = f"<dict_length_{len(value)}>"
                else:
                    serialized[key] = _serialize_params(value, _depth + 1)

            # -----------------------------------------------------------------
            # Handle complex objects (use string representation)
            # -----------------------------------------------------------------
            else:
                # For complex objects, use string representation with length limit
                str_repr = str(value)
                if len(str_repr) > 1000:
                    # Too long, just note the type and length
                    serialized[key] = f"<{type(value).__name__}_length_{len(str_repr)}>"
                else:
                    serialized[key] = str_repr
        except Exception:
            # -----------------------------------------------------------------
            # Fallback for problematic values
            # -----------------------------------------------------------------
            # If serialization fails, just note the type
            serialized[key] = f"<{type(value).__name__}>"

    return serialized


def _serialize_value(value: Any, _depth: int = 0) -> Any:
    """Serialize a single value to a stable representation.

    This is similar to _serialize_params but for individual values.

    Args:
        value: Value to serialize
        _depth: Current recursion depth (internal parameter)

    Returns:
        Serialized value
    """
    # Prevent infinite recursion
    if _depth > 10:
        return "<max_depth_exceeded>"

    # Primitives pass through
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    # Lists/tuples: recursively serialize with size limit
    elif isinstance(value, (list, tuple)):
        if len(value) > 100:
            return f"<{type(value).__name__}_length_{len(value)}>"
        return [_serialize_value(v, _depth + 1) for v in value]

    # Dicts: recursively serialize with size limit
    elif isinstance(value, dict):
        if len(value) > 50:
            return f"<dict_length_{len(value)}>"
        return _serialize_params(value, _depth + 1)

    # Complex objects: string representation with length limit
    else:
        try:
            str_repr = str(value)
            if len(str_repr) > 1000:
                return f"<{type(value).__name__}_length_{len(str_repr)}>"
            return str_repr
        except Exception:
            return f"<{type(value).__name__}>"


# =============================================================================
# CONTEXT SERIALIZATION
# =============================================================================


def _serialize_relevant_context(logical_plan: LogicalPlan) -> Dict[str, Any]:
    """Serialize context settings that affect caching results.

    Only includes DataContext settings that actually affect computation results.
    We don't include settings that only affect performance or logging.

    Args:
        logical_plan: The logical plan (contains context)

    Returns:
        Dictionary of relevant context settings

    Included Settings:
        - target_max_block_size: Affects block boundaries and potentially results
        - use_polars_sort: Affects sort algorithm (could affect edge cases)
        - use_push_based_shuffle: Affects shuffle algorithm

    Excluded Settings (don't affect results):
        - Logging settings
        - Progress bar settings
        - Debug settings
        - Resource settings (these don't change results, only performance)
    """
    context = logical_plan.context
    relevant_settings = {}

    # Only include settings that affect computation results
    # (not performance/logging settings)

    if hasattr(context, "target_max_block_size"):
        relevant_settings["target_max_block_size"] = context.target_max_block_size

    if hasattr(context, "use_polars_sort"):
        relevant_settings["use_polars_sort"] = context.use_polars_sort

    if hasattr(context, "use_push_based_shuffle"):
        relevant_settings["use_push_based_shuffle"] = context.use_push_based_shuffle

    return relevant_settings
