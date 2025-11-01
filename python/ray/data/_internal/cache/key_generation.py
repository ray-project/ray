"""Cache key generation for Ray Data caching.

Generates stable, unique cache keys from logical plans using SHA-256 hashing.
"""

import hashlib
import json
from typing import Any, Dict

from ray.data._internal.logical.interfaces import LogicalPlan

CACHE_KEY_HASH_LENGTH = 16


def make_cache_key(logical_plan: LogicalPlan, operation_name: str, **params) -> str:
    """Create a stable, unique cache key from logical plan and parameters."""
    try:
        key_input = json.dumps(
            {
                "operation": operation_name,
                "dag": logical_plan.dag.dag_str,
                "params": _serialize_params(params),
            },
            sort_keys=True,
            default=str,
        )
        hash_obj = hashlib.sha256(key_input.encode("utf-8"))
        hash_hex = hash_obj.hexdigest()[:CACHE_KEY_HASH_LENGTH]
        return f"{operation_name}_{hash_hex}"
    except Exception:
        try:
            fallback_input = f"{operation_name}|{logical_plan.dag.dag_str}|{str(params)}"
            fallback_hash = hashlib.sha256(fallback_input.encode("utf-8")).hexdigest()
            return f"{operation_name}_{fallback_hash[:CACHE_KEY_HASH_LENGTH]}"
        except Exception:
            return f"{operation_name}_fallback_error"


def _serialize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize parameters to a stable, hashable representation."""
    serialized: Dict[str, Any] = {}

    for key, value in params.items():
        try:
            if value is None or isinstance(value, (bool, int, float, str)):
                serialized[key] = value
            elif isinstance(value, (list, tuple)):
                if len(value) > 100:
                    serialized[key] = f"<list_length_{len(value)}>"
                else:
                    serialized[key] = [str(v) for v in value]
            elif isinstance(value, dict):
                if len(value) > 50:
                    serialized[key] = f"<dict_length_{len(value)}>"
                else:
                    serialized[key] = {k: str(v) for k, v in value.items()}
            else:
                str_repr = str(value)
                if len(str_repr) > 1000:
                    serialized[key] = f"<{type(value).__name__}_length_{len(str_repr)}>"
                else:
                    serialized[key] = str_repr
        except Exception:
            serialized[key] = f"<{type(value).__name__}>"

    return serialized
