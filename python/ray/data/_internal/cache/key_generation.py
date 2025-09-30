"""
Simple cache key generation for Ray Data caching.
"""

import hashlib

from ray.data._internal.logical.interfaces import LogicalPlan

# Configuration constants

# Number of characters to use from SHA-256 hash for cache keys.
# 12 characters provides good collision resistance while keeping keys readable.
CACHE_KEY_HASH_LENGTH = 12

# Simple fallback hash modulo for error cases.
# Used when SHA-256 hashing fails, provides reasonable distribution.
FALLBACK_HASH_MODULO = 1000000


def make_cache_key(logical_plan: LogicalPlan, operation_name: str, **params) -> str:
    """Create a cache key from logical plan and parameters."""
    try:
        # Simple string representation of the plan
        plan_str = (
            str(logical_plan.dag) if hasattr(logical_plan, "dag") else str(logical_plan)
        )

        # Simple parameter string
        param_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))

        # Combine and hash
        key_input = f"{operation_name}|{plan_str}|{param_str}"
        hash_obj = hashlib.sha256(key_input.encode("utf-8"))
        hash_hex = hash_obj.hexdigest()[:CACHE_KEY_HASH_LENGTH]

        return f"{operation_name}_{hash_hex}"

    except Exception:
        # Simple fallback
        return f"{operation_name}_{hash(str(logical_plan)) % FALLBACK_HASH_MODULO}"
