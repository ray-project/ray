import os
# TODO(iycheng): Remove deprecated one
if os.environ.get("USE_GSE_KV"):
    from ray.experimental.internal_kv.gcs_kv import (
        _internal_kv_initialized, _internal_kv_get, _internal_kv_exists,
        _internal_kv_put, _internal_kv_del, _internal_kv_list)
else:
    from ray.experimental.internal_kv.deprecated_kv import (
        _internal_kv_initialized, _internal_kv_get, _internal_kv_exists,
        _internal_kv_put, _internal_kv_del, _internal_kv_list)

__all__ = [
    "_internal_kv_initialized", "_internal_kv_get", "_internal_kv_exists",
    "_internal_kv_put", "_internal_kv_put", "_internal_kv_del",
    "_internal_kv_list"
]
