cached_is_autoscaler_v2 = None


def is_autoscaler_v2() -> bool:
    """
    Check if the autoscaler is v2 from reading GCS internal KV.

    If the method is called multiple times, the result will be cached in the module.

    Returns:
        is_v2: True if the autoscaler is v2, False otherwise.

    Raises:
        Exception: if GCS address could not be resolved (e.g. ray.init() not called)
    """

    # If env var is set to enable autoscaler v2, we should always return True.
    import ray
    from ray._private.ray_constants import (
        AUTOSCALER_NAMESPACE,
        AUTOSCALER_V2_ENABLED_KEY,
    )

    if ray._config.enable_autoscaler_v2():
        # TODO(rickyx): Once we migrate completely to v2, we should remove this.
        # While this short-circuit may allow client-server inconsistency
        # (e.g. client running v1, while server running v2), it's currently
        # not possible with existing use-cases.
        return True

    global cached_is_autoscaler_v2
    if cached_is_autoscaler_v2 is not None:
        return cached_is_autoscaler_v2

    from ray.experimental.internal_kv import _internal_kv_get, _internal_kv_initialized

    if not _internal_kv_initialized():
        raise Exception(
            "GCS address could not be resolved (e.g. ray.init() not called)"
        )

    # See src/ray/common/constants.h for the definition of this key.
    cached_is_autoscaler_v2 = (
        _internal_kv_get(
            AUTOSCALER_V2_ENABLED_KEY.encode(), namespace=AUTOSCALER_NAMESPACE.encode()
        )
        == b"1"
    )

    return cached_is_autoscaler_v2
