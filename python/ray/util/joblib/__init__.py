from joblib.parallel import register_parallel_backend


def register_ray(**pool_defaults):
    """Register the Ray joblib backend under the name ``"ray"``.

    Select it with ``joblib.parallel_backend("ray")``.

    Pass any of ``min_size``, ``max_size``, ``initial_size``, or
    ``idle_timeout_s`` (here, or per-call via ``parallel_backend("ray", ...)``)
    to make the actor pool autoscale on every ``Parallel`` call: grow on
    demand and shrink when idle (see
    :class:`ray.util.multiprocessing.pool.Pool`). Otherwise the pool is a
    fixed-size pool of ``n_jobs`` actors. ``pool_defaults`` are applied to
    every pool unless overridden per-call.
    """
    try:
        from ray.util.joblib.ray_backend import RayBackend
    except ImportError:
        msg = (
            "To use the ray backend you must install ray."
            "Try running 'pip install ray'."
            "See https://docs.ray.io/en/master/installation.html"
            "for more information."
        )
        raise ImportError(msg)

    def factory(**backend_params):
        # Per-call params override register-time defaults.
        return RayBackend(**{**pool_defaults, **backend_params})

    register_parallel_backend("ray", factory)


__all__ = ["register_ray"]
