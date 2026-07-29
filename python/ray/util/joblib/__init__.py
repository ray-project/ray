from joblib.parallel import register_parallel_backend


def register_ray(autoscale: bool = False, **pool_defaults):
    """Register the Ray joblib backend under the name ``"ray"``.

    Select it with ``joblib.parallel_backend("ray")``.

    Pass ``autoscale=True`` to make the actor pool grow on demand and shrink
    when idle on every ``Parallel`` call (see
    :class:`ray.util.multiprocessing.pool.Pool`). ``pool_defaults``
    (``min_size``, ``max_size``, ``initial_size``, ``idle_timeout_s``) are
    applied to every pool unless overridden per-call via
    ``parallel_backend("ray", ...)``.
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
        return RayBackend(**{"autoscale": autoscale, **pool_defaults, **backend_params})

    register_parallel_backend("ray", factory)


__all__ = ["register_ray"]
