from joblib.parallel import register_parallel_backend


def register_ray():
    """Register Ray Backend to be called with ``parallel_backend("ray")``.

    Pass ``min_size``, ``max_size``, or ``idle_timeout_s`` to opt into elastic
    actor capacity. Joblib's ``n_jobs`` remains the concurrency ceiling.
    """
    try:
        from ray.util.joblib.ray_backend import RayBackend

        register_parallel_backend("ray", RayBackend)
    except ImportError:
        msg = (
            "To use the ray backend you must install ray."
            "Try running 'pip install ray'."
            "See https://docs.ray.io/en/master/installation.html"
            "for more information."
        )
        raise ImportError(msg)


__all__ = ["register_ray"]
