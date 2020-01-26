from joblib.parallel import register_parallel_backend
def register_ray():
    """ Register Ray Backend if called with parallel_backend("ray") """
    try:
        from ._raybackend import RayBackend
        register_parallel_backend('ray', RayBackend)
    except ImportError:
        msg = ("To use the ray backend you must install ray"
                "try running pip install ray"
               "See https://ray.readthedocs.io for more "
               "information.")
        raise ImportError(msg)

__all__ = ['register_ray']
