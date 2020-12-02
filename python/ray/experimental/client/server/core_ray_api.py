# Along with `api.py` this is the stub that interfaces with
# the real (C-binding, raylet) ray core.
#
# Ideally, the first import line is the only time we actually
# import ray in this library (excluding the main function for the server)
#
# While the stub is trivial, it allows us to check that the calls we're
# making into the core-ray module are contained and well-defined.

import ray

from ray.experimental.client.api import APIImpl
from ray.experimental.client.common import ClientRemoteFunc


class CoreRayAPI(APIImpl):
    def get(self, *args, **kwargs):
        return ray.get(*args, **kwargs)

    def put(self, *args, **kwargs):
        return ray.put(*args, **kwargs)

    def wait(self, *args, **kwargs):
        return ray.wait(*args, **kwargs)

    def remote(self, *args, **kwargs):
        return ray.remote(*args, **kwargs)

    def call_remote(self, f: ClientRemoteFunc, kind: int, *args, **kwargs):
        if f._raylet_remote_func is None:
            f._raylet_remote_func = ray.remote(f._func)
        return f._raylet_remote_func.remote(*args, **kwargs)

    def close(self, *args, **kwargs):
        return None

    # Allow for generic fallback to ray.* in remote methods. This allows calls
    # like ray.nodes() to be run in remote functions even though the client
    # doesn't currently support them.
    def __getattr__(self, key: str):
        return getattr(ray, key)
