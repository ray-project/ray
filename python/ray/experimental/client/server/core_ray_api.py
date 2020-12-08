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
from ray.experimental.client.common import ClientActorNameRef
from ray.experimental.client.common import ClientObjectRef


class CoreRayAPI(APIImpl):
    """
    Implements the equivalent client-side Ray API by simply passing along to the
    Core Ray API. Primarily used inside of Ray Workers as a trampoline back to
    core ray when passed client stubs.
    """

    def get(self, *args, **kwargs):
        return ray.get(*args, **kwargs)

    def put(self, *args, **kwargs):
        return ray.put(*args, **kwargs)

    def wait(self, *args, **kwargs):
        return ray.wait(*args, **kwargs)

    def remote(self, *args, **kwargs):
        return ray.remote(*args, **kwargs)

    def call_remote(self, instance, *args, **kwargs):
        return instance._get_ray_remote_impl().remote(*args, **kwargs)

    def get_actor_from_object(self, actor_id: ClientActorNameRef):
        return ray.get_actor(actor_id.id.hex())

    def close(self, *args, **kwargs):
        return None

    # Allow for generic fallback to ray.* in remote methods. This allows calls
    # like ray.nodes() to be run in remote functions even though the client
    # doesn't currently support them.
    def __getattr__(self, key: str):
        return getattr(ray, key)


class RayServerAPI(CoreRayAPI):
    """
    Ray Client server-side API shim. By default, simply calls the default Core
    Ray API calls, but also accepts scheduling calls from functions running
    inside of other remote functions that need to create more work.
    """

    def __init__(self, server_instance):
        self.server = server_instance

    def put(self, vals, *args, **kwargs):
        to_put = []
        single = False
        if isinstance(vals, list):
            to_put = vals
        else:
            single = True
            to_put.append(vals)

        out = [self._put(x) for x in to_put]
        if single:
            out = out[0]
        return out

    def _put(self, val):
        resp = self.server._put_and_retain_obj(val)
        return ClientObjectRef(resp.id)

    def get_actor_from_object(self, ref: ClientActorNameRef):
        return self.server.actor_refs[ref.id]

    def call_remote(self, instance, *args, **kwargs):
        task = instance._prepare_client_task()
        ticket = self.server.Schedule(task, prepared_args=args)
        return ClientObjectRef(ticket.return_id)
