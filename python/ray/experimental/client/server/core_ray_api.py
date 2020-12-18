# Along with `api.py` this is the stub that interfaces with
# the real (C-binding, raylet) ray core.
#
# Ideally, the first import line is the only time we actually
# import ray in this library (excluding the main function for the server)
#
# While the stub is trivial, it allows us to check that the calls we're
# making into the core-ray module are contained and well-defined.

from typing import Any
from typing import Optional
from typing import Union

import logging
import ray

from ray.experimental.client.api import APIImpl
from ray.experimental.client.common import ClientObjectRef
from ray.experimental.client.common import ClientStub

logger = logging.getLogger(__name__)


class CoreRayAPI(APIImpl):
    """
    Implements the equivalent client-side Ray API by simply passing along to
    the Core Ray API. Primarily used inside of Ray Workers as a trampoline back
    to core ray when passed client stubs.
    """

    def get(self, vals, *, timeout: Optional[float] = None) -> Any:
        return ray.get(vals, timeout=timeout)

    def put(self, vals: Any, *args,
            **kwargs) -> Union[ClientObjectRef, ray._raylet.ObjectRef]:
        return ray.put(vals, *args, **kwargs)

    def wait(self, *args, **kwargs):
        return ray.wait(*args, **kwargs)

    def remote(self, *args, **kwargs):
        return ray.remote(*args, **kwargs)

    def call_remote(self, instance: ClientStub, *args, **kwargs):
        raise NotImplementedError(
            "Should not attempt execution of a client stub inside the raylet")

    def close(self) -> None:
        return None

    def kill(self, actor, *, no_restart=True):
        return ray.kill(actor, no_restart=no_restart)

    def cancel(self, obj, *, force=False, recursive=True):
        return ray.cancel(obj, force=force, recursive=recursive)

    def is_initialized(self) -> bool:
        return ray.is_initialized()

    def call_release(self, id: bytes) -> None:
        return None

    def call_retain(self, id: bytes) -> None:
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

    def call_remote(self, instance: ClientStub, *args, **kwargs) -> bytes:
        task = instance._prepare_client_task()
        ticket = self.server.Schedule(task, prepared_args=args)
        return ticket.return_id
