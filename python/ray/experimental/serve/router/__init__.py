from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.experimental.serve.router.routers import (DeadlineAwareRouter,
                                                   SingleQuery)
import ray


def start_router(router_class, router_name):
    """Wrapper for starting a router and register it.

    Args:
        router_class: The router class to instantiate.
        router_name: The name to give to the router.

    Returns:
        A handle to newly started router actor.
    """
    handle = router_class.remote(router_name)
    ray.experimental.register_actor(router_name, handle)
    handle.start.remote()
    return handle


__all__ = ["DeadlineAwareRouter", "SingleQuery"]
