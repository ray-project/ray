from .routers import DeadlineAwareRouter, SingleQuery

import ray


def start_router(router_class, router_name):
    handle = router_class.remote(router_name)
    ray.experimental.register_actor(router_name, handle)
    handle.start.remote()
    return handle
