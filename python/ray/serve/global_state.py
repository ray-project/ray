import ray
from ray.serve.constants import (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT,
                                 SERVE_MASTER_NAME, ASYNC_CONCURRENCY)
from ray.serve.kv_store_service import (BackendTable, RoutingTable,
                                        TrafficPolicyTable)
from ray.serve.metric import (MetricMonitor, start_metric_monitor_loop)

from ray.serve.policy import RoutePolicy
from ray.serve.http_proxy import HTTPProxyActor


@ray.remote
class ServeMaster:
    """Initialize and store all actor handles.

    Note:
        This actor is necessary because ray will destory actors when the
        original actor handle goes out of scope (when driver exit). Therefore
        we need to initialize and store actor handles in a seperate actor.
    """

    def __init__(self, kv_store_connector):
        self.kv_store_connector = kv_store_connector
        self.tag_to_actor_handles = dict()

        self.router_handle = None

    def get_kv_store_connector(self):
        return self.kv_store_connector

    def start_actor(self,
                    actor_cls,
                    tag,
                    init_args=(),
                    init_kwargs={},
                    is_asyncio=False):
        """Start an actor and add it to the nursery"""
        # Avoid double initialization
        if tag in self.tag_to_actor_handles.keys():
            return [self.tag_to_actor_handles[tag]]

        max_concurrency = ASYNC_CONCURRENCY if is_asyncio else None
        handle = (actor_cls.options(max_concurrency=max_concurrency).remote(
            *init_args, **init_kwargs))
        self.tag_to_actor_handles[tag] = handle
        return [handle]

    def start_router(self, router_class, init_kwargs):
        assert self.router_handle is None
        self.router_handle = router_class.options(
            max_concurrency=ASYNC_CONCURRENCY).remote(**init_kwargs)

    def get_router(self):
        assert self.router_handle is not None
        return [self.router_handle]

    def start_actor_with_creator(self, creator, kwargs, tag):
        """
        Args:
            creator (Callable[Dict]): a closure that should return
                a newly created actor handle when called with kwargs.
                The kwargs input is passed to `ActorCls_remote` method.
        """
        handle = creator(kwargs)
        self.tag_to_actor_handles[tag] = handle
        return [handle]

    def get_all_handles(self):
        return self.tag_to_actor_handles

    def get_handle(self, actor_tag):
        return [self.tag_to_actor_handles[actor_tag]]

    def remove_handle(self, actor_tag):
        if actor_tag in self.tag_to_actor_handles.keys():
            self.tag_to_actor_handles.pop(actor_tag)


class GlobalState:
    """Encapsulate all global state in the serving system.

    The information is fetch lazily from
        1. A collection of namespaced key value stores
        2. A actor supervisor service
    """

    def __init__(self, master_actor_handle=None):
        # Get actor nursery handle
        if master_actor_handle is None:
            master_actor_handle = ray.util.get_actor(SERVE_MASTER_NAME)
        self.master_actor_handle = master_actor_handle

        # Connect to all the tables.
        kv_store_connector = ray.get(
            self.master_actor_handle.get_kv_store_connector.remote())
        self.route_table = RoutingTable(kv_store_connector)
        self.backend_table = BackendTable(kv_store_connector)
        self.policy_table = TrafficPolicyTable(kv_store_connector)

        self.refresh_actor_handle_cache()

    def refresh_actor_handle_cache(self):
        self.actor_handle_cache = ray.get(
            self.master_actor_handle.get_all_handles.remote())

    def init_or_get_http_proxy(self,
                               host=DEFAULT_HTTP_HOST,
                               port=DEFAULT_HTTP_PORT):
        if "http_proxy" not in self.actor_handle_cache:
            [handle] = ray.get(
                self.master_actor_handle.start_actor.remote(
                    HTTPProxyActor, tag="http_proxy"))

            handle.run.remote(host=host, port=port)
            self.refresh_actor_handle_cache()
        return self.actor_handle_cache["http_proxy"]

    def _get_queueing_policy(self, default_policy):
        return_policy = default_policy
        # check if there is already a queue_actor running
        # with policy as p.name for the case where
        # serve nursery exists: ray.util.get_actor(SERVE_MASTER_NAME)
        for p in RoutePolicy:
            queue_actor_tag = "queue_actor::" + p.name
            if queue_actor_tag in self.actor_handle_cache:
                return_policy = p
                break
        return return_policy

    def get_router(self):
        return ray.get(self.master_actor_handle.get_router.remote())[0]

    def init_or_get_metric_monitor(self, gc_window_seconds=3600):
        if "metric_monitor" not in self.actor_handle_cache:
            [handle] = ray.get(
                self.master_actor_handle.start_actor.remote(
                    MetricMonitor,
                    init_args=(gc_window_seconds, ),
                    tag="metric_monitor"))

            start_metric_monitor_loop.remote(handle)

            if "queue_actor" in self.actor_handle_cache:
                handle.add_target.remote(
                    self.actor_handle_cache["queue_actor"])

            self.refresh_actor_handle_cache()

        return self.actor_handle_cache["metric_monitor"]
