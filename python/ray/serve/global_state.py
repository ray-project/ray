import ray
from ray.serve.constants import (SERVE_MASTER_NAME, ASYNC_CONCURRENCY)
from ray.serve.kv_store_service import (BackendTable, RoutingTable,
                                        TrafficPolicyTable)
from ray.serve.metric import (MetricMonitor, start_metric_monitor_loop)

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

        self.router = None
        self.http_proxy = None
        self.metric_monitor = None

    def get_kv_store_connector(self):
        return self.kv_store_connector

    def start_router(self, router_class, init_kwargs):
        assert self.router is None, "Router already started."
        self.router = router_class.options(
            max_concurrency=ASYNC_CONCURRENCY).remote(**init_kwargs)

    def get_router(self):
        assert self.router is not None, "Router not started yet."
        return [self.router]

    def start_http_proxy(self, host, port):
        assert self.http_proxy is None, "HTTP proxy already started."
        assert self.router is not None, (
            "Router must be started before HTTP proxy.")
        self.http_proxy = HTTPProxyActor.options(
            max_concurrency=ASYNC_CONCURRENCY).remote()
        self.http_proxy.run.remote(host, port)
        ray.get(self.http_proxy.set_router_handle.remote(self.router))

    def get_http_proxy(self):
        assert self.http_proxy is not None, "HTTP proxy not started yet."
        return [self.http_proxy]

    def start_metric_monitor(self, gc_window_seconds):
        assert self.metric_monitor is None, "Metric monitor already started."
        self.metric_monitor = MetricMonitor.remote(gc_window_seconds)
        # TODO(edoakes): this should be an actor method, not a separate task.
        start_metric_monitor_loop.remote(self.metric_monitor)
        self.metric_monitor.add_target.remote(self.router)

    def get_metric_monitor(self, gc_window_seconds=3600):
        assert self.metric_monitor is not None, (
            "Metric monitor not started yet.")
        return [self.metric_monitor]

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

    def __init__(self, master_actor=None):
        # Get actor nursery handle
        if master_actor is None:
            master_actor = ray.util.get_actor(SERVE_MASTER_NAME)
        self.master_actor = master_actor

        # Connect to all the tables.
        kv_store_connector = ray.get(
            self.master_actor.get_kv_store_connector.remote())
        self.route_table = RoutingTable(kv_store_connector)
        self.backend_table = BackendTable(kv_store_connector)
        self.policy_table = TrafficPolicyTable(kv_store_connector)

    def get_http_proxy(self):
        return ray.get(self.master_actor.get_http_proxy.remote())[0]

    def get_router(self):
        return ray.get(self.master_actor.get_router.remote())[0]

    def get_metric_monitor(self):
        return ray.get(self.master_actor.get_metric_monitor.remote())[0]
