import ray
from ray.experimental.serve.constants import (
    BOOTSTRAP_KV_STORE_CONN_KEY, DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT,
    SERVE_NURSERY_NAME)
from ray.experimental.serve.kv_store_service import (
    BackendTable, RoutingTable, TrafficPolicyTable)
from ray.experimental.serve.metric import (MetricMonitor,
                                           start_metric_monitor_loop)
from ray.experimental.serve.queues import CentralizedQueuesActor
from ray.experimental.serve.server import HTTPActor


def start_initial_state(kv_store_connector):
    nursery_handle = ActorNursery.remote()
    ray.experimental.register_actor(SERVE_NURSERY_NAME, nursery_handle)

    ray.get(
        nursery_handle.store_bootstrap_state.remote(
            BOOTSTRAP_KV_STORE_CONN_KEY, kv_store_connector))
    return nursery_handle


@ray.remote
class ActorNursery:
    """Initialize and store all actor handles.

    Note:
        This actor is necessary because ray will destory actors when the
        original actor handle goes out of scope (when driver exit). Therefore
        we need to initialize and store actor handles in a seperate actor.
    """

    def __init__(self):
        # Dict: Actor handles -> tag
        self.actor_handles = dict()

        self.bootstrap_state = dict()

    def start_actor(self, actor_cls, init_args, tag):
        """Start an actor and add it to the nursery"""
        handle = actor_cls.remote(*init_args)
        self.actor_handles[handle] = tag
        return [handle]

    def start_actor_with_creator(self, creator, tag):
        handle = creator()
        self.actor_handles[handle] = tag
        return [handle]

    def get_all_handles(self):
        return {tag: handle for handle, tag in self.actor_handles.items()}

    def get_handle(self, actor_tag):
        return [self.get_all_handles()[actor_tag]]

    def remove_handle(self, actor_tag):
        [handle] = self.get_handle(actor_tag)
        self.actor_handles.pop(handle)
        del handle

    def store_bootstrap_state(self, key, value):
        self.bootstrap_state[key] = value

    def get_bootstrap_state_dict(self):
        return self.bootstrap_state


class GlobalState:
    """Encapsulate all global state in the serving system.

    The information is fetch lazily from
        1. A collection of namespaced key value stores
        2. A actor supervisor service
    """

    def __init__(self, actor_nursery_handle=None):
        # Get actor nursery handle
        if actor_nursery_handle is None:
            actor_nursery_handle = ray.experimental.get_actor(
                SERVE_NURSERY_NAME)
        self.actor_nursery_handle = actor_nursery_handle

        # Connect to all the table
        bootstrap_config = ray.get(
            self.actor_nursery_handle.get_bootstrap_state_dict.remote())
        kv_store_connector = bootstrap_config[BOOTSTRAP_KV_STORE_CONN_KEY]
        self.route_table = RoutingTable(kv_store_connector)
        self.backend_table = BackendTable(kv_store_connector)
        self.policy_table = TrafficPolicyTable(kv_store_connector)

        self.refresh_actor_handle_cache()

    def refresh_actor_handle_cache(self):
        self.actor_handle_cache = ray.get(
            self.actor_nursery_handle.get_all_handles.remote())

    def init_or_get_http_server(self,
                                host=DEFAULT_HTTP_HOST,
                                port=DEFAULT_HTTP_PORT):
        if "http_server" not in self.actor_handle_cache:
            [handle] = ray.get(
                self.actor_nursery_handle.start_actor.remote(
                    HTTPActor, init_args=(), tag="http_server"))
            handle.run.remote(host=host, port=port)
            self.refresh_actor_handle_cache()
        return self.actor_handle_cache["http_server"]

    def init_or_get_router(self):
        if "queue_actor" not in self.actor_handle_cache:
            [handle] = ray.get(
                self.actor_nursery_handle.start_actor.remote(
                    CentralizedQueuesActor, init_args=(), tag="queue_actor"))
            handle.register_self_handle.remote(handle)
            self.refresh_actor_handle_cache()

        return self.actor_handle_cache["queue_actor"]

    def init_or_get_metric_monitor(self, gc_window_seconds=3600):
        if "metric_monitor" not in self.actor_handle_cache:
            [handle] = ray.get(
                self.actor_nursery_handle.start_actor.remote(
                    MetricMonitor,
                    init_args=(gc_window_seconds, ),
                    tag="metric_monitor"))

            start_metric_monitor_loop.remote(handle)

            if "queue_actor" in self.actor_handle_cache:
                handle.add_target.remote(
                    self.actor_handle_cache["queue_actor"])

            self.refresh_actor_handle_cache()

        return self.actor_handle_cache["metric_monitor"]
