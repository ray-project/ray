import ray
from ray.serve.backend_config import BackendConfig
from ray.serve.constants import (SERVE_MASTER_NAME, ASYNC_CONCURRENCY)
from ray.serve.http_proxy import HTTPProxyActor
from ray.serve.kv_store_service import (BackendTable, RoutingTable,
                                        TrafficPolicyTable)
from ray.serve.metric import (MetricMonitor, start_metric_monitor_loop)
from ray.serve.utils import expand, get_random_letters

import numpy as np


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
        self.route_table = RoutingTable(kv_store_connector)
        self.backend_table = BackendTable(kv_store_connector)
        self.policy_table = TrafficPolicyTable(kv_store_connector)
        self.tag_to_actor_handles = dict()

        self.router = None
        self.http_proxy = None
        self.metric_monitor = None

    def get_kv_store_connector(self):
        return self.kv_store_connector

    def get_traffic_policy(self, endpoint_name):
        return self.policy_table.list_traffic_policy()[endpoint_name]

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

    def get_metric_monitor(self):
        assert self.metric_monitor is not None, (
            "Metric monitor not started yet.")
        return [self.metric_monitor]

    def scale_replicas(self, backend_tag, num_replicas):
        assert (backend_tag in self.backend_table.list_backends()
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        current_num_replicas = len(
            self.backend_table.list_replicas(backend_tag))
        delta_num_replicas = num_replicas - current_num_replicas

        if delta_num_replicas > 0:
            for _ in range(delta_num_replicas):
                self._start_backend_replica(backend_tag)
        elif delta_num_replicas < 0:
            for _ in range(-delta_num_replicas):
                self._remove_backend_replica(backend_tag)

    def _start_backend_replica(self, backend_tag):
        assert (backend_tag in self.backend_table.list_backends()
                ), "Backend {} is not registered.".format(backend_tag)

        replica_tag = "{}#{}".format(backend_tag, get_random_letters(length=6))

        # Fetch the info to start the replica from the backend table.
        creator = self.backend_table.get_backend_creator(backend_tag)
        backend_config_dict = self.backend_table.get_info(backend_tag)
        backend_config = BackendConfig(**backend_config_dict)
        init_args = self.backend_table.get_init_args(backend_tag)
        kwargs = backend_config.get_actor_creation_args(init_args)

        runner_handle = creator(kwargs)
        self.tag_to_actor_handles[replica_tag] = runner_handle

        # Set up the worker.
        ray.get(
            runner_handle._ray_serve_setup.remote(backend_tag,
                                                  self.get_router()[0],
                                                  runner_handle))
        ray.get(runner_handle._ray_serve_fetch.remote())

        # Register the worker in config tables and metric monitor.
        self.backend_table.add_replica(backend_tag, replica_tag)
        self.get_metric_monitor()[0].add_target.remote(runner_handle)

    def _remove_backend_replica(self, backend_tag):
        assert (backend_tag in self.backend_table.list_backends()
                ), "Backend {} is not registered.".format(backend_tag)
        assert (
            len(self.backend_table.list_replicas(backend_tag)) > 0
        ), "Backend {} does not have enough replicas to be removed.".format(
            backend_tag)

        replica_tag = self.backend_table.remove_replica(backend_tag)
        assert replica_tag in self.tag_to_actor_handles
        replica_handle = self.tag_to_actor_handles.pop(replica_tag)

        # Remove the replica from metric monitor.
        [monitor] = self.get_metric_monitor()
        ray.get(monitor.remove_target.remote(replica_handle))

        # Remove the replica from router.
        # This will also destroy the actor handle.
        [router] = self.get_router()
        ray.get(
            router.remove_and_destory_replica.remote(backend_tag,
                                                     replica_handle))

    def get_all_handles(self):
        return self.tag_to_actor_handles

    def get_all_endpoints(self):
        return expand(
            self.route_table.list_service(include_headless=True).values())

    def split_traffic(self, endpoint_name, traffic_policy_dictionary):
        assert endpoint_name in expand(
            self.route_table.list_service(include_headless=True).values())

        assert isinstance(traffic_policy_dictionary,
                          dict), "Traffic policy must be dictionary"
        prob = 0
        for backend, weight in traffic_policy_dictionary.items():
            prob += weight
            assert (backend in self.backend_table.list_backends()
                    ), "backend {} is not registered".format(backend)
        assert np.isclose(
            prob, 1, atol=0.02
        ), "weights must sum to 1, currently it sums to {}".format(prob)

        self.policy_table.register_traffic_policy(endpoint_name,
                                                  traffic_policy_dictionary)
        [router] = self.get_router()
        ray.get(
            router.set_traffic.remote(endpoint_name,
                                      traffic_policy_dictionary))

    def create_endpoint(self, route, endpoint_name, methods):
        self.route_table.register_service(
            route, endpoint_name, methods=methods)
        [http_proxy] = self.get_http_proxy()
        ray.get(
            http_proxy.set_route_table.remote(
                self.route_table.list_service(
                    include_methods=True, include_headless=False)))


class GlobalState:
    """Encapsulate all global state in the serving system.

    The information is fetch lazily from
        1. A collection of namespaced key value stores
        2. A actor supervisor service
    """

    def __init__(self, master_actor=None):
        # Get actor nursery handle.
        if master_actor is None:
            master_actor = ray.util.get_actor(SERVE_MASTER_NAME)
        self.master_actor = master_actor

        # Connect to all the tables.
        kv_store_connector = ray.get(
            self.master_actor.get_kv_store_connector.remote())
        self.backend_table = BackendTable(kv_store_connector)

    def get_router(self):
        return ray.get(self.master_actor.get_router.remote())[0]

    def get_metric_monitor(self):
        return ray.get(self.master_actor.get_metric_monitor.remote())[0]

    def get_traffic_policy(self, endpoint_name):
        return ray.get(
            self.master_actor.get_traffic_policy.remote(endpoint_name))

    def get_all_endpoints(self):
        return ray.get(self.master_actor.get_all_endpoints.remote())
