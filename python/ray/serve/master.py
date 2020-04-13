import ray
from ray.serve.backend_config import BackendConfig
from ray.serve.constants import ASYNC_CONCURRENCY
from ray.serve.exceptions import batch_annotation_not_found
from ray.serve.http_proxy import HTTPProxyActor
from ray.serve.kv_store_service import (BackendTable, RoutingTable,
                                        TrafficPolicyTable)
from ray.serve.metric import (MetricMonitor, start_metric_monitor_loop)
from ray.serve.backend_worker import create_backend_worker
from ray.serve.utils import expand, get_random_letters

import numpy as np


@ray.remote
class ServeMaster:
    """Initialize and store all actor handles.

    Note:
        This actor is necessary because ray will destroy actors when the
        original actor handle goes out of scope (when driver exit). Therefore
        we need to initialize and store actor handles in a seperate actor.
    """

    def __init__(self, kv_store_connector):
        self.kv_store_connector = kv_store_connector
        self.route_table = RoutingTable(kv_store_connector)
        self.backend_table = BackendTable(kv_store_connector)
        self.policy_table = TrafficPolicyTable(kv_store_connector)
        self.replica_tag_to_workers = dict()

        self.router = None
        self.http_proxy = None
        self.metric_monitor = None

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
        """Start the HTTP proxy on the given host:port.

        On startup (or restart), the HTTP proxy will fetch its config via
        get_http_proxy_config.
        """
        assert self.http_proxy is None, "HTTP proxy already started."
        assert self.router is not None, (
            "Router must be started before HTTP proxy.")
        self.http_proxy = HTTPProxyActor.options(
            max_concurrency=ASYNC_CONCURRENCY,
            max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
        ).remote(host, port)

    async def get_http_proxy_config(self):
        route_table = self.route_table.list_service(
            include_methods=True, include_headless=False)
        return route_table, self.get_router()

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

    def _list_replicas(self, backend_tag):
        return self.backend_table.list_replicas(backend_tag)

    async def scale_replicas(self, backend_tag, num_replicas):
        """Scale the given backend to the number of replicas.

        This requires the master actor to be an async actor because we wait
        synchronously for backends to start up and they may make calls into
        the master actor while initializing (e.g., by calling get_handle()).
        """
        assert (backend_tag in self.backend_table.list_backends()
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        current_num_replicas = len(self._list_replicas(backend_tag))
        delta_num_replicas = num_replicas - current_num_replicas

        if delta_num_replicas > 0:
            for _ in range(delta_num_replicas):
                await self._start_backend_replica(backend_tag)
        elif delta_num_replicas < 0:
            for _ in range(-delta_num_replicas):
                self._remove_backend_replica(backend_tag)

    async def get_backend_worker_config(self):
        return self.get_router()

    async def _start_backend_replica(self, backend_tag):
        assert (backend_tag in self.backend_table.list_backends()
                ), "Backend {} is not registered.".format(backend_tag)

        replica_tag = "{}#{}".format(backend_tag, get_random_letters(length=6))

        # Register the worker in the DB.
        # TODO(edoakes): we should guarantee that if calls to the master
        # succeed, the cluster state has changed and if they fail, it hasn't.
        # Once we have master actor fault tolerance, this breaks that guarantee
        # because this method could fail after writing the replica to the DB.
        self.backend_table.add_replica(backend_tag, replica_tag)

        # Fetch the info to start the replica from the backend table.
        backend_actor = ray.remote(
            self.backend_table.get_backend_creator(backend_tag))
        backend_config_dict = self.backend_table.get_info(backend_tag)
        backend_config = BackendConfig(**backend_config_dict)
        init_args = [
            backend_tag, replica_tag,
            self.backend_table.get_init_args(backend_tag)
        ]
        kwargs = backend_config.get_actor_creation_args(init_args)
        kwargs[
            "max_reconstructions"] = ray.ray_constants.INFINITE_RECONSTRUCTION

        # Start the worker.
        worker_handle = backend_actor._remote(**kwargs)
        self.replica_tag_to_workers[replica_tag] = worker_handle

        # Wait for the worker to start up.
        await worker_handle.ready.remote()
        await self.get_router()[0].add_new_worker.remote(
            backend_tag, worker_handle)

        # Register the worker with the metric monitor.
        self.get_metric_monitor()[0].add_target.remote(worker_handle)

    def _remove_backend_replica(self, backend_tag):
        assert (backend_tag in self.backend_table.list_backends()
                ), "Backend {} is not registered.".format(backend_tag)
        assert (len(self._list_replicas(backend_tag)) >
                0), "Tried to remove replica from empty backend ({}).".format(
                    backend_tag)

        replica_tag = self.backend_table.remove_replica(backend_tag)
        assert replica_tag in self.replica_tag_to_workers
        replica_handle = self.replica_tag_to_workers.pop(replica_tag)

        # Remove the replica from metric monitor.
        [monitor] = self.get_metric_monitor()
        ray.get(monitor.remove_target.remote(replica_handle))

        # Remove the replica from router.
        # This will also destroy the actor handle.
        [router] = self.get_router()
        ray.get(
            router.remove_and_destroy_replica.remote(backend_tag,
                                                     replica_handle))

    def get_all_worker_handles(self):
        return self.replica_tag_to_workers

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

    async def create_backend(self, backend_tag, backend_config, func_or_class,
                             actor_init_args):
        backend_config_dict = dict(backend_config)
        backend_worker = create_backend_worker(func_or_class)

        # Save creator which starts replicas.
        self.backend_table.register_backend(backend_tag, backend_worker)

        # Save information about configurations needed to start the replicas.
        self.backend_table.register_info(backend_tag, backend_config_dict)

        # Save the initial arguments needed by replicas.
        self.backend_table.save_init_args(backend_tag, actor_init_args)

        # Set the backend config inside the router
        # (particularly for max-batch-size).
        [router] = self.get_router()
        ray.get(
            router.set_backend_config.remote(backend_tag, backend_config_dict))
        await self.scale_replicas(backend_tag,
                                  backend_config_dict["num_replicas"])

    async def set_backend_config(self, backend_tag, backend_config):
        assert (backend_tag in self.backend_table.list_backends()
                ), "Backend {} is not registered.".format(backend_tag)
        assert isinstance(backend_config,
                          BackendConfig), ("backend_config must be"
                                           " of instance BackendConfig")
        backend_config_dict = dict(backend_config)
        old_backend_config_dict = self.backend_table.get_info(backend_tag)

        if (not old_backend_config_dict["has_accept_batch_annotation"]
                and backend_config.max_batch_size is not None):
            raise batch_annotation_not_found

        self.backend_table.register_info(backend_tag, backend_config_dict)

        # Inform the router about change in configuration
        # (particularly for setting max_batch_size).
        [router] = self.get_router()
        ray.get(
            router.set_backend_config.remote(backend_tag, backend_config_dict))

        # Restart replicas if there is a change in the backend config related
        # to restart_configs.
        need_to_restart_replicas = any(
            old_backend_config_dict[k] != backend_config_dict[k]
            for k in BackendConfig.restart_on_change_fields)
        if need_to_restart_replicas:
            # Kill all the replicas for restarting with new configurations.
            await self.scale_replicas(backend_tag, 0)

        # Scale the replicas with the new configuration.
        await self.scale_replicas(backend_tag,
                                  backend_config_dict["num_replicas"])

    def get_backend_config(self, backend_tag):
        assert (backend_tag in self.backend_table.list_backends()
                ), "Backend {} is not registered.".format(backend_tag)
        backend_config_dict = self.backend_table.get_info(backend_tag)
        return BackendConfig(**backend_config_dict)
