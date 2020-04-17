import asyncio
from collections import defaultdict
from functools import wraps
import inspect

import ray
from ray.serve.backend_config import BackendConfig
from ray.serve.constants import ASYNC_CONCURRENCY
from ray.serve.exceptions import batch_annotation_not_found
from ray.serve.http_proxy import HTTPProxyActor
from ray.serve.metric import (MetricMonitor, start_metric_monitor_loop)
from ray.serve.backend_worker import create_backend_worker
from ray.serve.utils import get_random_letters, logger

import numpy as np


def async_retryable(cls):
    """Make all actor method invocations on the class retryable.

    Note: This will retry actor_handle.method_name.remote(), but it must
    be invoked in an async context.

    Usage:
        @ray.remote(max_reconstructions=10000)
        @retryable
        class A:
            pass
    """
    for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):

        def decorate_with_retry(f):
            @wraps(f)
            async def retry_method(*args, **kwargs):
                while True:
                    result = await f(*args, **kwargs)
                    if isinstance(result, ray.exceptions.RayActorError):
                        logger.warning(
                            "Actor method '{}' failed, retrying after 100ms.".
                            format(name))
                        await asyncio.sleep(0.1)
                    else:
                        return result

            return retry_method

        method.__ray_invocation_decorator__ = decorate_with_retry
    return cls


@ray.remote
class ServeMaster:
    """Initialize and store all actor handles.

    Note:
        This actor is necessary because ray will destroy actors when the
        original actor handle goes out of scope (when driver exit). Therefore
        we need to initialize and store actor handles in a seperate actor.
    """

    def __init__(self, kv_store_connector):
        self.kv_store_client = kv_store_connector("serve_checkpoints")
        # path -> (endpoint, methods).
        self.routes = {}
        # backend -> (worker_creator, init_args, backend_config).
        self.backends = {}
        # backend -> replica_tags.
        self.replicas = defaultdict(list)
        # endpoint -> traffic_dict
        self.traffic_policies = dict()
        # Dictionary of backend tag to dictionaries of replica tag to worker.
        self.workers = defaultdict(dict)

        self.router = None
        self.http_proxy = None
        self.metric_monitor = None

    def checkpoint(self):
        checkpoint = pickle.dumps((
            self.routes, self.backends,
            self.replicas, self.traffic_policies))
        self.kv_store_client.put("checkpoint", checkpoint)

    def load_checkpoint(self, checkpoint):
        checkpoint = self.kv_store_client.get("checkpoint")
        (self.routes, self.backends,
         self.replicas, self.traffic_policies) = pickle.loads(checkpoint)

    def _list_replicas(self, backend_tag):
        return self.replicas[backend_tag]

    def get_traffic_policy(self, endpoint):
        return self.traffic_policies[endpoint]

    def start_router(self, router_class, init_kwargs):
        assert self.router is None, "Router already started."
        self.router = async_retryable(router_class).options(
            max_concurrency=ASYNC_CONCURRENCY,
            max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
        ).remote(**init_kwargs)

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
        self.http_proxy = async_retryable(HTTPProxyActor).options(
            max_concurrency=ASYNC_CONCURRENCY,
            max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
        ).remote(host, port)

    async def get_http_proxy_config(self):
        return self.routes, self.get_router()

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

    async def scale_replicas(self, backend_tag, num_replicas):
        """Scale the given backend to the number of replicas.

        This requires the master actor to be an async actor because we wait
        synchronously for backends to start up and they may make calls into
        the master actor while initializing (e.g., by calling get_handle()).
        """
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        current_num_replicas = len(self.replicas[backend_tag])
        delta_num_replicas = num_replicas - current_num_replicas

        if delta_num_replicas > 0:
            for _ in range(delta_num_replicas):
                await self._start_backend_replica(backend_tag)
        elif delta_num_replicas < 0:
            for _ in range(-delta_num_replicas):
                await self._remove_backend_replica(backend_tag)

    async def get_backend_worker_config(self):
        return self.get_router()

    async def _start_backend_replica(self, backend_tag):
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)

        replica_tag = "{}#{}".format(backend_tag, get_random_letters(length=6))

        # Register the worker in the DB.
        # TODO(edoakes): we should guarantee that if calls to the master
        # succeed, the cluster state has changed and if they fail, it hasn't.
        # Once we have master actor fault tolerance, this breaks that guarantee
        # because this method could fail after writing the replica to the DB.
        self.replicas[backend_tag].append(replica_tag)

        # Fetch the info to start the replica from the backend table.
        worker_creator, init_args, config_dict = self.backends[backend_tag]
        backend_config = BackendConfig(**config_dict)
        init_args = [backend_tag, replica_tag, init_args]

        kwargs = backend_config.get_actor_creation_args(init_args)
        kwargs[
            "max_reconstructions"] = ray.ray_constants.INFINITE_RECONSTRUCTION

        # Start the worker.
        worker_handle = ray.remote(worker_creator)._remote(**kwargs)
        self.workers[backend_tag][replica_tag] = worker_handle

        # Wait for the worker to start up.
        await worker_handle.ready.remote()

        [router] = self.get_router()
        await router.add_new_worker.remote(backend_tag, worker_handle)

        # Register the worker with the metric monitor.
        self.get_metric_monitor()[0].add_target.remote(worker_handle)

    async def _remove_backend_replica(self, backend_tag):
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        assert (len(self.replicas[backend_tag]) >
                0), "Tried to remove replica from empty backend ({}).".format(
                    backend_tag)

        assert backend_tag in self.replicas
        replica_tag = self.replicas[backend_tag].pop()

        assert backend_tag in self.workers
        assert replica_tag in self.workers[backend_tag]
        replica_handle = self.workers[backend_tag].pop(replica_tag)
        if len(self.workers[backend_tag]) == 0:
            del self.workers[backend_tag]

        # Remove the replica from metric monitor.
        [monitor] = self.get_metric_monitor()
        await monitor.remove_target.remote(replica_handle)

        # Remove the replica from router.
        # This will also destroy the actor handle.
        [router] = self.get_router()
        await router.remove_worker.remote(backend_tag, replica_handle)

    def get_all_worker_handles(self):
        return self.workers

    def get_all_endpoints(self):
        return [endpoint for endpoint, methods in self.routes.values()]

    async def split_traffic(self, endpoint_name, traffic_policy_dictionary):
        assert endpoint_name in self.get_all_endpoints()

        assert isinstance(traffic_policy_dictionary,
                          dict), "Traffic policy must be dictionary"
        prob = 0
        for backend, weight in traffic_policy_dictionary.items():
            prob += weight
            assert (backend in self.backends
                    ), "backend {} is not registered".format(backend)
        assert np.isclose(
            prob, 1, atol=0.02
        ), "weights must sum to 1, currently it sums to {}".format(prob)

        self.traffic_policies[endpoint_name] = traffic_policy_dictionary

        [router] = self.get_router()
        await router.set_traffic.remote(endpoint_name,
                                        traffic_policy_dictionary)

    async def create_endpoint(self, route, endpoint, methods):
        logger.debug(
            "Registering route {} to endpoint {} with methods {}.".format(
                route, endpoint, methods))
        # TODO(edoakes): reject existing routes.
        self.routes[route] = (endpoint, methods)

        [http_proxy] = self.get_http_proxy()
        await http_proxy.set_route_table.remote(self.routes)

    async def create_backend(self, backend_tag, backend_config, func_or_class,
                             actor_init_args):
        backend_config_dict = dict(backend_config)
        backend_worker = create_backend_worker(func_or_class)

        # Save creator that starts replicas, the arguments to be passed in,
        # and the configuration for the backends.
        self.backends[backend_tag] = (backend_worker, actor_init_args,
                                      backend_config_dict)

        # Set the backend config inside the router
        # (particularly for max-batch-size).
        [router] = self.get_router()
        await router.set_backend_config.remote(backend_tag,
                                               backend_config_dict)

        await self.scale_replicas(backend_tag,
                                  backend_config_dict["num_replicas"])

    async def set_backend_config(self, backend_tag, backend_config):
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        assert isinstance(backend_config,
                          BackendConfig), ("backend_config must be"
                                           " of instance BackendConfig")
        backend_config_dict = dict(backend_config)
        backend_worker, init_args, old_backend_config_dict = self.backends[
            backend_tag]

        if (not old_backend_config_dict["has_accept_batch_annotation"]
                and backend_config.max_batch_size is not None):
            raise batch_annotation_not_found

        self.backends[backend_tag] = (backend_worker, init_args,
                                      backend_config_dict)

        # Inform the router about change in configuration
        # (particularly for setting max_batch_size).
        [router] = self.get_router()
        await router.set_backend_config.remote(backend_tag,
                                               backend_config_dict)

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
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        return BackendConfig(**self.backends[backend_tag][2])
