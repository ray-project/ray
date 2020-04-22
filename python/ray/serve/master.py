from collections import defaultdict
import time

import ray
import ray.cloudpickle as pickle
from ray.serve.backend_config import BackendConfig
from ray.serve.constants import (ASYNC_CONCURRENCY, SERVE_ROUTER_NAME,
                                 SERVE_PROXY_NAME, SERVE_METRIC_MONITOR_NAME)
from ray.serve.exceptions import batch_annotation_not_found
from ray.serve.http_proxy import HTTPProxyActor
from ray.serve.metric import (MetricMonitor, start_metric_monitor_loop)
from ray.serve.backend_worker import create_backend_worker
from ray.serve.utils import get_random_letters, logger

import numpy as np


@ray.remote
class ServeMaster:
    """Initialize and store all actor handles.

    Note:
        This actor is necessary because ray will destroy actors when the
        original actor handle goes out of scope (when driver exit). Therefore
        we need to initialize and store actor handles in a seperate actor.
    """

    def __init__(self, kv_store_connector, router_class, router_kwargs,
                 start_http_proxy, http_proxy_host, http_proxy_port,
                 metric_gc_window_s):
        self.kv_store_client = kv_store_connector("serve_checkpoints")
        # path -> (endpoint, methods).
        self.routes = {}
        # backend -> (worker_creator, init_args, backend_config).
        self.backends = {}
        # backend -> replica_tags.
        self.replicas = defaultdict(list)
        self.replicas_to_start = defaultdict(list)
        self.replicas_to_stop = defaultdict(list)
        # endpoint -> traffic_dict
        self.traffic_policies = dict()
        # Dictionary of backend tag to dictionaries of replica tag to worker.
        # TODO(edoakes): consider removing this and just using the names.
        self.workers = defaultdict(dict)

        self.router = None
        self.http_proxy = None
        self.metric_monitor = None

        self._get_or_start_router(router_class, router_kwargs)
        if start_http_proxy:
            self._get_or_start_http_proxy(http_proxy_host, http_proxy_port)
        # self._get_or_start_metric_monitor(metric_gc_window_s)

        self._recover_from_checkpoint()

    def _get_or_start_router(self, router_class, router_kwargs):
        try:
            self.router = ray.util.get_actor(SERVE_ROUTER_NAME)
            logger.info("Router already running")
        except ValueError:
            logger.info(
                "Starting router with name '{}'".format(SERVE_ROUTER_NAME))
            self.router = router_class.options(
                detached=True,
                name=SERVE_ROUTER_NAME,
                max_concurrency=ASYNC_CONCURRENCY,
                max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
            ).remote(**router_kwargs)

    def _get_or_start_http_proxy(self, host, port):
        """Start the HTTP proxy on the given host:port if not already started.

        On startup (or restart), the HTTP proxy will fetch its config via
        get_http_proxy_config.
        """
        try:
            self.http_proxy = ray.util.get_actor(SERVE_PROXY_NAME)
            logger.info("HTTP proxy already running")
        except ValueError:
            logger.info(
                "Starting HTTP proxy with name '{}'".format(SERVE_PROXY_NAME))
            self.http_proxy = HTTPProxyActor.options(
                detached=True,
                name=SERVE_PROXY_NAME,
                max_concurrency=ASYNC_CONCURRENCY,
                max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
            ).remote(host, port)

    def _get_or_start_metric_monitor(self, gc_window_s):
        try:
            self.metric_monitor = ray.util.get_actor(SERVE_METRIC_MONITOR_NAME)
            logger.info("Metric monitor already running")
        except ValueError:
            logger.info("Starting metric monitor with name '{}'".format(
                SERVE_METRIC_MONITOR_NAME))
            self.metric_monitor = MetricMonitor.options(
                detached=True,
                name=SERVE_METRIC_MONITOR_NAME).remote(gc_window_s)
            # TODO(edoakes): move these into the constructor.
            start_metric_monitor_loop.remote(self.metric_monitor)
            self.metric_monitor.add_target.remote(self.router)

    def _checkpoint(self):
        logger.debug("Master actor writing checkpoint")
        checkpoint = pickle.dumps(
            (self.routes, self.backends, self.traffic_policies, self.replicas,
             self.replicas_to_start, self.replicas_to_stop))
        self.kv_store_client.put("checkpoint", checkpoint)

    def _recover_from_checkpoint(self):
        checkpoint = self.kv_store_client.get("checkpoint")
        if checkpoint is None:
            logger.info("No checkpoint found")
            return

        start = time.time()
        logger.info("Recovering from checkpoint")
        # 1) Check "base components" - they might not have been started yet, in
        # which case we need to start them.
        # 2) Check workers - they might not have been started yet, in which
        # case we need to start them.
        # 3) Delete the workers that we're removing (take a new checkpoint?)
        (self.routes, self.backends, self.traffic_policies, self.replicas,
         self.replicas_to_start,
         self.replicas_to_stop) = pickle.loads(checkpoint)

        logger.info("replicas " + str(self.replicas))

        for backend_tag, replica_tags in self.replicas.items():
            for replica_tag in replica_tags:
                self.workers[backend_tag][replica_tag] = ray.util.get_actor(
                    replica_tag)

        # TODO(edoakes): should we make this a pull-only model for simplicity?
        for endpoint, traffic_policy in self.traffic_policies.items():
            self.router.set_traffic.remote(endpoint, traffic_policy)

        for backend_tag, replica_dict in self.workers.items():
            for replica_tag, worker in replica_dict.items():
                self.router.add_new_worker.remote(backend_tag, replica_tag,
                                                  worker)

        for backend, (_, _, backend_config_dict) in self.backends.items():
            self.router.set_backend_config.remote(backend, backend_config_dict)

        self.http_proxy.set_route_table.remote(self.routes)

        self._start_pending_replicas()
        self._stop_pending_replicas()

        logger.info(
            "Recovered from checkpoint in {:.3f}s".format(time.time() - start))

    def get_backend_configs(self):
        backend_configs = {}
        for backend, (_, _, backend_config_dict) in self.backends.items():
            backend_configs[backend] = backend_config_dict
        return backend_configs

    def get_traffic_policies(self):
        return self.traffic_policies

    def _list_replicas(self, backend_tag):
        return self.replicas[backend_tag]

    def get_traffic_policy(self, endpoint):
        return self.traffic_policies[endpoint]

    def get_router(self):
        return [self.router]

    def get_http_proxy_config(self):
        return self.routes, self.get_router()

    def get_http_proxy(self):
        return [self.http_proxy]

    def get_metric_monitor(self):
        return [self.metric_monitor]

    def _start_backend_worker(self, backend_tag, replica_tag):
        logger.debug("Starting worker '{}' for backend '{}'.".format(
            replica_tag, backend_tag))
        worker_creator, init_args, config_dict = self.backends[backend_tag]
        # TODO(edoakes): just store the BackendConfig in self.backends.
        backend_config = BackendConfig(**config_dict)
        kwargs = backend_config.get_actor_creation_args()

        worker_handle = ray.remote(worker_creator).options(
            detached=True,
            name=replica_tag,
            max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
            **kwargs).remote(backend_tag, replica_tag, init_args)
        # TODO(edoakes): we should probably have a timeout here.
        ray.get(worker_handle.ready.remote())
        return worker_handle

    def _start_pending_replicas(self):
        # Note: they may already be created if we failed during this operation.
        for backend_tag, replicas_to_create in self.replicas_to_start.items():
            for replica_tag in replicas_to_create:
                try:
                    worker_handle = ray.util.get_actor(replica_tag)
                except ValueError:
                    if replica_tag.startswith("counter"):
                        logger.info("starting worker " + replica_tag)
                    worker_handle = self._start_backend_worker(
                        backend_tag, replica_tag)

                self.replicas[backend_tag].append(replica_tag)
                self.workers[backend_tag][replica_tag] = worker_handle

                # Register the worker with the router.
                self.router.add_new_worker.remote(backend_tag, replica_tag,
                                                  worker_handle)

                # Register the worker with the metric monitor.
                #self.get_metric_monitor()[0].add_target.remote(worker_handle)

        self.replicas_to_start.clear()

    def _stop_pending_replicas(self):
        # Note: they may already be deleted if we failed during this operation.
        for backend_tag, replicas_to_stop in self.replicas_to_stop.items():
            for replica_tag in replicas_to_stop:
                try:
                    worker_handle = ray.util.get_actor(replica_tag)
                    # Remove the replica from metric monitor.
                    #[monitor] = self.get_metric_monitor()
                    #monitor.remove_target.remote(worker_handle)

                    # Remove the replica from router.
                    # This will also submit __ray_terminate__ on the worker.
                    # We kill the worker from the router to guarantee that the
                    # router won't submit any more requests on it.
                    self.router.remove_worker.remote(backend_tag, replica_tag)
                except ValueError:
                    pass

        self.replicas_to_stop.clear()

    def scale_replicas(self, backend_tag, num_replicas):
        """Scale the given backend to the number of replicas."""
        logger.debug("Scaling backend '{}' to {} replicas".format(
            backend_tag, num_replicas))
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        current_num_replicas = len(self.replicas[backend_tag])
        delta_num_replicas = num_replicas - current_num_replicas

        if delta_num_replicas > 0:
            logger.info("Adding {} replicas".format(delta_num_replicas))
            for _ in range(delta_num_replicas):
                replica_tag = "{}#{}".format(backend_tag, get_random_letters())
                self.replicas_to_start[backend_tag].append(replica_tag)

        elif delta_num_replicas < 0:
            logger.info("Removing {} replicas".format(-delta_num_replicas))
            assert len(self.replicas[backend_tag]) >= delta_num_replicas
            for _ in range(-delta_num_replicas):
                replica_tag = self.replicas[backend_tag].pop()
                if len(self.replicas[backend_tag]) == 0:
                    del self.replicas[backend_tag]
                del self.workers[backend_tag][replica_tag]
                if len(self.workers[backend_tag]) == 0:
                    del self.workers[backend_tag]

                self.replicas_to_stop[backend_tag].append(replica_tag)

    def get_all_worker_handles(self):
        return self.workers

    def get_all_endpoints(self):
        return [endpoint for endpoint, methods in self.routes.values()]

    def set_traffic(self, endpoint_name, traffic_policy_dictionary):
        assert endpoint_name in self.get_all_endpoints(), \
            "Attempted to assign traffic for an endpoint '{}'" \
            " that is not registered.".format(endpoint_name)

        assert isinstance(traffic_policy_dictionary,
                          dict), "Traffic policy must be dictionary"
        prob = 0
        for backend, weight in traffic_policy_dictionary.items():
            prob += weight
            assert backend in self.backends, \
                "Attempted to assign traffic to a backend '{}' that " \
                "is not registered.".format(backend)

        assert np.isclose(
            prob, 1, atol=0.02
        ), "weights must sum to 1, currently they sum to {}".format(prob)

        self.traffic_policies[endpoint_name] = traffic_policy_dictionary

        self._checkpoint()

        # XXX: comment that this should be separate?
        self.router.set_traffic.remote(endpoint_name,
                                       traffic_policy_dictionary)

    def create_endpoint(self, route, endpoint, methods):
        # If this is a headless service with no route, key the endpoint based
        # on its name.
        # TODO(edoakes): we should probably just store routes and endpoints
        # separately.
        if route is None:
            route = endpoint

        # TODO(edoakes): move this to client side.
        #err_prefix = "Cannot create endpoint."
        #assert route not in self.routes, \
        #"{} Route '{}' is already registered.".format(err_prefix, route)
        #assert endpoint not in self.get_all_endpoints(), \
        #"{} Endpoint '{}' is already registered.".format(err_prefix,
        #endpoint)
        logger.debug(
            "Registering route {} to endpoint {} with methods {}.".format(
                route, endpoint, methods))
        # TODO(edoakes): reject existing routes.
        self.routes[route] = (endpoint, methods)

        self._checkpoint()

        [http_proxy] = self.get_http_proxy()
        # XXX: comment that this should be separate?
        http_proxy.set_route_table.remote(self.routes)

    # XXX: this is no longer synchronous. What do?
    def create_backend(self, backend_tag, backend_config, func_or_class,
                       actor_init_args):
        backend_config_dict = dict(backend_config)
        backend_worker = create_backend_worker(func_or_class)

        # TODO(edoakes): move this to client side.
        #assert backend_tag not in self.backends, \
        #"Cannot create backend '{}' because a backend" \
        #"with that name already exists.".format(backend_tag)

        # Save creator that starts replicas, the arguments to be passed in,
        # and the configuration for the backends.
        self.backends[backend_tag] = (backend_worker, actor_init_args,
                                      backend_config_dict)

        self.scale_replicas(backend_tag, backend_config_dict["num_replicas"])

        self._checkpoint()

        # XXX: comment that these should get placed on event loop?
        self._start_pending_replicas()
        self._stop_pending_replicas()

        # Set the backend config inside the router
        # (particularly for max-batch-size).
        self.router.set_backend_config.remote(backend_tag, backend_config_dict)

    def set_backend_config(self, backend_tag, backend_config):
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
        self.router.set_backend_config.remote(backend_tag, backend_config_dict)

        # Restart replicas if there is a change in the backend config related
        # to restart_configs.
        need_to_restart_replicas = any(
            old_backend_config_dict[k] != backend_config_dict[k]
            for k in BackendConfig.restart_on_change_fields)
        if need_to_restart_replicas:
            # Kill all the replicas for restarting with new configurations.
            self.scale_replicas(backend_tag, 0)

        # Scale the replicas with the new configuration.
        self.scale_replicas(backend_tag, backend_config_dict["num_replicas"])

        self._checkpoint()

        # XXX: comment that these should get placed on event loop?
        self._start_pending_replicas()
        self._stop_pending_replicas()

    def get_backend_config(self, backend_tag):
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        return BackendConfig(**self.backends[backend_tag][2])
