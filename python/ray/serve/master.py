import asyncio
from collections import defaultdict
import os
import random
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
from ray.serve.utils import async_retryable, get_random_letters, logger

import numpy as np

# Used for testing purposes only. If this is set, the master actor will crash
# after writing each checkpoint with the specified probability.
_CRASH_AFTER_CHECKPOINT_PROBABILITY = 0.0


@ray.remote
class ServeMaster:
    """Responsible for managing the state of the serving system.

    The master actor implements fault tolerance by persisting its state in
    a new checkpoint each time a state change is made. If the actor crashes,
    the latest checkpoint is loaded and the state is recovered. Checkpoints
    are written/read using a provided KV-store interface.

    All hard state in the system is maintained by this actor and persisted via
    these checkpoints. Soft state required by other components is fetched by
    those actors from this actor on startup and updates are pushed out from
    this actor.

    All other actors started by the master actor are named, detached actors
    so they will not fate share with the master if it crashes.

    The following guarantees are provided for state-changing calls to the
    master actor:
        - If the call succeeds, the change was made and will be reflected in
          the system even if the master actor or other actors die unexpectedly.
        - If the call fails, the change may have been made but isn't guaranteed
          to have been. The client should retry in this case. Note that this
          requires all implementations here to be idempotent.
    """

    async def __init__(self, kv_store_connector, router_class, router_kwargs,
                       start_http_proxy, http_proxy_host, http_proxy_port,
                       metric_gc_window_s):
        # Used to read/write checkpoints.
        # TODO(edoakes): namespace the master actor and its checkpoints.
        self.kv_store_client = kv_store_connector("serve_checkpoints")
        # path -> (endpoint, methods).
        self.routes = {}
        # backend -> (worker_creator, init_args, backend_config).
        self.backends = {}
        # backend -> replica_tags.
        self.replicas = defaultdict(list)
        # replicas that should be started if recovering from a checkpoint.
        self.replicas_to_start = defaultdict(list)
        # replicas that should be stopped if recovering from a checkpoint.
        self.replicas_to_stop = defaultdict(list)
        # endpoint -> traffic_dict
        self.traffic_policies = dict()
        # Dictionary of backend tag to dictionaries of replica tag to worker.
        # TODO(edoakes): consider removing this and just using the names.
        self.workers = defaultdict(dict)

        # Used to ensure that only a single state-changing operation happens
        # at any given time.
        self.write_lock = asyncio.Lock()

        # Cached handles to actors in the system.
        self.router = None
        self.http_proxy = None
        self.metric_monitor = None

        # If starting the actor for the first time, starts up the other system
        # components. If recovering, fetches their actor handles.
        self._get_or_start_router(router_class, router_kwargs)
        if start_http_proxy:
            self._get_or_start_http_proxy(http_proxy_host, http_proxy_port)
        self._get_or_start_metric_monitor(metric_gc_window_s)

        # NOTE(edoakes): unfortunately, we can't completely recover from a
        # checkpoint in the constructor because we block while waiting for
        # other actors to start up, and those actors fetch soft state from
        # this actor. Because no other tasks will start executing until after
        # the constructor finishes, if we were to run this logic in the
        # constructor it could lead to deadlock between this actor and a child.
        # However we do need to guarantee that we have fully recovered from a
        # checkpoint before any other state-changing calls run. We address this
        # by acquiring the write_lock and then posting the task to recover from
        # a checkpoint to the event loop. Other state-changing calls acquire
        # this lock and will be blocked until recovering from the checkpoint
        # finishes.
        checkpoint = self.kv_store_client.get("checkpoint")
        if checkpoint is None:
            logger.debug("No checkpoint found")
        else:
            await self.write_lock.acquire()
            asyncio.get_event_loop().create_task(
                self._recover_from_checkpoint(checkpoint))

    def _get_or_start_router(self, router_class, router_kwargs):
        """Get the router belonging to this serve cluster.

        If the router does not already exist, it will be started.
        """
        try:
            self.router = ray.util.get_actor(SERVE_ROUTER_NAME)
        except ValueError:
            logger.info(
                "Starting router with name '{}'".format(SERVE_ROUTER_NAME))
            self.router = async_retryable(router_class).options(
                detached=True,
                name=SERVE_ROUTER_NAME,
                max_concurrency=ASYNC_CONCURRENCY,
                max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
            ).remote(**router_kwargs)

    def get_router(self):
        """Returns a handle to the router managed by this actor."""
        return [self.router]

    def _get_or_start_http_proxy(self, host, port):
        """Get the HTTP proxy belonging to this serve cluster.

        If the HTTP proxy does not already exist, it will be started.
        """
        try:
            self.http_proxy = ray.util.get_actor(SERVE_PROXY_NAME)
        except ValueError:
            logger.info(
                "Starting HTTP proxy with name '{}'".format(SERVE_PROXY_NAME))
            self.http_proxy = async_retryable(HTTPProxyActor).options(
                detached=True,
                name=SERVE_PROXY_NAME,
                max_concurrency=ASYNC_CONCURRENCY,
                max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
            ).remote(host, port)

    def get_http_proxy(self):
        """Returns a handle to the HTTP proxy managed by this actor."""
        return [self.http_proxy]

    def get_http_proxy_config(self):
        """Called by the HTTP proxy on startup to fetch required state."""
        return self.routes, self.get_router()

    def _get_or_start_metric_monitor(self, gc_window_s):
        """Get the metric monitor belonging to this serve cluster.

        If the metric monitor does not already exist, it will be started.
        """
        try:
            self.metric_monitor = ray.util.get_actor(SERVE_METRIC_MONITOR_NAME)
        except ValueError:
            logger.info("Starting metric monitor with name '{}'".format(
                SERVE_METRIC_MONITOR_NAME))
            self.metric_monitor = MetricMonitor.options(
                detached=True,
                name=SERVE_METRIC_MONITOR_NAME).remote(gc_window_s)
            # TODO(edoakes): move these into the constructor.
            start_metric_monitor_loop.remote(self.metric_monitor)
            self.metric_monitor.add_target.remote(self.router)

    def get_metric_monitor(self):
        """Returns a handle to the metric monitor managed by this actor."""
        return [self.metric_monitor]

    def _checkpoint(self):
        """Checkpoint internal state and write it to the KV store."""
        logger.debug("Writing checkpoint")
        start = time.time()
        checkpoint = pickle.dumps(
            (self.routes, self.backends, self.traffic_policies, self.replicas,
             self.replicas_to_start, self.replicas_to_stop))

        self.kv_store_client.put("checkpoint", checkpoint)
        logger.debug("Wrote checkpoint in {:.2f}".format(time.time() - start))

        if random.random() < _CRASH_AFTER_CHECKPOINT_PROBABILITY:
            logger.warning("Intentionally crashing after checkpoint")
            os._exit(0)

    async def _recover_from_checkpoint(self, checkpoint_bytes):
        """Recover the cluster state from the provided checkpoint.

        Performs the following operations:
            1) Deserializes the internal state from the checkpoint.
            2) Pushes the latest configuration to the HTTP proxy and router
               in case we crashed before updating them.
            3) Starts/stops any worker replicas that are pending creation or
               deletion.

        NOTE: this requires that self.write_lock is already acquired and will
        release it before returning.
        """
        assert self.write_lock.locked()

        start = time.time()
        logger.info("Recovering from checkpoint")

        # Load internal state from the checkpoint data.
        (self.routes, self.backends, self.traffic_policies, self.replicas,
         self.replicas_to_start,
         self.replicas_to_stop) = pickle.loads(checkpoint_bytes)

        # Fetch actor handles for all of the backend replicas in the system.
        # All of these workers are guaranteed to already exist because they
        # would not be written to a checkpoint in self.workers until they
        # were created.
        for backend_tag, replica_tags in self.replicas.items():
            for replica_tag in replica_tags:
                self.workers[backend_tag][replica_tag] = ray.util.get_actor(
                    replica_tag)

        # Push configuration state to the router.
        # TODO(edoakes): should we make this a pull-only model for simplicity?
        for endpoint, traffic_policy in self.traffic_policies.items():
            await self.router.set_traffic.remote(endpoint, traffic_policy)

        for backend_tag, replica_dict in self.workers.items():
            for replica_tag, worker in replica_dict.items():
                await self.router.add_new_worker.remote(
                    backend_tag, replica_tag, worker)

        for backend, (_, _, backend_config_dict) in self.backends.items():
            await self.router.set_backend_config.remote(
                backend, backend_config_dict)

        # Push configuration state to the HTTP proxy.
        await self.http_proxy.set_route_table.remote(self.routes)

        # Start/stop any pending backend replicas.
        await self._start_pending_replicas()
        await self._stop_pending_replicas()

        logger.info(
            "Recovered from checkpoint in {:.3f}s".format(time.time() - start))

        self.write_lock.release()

    def get_backend_configs(self):
        """Fetched by the router on startup."""
        backend_configs = {}
        for backend, (_, _, backend_config_dict) in self.backends.items():
            backend_configs[backend] = backend_config_dict
        return backend_configs

    def get_traffic_policies(self):
        """Fetched by the router on startup."""
        return self.traffic_policies

    def _list_replicas(self, backend_tag):
        """Used only for testing."""
        return self.replicas[backend_tag]

    def get_traffic_policy(self, endpoint):
        """Fetched by serve handles."""
        return self.traffic_policies[endpoint]

    async def _start_backend_worker(self, backend_tag, replica_tag):
        """Creates a backend worker and waits for it to start up.

        Assumes that the backend configuration has already been registered
        in self.backends.
        """
        logger.debug("Starting worker '{}' for backend '{}'.".format(
            replica_tag, backend_tag))
        worker_creator, init_args, config_dict = self.backends[backend_tag]
        # TODO(edoakes): just store the BackendConfig in self.backends.
        backend_config = BackendConfig(**config_dict)
        kwargs = backend_config.get_actor_creation_args()

        worker_handle = async_retryable(ray.remote(worker_creator)).options(
            detached=True,
            name=replica_tag,
            max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
            **kwargs).remote(backend_tag, replica_tag, init_args)
        # TODO(edoakes): we should probably have a timeout here.
        await worker_handle.ready.remote()
        return worker_handle

    async def _start_pending_replicas(self):
        """Starts the pending backend replicas in self.replicas_to_start.

        Starts the worker, then pushes an update to the router to add it to
        the proper backend. If the worker has already been started, only
        updates the router.

        Clears self.replicas_to_start.
        """
        for backend_tag, replicas_to_create in self.replicas_to_start.items():
            for replica_tag in replicas_to_create:
                # NOTE(edoakes): the replicas may already be created if we
                # failed after creating them but before writing a checkpoint.
                try:
                    worker_handle = ray.util.get_actor(replica_tag)
                except ValueError:
                    worker_handle = await self._start_backend_worker(
                        backend_tag, replica_tag)

                self.replicas[backend_tag].append(replica_tag)
                self.workers[backend_tag][replica_tag] = worker_handle

                # Register the worker with the router.
                await self.router.add_new_worker.remote(
                    backend_tag, replica_tag, worker_handle)

                # Register the worker with the metric monitor.
                self.metric_monitor.add_target.remote(worker_handle)

        self.replicas_to_start.clear()

    async def _stop_pending_replicas(self):
        """Stops the pending backend replicas in self.replicas_to_stop.

        Stops workers by telling the router to remove them.

        Clears self.replicas_to_stop.
        """
        for backend_tag, replicas_to_stop in self.replicas_to_stop.items():
            for replica_tag in replicas_to_stop:
                # NOTE(edoakes): the replicas may already be stopped if we
                # failed after stopping them but before writing a checkpoint.
                try:
                    # Remove the replica from router.
                    # This will also submit __ray_terminate__ on the worker.
                    # NOTE(edoakes): we currently need to kill the worker from
                    # the router to guarantee that the router won't submit any
                    # more requests to it.
                    await self.router.remove_worker.remote(
                        backend_tag, replica_tag)
                except ValueError:
                    pass

        self.replicas_to_stop.clear()

    def _scale_replicas(self, backend_tag, num_replicas):
        """Scale the given backend to the number of replicas.

        NOTE: this does not actually start or stop the replicas, but instead
        adds the intention to start/stop them to self.workers_to_start and
        self.workers_to_stop. The caller is responsible for then first writing
        a checkpoint and then actually starting/stopping the intended replicas.
        This avoids inconsistencies with starting/stopping a worker and then
        crashing before writing a checkpoint.
        """
        logger.debug("Scaling backend '{}' to {} replicas".format(
            backend_tag, num_replicas))
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        current_num_replicas = len(self.replicas[backend_tag])
        delta_num_replicas = num_replicas - current_num_replicas

        if delta_num_replicas > 0:
            logger.debug("Adding {} replicas to backend {}".format(
                delta_num_replicas, backend_tag))
            for _ in range(delta_num_replicas):
                replica_tag = "{}#{}".format(backend_tag, get_random_letters())
                self.replicas_to_start[backend_tag].append(replica_tag)

        elif delta_num_replicas < 0:
            logger.debug("Removing {} replicas from backend {}".format(
                -delta_num_replicas, backend_tag))
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
        """Fetched by the router on startup."""
        return self.workers

    def get_all_endpoints(self):
        """Used for validation by the API client."""
        return [endpoint for endpoint, methods in self.routes.values()]

    async def set_traffic(self, endpoint_name, traffic_policy_dictionary):
        """Sets the traffic policy for the specified endpoint."""
        async with self.write_lock:
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

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()
            await self.router.set_traffic.remote(endpoint_name,
                                                 traffic_policy_dictionary)

    async def create_endpoint(self, route, endpoint, methods):
        """Create a new endpoint with the specified route and methods.

        If the route is None, this is a "headless" endpoint that will not
        be added to the HTTP proxy (can only be accessed via a handle).
        """
        async with self.write_lock:
            # If this is a headless service with no route, key the endpoint
            # based on its name.
            # TODO(edoakes): we should probably just store routes and endpoints
            # separately.
            if route is None:
                route = endpoint

            # TODO(edoakes): move this to client side.
            err_prefix = "Cannot create endpoint."
            if route in self.routes:
                if self.routes[route] == (endpoint, methods):
                    return
                else:
                    raise ValueError(
                        "{} Route '{}' is already registered.".format(
                            err_prefix, route))

            if endpoint in self.get_all_endpoints():
                raise ValueError(
                    "{} Endpoint '{}' is already registered.".format(
                        err_prefix, endpoint))

            logger.info(
                "Registering route {} to endpoint {} with methods {}.".format(
                    route, endpoint, methods))

            self.routes[route] = (endpoint, methods)

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()
            await self.http_proxy.set_route_table.remote(self.routes)

    async def create_backend(self, backend_tag, backend_config, func_or_class,
                             actor_init_args):
        """Register a new backend under the specified tag."""
        async with self.write_lock:
            backend_config_dict = dict(backend_config)
            backend_worker = create_backend_worker(func_or_class)

            # Save creator that starts replicas, the arguments to be passed in,
            # and the configuration for the backends.
            self.backends[backend_tag] = (backend_worker, actor_init_args,
                                          backend_config_dict)

            self._scale_replicas(backend_tag,
                                 backend_config_dict["num_replicas"])

            # NOTE(edoakes): we must write a checkpoint before starting new
            # or pushing the updated config to avoid inconsistent state if we
            # crash while making the change.
            self._checkpoint()
            await self._start_pending_replicas()
            await self._stop_pending_replicas()

            # Set the backend config inside the router
            # (particularly for max-batch-size).
            await self.router.set_backend_config.remote(
                backend_tag, backend_config_dict)

    async def set_backend_config(self, backend_tag, backend_config):
        """Set the config for the specified backend."""
        async with self.write_lock:
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

            # Restart replicas if there is a change in the backend config
            # related to restart_configs.
            need_to_restart_replicas = any(
                old_backend_config_dict[k] != backend_config_dict[k]
                for k in BackendConfig.restart_on_change_fields)
            if need_to_restart_replicas:
                # Kill all the replicas for restarting with new configurations.
                self._scale_replicas(backend_tag, 0)

            # Scale the replicas with the new configuration.
            self._scale_replicas(backend_tag,
                                 backend_config_dict["num_replicas"])

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()

            # Inform the router about change in configuration
            # (particularly for setting max_batch_size).
            await self.router.set_backend_config.remote(
                backend_tag, backend_config_dict)

            await self._start_pending_replicas()
            await self._stop_pending_replicas()

    def get_backend_config(self, backend_tag):
        """Get the current config for the specified backend."""
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        return BackendConfig(**self.backends[backend_tag][2])
