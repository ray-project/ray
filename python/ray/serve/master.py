import asyncio
from collections import defaultdict
import os
import random
import time

import ray
import ray.cloudpickle as pickle
from ray.serve.constants import (ASYNC_CONCURRENCY, SERVE_ROUTER_NAME,
                                 SERVE_PROXY_NAME, SERVE_METRIC_MONITOR_NAME)
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
        # backend -> (backend_worker, backend_config, replica_config).
        self.backends = {}
        # backend -> replica_tags.
        self.replicas = defaultdict(list)
        # replicas that should be started if recovering from a checkpoint.
        self.replicas_to_start = defaultdict(list)
        # replicas that should be stopped if recovering from a checkpoint.
        self.replicas_to_stop = defaultdict(list)
        # backends that should be removed from the router if recovering from a
        # checkpoint.
        self.backends_to_remove = list()
        # endpoints that should be removed from the router if recovering from a
        # checkpoint.
        self.endpoints_to_remove = list()
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
             self.replicas_to_start, self.replicas_to_stop,
             self.backends_to_remove, self.endpoints_to_remove))

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
         self.replicas_to_start, self.replicas_to_stop,
         self.backends_to_remove,
         self.endpoints_to_remove) = pickle.loads(checkpoint_bytes)

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

        for backend, (_, backend_config, _) in self.backends.items():
            await self.router.set_backend_config.remote(
                backend, backend_config)

        # Push configuration state to the HTTP proxy.
        await self.http_proxy.set_route_table.remote(self.routes)

        # Start/stop any pending backend replicas.
        await self._start_pending_replicas()
        await self._stop_pending_replicas()

        # Remove any pending backends and endpoints.
        await self._remove_pending_backends()
        await self._remove_pending_endpoints()

        logger.info(
            "Recovered from checkpoint in {:.3f}s".format(time.time() - start))

        self.write_lock.release()

    def get_backend_configs(self):
        """Fetched by the router on startup."""
        backend_configs = {}
        for backend, (_, backend_config, _) in self.backends.items():
            backend_configs[backend] = backend_config
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
        (backend_worker, backend_config,
         replica_config) = self.backends[backend_tag]

        worker_handle = async_retryable(ray.remote(backend_worker)).options(
            detached=True,
            name=replica_tag,
            max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
            **replica_config.ray_actor_options).remote(
                backend_tag, replica_tag, replica_config.actor_init_args)
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

    async def _remove_pending_backends(self):
        """Removes the pending backends in self.backends_to_remove.

        Clears self.backends_to_remove.
        """
        for backend_tag in self.backends_to_remove:
            await self.router.remove_backend.remote(backend_tag)
        self.backends_to_remove.clear()

    async def _remove_pending_endpoints(self):
        """Removes the pending endpoints in self.endpoints_to_remove.

        Clears self.endpoints_to_remove.
        """
        for endpoint_tag in self.endpoints_to_remove:
            await self.router.remove_service.remote(endpoint_tag)
        self.endpoints_to_remove.clear()

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
            if endpoint_name not in self.get_all_endpoints():
                raise ValueError(
                    "Attempted to assign traffic for an endpoint '{}'"
                    " that is not registered.".format(endpoint_name))

            assert isinstance(traffic_policy_dictionary,
                              dict), "Traffic policy must be dictionary"
            prob = 0
            for backend, weight in traffic_policy_dictionary.items():
                prob += weight
                if backend not in self.backends:
                    raise ValueError(
                        "Attempted to assign traffic to a backend '{}' that "
                        "is not registered.".format(backend))

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

    async def delete_endpoint(self, endpoint):
        """Delete the specified endpoint.

        Does not modify any corresponding backends.
        """
        logger.info("Deleting endpoint '{}'".format(endpoint))
        async with self.write_lock:
            # This method must be idempotent. We should validate that the
            # specified endpoint exists on the client.
            if endpoint not in self.traffic_policies:
                logger.info("Endpoint '{}' doesn't exist".format(endpoint))
                return

            # Remove the traffic policy entry.
            del self.traffic_policies[endpoint]

            for route, (route_endpoint, _) in self.routes.items():
                if route_endpoint == endpoint:
                    route_to_delete = route
                    break
            else:
                # This should never happen, we either add to or delete from
                # both self.traffic_policies and self.routes.
                assert False, "No route found for endpoint '{}'".format(
                    endpoint)

            # Remove the routing entry.
            del self.routes[route_to_delete]

            self.endpoints_to_remove.append(endpoint)

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # updates to the HTTP proxy and router to avoid inconsistent state
            # if we crash after pushing the update.
            self._checkpoint()

            # Update the HTTP proxy first to ensure no new requests for the
            # endpoint are sent to the router.
            await self.http_proxy.set_route_table.remote(self.routes)
            await self._remove_pending_endpoints()

    async def create_backend(self, backend_tag, backend_config,
                             replica_config):
        """Register a new backend under the specified tag."""
        async with self.write_lock:
            backend_worker = create_backend_worker(
                replica_config.func_or_class)

            # Save creator that starts replicas, the arguments to be passed in,
            # and the configuration for the backends.
            self.backends[backend_tag] = (backend_worker, backend_config,
                                          replica_config)

            self._scale_replicas(backend_tag, backend_config.num_replicas)

            # NOTE(edoakes): we must write a checkpoint before starting new
            # or pushing the updated config to avoid inconsistent state if we
            # crash while making the change.
            self._checkpoint()
            await self._start_pending_replicas()

            # Set the backend config inside the router
            # (particularly for max-batch-size).
            await self.router.set_backend_config.remote(
                backend_tag, backend_config)

    async def delete_backend(self, backend_tag):
        async with self.write_lock:
            # This method must be idempotent. We should validate that the
            # specified backend exists on the client.
            if backend_tag not in self.backends:
                return

            # Check that the specified backend isn't used by any endpoints.
            for endpoint, traffic_dict in self.traffic_policies.items():
                if backend_tag in traffic_dict:
                    raise ValueError("Backend '{}' is used by endpoint '{}' "
                                     "and cannot be deleted. Please remove "
                                     "the backend from all endpoints and try "
                                     "again.".format(backend_tag, endpoint))

            # Scale its replicas down to 0. This will also remove the backend
            # from self.backends and self.replicas.
            self._scale_replicas(backend_tag, 0)

            # Remove the backend's metadata.
            del self.backends[backend_tag]

            # Add the intention to remove the backend from the router.
            self.backends_to_remove.append(backend_tag)

            # NOTE(edoakes): we must write a checkpoint before removing the
            # backend from the router to avoid inconsistent state if we crash
            # after pushing the update.
            self._checkpoint()
            await self._stop_pending_replicas()
            await self._remove_pending_backends()

    async def update_backend_config(self, backend_tag, config_options):
        """Set the config for the specified backend."""
        async with self.write_lock:
            assert (backend_tag in self.backends
                    ), "Backend {} is not registered.".format(backend_tag)
            assert isinstance(config_options, dict)
            backend_worker, backend_config, replica_config = self.backends[
                backend_tag]

            backend_config.update(config_options)
            self.backends[backend_tag] = (backend_worker, backend_config,
                                          replica_config)

            # Scale the replicas with the new configuration.
            self._scale_replicas(backend_tag, backend_config.num_replicas)

            # NOTE(edoakes): we must write a checkpoint before pushing the
            # update to avoid inconsistent state if we crash after pushing the
            # update.
            self._checkpoint()

            # Inform the router about change in configuration
            # (particularly for setting max_batch_size).
            await self.router.set_backend_config.remote(
                backend_tag, backend_config)

            await self._start_pending_replicas()
            await self._stop_pending_replicas()

    def get_backend_config(self, backend_tag):
        """Get the current config for the specified backend."""
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        return self.backends[backend_tag][2]
