import asyncio
import json
import logging
import os
import random
import time
import traceback
from typing import Dict, List, Set, Tuple

import ray
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from ray._raylet import GcsClient
from ray.serve.config import HTTPOptions, DeploymentMode
from ray.serve._private.constants import (
    ASYNC_CONCURRENCY,
    PROXY_HEALTH_CHECK_TIMEOUT_S,
    SERVE_LOGGER_NAME,
    SERVE_PROXY_NAME,
    SERVE_NAMESPACE,
    PROXY_HEALTH_CHECK_PERIOD_S,
    PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
    PROXY_READY_CHECK_TIMEOUT_S,
)
from ray.serve._private.http_proxy import HTTPProxyActor
from ray.serve._private.utils import (
    format_actor_name,
    get_all_node_ids,
)
from ray.serve._private.common import EndpointTag, NodeId, HTTPProxyStatus
from ray.serve.schema import HTTPProxyDetails

logger = logging.getLogger(SERVE_LOGGER_NAME)


class HTTPProxyState:
    def __init__(
        self,
        actor_handle: ActorHandle,
        actor_name: str,
        node_id: str,
        node_ip: str,
        controller_name: str,
    ):
        self._actor_handle = actor_handle
        self._actor_name = actor_name
        self._node_id = node_id
        self._ready_obj_ref = self._actor_handle.ready.remote()
        self._status = HTTPProxyStatus.STARTING
        self._health_check_obj_ref = None
        self._last_health_check_time: float = time.time()
        self._shutting_down = False
        self._consecutive_health_check_failures: int = 0

        self._actor_details = HTTPProxyDetails(
            node_id=node_id,
            node_ip=node_ip,
            actor_id=self._actor_handle._actor_id.hex(),
            actor_name=self._actor_name,
            status=self._status,
        )

    @property
    def actor_handle(self) -> ActorHandle:
        return self._actor_handle

    @property
    def actor_name(self) -> str:
        return self._actor_name

    @property
    def status(self) -> HTTPProxyStatus:
        return self._status

    @property
    def actor_details(self) -> HTTPProxyDetails:
        return self._actor_details

    def set_status(self, status: HTTPProxyStatus) -> None:
        """Sets _status and updates _actor_details with the new status."""
        self._status = status
        self.update_actor_details(status=self._status)

    def try_update_status(self, status: HTTPProxyStatus):
        """Try update with the new status and only update when the conditions are met.

        Status will only set to UNHEALTHY after PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD
        consecutive failures. A warning will be logged when the status is set to
        UNHEALTHY. Also, when status is set to HEALTHY, we will reset
        self._consecutive_health_check_failures to 0.
        """

        if status == HTTPProxyStatus.UNHEALTHY:
            self._consecutive_health_check_failures += 1
            # Early return to skip setting UNHEALTHY status if there are still room for
            # retry.
            if (
                self._consecutive_health_check_failures
                < PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD
            ):
                return

        # Reset self._consecutive_health_check_failures when status is not UNHEALTHY.
        if status != HTTPProxyStatus.UNHEALTHY:
            self._consecutive_health_check_failures = 0

        self.set_status(status=status)

        # If all retries have been exhausted and setting the status to UNHEALTHY, log a
        # warning message to the user.
        if status == HTTPProxyStatus.UNHEALTHY:
            logger.warning(
                f"HTTP proxy {self._actor_name} failed the health check "
                f"{self._consecutive_health_check_failures} times in a row, marking it "
                f"unhealthy."
            )

    def update_actor_details(self, **kwargs) -> None:
        """Updates _actor_details with passed in kwargs."""
        details_kwargs = self._actor_details.dict()
        details_kwargs.update(kwargs)
        self._actor_details = HTTPProxyDetails(**details_kwargs)

    def update(self, draining: bool = False):
        """Update the status of the current HTTP proxy.

        1) When the HTTP proxy is already shutting down, do nothing.
        2) When the HTTP proxy is starting, check ready object reference. If ready
        object reference returns a successful call and the draining flag is false, set
        status to HEALTHY. If the draining flag is true, set status to DRAINING. If the
        call to ready() on the HTTP Proxy actor has any exception or timeout, increment
        the consecutive health check failure counter and retry on the next update call.
        The status is only set to UNHEALTHY when all retries have exhausted.
        3) When the HTTP proxy already has an in-progress health check. If health check
        object returns a successful call and the draining flag is false, set status to
        HEALTHY. If the draining flag is true, set status to DRAINING. If the call has
        any exception or timeout, count towards 1 of the consecutive health check
        failures and retry on the next update call. The status is only set to UNHEALTHY
        when all retries have exhausted.
        4) When the HTTP proxy need to setup another health check (when none of the
        above met and the time since the last health check is longer than
        PROXY_HEALTH_CHECK_PERIOD_S with some margin). Reset
        self._last_health_check_time and set up a new health check object so the next
        update can call healthy check again.
        """
        if self._shutting_down:
            return

        if self._status == HTTPProxyStatus.STARTING:
            finished, _ = ray.wait([self._ready_obj_ref], timeout=0)
            if finished:
                try:
                    worker_id, log_file_path = json.loads(ray.get(finished[0]))
                    status = (
                        HTTPProxyStatus.HEALTHY
                        if not draining
                        else HTTPProxyStatus.DRAINING
                    )
                    self.try_update_status(status)
                    self.update_actor_details(
                        worker_id=worker_id,
                        log_file_path=log_file_path,
                        status=self._status,
                    )
                except Exception:
                    self.try_update_status(HTTPProxyStatus.UNHEALTHY)
                    logger.warning(
                        "Unexpected error occurred when checking readiness of HTTP "
                        f"Proxy on node {self._node_id}:\n{traceback.format_exc()}"
                    )
            elif (
                time.time() - self._last_health_check_time > PROXY_READY_CHECK_TIMEOUT_S
            ):
                # Ready check hasn't returned and the timeout is up, consider it failed.
                self.set_status(HTTPProxyStatus.UNHEALTHY)
                logger.warning(
                    "Didn't receive ready check response for HTTP proxy "
                    f"{self._node_id} after {PROXY_READY_CHECK_TIMEOUT_S}s."
                )
            return

        # Perform periodic health checks.
        if self._health_check_obj_ref:
            finished, _ = ray.wait([self._health_check_obj_ref], timeout=0)
            if finished:
                self._health_check_obj_ref = None
                try:
                    ray.get(finished[0])
                    status = (
                        HTTPProxyStatus.HEALTHY
                        if not draining
                        else HTTPProxyStatus.DRAINING
                    )
                    self.try_update_status(status)
                except Exception as e:
                    logger.warning(
                        f"Health check for HTTP proxy {self._actor_name} failed: {e}"
                    )
                    self.try_update_status(HTTPProxyStatus.UNHEALTHY)
            elif (
                time.time() - self._last_health_check_time
                > PROXY_HEALTH_CHECK_TIMEOUT_S
            ):
                # Health check hasn't returned and the timeout is up, consider it
                # failed.
                self._health_check_obj_ref = None
                logger.warning(
                    "Didn't receive health check response for HTTP proxy "
                    f"{self._node_id} after {PROXY_HEALTH_CHECK_TIMEOUT_S}s"
                )
                self.try_update_status(HTTPProxyStatus.UNHEALTHY)
            else:
                # This return is important to not trigger a new health check when
                # there is an in progress health check. When the health check object
                # is still in progress and before the timeout is triggered, we will
                # do an early return here to signal the completion of this update call
                # and to prevent another health check object from recreated in the
                # code below.
                return

        # If there's no active in-progress health check and it has been more than 10
        # seconds since the last health check, perform another health check.
        randomized_period_s = PROXY_HEALTH_CHECK_PERIOD_S * random.uniform(0.9, 1.1)
        if time.time() - self._last_health_check_time > randomized_period_s:
            self._last_health_check_time = time.time()
            self._health_check_obj_ref = self._actor_handle.check_health.remote()

    def shutdown(self):
        self._shutting_down = True
        ray.kill(self.actor_handle, no_restart=True)

    def is_ready_for_shutdown(self) -> bool:
        """Return whether the HTTP proxy actor is shutdown.

        For an HTTP proxy actor to be considered shutdown, it must be marked as
        _shutting_down and the actor must be dead. If the actor is dead, the health
        check will return RayActorError.
        """
        if not self._shutting_down:
            return False

        try:
            ray.get(self._actor_handle.check_health.remote(), timeout=0.001)
        except ray.exceptions.RayActorError:
            # The actor is dead, so it's ready for shutdown.
            return True
        except ray.exceptions.GetTimeoutError:
            # The actor is still alive, so it's not ready for shutdown.
            return False

        return False


class HTTPState:
    """Manages all state for HTTP proxies in the system.

    This class is *not* thread safe, so any state-modifying methods should be
    called with a lock held.
    """

    def __init__(
        self,
        controller_name: str,
        detached: bool,
        config: HTTPOptions,
        head_node_id: str,
        gcs_client: GcsClient,
        # Used by unit testing
        _start_proxies_on_init: bool = True,
    ):
        self._controller_name = controller_name
        self._detached = detached
        if config is not None:
            self._config = config
        else:
            self._config = HTTPOptions()
        self._proxy_states: Dict[NodeId, HTTPProxyState] = dict()
        self._head_node_id: str = head_node_id

        self._gcs_client = gcs_client

        assert isinstance(head_node_id, str)

        # Will populate self.proxy_actors with existing actors.
        if _start_proxies_on_init:
            self._start_proxies_if_needed()

    def shutdown(self) -> None:
        for proxy_state in self._proxy_states.values():
            proxy_state.shutdown()

    def is_ready_for_shutdown(self) -> bool:
        """Return whether all proxies are shutdown.

        Iterate through all proxy states and check if all their proxy actors
        are shutdown.
        """
        return all(
            proxy_state.is_ready_for_shutdown()
            for proxy_state in self._proxy_states.values()
        )

    def get_config(self):
        return self._config

    def get_http_proxy_handles(self) -> Dict[NodeId, ActorHandle]:
        return {
            node_id: state.actor_handle for node_id, state in self._proxy_states.items()
        }

    def get_http_proxy_names(self) -> Dict[NodeId, str]:
        return {
            node_id: state.actor_name for node_id, state in self._proxy_states.items()
        }

    def get_http_proxy_details(self) -> Dict[NodeId, HTTPProxyDetails]:
        return {
            node_id: state.actor_details
            for node_id, state in self._proxy_states.items()
        }

    def update(self, active_nodes: Set[NodeId] = None):
        """Update the state of all HTTP proxies.

        Start proxies on all nodes if not already exist and stop the proxies on nodes
        that are no longer exist. Update all proxy states. Kill and restart
        unhealthy proxies.
        """
        # Ensure head node is always active.
        if active_nodes is None:
            active_nodes = {self._head_node_id}
        else:
            active_nodes.add(self._head_node_id)

        self._start_proxies_if_needed()
        self._stop_proxies_if_needed()
        for node_id, proxy_state in self._proxy_states.items():
            draining = node_id not in active_nodes
            proxy_state.update(draining)

    def _get_target_nodes(self) -> List[Tuple[str, str]]:
        """Return the list of (node_id, ip_address) to deploy HTTP servers on."""
        location = self._config.location
        target_nodes = get_all_node_ids(self._gcs_client)

        if location == DeploymentMode.NoServer:
            return []

        if location == DeploymentMode.HeadOnly:
            nodes = [
                (node_id, ip_address)
                for node_id, ip_address in target_nodes
                if node_id == self._head_node_id
            ]
            assert len(nodes) == 1, (
                f"Head node not found! Head node id: {self._head_node_id}, "
                f"all nodes: {target_nodes}."
            )
            return nodes

        if location == DeploymentMode.FixedNumber:
            num_replicas = self._config.fixed_number_replicas
            if num_replicas > len(target_nodes):
                logger.warning(
                    "You specified fixed_number_replicas="
                    f"{num_replicas} but there are only "
                    f"{len(target_nodes)} total nodes. Serve will start one "
                    "HTTP proxy per node."
                )
                num_replicas = len(target_nodes)

            # Seed the random state so sample is deterministic.
            # i.e. it will always return the same set of nodes.
            random.seed(self._config.fixed_number_selection_seed)
            return random.sample(sorted(target_nodes), k=num_replicas)

        return target_nodes

    def _generate_actor_name(self, node_id: str) -> str:
        return format_actor_name(SERVE_PROXY_NAME, self._controller_name, node_id)

    def _start_proxy(
        self, name: str, node_id: str, node_ip_address: str
    ) -> ActorHandle:
        """Helper to start a single HTTP proxy.

        Takes the name of the proxy, the node id, and the node ip address. and creates a
        new HTTPProxyActor actor handle for the proxy. Also, setting up
        `TEST_WORKER_NODE_PORT` env var will help head node and worker nodes to be
        opening on different ports.
        """
        port = self._config.port

        if (
            node_id != self._head_node_id
            and os.getenv("TEST_WORKER_NODE_PORT") is not None
        ):
            logger.warning(
                f"`TEST_WORKER_NODE_PORT` env var is set. "
                f"Using it for worker node {node_id}."
            )
            port = int(os.getenv("TEST_WORKER_NODE_PORT"))

        proxy = HTTPProxyActor.options(
            num_cpus=self._config.num_cpus,
            name=name,
            namespace=SERVE_NAMESPACE,
            lifetime="detached" if self._detached else None,
            max_concurrency=ASYNC_CONCURRENCY,
            max_restarts=-1,
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
        ).remote(
            self._config.host,
            port,
            self._config.root_path,
            controller_name=self._controller_name,
            node_ip_address=node_ip_address,
            node_id=node_id,
            http_middlewares=self._config.middlewares,
            request_timeout_s=self._config.request_timeout_s,
        )
        return proxy

    def _start_proxies_if_needed(self) -> None:
        """Start a proxy on every node if it doesn't already exist."""

        for node_id, node_ip_address in self._get_target_nodes():
            if node_id in self._proxy_states:
                continue

            name = self._generate_actor_name(node_id=node_id)
            try:
                proxy = ray.get_actor(name, namespace=SERVE_NAMESPACE)
            except ValueError:
                logger.info(
                    f"Starting HTTP proxy with name '{name}' on node '{node_id}' "
                    f"listening on '{self._config.host}:{self._config.port}'",
                    extra={"log_to_stderr": False},
                )
                proxy = self._start_proxy(
                    name=name,
                    node_id=node_id,
                    node_ip_address=node_ip_address,
                )

            self._proxy_states[node_id] = HTTPProxyState(
                proxy, name, node_id, node_ip_address, self._controller_name
            )

    def _stop_proxies_if_needed(self) -> bool:
        """Removes proxy actors.

        Removes proxy actors from any nodes that no longer exist or unhealthy proxy.
        """
        all_node_ids = {node_id for node_id, _ in get_all_node_ids(self._gcs_client)}
        to_stop = []
        for node_id, proxy_state in self._proxy_states.items():
            if node_id not in all_node_ids:
                logger.info(f"Removing HTTP proxy on removed node '{node_id}'.")
                to_stop.append(node_id)
            elif proxy_state.status == HTTPProxyStatus.UNHEALTHY:
                logger.info(
                    f"HTTP proxy on node '{node_id}' UNHEALTHY. Shutting down "
                    "the unhealthy proxy and starting a new one."
                )
                to_stop.append(node_id)

        for node_id in to_stop:
            proxy_state = self._proxy_states.pop(node_id)
            proxy_state.shutdown()

    async def ensure_http_route_exists(self, endpoint: EndpointTag, timeout_s: float):
        """Block until the route has been propagated to all HTTP proxies.
        When the timeout occur in any of the http proxy, the whole method will
        re-throw the TimeoutError.
        """
        await asyncio.gather(
            *[
                proxy.actor_handle.block_until_endpoint_exists.remote(
                    endpoint, timeout_s=timeout_s
                )
                for proxy in self._proxy_states.values()
            ]
        )
