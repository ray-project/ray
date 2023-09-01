import json
import logging
import os
import random
import time
import traceback
from typing import Dict, List, Optional, Set, Tuple

import ray
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from ray.serve.config import gRPCOptions, HTTPOptions, DeploymentMode
from ray.serve._private.constants import (
    ASYNC_CONCURRENCY,
    PROXY_HEALTH_CHECK_TIMEOUT_S,
    SERVE_LOGGER_NAME,
    SERVE_PROXY_NAME,
    SERVE_NAMESPACE,
    PROXY_HEALTH_CHECK_PERIOD_S,
    PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
    PROXY_READY_CHECK_TIMEOUT_S,
    PROXY_DRAIN_CHECK_PERIOD_S,
)
from ray.serve._private import http_proxy
from ray.serve._private.utils import (
    format_actor_name,
)
from ray.serve._private.common import NodeId, ProxyStatus
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve.schema import ProxyDetails

logger = logging.getLogger(SERVE_LOGGER_NAME)


class HTTPProxyState:
    def __init__(
        self, actor_handle: ActorHandle, actor_name: str, node_id: str, node_ip: str
    ):
        self._actor_handle = actor_handle
        self._actor_name = actor_name
        self._node_id = node_id
        self._ready_obj_ref = self._actor_handle.ready.remote()
        self._status = ProxyStatus.STARTING
        self._health_check_obj_ref = None
        self._last_health_check_time: float = time.time()
        self._shutting_down = False
        self._consecutive_health_check_failures: int = 0

        self._update_draining_obj_ref = None
        self._is_drained_obj_ref = None
        self._last_drain_check_time: float = None

        self._actor_details = ProxyDetails(
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
    def status(self) -> ProxyStatus:
        return self._status

    @property
    def actor_details(self) -> ProxyDetails:
        return self._actor_details

    def set_status(self, status: ProxyStatus) -> None:
        """Sets _status and updates _actor_details with the new status."""
        self._status = status
        self.update_actor_details(status=self._status)

    def try_update_status(self, status: ProxyStatus):
        """Try update with the new status and only update when the conditions are met.

        Status will only set to UNHEALTHY after PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD
        consecutive failures. A warning will be logged when the status is set to
        UNHEALTHY. Also, when status is set to HEALTHY, we will reset
        self._consecutive_health_check_failures to 0.
        """

        if status == ProxyStatus.UNHEALTHY:
            self._consecutive_health_check_failures += 1
            # Early return to skip setting UNHEALTHY status if there are still room for
            # retry.
            if (
                self._consecutive_health_check_failures
                < PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD
            ):
                return

        # Reset self._consecutive_health_check_failures when status is not UNHEALTHY.
        if status != ProxyStatus.UNHEALTHY:
            self._consecutive_health_check_failures = 0

        self.set_status(status=status)

        # If all retries have been exhausted and setting the status to UNHEALTHY, log a
        # warning message to the user.
        if status == ProxyStatus.UNHEALTHY:
            logger.warning(
                f"HTTP proxy {self._actor_name} failed the health check "
                f"{self._consecutive_health_check_failures} times in a row, marking it "
                f"unhealthy."
            )

    def update_actor_details(self, **kwargs) -> None:
        """Updates _actor_details with passed in kwargs."""
        details_kwargs = self._actor_details.dict()
        details_kwargs.update(kwargs)
        self._actor_details = ProxyDetails(**details_kwargs)

    def _health_check(self):
        """Perform periodic health checks."""
        assert self._status in {ProxyStatus.HEALTHY, ProxyStatus.DRAINING}

        if self._health_check_obj_ref:
            finished, _ = ray.wait([self._health_check_obj_ref], timeout=0)
            if finished:
                self._health_check_obj_ref = None
                try:
                    ray.get(finished[0])
                    # Call to reset _consecutive_health_check_failures
                    # the status should be unchanged.
                    self.try_update_status(self._status)
                except ray.exceptions.RayActorError:
                    # The proxy actor dies.
                    self.set_status(ProxyStatus.UNHEALTHY)
                except Exception as e:
                    logger.warning(
                        f"Health check for HTTP proxy {self._actor_name} failed: {e}"
                    )
                    self.try_update_status(ProxyStatus.UNHEALTHY)
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
                self.try_update_status(ProxyStatus.UNHEALTHY)

        # If there's no active in-progress health check and it has been more than 10
        # seconds since the last health check, perform another health check.
        if self._health_check_obj_ref:
            return
        randomized_period_s = PROXY_HEALTH_CHECK_PERIOD_S * random.uniform(0.9, 1.1)
        if time.time() - self._last_health_check_time > randomized_period_s:
            self._last_health_check_time = time.time()
            self._health_check_obj_ref = self._actor_handle.check_health.remote()

    def _drain_check(self):
        """Check whether the proxy actor is drained or not."""
        assert self._status == ProxyStatus.DRAINING

        if self._is_drained_obj_ref:
            finished, _ = ray.wait([self._is_drained_obj_ref], timeout=0)
            if finished:
                self._is_drained_obj_ref = None
                try:
                    is_drained = ray.get(finished[0])
                    if is_drained:
                        self.set_status(ProxyStatus.DRAINED)
                except Exception as e:
                    logger.warning(
                        f"Drain check for HTTP proxy {self._actor_name} failed: {e}."
                    )
        elif time.time() - self._last_drain_check_time > PROXY_DRAIN_CHECK_PERIOD_S:
            self._last_drain_check_time = time.time()
            self._is_drained_obj_ref = self._actor_handle.is_drained.remote(
                _after=self._update_draining_obj_ref
            )

    def update(self, draining: bool = False):
        """Update the status of the current HTTP proxy.

        The state machine is:
        STARTING -> HEALTHY or UNHEALTHY
        HEALTHY -> DRAINING or UNHEALTHY
        DRAINING -> HEALTHY or UNHEALTHY or DRAINED

        1) When the HTTP proxy is already shutting down, do nothing.
        2) When the HTTP proxy is starting, check ready object reference. If ready
        object reference returns a successful call set status to HEALTHY. If the
        call to ready() on the HTTP Proxy actor has any exception or timeout, increment
        the consecutive health check failure counter and retry on the next update call.
        The status is only set to UNHEALTHY when all retries have exhausted.
        3) When the HTTP proxy already has an in-progress health check. If health check
        object returns a successful call, keep the current status. If the call has
        any exception or timeout, count towards 1 of the consecutive health check
        failures and retry on the next update call. The status is only set to UNHEALTHY
        when all retries have exhausted.
        4) When the HTTP proxy need to setup another health check (when none of the
        above met and the time since the last health check is longer than
        PROXY_HEALTH_CHECK_PERIOD_S with some margin). Reset
        self._last_health_check_time and set up a new health check object so the next
        update can call healthy check again.
        5) Transition the status between HEALTHY and DRAINING.
        6) When the HTTP proxy is draining, check whether it's drained or not.
        """
        if self._shutting_down:
            return

        if self._status == ProxyStatus.STARTING:
            finished, _ = ray.wait([self._ready_obj_ref], timeout=0)
            if finished:
                try:
                    worker_id, log_file_path = json.loads(ray.get(finished[0]))
                    self.try_update_status(ProxyStatus.HEALTHY)
                    self.update_actor_details(
                        worker_id=worker_id,
                        log_file_path=log_file_path,
                        status=self._status,
                    )
                except ray.exceptions.RayActorError:
                    self.set_status(ProxyStatus.UNHEALTHY)
                    logger.warning(
                        "Unexpected actor death when checking readiness of HTTP "
                        f"Proxy on node {self._node_id}:\n{traceback.format_exc()}"
                    )
                except Exception:
                    self.try_update_status(ProxyStatus.UNHEALTHY)
                    logger.warning(
                        "Unexpected error occurred when checking readiness of HTTP "
                        f"Proxy on node {self._node_id}:\n{traceback.format_exc()}"
                    )
            elif (
                time.time() - self._last_health_check_time > PROXY_READY_CHECK_TIMEOUT_S
            ):
                # Ready check hasn't returned and the timeout is up, consider it failed.
                self.set_status(ProxyStatus.UNHEALTHY)
                logger.warning(
                    "Didn't receive ready check response for HTTP proxy "
                    f"{self._node_id} after {PROXY_READY_CHECK_TIMEOUT_S}s."
                )
            return

        # At this point, the proxy is either in HEALTHY or DRAINING status.
        assert self._status in {ProxyStatus.HEALTHY, ProxyStatus.DRAINING}

        self._health_check()
        if self._status == ProxyStatus.UNHEALTHY:
            return

        if (self._status == ProxyStatus.HEALTHY) and draining:
            logger.info(f"Start to drain the proxy actor on node {self._node_id}")
            self.set_status(ProxyStatus.DRAINING)
            # All the update_draining calls are ordered via `_after`.
            self._update_draining_obj_ref = self._actor_handle.update_draining.remote(
                True, _after=self._update_draining_obj_ref
            )
            assert self._is_drained_obj_ref is None
            assert self._last_drain_check_time is None
            self._last_drain_check_time = time.time()

        if (self._status == ProxyStatus.DRAINING) and not draining:
            logger.info(f"Stop draining the proxy actor on node {self._node_id}")
            self.set_status(ProxyStatus.HEALTHY)
            self._update_draining_obj_ref = self._actor_handle.update_draining.remote(
                False, _after=self._update_draining_obj_ref
            )
            self._is_drained_obj_ref = None
            self._last_drain_check_time = None

        if self._status == ProxyStatus.DRAINING:
            self._drain_check()

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


class HTTPProxyStateManager:
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
        cluster_node_info_cache: ClusterNodeInfoCache,
        grpc_options: Optional[gRPCOptions] = None,
    ):
        self._controller_name = controller_name
        self._detached = detached
        if config is not None:
            self._config = config
        else:
            self._config = HTTPOptions()
        self._grpc_options = grpc_options or gRPCOptions()
        self._proxy_states: Dict[NodeId, HTTPProxyState] = dict()
        self._head_node_id: str = head_node_id

        self._cluster_node_info_cache = cluster_node_info_cache

        assert isinstance(head_node_id, str)

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

    def get_config(self) -> HTTPOptions:
        return self._config

    def get_grpc_config(self) -> gRPCOptions:
        return self._grpc_options

    def get_http_proxy_handles(self) -> Dict[NodeId, ActorHandle]:
        return {
            node_id: state.actor_handle for node_id, state in self._proxy_states.items()
        }

    def get_http_proxy_names(self) -> Dict[NodeId, str]:
        return {
            node_id: state.actor_name for node_id, state in self._proxy_states.items()
        }

    def get_proxy_details(self) -> Dict[NodeId, ProxyDetails]:
        return {
            node_id: state.actor_details
            for node_id, state in self._proxy_states.items()
        }

    def update(self, http_proxy_nodes: Set[NodeId] = None):
        """Update the state of all HTTP proxies.

        Start proxies on all nodes if not already exist and stop the proxies on nodes
        that are no longer exist. Update all proxy states. Kill and restart
        unhealthy proxies.
        """
        # Ensure head node always has a proxy.
        if http_proxy_nodes is None:
            http_proxy_nodes = {self._head_node_id}
        else:
            http_proxy_nodes.add(self._head_node_id)

        target_nodes = self._get_target_nodes(http_proxy_nodes)
        target_node_ids = {node_id for node_id, _ in target_nodes}

        for node_id, proxy_state in self._proxy_states.items():
            draining = node_id not in target_node_ids
            proxy_state.update(draining)

        self._stop_proxies_if_needed()
        self._start_proxies_if_needed(target_nodes)

    def _get_target_nodes(self, http_proxy_nodes) -> List[Tuple[str, str]]:
        """Return the list of (node_id, ip_address) to deploy HTTP servers on."""
        location = self._config.location

        if location == DeploymentMode.NoServer:
            return []

        target_nodes = [
            (node_id, ip_address)
            for node_id, ip_address in self._cluster_node_info_cache.get_alive_nodes()
            if node_id in http_proxy_nodes
        ]

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
                    f"{len(target_nodes)} target nodes. Serve will start one "
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
        new HTTPProxyActor actor handle for the proxy. In addition, setting up
        `TEST_WORKER_NODE_HTTP_PORT` env var will help head node and worker nodes to be
        opening on different HTTP ports. Setting up `TEST_WORKER_NODE_GRPC_PORT` env var
        will help head node and worker nodes to be opening on different gRPC ports.
        """
        port = self._config.port
        grpc_options = self._grpc_options

        if (
            node_id != self._head_node_id
            and os.getenv("TEST_WORKER_NODE_HTTP_PORT") is not None
        ):
            logger.warning(
                f"`TEST_WORKER_NODE_HTTP_PORT` env var is set. "
                f"Using it for worker node {node_id}."
            )
            port = int(os.getenv("TEST_WORKER_NODE_HTTP_PORT"))

        if (
            node_id != self._head_node_id
            and os.getenv("TEST_WORKER_NODE_GRPC_PORT") is not None
        ):
            logger.warning(
                f"`TEST_WORKER_NODE_GRPC_PORT` env var is set. "
                f"Using it for worker node {node_id}."
                f"{int(os.getenv('TEST_WORKER_NODE_GRPC_PORT'))}"
            )
            grpc_options.port = int(os.getenv("TEST_WORKER_NODE_GRPC_PORT"))

        proxy = http_proxy.HTTPProxyActor.options(
            num_cpus=self._config.num_cpus,
            name=name,
            namespace=SERVE_NAMESPACE,
            lifetime="detached" if self._detached else None,
            max_concurrency=ASYNC_CONCURRENCY,
            max_restarts=0,
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
            keep_alive_timeout_s=self._config.keep_alive_timeout_s,
            grpc_options=grpc_options,
        )
        return proxy

    def _start_proxies_if_needed(self, target_nodes) -> None:
        """Start a proxy on every node if it doesn't already exist."""

        for node_id, node_ip_address in target_nodes:
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
                proxy, name, node_id, node_ip_address
            )

    def _stop_proxies_if_needed(self) -> bool:
        """Removes proxy actors.

        Removes proxy actors from any nodes that no longer exist or unhealthy proxy.
        """
        alive_node_ids = self._cluster_node_info_cache.get_alive_node_ids()
        to_stop = []
        for node_id, proxy_state in self._proxy_states.items():
            if node_id not in alive_node_ids:
                logger.info(f"Removing HTTP proxy on removed node '{node_id}'.")
                to_stop.append(node_id)
            elif proxy_state.status == ProxyStatus.UNHEALTHY:
                logger.info(
                    f"HTTP proxy on node '{node_id}' UNHEALTHY. Shutting down "
                    "the unhealthy proxy and starting a new one."
                )
                to_stop.append(node_id)
            elif proxy_state.status == ProxyStatus.DRAINED:
                logger.info(f"Removing drained HTTP proxy on node '{node_id}'.")
                to_stop.append(node_id)

        for node_id in to_stop:
            proxy_state = self._proxy_states.pop(node_id)
            proxy_state.shutdown()
