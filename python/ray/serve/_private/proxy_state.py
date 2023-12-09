import json
import logging
import os
import random
import traceback
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Type

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayActorError
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray.serve._private.common import NodeId, ProxyStatus
from ray.serve._private.constants import (
    ASYNC_CONCURRENCY,
    PROXY_DRAIN_CHECK_PERIOD_S,
    PROXY_HEALTH_CHECK_PERIOD_S,
    PROXY_HEALTH_CHECK_TIMEOUT_S,
    PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
    PROXY_READY_CHECK_TIMEOUT_S,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
    SERVE_PROXY_NAME,
)
from ray.serve._private.proxy import ProxyActor
from ray.serve._private.utils import Timer, TimerBase, format_actor_name
from ray.serve.config import DeploymentMode, HTTPOptions, gRPCOptions
from ray.serve.schema import LoggingConfig, ProxyDetails
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ProxyWrapperCallStatus(str, Enum):
    PENDING = "PENDING"
    FINISHED_SUCCEED = "FINISHED_SUCCEED"
    FINISHED_FAILED = "FINISHED_FAILED"


class ProxyWrapper(ABC):
    @property
    @abstractmethod
    def actor_id(self) -> str:
        """Return the actor id of the proxy actor."""
        raise NotImplementedError

    @abstractmethod
    def start_new_ready_check(self):
        """Start a new ready check on the proxy actor."""
        raise NotImplementedError

    @abstractmethod
    def start_new_health_check(self):
        """Start a new health check on the proxy actor."""
        raise NotImplementedError

    @abstractmethod
    def start_new_drained_check(self):
        """Start a new drained check on the proxy actor.

        This is triggered once the proxy actor is set to draining. We will leave some
        time padding for the proxy actor to finish the ongoing requests. Once all
        ongoing requests are finished and the minimum draining time is met, the proxy
        actor will be transition to drained state and ready to be killed.
        """
        raise NotImplementedError

    @abstractmethod
    def is_ready(self) -> ProxyWrapperCallStatus:
        """Return the payload from proxy ready check when ready."""
        raise NotImplementedError

    @abstractmethod
    def is_healthy(self) -> ProxyWrapperCallStatus:
        """Return whether the proxy actor is healthy or not."""
        raise NotImplementedError

    @abstractmethod
    def is_drained(self) -> ProxyWrapperCallStatus:
        """Return whether the proxy actor is drained or not."""
        raise NotImplementedError

    @abstractmethod
    def is_shutdown(self):
        """Return whether the proxy actor is shutdown."""
        raise NotImplementedError

    @abstractmethod
    def update_draining(self, draining: bool):
        """Update the draining status of the proxy actor."""
        raise NotImplementedError

    @abstractmethod
    def kill(self):
        """Kill the proxy actor."""
        raise NotImplementedError


class ActorProxyWrapper(ProxyWrapper):
    def __init__(
        self,
        logging_config: LoggingConfig,
        actor_handle: Optional[ActorHandle] = None,
        config: Optional[HTTPOptions] = None,
        grpc_options: Optional[gRPCOptions] = None,
        controller_name: Optional[str] = None,
        name: Optional[str] = None,
        node_id: Optional[str] = None,
        node_ip_address: Optional[str] = None,
        port: Optional[int] = None,
        proxy_actor_class: Type[ProxyActor] = ProxyActor,
    ):
        # initialize with provided proxy actor handle or get or create a new one.
        self._actor_handle = actor_handle or self._get_or_create_proxy_actor(
            config=config,
            grpc_options=grpc_options,
            controller_name=controller_name,
            name=name,
            node_id=node_id,
            node_ip_address=node_ip_address,
            port=port,
            proxy_actor_class=proxy_actor_class,
            logging_config=logging_config,
        )
        self._ready_obj_ref = None
        self._health_check_obj_ref = None
        self._is_drained_obj_ref = None
        self._update_draining_obj_ref = None
        self.worker_id = None
        self.log_file_path = None

    @staticmethod
    def _get_or_create_proxy_actor(
        config: HTTPOptions,
        grpc_options: gRPCOptions,
        controller_name: str,
        name: str,
        node_id: str,
        node_ip_address: str,
        port: int,
        logging_config: LoggingConfig,
        proxy_actor_class: Type[ProxyActor] = ProxyActor,
    ) -> ProxyWrapper:
        """Helper to start or reuse existing proxy.

        Takes the name of the proxy, the node id, and the node ip address, and look up
        or creates a new ProxyActor actor handle for the proxy.
        """
        proxy = None
        try:
            proxy = ray.get_actor(name, namespace=SERVE_NAMESPACE)
        except ValueError:
            logger.info(
                f"Starting proxy with name '{name}' on node '{node_id}' "
                f"listening on '{config.host}:{port}'",
                extra={"log_to_stderr": False},
            )

        proxy = proxy or proxy_actor_class.options(
            num_cpus=config.num_cpus,
            name=name,
            namespace=SERVE_NAMESPACE,
            lifetime="detached",
            max_concurrency=ASYNC_CONCURRENCY,
            max_restarts=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
        ).remote(
            config.host,
            port,
            config.root_path,
            controller_name=controller_name,
            node_ip_address=node_ip_address,
            node_id=node_id,
            http_middlewares=config.middlewares,
            request_timeout_s=config.request_timeout_s,
            keep_alive_timeout_s=config.keep_alive_timeout_s,
            grpc_options=grpc_options,
            logging_config=logging_config,
        )
        return proxy

    @property
    def actor_id(self) -> str:
        """Return the actor id of the proxy actor."""
        return self._actor_handle._actor_id.hex()

    @property
    def actor_handle(self) -> ActorHandle:
        """Return the actor handle of the proxy actor.

        This is used in _start_controller() in _private/controller.py to check whether
        the proxies exist. It is also used in some tests to access proxy's actor handle.
        """
        return self._actor_handle

    @property
    def health_check_ongoing(self) -> bool:
        """Return whether the health check is ongoing or not."""
        return self._health_check_obj_ref is not None

    @property
    def is_draining(self) -> bool:
        """Return whether the drained check is ongoing or not."""
        return self._is_drained_obj_ref is not None

    def reset_drained_check(self):
        """Reset the drained check object reference."""
        self._is_drained_obj_ref = None

    def reset_health_check(self):
        """Reset the health check object reference."""
        self._health_check_obj_ref = None

    def start_new_ready_check(self):
        """Start a new ready check on the proxy actor."""
        self._ready_obj_ref = self._actor_handle.ready.remote()

    def start_new_health_check(self):
        """Start a new health check on the proxy actor."""
        self._health_check_obj_ref = self._actor_handle.check_health.remote()

    def start_new_drained_check(self):
        """Start a new drained check on the proxy actor.

        This is triggered once the proxy actor is set to draining. We will leave some
        time padding for the proxy actor to finish the ongoing requests. Once all
        ongoing requests are finished and the minimum draining time is met, the proxy
        actor will be transition to drained state and ready to be killed.
        """
        self._is_drained_obj_ref = self._actor_handle.is_drained.remote(
            _after=self._update_draining_obj_ref
        )

    def is_ready(self) -> ProxyWrapperCallStatus:
        """Return the payload from proxy ready check when ready.

        If the ongoing ready check is finished, and the value can be retrieved and
        unpacked, set the worker_id and log_file_path attributes of the proxy actor
        and return FINISHED_SUCCEED status. If the ongoing ready check is not finished,
        return PENDING status. If the RayActorError is raised, meaning that the actor
        is dead, return FINISHED_FAILED status.
        """
        try:
            finished, _ = ray.wait([self._ready_obj_ref], timeout=0)
            if finished:
                worker_id, log_file_path = json.loads(ray.get(finished[0]))
                self.worker_id = worker_id
                self.log_file_path = log_file_path
                return ProxyWrapperCallStatus.FINISHED_SUCCEED
            else:
                return ProxyWrapperCallStatus.PENDING
        except RayActorError:
            return ProxyWrapperCallStatus.FINISHED_FAILED

    def is_healthy(self) -> ProxyWrapperCallStatus:
        """Return whether the proxy actor is healthy or not.

        If the ongoing health check is finished, and the value can be retrieved,
        reset _health_check_obj_ref to enable the next health check and return
        FINISHED_SUCCEED status. If the ongoing ready check is not finished,
        return PENDING status. If the RayActorError is raised, meaning that the actor
        is dead, return FINISHED_FAILED status.
        """
        try:
            finished, _ = ray.wait([self._health_check_obj_ref], timeout=0)
            if finished:
                self._health_check_obj_ref = None
                ray.get(finished[0])
                return ProxyWrapperCallStatus.FINISHED_SUCCEED
            else:
                return ProxyWrapperCallStatus.PENDING
        except RayActorError:
            return ProxyWrapperCallStatus.FINISHED_FAILED

    def is_drained(self) -> ProxyWrapperCallStatus:
        """Return whether the proxy actor is drained or not.

        If the ongoing drained check is finished, and the value can be retrieved,
        reset _is_drained_obj_ref to ensure drained check is finished and return
        FINISHED_SUCCEED status. If the ongoing ready check is not finished,
        return PENDING status.
        """
        finished, _ = ray.wait([self._is_drained_obj_ref], timeout=0)
        if finished:
            self._is_drained_obj_ref = None
            is_drained = ray.get(finished[0])
            if is_drained:
                return ProxyWrapperCallStatus.FINISHED_SUCCEED
            else:
                # NOTE: Even though call returned successfully, we have to
                #       report it as FINISHED_FAILED to make sure that
                #       draining process doesn't move forward until draining
                #       completes
                return ProxyWrapperCallStatus.FINISHED_FAILED
        else:
            return ProxyWrapperCallStatus.PENDING

    def is_shutdown(self) -> bool:
        """Return whether the proxy actor is shutdown.

        If the actor is dead, the health check will return RayActorError.
        """
        try:
            ray.get(self._actor_handle.check_health.remote(), timeout=0)
        except RayActorError:
            # The actor is dead, so it's ready for shutdown.
            return True

        # The actor is still alive, so it's not ready for shutdown.
        return False

    def update_draining(self, draining: bool):
        """Update the draining status of the proxy actor."""
        self._update_draining_obj_ref = self._actor_handle.update_draining.remote(
            draining, _after=self._update_draining_obj_ref
        )

    def kill(self):
        """Kill the proxy actor."""
        ray.kill(self._actor_handle, no_restart=True)


class ProxyState:
    def __init__(
        self,
        actor_proxy_wrapper: ProxyWrapper,
        actor_name: str,
        node_id: str,
        node_ip: str,
        proxy_restart_count: int = 0,
        timer: TimerBase = Timer(),
    ):
        self._actor_proxy_wrapper = actor_proxy_wrapper
        self._actor_proxy_wrapper.start_new_ready_check()
        self._actor_name = actor_name
        self._node_id = node_id
        self._status = ProxyStatus.STARTING
        self._timer = timer
        self._last_health_check_time: float = self._timer.time()
        self._shutting_down = False
        self._consecutive_health_check_failures: int = 0
        self._proxy_restart_count = proxy_restart_count
        self._last_drain_check_time: float = None

        self._actor_details = ProxyDetails(
            node_id=node_id,
            node_ip=node_ip,
            actor_id=self._actor_proxy_wrapper.actor_id,
            actor_name=self._actor_name,
            status=self._status,
        )

    @property
    def actor_handle(self) -> ActorHandle:
        return self._actor_proxy_wrapper.actor_handle

    @property
    def actor_name(self) -> str:
        return self._actor_name

    @property
    def status(self) -> ProxyStatus:
        return self._status

    @property
    def actor_details(self) -> ProxyDetails:
        return self._actor_details

    @property
    def proxy_restart_count(self) -> int:
        return self._proxy_restart_count

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
                f"Proxy {self._actor_name} failed the health check "
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

        if self._actor_proxy_wrapper.health_check_ongoing:
            try:
                healthy_call_status = self._actor_proxy_wrapper.is_healthy()
                if healthy_call_status == ProxyWrapperCallStatus.FINISHED_SUCCEED:
                    # Call to reset _consecutive_health_check_failures
                    # the status should be unchanged.
                    self.try_update_status(self._status)
                elif healthy_call_status == ProxyWrapperCallStatus.FINISHED_FAILED:
                    self.try_update_status(ProxyStatus.UNHEALTHY)
                elif (
                    self._timer.time() - self._last_health_check_time
                    > PROXY_HEALTH_CHECK_TIMEOUT_S
                ):
                    # Health check hasn't returned and the timeout is up, consider it
                    # failed.
                    self._actor_proxy_wrapper.reset_health_check()
                    logger.warning(
                        "Didn't receive health check response for proxy "
                        f"{self._node_id} after {PROXY_HEALTH_CHECK_TIMEOUT_S}s"
                    )
                    self.try_update_status(ProxyStatus.UNHEALTHY)
            except Exception as e:
                logger.warning(f"Health check for proxy {self._actor_name} failed: {e}")
                self.try_update_status(ProxyStatus.UNHEALTHY)

        # If there's no active in-progress health check, and it has been more than 10
        # seconds since the last health check, perform another health check.
        if self._actor_proxy_wrapper.health_check_ongoing:
            return
        randomized_period_s = PROXY_HEALTH_CHECK_PERIOD_S * random.uniform(0.9, 1.1)
        if self._timer.time() - self._last_health_check_time > randomized_period_s:
            self._last_health_check_time = self._timer.time()
            self._actor_proxy_wrapper.start_new_health_check()

    def _drain_check(self):
        """Check whether the proxy actor is drained or not."""
        assert self._status == ProxyStatus.DRAINING

        if self._actor_proxy_wrapper.is_draining:
            try:
                drained_call_status = self._actor_proxy_wrapper.is_drained()
                if drained_call_status == ProxyWrapperCallStatus.FINISHED_SUCCEED:
                    self.set_status(ProxyStatus.DRAINED)
            except Exception as e:
                logger.warning(f"Drain check for proxy {self._actor_name} failed: {e}.")
        elif (
            self._timer.time() - self._last_drain_check_time
            > PROXY_DRAIN_CHECK_PERIOD_S
        ):
            self._last_drain_check_time = self._timer.time()
            self._actor_proxy_wrapper.start_new_drained_check()

    def update(self, draining: bool = False):
        """Update the status of the current proxy.

        The state machine is:
        STARTING -> HEALTHY or UNHEALTHY
        HEALTHY -> DRAINING or UNHEALTHY
        DRAINING -> HEALTHY or UNHEALTHY or DRAINED

        1) When the proxy is already shutting down, in DRAINED or UNHEALTHY status,
        do nothing.
        2) When the proxy is starting, check ready object reference. If ready
        object reference returns a successful call set status to HEALTHY. If the
        call to ready() on the proxy actor has any exception or timeout, increment
        the consecutive health check failure counter and retry on the next update call.
        The status is only set to UNHEALTHY when all retries have exhausted.
        3) When the proxy already has an in-progress health check. If health check
        object returns a successful call, keep the current status. If the call has
        any exception or timeout, count towards 1 of the consecutive health check
        failures and retry on the next update call. The status is only set to UNHEALTHY
        when all retries have exhausted.
        4) When the proxy need to setup another health check (when none of the
        above met and the time since the last health check is longer than
        PROXY_HEALTH_CHECK_PERIOD_S with some margin). Reset
        self._last_health_check_time and set up a new health check object so the next
        update can call healthy check again.
        5) Transition the status between HEALTHY and DRAINING.
        6) When the proxy is draining, check whether it's drained or not.
        """
        if (
            self._shutting_down
            or self._status == ProxyStatus.DRAINED
            or self._status == ProxyStatus.UNHEALTHY
        ):
            return

        # Doing a linear backoff for the ready check timeout.
        ready_check_timeout = (
            self.proxy_restart_count + 1
        ) * PROXY_READY_CHECK_TIMEOUT_S
        if self._status == ProxyStatus.STARTING:
            try:
                ready_call_status = self._actor_proxy_wrapper.is_ready()
                if ready_call_status == ProxyWrapperCallStatus.FINISHED_SUCCEED:
                    self.try_update_status(ProxyStatus.HEALTHY)
                    self.update_actor_details(
                        worker_id=self._actor_proxy_wrapper.worker_id,
                        log_file_path=self._actor_proxy_wrapper.log_file_path,
                        status=self._status,
                    )
                elif ready_call_status == ProxyWrapperCallStatus.FINISHED_FAILED:
                    self.set_status(ProxyStatus.UNHEALTHY)
                    logger.warning(
                        "Unexpected actor death when checking readiness of "
                        f"proxy on node {self._node_id}:\n{traceback.format_exc()}"
                    )
                elif (
                    self._timer.time() - self._last_health_check_time
                    > ready_check_timeout
                ):
                    # Ready check hasn't returned and the timeout is up, consider it
                    # failed.
                    self.set_status(ProxyStatus.UNHEALTHY)
                    logger.warning(
                        "Didn't receive ready check response for proxy "
                        f"{self._node_id} after {ready_check_timeout}s."
                    )
            except Exception:
                self.try_update_status(ProxyStatus.UNHEALTHY)
                logger.warning(
                    "Unexpected error occurred when checking readiness of "
                    f"proxy on node {self._node_id}:\n{traceback.format_exc()}"
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
            self._actor_proxy_wrapper.update_draining(draining=True)
            assert self._actor_proxy_wrapper.is_draining is False
            assert self._last_drain_check_time is None
            self._last_drain_check_time = self._timer.time()

        if (self._status == ProxyStatus.DRAINING) and not draining:
            logger.info(f"Stop draining the proxy actor on node {self._node_id}")
            self.set_status(ProxyStatus.HEALTHY)
            self._actor_proxy_wrapper.update_draining(draining=False)
            self._actor_proxy_wrapper.reset_drained_check()
            self._last_drain_check_time = None

        if self._status == ProxyStatus.DRAINING:
            self._drain_check()

    def shutdown(self):
        self._shutting_down = True
        self._actor_proxy_wrapper.kill()

    def is_ready_for_shutdown(self) -> bool:
        """Return whether the proxy actor is shutdown.

        For a proxy actor to be considered shutdown, it must be marked as
        _shutting_down and the actor must be shut down.
        """
        if not self._shutting_down:
            return False

        return self._actor_proxy_wrapper.is_shutdown()


class ProxyStateManager:
    """Manages all state for proxies in the system.

    This class is *not* thread safe, so any state-modifying methods should be
    called with a lock held.
    """

    def __init__(
        self,
        controller_name: str,
        config: HTTPOptions,
        head_node_id: str,
        cluster_node_info_cache: ClusterNodeInfoCache,
        logging_config: LoggingConfig,
        grpc_options: Optional[gRPCOptions] = None,
        proxy_actor_class: Type[ProxyActor] = ProxyActor,
        actor_proxy_wrapper_class: Type[ProxyWrapper] = ActorProxyWrapper,
        timer: TimerBase = Timer(),
    ):
        self.logging_config = logging_config
        self._controller_name = controller_name
        if config is not None:
            self._config = config
        else:
            self._config = HTTPOptions()
        self._grpc_options = grpc_options or gRPCOptions()
        self._proxy_states: Dict[NodeId, ProxyState] = dict()
        self._proxy_restart_counts: Dict[NodeId, int] = dict()
        self._head_node_id: str = head_node_id
        self._proxy_actor_class = proxy_actor_class
        self._actor_proxy_wrapper_class = actor_proxy_wrapper_class
        self._timer = timer

        self._cluster_node_info_cache = cluster_node_info_cache

        assert isinstance(head_node_id, str)

    def reconfiture_logging_config(self, logging_config: LoggingConfig):
        self.logging_config = logging_config

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

    def get_proxy_handles(self) -> Dict[NodeId, ActorHandle]:
        return {
            node_id: state.actor_handle for node_id, state in self._proxy_states.items()
        }

    def get_proxy_names(self) -> Dict[NodeId, str]:
        return {
            node_id: state.actor_name for node_id, state in self._proxy_states.items()
        }

    def get_proxy_details(self) -> Dict[NodeId, ProxyDetails]:
        return {
            node_id: state.actor_details
            for node_id, state in self._proxy_states.items()
        }

    def update(self, proxy_nodes: Set[NodeId] = None):
        """Update the state of all proxies.

        Start proxies on all nodes if not already exist and stop the proxies on nodes
        that are no longer exist. Update all proxy states. Kill and restart
        unhealthy proxies.
        """
        # Ensure head node always has a proxy.
        if proxy_nodes is None:
            proxy_nodes = {self._head_node_id}
        else:
            proxy_nodes.add(self._head_node_id)

        target_nodes = self._get_target_nodes(proxy_nodes)
        target_node_ids = {node_id for node_id, _ in target_nodes}

        for node_id, proxy_state in self._proxy_states.items():
            draining = node_id not in target_node_ids
            proxy_state.update(draining)

        self._stop_proxies_if_needed()
        self._start_proxies_if_needed(target_nodes)

    def _get_target_nodes(self, proxy_nodes) -> List[Tuple[str, str]]:
        """Return the list of (node_id, ip_address) to deploy HTTP and gRPC servers
        on."""
        location = self._config.location

        if location == DeploymentMode.NoServer:
            return []

        target_nodes = [
            (node_id, ip_address)
            for node_id, ip_address in self._cluster_node_info_cache.get_alive_nodes()
            if node_id in proxy_nodes
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

        return target_nodes

    def _generate_actor_name(self, node_id: str) -> str:
        return format_actor_name(SERVE_PROXY_NAME, self._controller_name, node_id)

    def _start_proxy(
        self,
        name: str,
        node_id: str,
        node_ip_address: str,
    ) -> ProxyWrapper:
        """Helper to start or reuse existing proxy and wrap in the proxy actor wrapper.

        Compute the HTTP port based on `TEST_WORKER_NODE_HTTP_PORT` env var and gRPC
        port based on `TEST_WORKER_NODE_GRPC_PORT` env var. Passed all the required
        variables into the proxy actor wrapper class and return the proxy actor wrapper.
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

        return self._actor_proxy_wrapper_class(
            logging_config=self.logging_config,
            config=self._config,
            grpc_options=grpc_options,
            controller_name=self._controller_name,
            name=name,
            node_id=node_id,
            node_ip_address=node_ip_address,
            port=port,
            proxy_actor_class=self._proxy_actor_class,
        )

    def _start_proxies_if_needed(self, target_nodes) -> None:
        """Start a proxy on every node if it doesn't already exist."""

        for node_id, node_ip_address in target_nodes:
            if node_id in self._proxy_states:
                continue

            name = self._generate_actor_name(node_id=node_id)
            actor_proxy_wrapper = self._start_proxy(
                name=name,
                node_id=node_id,
                node_ip_address=node_ip_address,
            )

            self._proxy_states[node_id] = ProxyState(
                actor_proxy_wrapper=actor_proxy_wrapper,
                actor_name=name,
                node_id=node_id,
                node_ip=node_ip_address,
                proxy_restart_count=self._proxy_restart_counts.get(node_id, 0),
                timer=self._timer,
            )

    def _stop_proxies_if_needed(self) -> bool:
        """Removes proxy actors.

        Removes proxy actors from any nodes that no longer exist or unhealthy proxy.
        """
        alive_node_ids = self._cluster_node_info_cache.get_alive_node_ids()
        to_stop = []
        for node_id, proxy_state in self._proxy_states.items():
            if node_id not in alive_node_ids:
                logger.info(f"Removing proxy on removed node '{node_id}'.")
                to_stop.append(node_id)
            elif proxy_state.status == ProxyStatus.UNHEALTHY:
                logger.info(
                    f"Proxy on node '{node_id}' UNHEALTHY. Shutting down "
                    "the unhealthy proxy and starting a new one."
                )
                to_stop.append(node_id)
            elif proxy_state.status == ProxyStatus.DRAINED:
                logger.info(f"Removing drained proxy on node '{node_id}'.")
                to_stop.append(node_id)

        for node_id in to_stop:
            proxy_state = self._proxy_states.pop(node_id)
            self._proxy_restart_counts[node_id] = proxy_state.proxy_restart_count + 1
            proxy_state.shutdown()
