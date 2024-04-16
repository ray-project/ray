import asyncio
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, Tuple, Type

import ray
from ray import ObjectRef
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
    RAY_SERVE_ALWAYS_RUN_PROXY_ON_HEAD_NODE,
    RAY_SERVE_ENABLE_TASK_EVENTS,
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


class ProxyWrapper(ABC):
    @property
    @abstractmethod
    def actor_id(self) -> str:
        """Return the actor id of the proxy actor."""
        raise NotImplementedError

    @abstractmethod
    def is_ready(self, timeout_s: float) -> Optional[bool]:
        """Return whether proxy is ready to be serving requests.

        Since actual readiness check is asynchronous, this method could return
        any of the following statuses:
            - None: Readiness check is pending
            - True: Readiness check completed successfully (proxy is ready)
            - False: Readiness check completed with failure (either timing out
            or failing)
        """
        raise NotImplementedError

    @abstractmethod
    def is_healthy(self, timeout_s: float) -> Optional[bool]:
        """Return whether the proxy actor is healthy.

        Since actual health-check is asynchronous, this method could return
        either of the following statuses:
            - None: Health-check is pending
            - True: Health-check completed successfully (proxy is healthy)
            - False: Health-check completed with failure (either timing out or failing)
        """
        raise NotImplementedError

    @abstractmethod
    def is_drained(self, timeout_s: float) -> Optional[bool]:
        """Return whether the proxy actor is drained.

        Since actual check whether proxy is drained is asynchronous, this method could
        return either of the following statuses:
            - None: Drain-check is pending
            - True: Drain-check completed, node *is drained*
            - False: Drain-check completed, node is *NOT* drained
        """
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
            name=name,
            node_id=node_id,
            node_ip_address=node_ip_address,
            port=port,
            proxy_actor_class=proxy_actor_class,
            logging_config=logging_config,
        )
        self._ready_check_future = None
        self._health_check_future = None
        self._drained_check_future = None

        self._update_draining_obj_ref = None

        self._node_id = node_id

        self.worker_id = None
        self.log_file_path = None

    @staticmethod
    def _get_or_create_proxy_actor(
        config: HTTPOptions,
        grpc_options: gRPCOptions,
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
                f"Starting proxy on node '{node_id}' "
                f"listening on '{config.host}:{port}'.",
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
            enable_task_events=RAY_SERVE_ENABLE_TASK_EVENTS,
        ).remote(
            config.host,
            port,
            config.root_path,
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

    def is_ready(self, timeout_s: float) -> Optional[bool]:
        if self._ready_check_future is None:
            self._ready_check_future = wrap_as_future(
                self._actor_handle.ready.remote(), timeout_s=timeout_s
            )

        if not self._ready_check_future.done():
            return None

        try:
            worker_id, log_file_path = json.loads(self._ready_check_future.result())
            self.worker_id = worker_id
            self.log_file_path = log_file_path
            return True
        except TimeoutError:
            logger.warning(
                f"Proxy actor readiness check for proxy on {self._node_id}"
                f" didn't complete in {timeout_s}s."
            )
        except Exception:
            logger.exception(
                f"Unexpected error invoking readiness check for proxy"
                f" on {self._node_id}",
            )
        finally:
            self._ready_check_future = None

        return False

    def is_healthy(self, timeout_s: float) -> Optional[bool]:
        if self._health_check_future is None:
            self._health_check_future = wrap_as_future(
                self._actor_handle.check_health.remote(), timeout_s=timeout_s
            )

        if not self._health_check_future.done():
            return None

        try:
            # NOTE: Since `check_health` method is responding with nothing, sole
            #       purpose of fetching the result is to extract any potential
            #       exceptions
            self._health_check_future.result()
            return True
        except TimeoutError:
            logger.warning(
                f"Didn't receive health check response for proxy"
                f" on {self._node_id} after {timeout_s}s."
            )
        except Exception:
            logger.exception(
                f"Unexpected error invoking health check for proxy "
                f"on {self._node_id}",
            )
        finally:
            self._health_check_future = None

        return False

    def is_drained(self, timeout_s: float) -> Optional[bool]:
        if self._drained_check_future is None:
            self._drained_check_future = wrap_as_future(
                self._actor_handle.is_drained.remote(),
                timeout_s=timeout_s,
            )

        if not self._drained_check_future.done():
            return None

        try:
            is_drained = self._drained_check_future.result()
            return is_drained
        except TimeoutError:
            logger.warning(
                f"Didn't receive drain check response for proxy"
                f" on {self._node_id} after {timeout_s}s."
            )
        except Exception:
            logger.exception(
                f"Unexpected error invoking drain-check for proxy "
                f"on {self._node_id}",
            )
        finally:
            self._drained_check_future = None

        return False

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
        # NOTE: All update_draining calls are implicitly serialized, by specifying
        #       `ObjectRef` of the previous call
        self._update_draining_obj_ref = self._actor_handle.update_draining.remote(
            draining, _after=self._update_draining_obj_ref
        )
        # In case of cancelled draining, make sure pending draining check is cancelled
        # as well
        if not draining:
            future = self._drained_check_future
            self._drained_check_future = None
            if future:
                future.cancel()

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
        self._actor_name = actor_name
        self._node_id = node_id
        self._status = ProxyStatus.STARTING
        self._timer = timer
        self._shutting_down = False
        self._consecutive_health_check_failures: int = 0
        self._proxy_restart_count = proxy_restart_count
        self._last_health_check_time: Optional[float] = None
        self._last_drain_check_time: Optional[float] = None

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
    def actor_id(self) -> str:
        return self._actor_proxy_wrapper.actor_id

    @property
    def status(self) -> ProxyStatus:
        return self._status

    @property
    def actor_details(self) -> ProxyDetails:
        return self._actor_details

    @property
    def proxy_restart_count(self) -> int:
        return self._proxy_restart_count

    def _set_status(self, status: ProxyStatus) -> None:
        """Sets _status and updates _actor_details with the new status.

        NOTE: This method should not be used directly, instead please
              use `try_update_status` method
        """
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
            else:
                # If all retries have been exhausted and setting the status to
                # UNHEALTHY, log a warning message to the user.
                logger.warning(
                    f"Proxy {self._actor_name} failed the health check "
                    f"{self._consecutive_health_check_failures} times in a row, marking"
                    f" it unhealthy."
                )
        else:
            # Reset self._consecutive_health_check_failures when status is not
            # UNHEALTHY
            self._consecutive_health_check_failures = 0

        self._set_status(status=status)

    def update_actor_details(self, **kwargs) -> None:
        """Updates _actor_details with passed in kwargs."""
        details_kwargs = self._actor_details.dict()
        details_kwargs.update(kwargs)
        self._actor_details = ProxyDetails(**details_kwargs)

    def reconcile(self, draining: bool = False):
        try:
            self._reconcile_internal(draining)
        except Exception as e:
            self.try_update_status(ProxyStatus.UNHEALTHY)
            logger.error(
                "Unexpected error occurred when reconciling stae of "
                f"proxy on node {self._node_id}",
                exc_info=e,
            )

    def _reconcile_internal(self, draining: bool):
        """Update the status of the current proxy.

        The state machine is:
        STARTING -> HEALTHY or UNHEALTHY
        HEALTHY -> DRAINING or UNHEALTHY
        DRAINING -> HEALTHY or UNHEALTHY or DRAINED

        UNHEALTHY is a terminal state upon reaching which, Proxy is going to be
        restarted by the controller
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
            is_ready_response = self._actor_proxy_wrapper.is_ready(ready_check_timeout)
            if is_ready_response is not None:
                if is_ready_response:
                    self.try_update_status(ProxyStatus.HEALTHY)
                    self.update_actor_details(
                        worker_id=self._actor_proxy_wrapper.worker_id,
                        log_file_path=self._actor_proxy_wrapper.log_file_path,
                        status=self._status,
                    )
                else:
                    self.try_update_status(ProxyStatus.UNHEALTHY)
                    logger.warning(
                        f"Proxy actor reported not ready on node {self._node_id}"
                    )
        else:
            # At this point, the proxy is either in HEALTHY or DRAINING status.
            assert self._status in {ProxyStatus.HEALTHY, ProxyStatus.DRAINING}

            should_check_health = self._last_health_check_time is None or (
                self._timer.time() - self._last_health_check_time
                >= PROXY_HEALTH_CHECK_PERIOD_S
            )
            # Perform health-check for proxy's actor (if necessary)
            if should_check_health:
                is_healthy_response = self._actor_proxy_wrapper.is_healthy(
                    PROXY_HEALTH_CHECK_TIMEOUT_S
                )
                if is_healthy_response is not None:
                    if is_healthy_response:
                        # At this stage status is either HEALTHY or DRAINING, and here
                        # we simply reset the status
                        self.try_update_status(self._status)
                    else:
                        self.try_update_status(ProxyStatus.UNHEALTHY)

                    self._last_health_check_time = self._timer.time()

            # Handle state transitions (if necessary)
            if self._status == ProxyStatus.UNHEALTHY:
                return
            elif self._status == ProxyStatus.HEALTHY:
                if draining:
                    logger.info(f"Draining proxy on node '{self._node_id}'.")
                    assert self._last_drain_check_time is None

                    self._actor_proxy_wrapper.update_draining(draining=True)
                    self.try_update_status(ProxyStatus.DRAINING)
            elif self._status == ProxyStatus.DRAINING:
                if not draining:
                    logger.info(f"No longer draining proxy on node '{self._node_id}'.")
                    self._last_drain_check_time = None

                    self._actor_proxy_wrapper.update_draining(draining=False)
                    self.try_update_status(ProxyStatus.HEALTHY)
                else:
                    should_check_drain = self._last_drain_check_time is None or (
                        self._timer.time() - self._last_drain_check_time
                        >= PROXY_DRAIN_CHECK_PERIOD_S
                    )
                    if should_check_drain:
                        # NOTE: We use the same timeout as for readiness checking
                        is_drained_response = self._actor_proxy_wrapper.is_drained(
                            PROXY_READY_CHECK_TIMEOUT_S
                        )
                        if is_drained_response is not None:
                            if is_drained_response:
                                self.try_update_status(ProxyStatus.DRAINED)

                            self._last_drain_check_time = self._timer.time()

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

    def reconfigure_logging_config(self, logging_config: LoggingConfig):
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

    def get_alive_proxy_actor_ids(self) -> Set[str]:
        return {state.actor_id for state in self._proxy_states.values()}

    def update(self, proxy_nodes: Set[NodeId] = None) -> Set[str]:
        """Update the state of all proxies.

        Start proxies on all nodes if not already exist and stop the proxies on nodes
        that are no longer exist. Update all proxy states. Kill and restart
        unhealthy proxies.
        """
        if proxy_nodes is None:
            proxy_nodes = set()

        # Ensure head node always has a proxy (unless FF'd off).
        if RAY_SERVE_ALWAYS_RUN_PROXY_ON_HEAD_NODE:
            proxy_nodes.add(self._head_node_id)

        target_nodes = self._get_target_nodes(proxy_nodes)
        target_node_ids = {node_id for node_id, _ in target_nodes}

        for node_id, proxy_state in self._proxy_states.items():
            draining = node_id not in target_node_ids
            proxy_state.reconcile(draining)

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
        return format_actor_name(SERVE_PROXY_NAME, node_id)

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
                    f"Proxy on node '{node_id}' is unhealthy. Shutting down "
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


def _try_set_exception(fut: asyncio.Future, e: Exception):
    if not fut.done():
        fut.set_exception(e)


def wrap_as_future(ref: ObjectRef, timeout_s: Optional[float] = None) -> asyncio.Future:
    loop = asyncio.get_running_loop()

    aio_fut = asyncio.wrap_future(ref.future())

    if timeout_s is not None:
        assert timeout_s >= 0, "Timeout value should be non-negative"
        # Schedule handler to complete future exceptionally
        timeout_handler = loop.call_later(
            max(timeout_s, 0),
            _try_set_exception,
            aio_fut,
            TimeoutError(f"Future cancelled after timeout {timeout_s}s"),
        )
        # Cancel timeout handler upon completion of the future
        aio_fut.add_done_callback(lambda _: timeout_handler.cancel())

    return aio_fut
