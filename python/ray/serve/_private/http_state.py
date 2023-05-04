import asyncio
import logging
import random
import time
from typing import Dict, List, Tuple

import ray
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from ray._raylet import GcsClient
from ray.serve.config import HTTPOptions, DeploymentMode
from ray.serve._private.constants import (
    ASYNC_CONCURRENCY,
    SERVE_LOGGER_NAME,
    SERVE_PROXY_NAME,
    SERVE_NAMESPACE,
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
    def __init__(self, actor_handle: ActorHandle, actor_name: str):
        self._actor_handle = actor_handle
        self._actor_name = actor_name

        self._status = HTTPProxyStatus.STARTING
        self._actor_started = False
        self._health_check_obj_ref = self._actor_handle.check_health.remote()
        self._last_health_check_time: float = time.time()
        self._consecutive_health_check_failures = 0

    @property
    def status(self) -> HTTPProxyStatus:
        return self._status

    def _check_health_obj_ref_result(self):
        """Check on the result of the health check.

        Should only be called after confirming the object ref is ready.
        Resets _health_check_obj_ref to None at the end.
        """
        assert len(ray.wait([self._health_check_obj_ref], timeout=0)[0])
        try:
            ray.get(self._health_check_obj_ref)
            self._status = HTTPProxyStatus.HEALTHY
            self._consecutive_health_check_failures = 0
        except Exception as e:
            logger.warning(
                f"Health check for HTTP proxy {self._actor_name} failed: {e}"
            )
            self._consecutive_health_check_failures += 1

        self._health_check_obj_ref = None

    def update(self):
        # Wait for first no-op health check to finish, indicating the actor has started
        if not self._actor_started:
            finished, _ = ray.wait([self._health_check_obj_ref], timeout=0)
            if not finished:
                return
            self._check_health_obj_ref_result()
            self._actor_started = True

        if self._health_check_obj_ref:
            finished, _ = ray.wait([self._health_check_obj_ref], timeout=0)
            if finished:
                self._check_health_obj_ref_result()
                self._health_check_obj_ref = None
                if self._consecutive_health_check_failures > 3:
                    self._status = HTTPProxyStatus.UNHEALTHY
            # If the HTTP Proxy has been blocked for more than 5 seconds, mark unhealthy
            elif time.time() - self._last_health_check_time > 5:
                self._status = HTTPProxyStatus.UNHEALTHY
                logger.warning(
                    f"Health check for HTTP Proxy {self._actor_name} took more than 5 "
                    "seconds."
                )
        # If there's no active in-progress health check and it has been more than 10
        # seconds since the last health check, perform another health check
        elif time.time() - self._last_health_check_time > 10 * random.uniform(0.9, 1.1):
            self._health_check_obj_ref = self._actor_handle.check_health.remote()
            self._last_health_check_time = time.time()


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
        self._proxy_actors: Dict[NodeId, ActorHandle] = dict()
        self._proxy_actor_names: Dict[NodeId, str] = dict()
        self._proxy_states: Dict[NodeId, HTTPProxyState] = dict()
        self._head_node_id: str = head_node_id

        self._gcs_client = gcs_client

        assert isinstance(head_node_id, str)

        # Will populate self.proxy_actors with existing actors.
        if _start_proxies_on_init:
            self._start_proxies_if_needed()

    def shutdown(self) -> None:
        for proxy in self.get_http_proxy_handles().values():
            ray.kill(proxy, no_restart=True)

    def get_config(self):
        return self._config

    def get_http_proxy_handles(self) -> Dict[NodeId, ActorHandle]:
        return self._proxy_actors

    def get_http_proxy_names(self) -> Dict[NodeId, str]:
        return self._proxy_actor_names

    def get_http_proxy_details(self) -> Dict[NodeId, HTTPProxyDetails]:
        return {
            node_id: HTTPProxyDetails(status=state.status)
            for node_id, state in self._proxy_states.items()
        }

    def update(self):
        self._start_proxies_if_needed()
        self._stop_proxies_if_needed()
        for proxy_state in self._proxy_states.values():
            proxy_state.update()

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

    def _start_proxies_if_needed(self) -> None:
        """Start a proxy on every node if it doesn't already exist."""

        for node_id, node_ip_address in self._get_target_nodes():
            if node_id in self._proxy_actors:
                continue

            name = format_actor_name(SERVE_PROXY_NAME, self._controller_name, node_id)
            try:
                proxy = ray.get_actor(name, namespace=SERVE_NAMESPACE)
            except ValueError:
                logger.info(
                    "Starting HTTP proxy with name '{}' on node '{}' "
                    "listening on '{}:{}'".format(
                        name, node_id, self._config.host, self._config.port
                    ),
                    extra={"log_to_stderr": False},
                )
                proxy = HTTPProxyActor.options(
                    num_cpus=self._config.num_cpus,
                    name=name,
                    namespace=SERVE_NAMESPACE,
                    lifetime="detached" if self._detached else None,
                    max_concurrency=ASYNC_CONCURRENCY,
                    max_restarts=-1,
                    max_task_retries=-1,
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id, soft=False
                    ),
                ).remote(
                    self._config.host,
                    self._config.port,
                    self._config.root_path,
                    controller_name=self._controller_name,
                    node_ip_address=node_ip_address,
                    http_middlewares=self._config.middlewares,
                )

            self._proxy_actors[node_id] = proxy
            self._proxy_actor_names[node_id] = name
            self._proxy_states[node_id] = HTTPProxyState(proxy, name)

    def _stop_proxies_if_needed(self) -> bool:
        """Removes proxy actors from any nodes that no longer exist."""
        all_node_ids = {node_id for node_id, _ in get_all_node_ids(self._gcs_client)}
        to_stop = []
        for node_id in self._proxy_actors:
            if node_id not in all_node_ids:
                logger.info("Removing HTTP proxy on removed node '{}'.".format(node_id))
                to_stop.append(node_id)

        for node_id in to_stop:
            proxy = self._proxy_actors.pop(node_id)
            del self._proxy_actor_names[node_id]
            ray.kill(proxy, no_restart=True)

    async def ensure_http_route_exists(self, endpoint: EndpointTag, timeout_s: float):
        """Block until the route has been propagated to all HTTP proxies.
        When the timeout occur in any of the http proxy, the whole method will
        re-throw the TimeoutError.
        """
        await asyncio.gather(
            *[
                proxy.block_until_endpoint_exists.remote(endpoint, timeout_s=timeout_s)
                for proxy in self._proxy_actors.values()
            ]
        )
