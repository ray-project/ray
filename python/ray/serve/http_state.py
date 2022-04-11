import asyncio
import random
from typing import Dict, List, Tuple, Optional

import ray
from ray.actor import ActorHandle
from ray.serve.config import HTTPOptions, DeploymentMode
from ray.serve.constants import ASYNC_CONCURRENCY, SERVE_PROXY_NAME
from ray.serve.http_proxy import HTTPProxyActor
from ray.serve.utils import (
    format_actor_name,
    logger,
    get_all_node_ids,
    get_current_node_resource_key,
)
from ray.serve.common import EndpointTag, NodeId


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
        _override_controller_namespace: Optional[str] = None,
        # Used by unit testing
        _start_proxies_on_init: bool = True,
    ):
        self._controller_name = controller_name
        self._controller_namespace = ray.serve.api._get_controller_namespace(
            detached, _override_controller_namespace=_override_controller_namespace
        )
        self._detached = detached
        self._config = config
        self._override_controller_namespace = _override_controller_namespace
        self._proxy_actors: Dict[NodeId, ActorHandle] = dict()
        self._proxy_actor_names: Dict[NodeId, str] = dict()

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

    def update(self):
        self._start_proxies_if_needed()
        self._stop_proxies_if_needed()

    def _get_target_nodes(self) -> List[Tuple[str, str]]:
        """Return the list of (id, resource_key) to deploy HTTP servers on."""
        location = self._config.location
        target_nodes = get_all_node_ids()

        if location == DeploymentMode.NoServer:
            return []

        if location == DeploymentMode.HeadOnly:
            head_node_resource_key = get_current_node_resource_key()
            return [
                (node_id, node_resource)
                for node_id, node_resource in target_nodes
                if node_resource == head_node_resource_key
            ][:1]

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
        for node_id, node_resource in self._get_target_nodes():
            if node_id in self._proxy_actors:
                continue

            name = format_actor_name(SERVE_PROXY_NAME, self._controller_name, node_id)
            try:
                proxy = ray.get_actor(name, namespace=self._controller_namespace)
            except ValueError:
                logger.info(
                    "Starting HTTP proxy with name '{}' on node '{}' "
                    "listening on '{}:{}'".format(
                        name, node_id, self._config.host, self._config.port
                    )
                )
                proxy = HTTPProxyActor.options(
                    num_cpus=self._config.num_cpus,
                    name=name,
                    namespace=self._controller_namespace,
                    lifetime="detached" if self._detached else None,
                    max_concurrency=ASYNC_CONCURRENCY,
                    max_restarts=-1,
                    max_task_retries=-1,
                    resources={node_resource: 0.01},
                ).remote(
                    self._config.host,
                    self._config.port,
                    self._config.root_path,
                    controller_name=self._controller_name,
                    controller_namespace=self._controller_namespace,
                    http_middlewares=self._config.middlewares,
                )

            self._proxy_actors[node_id] = proxy
            self._proxy_actor_names[node_id] = name

    def _stop_proxies_if_needed(self) -> bool:
        """Removes proxy actors from any nodes that no longer exist."""
        all_node_ids = {node_id for node_id, _ in get_all_node_ids()}
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
