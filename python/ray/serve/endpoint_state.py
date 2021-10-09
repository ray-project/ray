from typing import Dict, Any, Optional

from ray import cloudpickle
from ray.serve.common import (EndpointInfo, EndpointTag)
from ray.serve.long_poll import LongPollNamespace
from ray.serve.storage.kv_store import KVStoreBase
from ray.serve.long_poll import LongPollHost

CHECKPOINT_KEY = "serve-endpoint-state-checkpoint"


class EndpointState:
    """Manages all state for endpoints in the system.

    This class is *not* thread safe, so any state-modifying methods should be
    called with a lock held.
    """

    def __init__(self, kv_store: KVStoreBase, long_poll_host: LongPollHost):
        self._kv_store = kv_store
        self._long_poll_host = long_poll_host
        self._endpoints: Dict[EndpointTag, EndpointInfo] = dict()

        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            self._endpoints = cloudpickle.loads(checkpoint)

        self._notify_route_table_changed()

    def shutdown(self):
        self._kv_store.delete(CHECKPOINT_KEY)

    def _checkpoint(self):
        self._kv_store.put(CHECKPOINT_KEY, cloudpickle.dumps(self._endpoints))

    def _notify_route_table_changed(self):
        self._long_poll_host.notify_changed(LongPollNamespace.ROUTE_TABLE,
                                            self._endpoints)

    def _get_endpoint_for_route(self, route: str) -> Optional[EndpointTag]:
        for endpoint, info in self._endpoints.items():
            if info.route == route:
                return endpoint

        return None

    def update_endpoint(self, endpoint: EndpointTag,
                        endpoint_info: EndpointInfo) -> None:
        """Create or update the given endpoint.

        This method is idempotent - if the endpoint already exists it will be
        updated to match the given parameters. Calling this twice with the same
        arguments is a no-op.
        """
        existing_route_endpoint = self._get_endpoint_for_route(
            endpoint_info.route)
        if (endpoint_info.route is not None
                and existing_route_endpoint is not None
                and existing_route_endpoint != endpoint):
            raise ValueError(
                f"route_prefix '{endpoint_info.route}' is already registered.")

        if endpoint in self._endpoints:
            if (self._endpoints[endpoint] == endpoint_info):
                return

        self._endpoints[endpoint] = endpoint_info

        self._checkpoint()
        self._notify_route_table_changed()

    def get_endpoint_route(self, endpoint: EndpointTag) -> Optional[str]:
        if endpoint in self._endpoints:
            return self._endpoints[endpoint].route
        return None

    def get_endpoints(self) -> Dict[EndpointTag, Dict[str, Any]]:
        endpoints = {}
        for endpoint, info in self._endpoints.items():
            endpoints[endpoint] = {
                "route": info.route,
            }
        return endpoints

    def delete_endpoint(self, endpoint: EndpointTag) -> None:
        # This method must be idempotent. We should validate that the
        # specified endpoint exists on the client.
        if endpoint not in self._endpoints:
            return

        del self._endpoints[endpoint]

        self._checkpoint()
        self._notify_route_table_changed()
