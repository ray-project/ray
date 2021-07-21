from typing import Dict, Any, Optional

from ray import cloudpickle
from ray.serve.common import (BackendTag, EndpointInfo, EndpointTag,
                              TrafficPolicy)
from ray.serve.long_poll import LongPollNamespace
from ray.serve.kv_store import RayInternalKVStore
from ray.serve.long_poll import LongPollHost

CHECKPOINT_KEY = "serve-endpoint-state-checkpoint"


class EndpointState:
    """Manages all state for endpoints in the system.

    This class is *not* thread safe, so any state-modifying methods should be
    called with a lock held.
    """

    def __init__(self, kv_store: RayInternalKVStore,
                 long_poll_host: LongPollHost):
        self._kv_store = kv_store
        self._long_poll_host = long_poll_host
        self._endpoints: Dict[EndpointTag, EndpointInfo] = dict()
        self._traffic_policies: Dict[EndpointTag, TrafficPolicy] = dict()

        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            (self._endpoints,
             self._traffic_policies) = cloudpickle.loads(checkpoint)

        self._notify_route_table_changed()
        self._notify_traffic_policies_changed()

    def shutdown(self):
        self._kv_store.delete(CHECKPOINT_KEY)

    def _checkpoint(self):
        self._kv_store.put(
            CHECKPOINT_KEY,
            cloudpickle.dumps((self._endpoints, self._traffic_policies)))

    def _notify_route_table_changed(self):
        self._long_poll_host.notify_changed(LongPollNamespace.ROUTE_TABLE,
                                            self._endpoints)

    def _notify_traffic_policies_changed(
            self, filter_tag: Optional[EndpointTag] = None):
        for tag, policy in self._traffic_policies.items():
            if filter_tag is None or tag == filter_tag:
                self._long_poll_host.notify_changed(
                    (LongPollNamespace.TRAFFIC_POLICIES, tag),
                    policy,
                )

    def _get_endpoint_for_route(self, route: str) -> Optional[EndpointTag]:
        for endpoint, info in self._endpoints.items():
            if info.route == route:
                return endpoint

        return None

    def update_endpoint(self, endpoint: EndpointTag,
                        endpoint_info: EndpointInfo,
                        traffic_policy: TrafficPolicy) -> None:
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
            if (self._endpoints[endpoint] == endpoint_info
                    and self._traffic_policies == traffic_policy):
                return

        self._endpoints[endpoint] = endpoint_info
        self._traffic_policies[endpoint] = traffic_policy

        self._checkpoint()
        self._notify_route_table_changed()
        self._notify_traffic_policies_changed(endpoint)

    def create_endpoint(self, endpoint: EndpointTag,
                        endpoint_info: EndpointInfo,
                        traffic_policy: TrafficPolicy):
        err_prefix = "Cannot create endpoint."
        if endpoint_info.route is not None:
            existing_route_endpoint = self._get_endpoint_for_route(
                endpoint_info.route)
            if (existing_route_endpoint is not None
                    and existing_route_endpoint != endpoint):
                raise ValueError("{} Route '{}' is already registered.".format(
                    err_prefix, endpoint_info.route))

        if endpoint in self._endpoints:
            if (self._endpoints[endpoint] == endpoint_info
                    and self._traffic_policies == traffic_policy):
                return
            else:
                raise ValueError(
                    "{} Endpoint '{}' is already registered.".format(
                        err_prefix, endpoint))

        self._endpoints[endpoint] = endpoint_info
        self._traffic_policies[endpoint] = traffic_policy

        self._checkpoint()
        self._notify_route_table_changed()
        self._notify_traffic_policies_changed(endpoint)

    def set_traffic_policy(self, endpoint: EndpointTag,
                           traffic_policy: TrafficPolicy):
        if endpoint not in self._traffic_policies:
            raise ValueError("Attempted to assign traffic for an endpoint '{}'"
                             " that is not registered.".format(endpoint))

        self._traffic_policies[endpoint] = traffic_policy

        self._checkpoint()
        self._notify_traffic_policies_changed(endpoint)

    def shadow_traffic(self, endpoint: EndpointTag, backend: BackendTag,
                       proportion: float):
        if endpoint not in self._traffic_policies:
            raise ValueError("Attempted to shadow traffic from an "
                             "endpoint '{}' that is not registered."
                             .format(endpoint))

        self._traffic_policies[endpoint].set_shadow(backend, proportion)

        self._checkpoint()
        self._notify_traffic_policies_changed(endpoint)

    def get_endpoint_route(self, endpoint: EndpointTag) -> Optional[str]:
        if endpoint in self._endpoints:
            return self._endpoints[endpoint].route
        return None

    def get_endpoints(self) -> Dict[EndpointTag, Dict[str, Any]]:
        endpoints = {}
        for endpoint, info in self._endpoints.items():
            if endpoint in self._traffic_policies:
                traffic_policy = self._traffic_policies[endpoint]
                traffic_dict = traffic_policy.traffic_dict
                shadow_dict = traffic_policy.shadow_dict
            else:
                traffic_dict = {}
                shadow_dict = {}

            endpoints[endpoint] = {
                "route": info.route,
                "methods": info.http_methods,
                "traffic": traffic_dict,
                "shadows": shadow_dict,
                "python_methods": info.python_methods,
            }
        return endpoints

    def delete_endpoint(self, endpoint: EndpointTag) -> None:
        # This method must be idempotent. We should validate that the
        # specified endpoint exists on the client.
        if endpoint not in self._endpoints:
            return

        del self._endpoints[endpoint]
        del self._traffic_policies[endpoint]

        self._checkpoint()
        self._notify_route_table_changed()
