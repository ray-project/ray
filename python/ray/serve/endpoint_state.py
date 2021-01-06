from typing import Dict, Any, Tuple

import ray.cloudpickle as pickle
from ray.serve.common import BackendTag, EndpointTag, TrafficPolicy


class EndpointState:
    def __init__(self, checkpoint: bytes = None):
        self.routes: Dict[BackendTag, Tuple[EndpointTag, Any]] = dict()
        self.traffic_policies: Dict[EndpointTag, TrafficPolicy] = dict()

        if checkpoint is not None:
            self.routes, self.traffic_policies = pickle.loads(checkpoint)

    def checkpoint(self):
        return pickle.dumps((self.routes, self.traffic_policies))

    def get_endpoints(self) -> Dict[EndpointTag, Dict[str, Any]]:
        endpoints = {}
        for route, (endpoint, methods) in self.routes.items():
            if endpoint in self.traffic_policies:
                traffic_policy = self.traffic_policies[endpoint]
                traffic_dict = traffic_policy.traffic_dict
                shadow_dict = traffic_policy.shadow_dict
            else:
                traffic_dict = {}
                shadow_dict = {}

            endpoints[endpoint] = {
                "route": route if route.startswith("/") else None,
                "methods": methods,
                "traffic": traffic_dict,
                "shadows": shadow_dict,
            }
        return endpoints
