import abc
from typing import Dict, List

import ray


class AutoscalingRequester(abc.ABC):
    @abc.abstractmethod
    def request(self, bundles: List[Dict]):
        """Requests that the cluster autoscales until it can satisfy these bundles."""
        raise NotImplementedError

    @abc.abstractmethod
    def node_resources(self) -> List[Dict[str, float]]:
        """Return the per-node resources ready to use by the requester."""
        raise NotImplementedError

    @abc.abstractmethod
    def clear_request(self):
        """Clear the autoscaling request."""
        raise NotImplementedError


class TrainAutoscalingRequester(AutoscalingRequester):
    def request(self, bundles: List[Dict]):
        """Make a request to the Ray autoscaler to scale the cluster.

        Calling this will replace any previous request.
        """
        ray.autoscaler.sdk.request_resources(bundles=bundles)

    def node_resources(self) -> List[Dict[str, float]]:
        """Returns the resources of all alive nodes in the cluster."""
        return [
            node_info["Resources"] for node_info in ray.nodes() if node_info["Alive"]
        ]

    def clear_request(self):
        """Reset the autoscaling request so that idle nodes can downscale."""
        ray.autoscaler.sdk.request_resources(bundles=[])
