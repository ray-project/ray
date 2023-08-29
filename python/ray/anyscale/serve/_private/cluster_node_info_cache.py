from typing import Set

import ray
from ray._raylet import GcsClient
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache


class AnyscaleClusterNodeInfoCache(ClusterNodeInfoCache):
    def __init__(self, gcs_client: GcsClient):
        super().__init__(gcs_client)
        self._cached_draining_nodes = None

    def update(self):
        super().update()
        self._cached_draining_nodes = ray._private.state.state.get_draining_nodes()

    def get_draining_node_ids(self) -> Set[str]:
        return self._cached_draining_nodes
