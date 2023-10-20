# Copyright (2023 and onwards) Anyscale, Inc.

from typing import Optional, Set

import ray
from ray._raylet import GcsClient
from ray.anyscale._private.constants import ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL
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

    def get_node_az(self, node_id: str) -> Optional[str]:
        """Get availability zone of node.

        If node id is invalid or AZ information for the node doesn't
        exist, returns None.
        """

        if node_id in self._cached_node_labels:
            return self._cached_node_labels[node_id].get(
                ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL, None
            )
        return None
