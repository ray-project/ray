# Copyright (2023 and onwards) Anyscale, Inc.

import time
from typing import Dict, Optional, Set

import ray
from ray._raylet import GcsClient
from ray.anyscale._private.constants import ANYSCALE_RAY_NODE_AVAILABILITY_ZONE_LABEL
from ray.anyscale.serve._private.constants import (
    ANYSCALE_RAY_SERVE_DEFAULT_DRAINING_TIMEOUT_S,
)
from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache


class AnyscaleClusterNodeInfoCache(ClusterNodeInfoCache):
    def __init__(self, gcs_client: GcsClient):
        super().__init__(gcs_client)
        self._cached_draining_nodes: Set[str] = set()

    def update(self):
        super().update()
        curr_time = time.time()
        draining_nodes: Dict[str, int] = ray._private.state.state.get_draining_nodes()

        new_draining_nodes = set(draining_nodes) - set(self._cached_draining_nodes)
        for node_id, deadline in draining_nodes.items():
            if deadline == 0:
                if node_id in new_draining_nodes:
                    # Set deadline timestamp in milliseconds
                    draining_nodes[node_id] = 1000 * (
                        curr_time + ANYSCALE_RAY_SERVE_DEFAULT_DRAINING_TIMEOUT_S
                    )
                else:
                    draining_nodes[node_id] = self._cached_draining_nodes[node_id]

        self._cached_draining_nodes = draining_nodes

    def get_draining_nodes(self) -> Dict[str, int]:
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
