import ray
from typing import Dict, List


class VirtualCluster:
    """A handle to a virtual cluster."""

    def __init__(self, id):
        self._id = id


def virtual_cluster(bundles: List[Dict[str, float]]) -> VirtualCluster:
    worker = ray._private.worker.global_worker
    worker.check_connected()

    virtual_cluster_id = worker.core_worker.create_virtual_cluster(bundles)

    return VirtualCluster(virtual_cluster_id)
