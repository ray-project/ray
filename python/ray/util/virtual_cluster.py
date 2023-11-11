import ray
from typing import Dict, List
from ray._raylet import VirtualClusterID


class VirtualCluster:
    """A handle to a virtual cluster."""

    def __init__(self, id):
        self.id = id


def virtual_cluster(bundles: List[Dict[str, float]]) -> VirtualCluster:
    worker = ray._private.worker.global_worker
    worker.check_connected()

    virtual_cluster_id = worker.core_worker.create_virtual_cluster(bundles)

    return VirtualCluster(virtual_cluster_id)


def remove_virtual_cluster(virtual_cluster_id: str) -> None:
    worker = ray._private.worker.global_worker
    worker.check_connected()

    worker.core_worker.remove_virtual_cluster(
        VirtualClusterID.from_hex(virtual_cluster_id)
    )
