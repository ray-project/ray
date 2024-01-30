import ray
from ray._raylet import VirtualClusterID


class VirtualCluster:
    """A handle to a virtual cluster."""

    def __init__(self, id):
        self.id = id

    def __enter__(self):
        self._previous_virtual_cluster_id = (
            ray.get_runtime_context().get_virtual_cluster_id()
        )
        ray.get_runtime_context()._set_virtual_cluster_id(self.id.hex())

    def __exit__(self, *args):
        ray.get_runtime_context()._set_virtual_cluster_id(
            self._previous_virtual_cluster_id
        )

    def ready(self):
        @ray.remote(num_cpus=0)
        def _ready():
            return True

        with self:
            return _ready.remote()


def virtual_cluster(spec: str) -> VirtualCluster:
    worker = ray._private.worker.global_worker
    worker.check_connected()

    virtual_cluster_id = worker.core_worker.create_virtual_cluster(spec)

    return VirtualCluster(virtual_cluster_id)


def remove_virtual_cluster(virtual_cluster_id: str) -> None:
    worker = ray._private.worker.global_worker
    worker.check_connected()

    worker.core_worker.remove_virtual_cluster(
        VirtualClusterID.from_hex(virtual_cluster_id)
    )


def rewrite_virtual_cluster_resources(virtual_cluster_id, resources):
    rewritten_resources = {}
    for name, quantity in resources.items():
        if name == "bundle":
            rewritten_resources[name] = quantity
        else:
            rewritten_resources[f"{name}_vc_{virtual_cluster_id}"] = quantity
    return rewritten_resources


def rewrite_virtual_cluster_placement_group_bundles(virtual_cluster_id, bundles):
    rewritten_bundles = []
    for bundle in bundles:
        rewritten_bundles.append(
            rewrite_virtual_cluster_resources(virtual_cluster_id, bundle)
        )
    return rewritten_bundles
