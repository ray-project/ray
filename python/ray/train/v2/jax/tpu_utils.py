import logging
from typing import Optional

import ray
from ray._private.accelerators.tpu import infer_tpu_pod_type_from_topology
from ray._private.ray_constants import env_integer
from ray.train.constants import TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logging.getLogger(__name__)


def fetch_tpu_slice_name_from_pg(pg):
    @ray.remote(num_cpus=0)
    def _get_tpu_slice_name():
        import ray

        return (
            ray._private.accelerators.TPUAcceleratorManager.get_current_node_tpu_name()
        )

    tpu_name_ref = _get_tpu_slice_name.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=0
        )
    ).remote()

    return ray.get(tpu_name_ref)


def reserve_tpu_slice(
    topology: Optional[str],
    accelerator_type: Optional[str],
) -> Optional[str]:
    """Reserves a TPU slice using its head resource and returns the slice name.
    This enables gang scheduling of training workers with multi-host TPUs.
    This is used by JaxTrainer with TPUs in Ray Train.

    Args:
        topology: The TPU topology string (e.g. "2x2x2").
        accelerator_type: The accelerator type of the node (e.g. "TPU-V4").

    Returns:
        A string representing a unique TPU slice name.
    """
    if not (topology and accelerator_type):
        return None

    pod_type = infer_tpu_pod_type_from_topology(topology, accelerator_type)
    if pod_type is None:
        return None

    # Reserve a slice by creating a placement group on the
    # TPU head.
    head_label_selector = {
        "ray.io/tpu-worker-id": "0",
        "ray.io/tpu-pod-type": pod_type,
    }
    head_placement_group = ray.util.placement_group(
        bundles=[{f"TPU-{pod_type}-head": 1}],
        bundle_label_selector=[head_label_selector],
    )

    logger.debug("Waiting to reserve multi-host slice head.")
    timeout = env_integer(TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV, 100)
    ready, _ = ray.wait([head_placement_group.ready()], timeout=timeout)

    if not ready:
        raise TimeoutError(
            "Failed to reserve TPU head for slice with shape: {}. "
            "Ensure your cluster has sufficient resources. Requesting TPU "
            "head node with labels: {}. Current resources: {}".format(
                pod_type, head_label_selector, ray.available_resources()
            )
        )

    # Retrieve the unique slice ID.
    slice_name = fetch_tpu_slice_name_from_pg(head_placement_group)
    if slice_name is None:
        raise RuntimeError(
            "Failed to retrieve TPU slice name after reserving head placement group. "
            "Ensure that TPU slice metadata is available and correctly configured on multi-host nodes."
        )

    return slice_name
