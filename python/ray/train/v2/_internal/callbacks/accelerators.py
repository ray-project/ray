import logging
import os
from collections import defaultdict
from typing import List, Optional, Tuple

import ray
import ray._private.ray_constants as ray_constants
from ray._private.accelerators.nvidia_gpu import CUDA_VISIBLE_DEVICES_ENV_VAR
from ray._private.accelerators.tpu import (
    fetch_tpu_slice_name_from_pg,
    infer_tpu_pod_type_from_topology,
)
from ray._private.ray_constants import env_bool, env_integer
from ray.train import BackendConfig
from ray.train.constants import (
    ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV,
    TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV,
)
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.worker_group import ActorMetadata, WorkerGroup
from ray.train.v2._internal.util import ray_get_safe
from ray.train.v2.api.config import ScalingConfig
from ray.util.placement_group import (
    PlacementGroup,
)

logger = logging.getLogger(__name__)


class AcceleratorSetupCallback(WorkerGroupCallback):
    """Perform accelerator setup for workers.

    For example, this callback can be used to share CUDA_VISIBLE_DEVICES
    among workers on the same node.
    """

    def __init__(self, backend_config: BackendConfig, scaling_config: ScalingConfig):
        self._backend = backend_config.backend_cls()
        self._scaling_config = scaling_config

    def after_worker_group_start(self, worker_group: WorkerGroup):
        self._maybe_share_cuda_visible_devices(worker_group)
        # TODO: Add support for sharing other accelerator resources.

    def _maybe_share_cuda_visible_devices(self, worker_group: WorkerGroup):
        share_cuda_visible_devices_enabled = env_bool(
            ENABLE_SHARE_CUDA_VISIBLE_DEVICES_ENV,
            self._backend.share_cuda_visible_devices,
        )

        if (
            self._scaling_config._resources_per_worker_not_none.get("GPU", 0) > 0
            and share_cuda_visible_devices_enabled
        ):
            _share_cuda_visible_devices(worker_group)


def _share_cuda_visible_devices(worker_group: WorkerGroup):
    """Sets CUDA_VISIBLE_DEVICES on all workers.
    For each worker, CUDA_VISIBLE_DEVICES will be set to the GPU IDs
    visible to all workers on that worker's node.
    This allows GPU workers on the same node to communicate with one
    another.

    Example:
        Setup:
        - Node1:
            - Worker1: {0, 1}
            - Worker2: {2, 3}
        - Node2:
            - Worker3: {0, 1}
        CUDA_VISIBLE_DEVICES:
        - Worker1: "0,1,2,3"
        - Worker2: "0,1,2,3"
        - Worker2: "0,1"
    """
    _share_accelerator_ids(
        worker_group, ray_constants.GPU, CUDA_VISIBLE_DEVICES_ENV_VAR
    )


def _share_accelerator_ids(
    worker_group: WorkerGroup, accelerator_name: str, env_var: str
):
    """Sets the given env_var on all workers.
    For each worker, the cores/devices are visible to all the
    workers on that worker's node. This allows workers on the
    same node to communicate with one another.

    Example:
        Setup:
        - Node1:
            - Worker1: {0, 1}
            - Worker2: {2, 3}
        - Node2:
            - Worker3: {0, 1}
        NEURON_RT_VISIBLE_CORES/TPU_VISIBLE_CHIPS/...:
        - Worker1: "0,1,2,3"
        - Worker2: "0,1,2,3"
        - Worker2: "0,1"

    Args:
        accelerator_name: The name of the accelerator.
        env_var: The name of the environment variable to set.
    """
    if not worker_group.has_started():
        raise RuntimeError(
            "WorkerGroup must be started before sharing accelerator IDs."
        )

    worker_metadatas = [worker.metadata for worker in worker_group.get_workers()]
    visible_accelerator_ids_per_worker = _get_visible_accelerator_ids_per_worker(
        worker_metadatas=worker_metadatas, accelerator_name=accelerator_name
    )

    def set_accelerator_ids(accelerator_ids):
        os.environ[env_var] = accelerator_ids

    futures = []
    for rank, visible_accelerator_ids in enumerate(visible_accelerator_ids_per_worker):
        futures.append(
            worker_group.execute_single_async(
                rank, set_accelerator_ids, accelerator_ids=visible_accelerator_ids
            )
        )
    ray_get_safe(futures)


def _get_visible_accelerator_ids_per_worker(
    worker_metadatas: List[ActorMetadata], accelerator_name: str
) -> List[str]:
    """Returns a list of comma-separated accelerator IDs visible to each worker.

    All workers on a node should have the same set of visible accelerators,
    which is the union of accelerator ids of the workers.

    Returns:
        visible_accelerator_ids_per_worker: A list of comma-separated accelerator ID
            strings. This list is the same length as the number of workers.

    """
    for metadata in worker_metadatas:
        if accelerator_name not in metadata.accelerator_ids:
            raise ValueError(
                f"Accelerator '{accelerator_name}' is not available on all workers. "
                f"Got these available accelerators instead: {metadata.accelerator_ids}"
            )

    node_id_to_accelerator_ids = defaultdict(set)

    for metadata in worker_metadatas:
        node_id_to_accelerator_ids[metadata.node_id].update(
            metadata.accelerator_ids[accelerator_name]
        )

    visible_accelerator_ids_per_worker = []
    for worker_id in range(len(worker_metadatas)):
        node_id = worker_metadatas[worker_id].node_id
        accelerator_ids = sorted(node_id_to_accelerator_ids[node_id])
        all_resource_ids = ",".join([str(id) for id in accelerator_ids])
        visible_accelerator_ids_per_worker.append(all_resource_ids)

    return visible_accelerator_ids_per_worker


def create_placement_group_with_spmd(
    num_workers: int,
    resources_per_worker: dict,
    backend_config: BackendConfig,
) -> Optional[List[Tuple[PlacementGroup, range]]]:
    """Creates SPMD-aware heterogeneous placement groups. This currently only
    supports TPU with JaxTrainer.

    This creates one head PG (for index 0) and one slice PG (for index 1..N-1)
    by reserving the head node of a multi-host slice, retrieving unique slice
    information, and atomically scheduling the remaining workers to that slice.

    Returns:
        List of (PlacementGroup, worker_index_range) tuples.
    """
    if not getattr(backend_config, "use_tpu", False):
        return None

    topology = getattr(backend_config, "topology", None)
    accelerator_type = getattr(backend_config, "accelerator_type", None)

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
        bundles=[resources_per_worker],
        strategy="STRICT_PACK",
        bundle_label_selector=[head_label_selector],
    )

    logger.debug("Waiting to reserve multi-host slice head.")
    timeout = env_integer(TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV, 100)
    ready, _ = ray.wait([head_placement_group.ready()], timeout=timeout)

    if not ready:
        raise TimeoutError(
            f"Failed to reserve TPU head for slice with shape: {pod_type}."
            "Ensure your cluster has sufficient resources. Current "
            "resources: {}".format(ray.available_resources())
        )

    if num_workers == 1:
        logger.debug("Reserved single-host TPU placement group.")
        return (head_placement_group, range(0, 1))

    # Retrieve the unique slice ID.
    slice_name = fetch_tpu_slice_name_from_pg(head_placement_group)
    slice_label_selector = {
        "ray.io/tpu-slice-name": slice_name,
    }

    # Schedule the remaining multi-host workers together with the head bundle.
    slice_placement_group = ray.util.placement_group(
        bundles=[resources_per_worker] * (num_workers - 1),
        strategy="STRICT_SPREAD",
        bundle_label_selector=[slice_label_selector] * (num_workers - 1),
    )
    logger.debug("Waiting for multi-host slice placement group to start.")
    timeout = env_integer(TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV, 100)
    ready, _ = ray.wait([slice_placement_group.ready()], timeout=timeout)

    if ready:
        logger.debug("SPMD placement groups have started.")
    else:
        raise TimeoutError(
            "SPMD Placement group creation timed out. Make sure your "
            "cluster either has enough resources or use an "
            "autoscaling cluster. Ensure your cluster has multi-host nodes "
            "available for SPMD scheduling."
            "Current resources available: {}, resources requested by the "
            "placement groups: {}".format(
                ray.available_resources(), [resources_per_worker] * num_workers
            )
        )

    return [
        (head_placement_group, range(0, 1)),  # TPU head (rank 0)
        (slice_placement_group, range(1, num_workers)),  # 1...N-1 multi-host workers
    ]
