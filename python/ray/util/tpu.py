import atexit
import json
import logging
import math
import os
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import ray
from ray._private.accelerators import TPUAcceleratorManager
from ray._private.accelerators.tpu import (
    DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S,
    TPU_SUBSLICE_LABEL_PREFIX,
    VALID_TPU_TYPES,
    _build_subslice_labels,
    _get_default_chips_per_vm,
    _get_physical_worker_id_from_coords,
    _get_worker_dims_for_topology,
    _parse_topology_dims,
    get_chips_per_host,
    get_num_chips_from_topology,
    infer_tpu_pod_type_from_topology,
    reserve_tpu_slice,
)
from ray._private.client_mode_hook import client_mode_wrap
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI
from ray.util.placement_group import (
    PlacementGroup,
    placement_group,
    remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logging.getLogger(__name__)

RAY_TPU_RESOURCE_PER_CHIP_ENV_VAR = "RAY_TPU_RESOURCE_PER_CHIP"


@PublicAPI(stability="alpha")
def get_tpu_version_from_type(accelerator_type: str) -> str:
    """Extracts the version from the accelerator type.

    Args:
        accelerator_type: The full accelerator type string (e.g. "TPU-V6E").

    Returns:
        The version string (e.g. "v6e").

    Raises:
        ValueError: If the accelerator type is invalid.
    """
    accel_type_lower = accelerator_type.lower()

    if accel_type_lower.startswith("tpu-"):
        version = accel_type_lower.replace("tpu-", "")
    elif accel_type_lower.startswith("tpu"):
        version = accel_type_lower.replace("tpu", "v")
    else:
        version = accel_type_lower

    if version not in VALID_TPU_TYPES:
        raise ValueError(
            f"Invalid accelerator_type: {accelerator_type}. "
            f"Must be one of {list(VALID_TPU_TYPES)} or start with 'TPU-' followed by a valid type."
        )

    return version


@PublicAPI(stability="alpha")
def get_current_pod_name() -> Optional[str]:
    """
    Return the name of the TPU pod that the worker is a part of.

    Returns:
        The name of the TPU pod. Returns None if not part of a TPU pod.
    """
    tpu_name = TPUAcceleratorManager.get_current_node_tpu_name()
    if tpu_name == "":
        tpu_name = None
    return tpu_name


@PublicAPI(stability="alpha")
def get_current_pod_worker_count() -> Optional[int]:
    """
    Count the number of workers associated with the TPU pod that the worker belongs to.

    Returns:
        The total number of workers in the TPU pod. Returns None if the worker is not
        part of a TPU pod.
    """
    return TPUAcceleratorManager.get_num_workers_in_current_tpu_pod()


@PublicAPI(stability="alpha")
def get_num_tpu_chips_on_node() -> int:
    """
    Return the number of TPU chips on the node.
    Returns:
        The total number of chips on the TPU node. Returns 0 if none are found.
    """
    return TPUAcceleratorManager.get_current_node_num_accelerators()


@PublicAPI(stability="alpha")
def get_tpu_num_slices_for_workers(
    topology: str,
    accelerator_type: str,
    num_workers: int,
    resources_per_worker: Optional[Dict[str, float]] = None,
    tpu_resource_per_chip: Optional[int] = None,
) -> int:
    """
    Calculates the number of slices needed to accommodate the specified number of workers.

    Args:
        topology: The TPU topology string.
        accelerator_type: The accelerator type string.
        num_workers: The desired number of workers.
        resources_per_worker: Optional dict of resources per worker.
        tpu_resource_per_chip: The number of logical TPU resources per physical chip.

    Returns:
        The number of slices required. Returns 1 if inputs are invalid or incomplete.
    """
    if not topology or not accelerator_type:
        return 1

    if tpu_resource_per_chip is None:
        tpu_resource_per_chip = int(
            os.environ.get(RAY_TPU_RESOURCE_PER_CHIP_ENV_VAR, 1)
        )

    try:
        # Calculate how many workers fit in a single slice (num_slices=1)
        # given the topology and resources per worker.
        workers_per_slice, _ = get_tpu_worker_resources(
            topology=topology,
            accelerator_type=accelerator_type,
            resources_per_worker=resources_per_worker,
            num_slices=1,
            tpu_resource_per_chip=tpu_resource_per_chip,
        )

        if workers_per_slice == 0:
            return 1

        return max(1, math.ceil(num_workers / workers_per_slice))
    except Exception:
        # Fallback to 1 if calculation fails.
        return 1


@PublicAPI(stability="alpha")
def get_tpu_worker_resources(
    topology: str,
    accelerator_type: str,
    resources_per_worker: Optional[Dict[str, float]] = None,
    num_slices: int = 1,
    chips_per_vm: Optional[int] = None,
    tpu_resource_per_chip: Optional[int] = None,
) -> Tuple[int, Dict[str, float]]:
    """
    Calculates the number of workers and the resources required for each worker
    to run based on a TPU topology.

    Args:
        topology: The TPU topology string.
        accelerator_type: The accelerator string.
        resources_per_worker: Optional manual override for resources per worker. If
            unspecified, the number of TPU chips in a host is assumed.
        num_slices: The number of TPU slices.
        chips_per_vm: An optional override for the number of chips per VM.
            If unspecified, this is inferred automatically from the topology
            and accelerator type.
        tpu_resource_per_chip: The number of logical TPU resources per physical chip.
            This value scales the total number of logical TPU resources reserved by the
            slice.

    Returns:
        A tuple containing:
        - num_workers: Total workers required.
        - worker_resources: The resource dictionary for a single worker.
    """
    if tpu_resource_per_chip is None:
        tpu_resource_per_chip = int(
            os.environ.get(RAY_TPU_RESOURCE_PER_CHIP_ENV_VAR, 1)
        )

    if tpu_resource_per_chip <= 0:
        raise ValueError("`tpu_resource_per_chip` must be a positive integer.")

    accelerator_version = get_tpu_version_from_type(accelerator_type)

    # Determine the physical number of chips expected per VM (host).
    resolved_chips_per_vm = (
        chips_per_vm
        if chips_per_vm is not None
        else get_chips_per_host(topology, accelerator_version)
    )
    if resolved_chips_per_vm <= 0:
        raise ValueError("chips_per_vm must be positive.")

    # Scale physical chips to logical TPU resources per VM.
    resolved_chips_per_vm *= tpu_resource_per_chip

    # Calculate the total logical TPU resources in a single slice based on
    # topology and the resources per chip multiplier.
    total_tpus_per_slice = get_num_chips_from_topology(topology) * tpu_resource_per_chip

    # Total available logical TPU resources across all requested slices.
    total_tpus_available = total_tpus_per_slice * num_slices

    # Calculate the per-worker resources based on the TPU topology.
    final_resources = resources_per_worker.copy() if resources_per_worker else {}

    if "CPU" not in final_resources:
        final_resources["CPU"] = 1

    # If user didn't specify TPU, default to # of chips on 1 host.
    if "TPU" not in final_resources:
        final_resources["TPU"] = resolved_chips_per_vm

    tpus_per_worker = final_resources["TPU"]

    # Validate TPU resource values.
    if tpus_per_worker <= 0:
        raise ValueError("TPU resources must be positive.")

    if total_tpus_available % tpus_per_worker != 0:
        raise ValueError(
            f"Total TPU resources ({total_tpus_available}) not divisible by "
            f"TPUs requested per worker ({tpus_per_worker})."
        )

    if total_tpus_per_slice % tpus_per_worker != 0:
        raise ValueError(
            f"The requested resources per worker ({tpus_per_worker} TPU devices) do not "
            f"divide evenly into the TPU devices available per slice ({total_tpus_per_slice}). "
            "This configuration results in an uneven distribution of workers across slices, "
            "which is not supported."
        )

    num_workers = int(total_tpus_available // tpus_per_worker)

    return num_workers, final_resources


@PublicAPI(stability="alpha")
def get_tpu_coordinator_env_vars(
    coordinator_address: str,
    num_slices: int,
    slice_id: int,
    coordinator_port: str = "8081",
) -> Dict[str, str]:
    """
    Returns the environment variables required for JAX multi-slice coordination.

    Args:
        coordinator_address: The IP address or hostname of the coordinator.
        num_slices: The total number of slices in the cluster.
        slice_id: The index of the current slice.
        coordinator_port: The port the coordinator is listening on.

    Returns:
        A dictionary mapping environment variable names to their values.
    """
    return {
        "MEGASCALE_COORDINATOR_ADDRESS": coordinator_address,
        "MEGASCALE_PORT": coordinator_port,
        "MEGASCALE_NUM_SLICES": str(num_slices),
        "MEGASCALE_SLICE_ID": str(slice_id),
    }


@PublicAPI(stability="alpha")
def get_tpu_slice_name_from_node(node: Dict[str, Any]) -> Optional[str]:
    """Returns the TPU slice name for a given Ray node dictionary.

    Args:
        node: A dictionary representing a Ray node (returned by ray.nodes()).

    Returns:
        The TPU slice name if the node belongs to a multi-host slice, otherwise None.
    """
    return node.get("Labels", {}).get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY)


@PublicAPI(stability="alpha")
def get_tpu_nodes_for_slice(
    slice_name: str, nodes: Optional[List[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """Returns all alive Ray nodes belonging to the specified TPU slice.

    Args:
        slice_name: The TPU slice name to filter by.
        nodes: Optional list of Ray node dictionaries. If not provided,
            it will be fetched via `ray.nodes()` from GCS.

    Returns:
        A list of node dictionaries that are alive and belong to the specified TPU slice.
    """
    if nodes is None:
        if not ray.is_initialized():
            return []
        nodes = ray.nodes()

    return [
        node
        for node in nodes
        if node.get("Alive") and get_tpu_slice_name_from_node(node) == slice_name
    ]


def _get_intact_tpu_slices(
    topology: str,
    accelerator_type: str,
    tpu_resource_per_chip: int = 1,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Returns a mapping of slice names to lists of node dictionaries for all
    TPU slices of the specified topology that are physically intact (alive,
    matching total chip count, and having a head worker).
    """
    if not ray.is_initialized():
        return {}

    try:
        pod_type = infer_tpu_pod_type_from_topology(topology, accelerator_type)
        if not pod_type:
            return {}

        total_chips_expected = get_num_chips_from_topology(topology)

        # Scale physical chips by the resource per chip multiplier to
        # represent logically expected TPU resources on generations like tpu7x with 2
        # "chiplets" per chip that can run as discrete PJRT devices.
        total_chips_expected *= tpu_resource_per_chip

        if total_chips_expected <= 0:
            return {}
    except Exception as e:
        logger.warning(f"Failed to parse TPU topology for integrity check: {e}")
        return {}

    slice_to_nodes = {}
    for node in ray.nodes():
        if node.get("Alive"):
            labels = node.get("Labels") or {}
            if labels.get(ray._raylet.RAY_NODE_TPU_POD_TYPE_KEY) == pod_type:
                is_single_host = total_chips_expected <= (
                    node.get("Resources") or {}
                ).get("TPU", 0)
                if is_single_host:
                    # Single-host TPUs run on a single Ray node.
                    slice_name = node.get("NodeID")
                else:
                    slice_name = get_tpu_slice_name_from_node(node)

                if slice_name:
                    slice_to_nodes.setdefault(slice_name, []).append(node)

    intact_slices = {}
    for slice_name, nodes in slice_to_nodes.items():
        slice_tpu_chips = sum(
            (node.get("Resources") or {}).get("TPU", 0) for node in nodes
        )

        # Validate the slice has all its physical chips.
        if slice_tpu_chips != total_chips_expected:
            continue

        # TPU slices must have a head worker (rank 0).
        # Single-host TPUs are inherently their own head.
        has_head = any(
            (n.get("Labels") or {}).get(ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY) == "0"
            for n in nodes
        )
        if not has_head and len(nodes) == 1:
            has_head = True

        if not has_head:
            continue

        intact_slices[slice_name] = nodes

    return intact_slices


@PublicAPI(stability="alpha")
def get_num_ready_tpu_slices(
    topology: str,
    accelerator_type: str,
    tpu_resource_per_chip: Optional[int] = None,
) -> int:
    """
    Checks the cluster state to determine how many full TPU slices of the
    specified topology are currently intact and available.

    Args:
        topology: The TPU topology string (e.g. "2x4").
        accelerator_type: The accelerator type string (e.g. "TPU-V6E").
        tpu_resource_per_chip: The number of logical TPU resources per physical chip.
            This scales the total logical resources expected per slice.

    Returns:
        The integer count of fully ready and available TPU slices.
    """
    if tpu_resource_per_chip is None:
        tpu_resource_per_chip = int(
            os.environ.get(RAY_TPU_RESOURCE_PER_CHIP_ENV_VAR, 1)
        )
    intact_slices = _get_intact_tpu_slices(
        topology, accelerator_type, tpu_resource_per_chip
    )
    if not intact_slices:
        return 0

    # Fetch live resource usage via the State API to ensure slices are idle.
    from ray._private.state import available_resources_per_node

    node_avail_resources = available_resources_per_node()

    ready_and_available_slices = 0
    for slice_name, nodes in intact_slices.items():
        # Validate all nodes in this slice are completely idle to avoid
        # scheduling on multi-tenant slices currently in use.
        slice_is_idle = True
        for n in nodes:
            node_id = n.get("NodeID")
            total_tpus = n.get("Resources", {}).get("TPU", 0)

            # If the node is in ray.nodes() but hasn't heartbeated its State to GCS
            # yet, we default to assuming it's available since this means it was
            # just provisioned.
            avail_tpus = node_avail_resources.get(node_id, {}).get("TPU", total_tpus)

            # If available TPUs < total TPUs on this specific node, it is in use
            if avail_tpus < total_tpus:
                slice_is_idle = False
                break

        if slice_is_idle:
            ready_and_available_slices += 1

    return ready_and_available_slices


@DeveloperAPI
def get_num_tpu_slices(
    topology: str,
    accelerator_type: str,
    tpu_resource_per_chip: Optional[int] = None,
) -> int:
    """
    Checks the cluster state to determine how many full TPU slices of the
    specified topology are physically intact (all hosts alive with the
    expected chip count).

    Unlike :func:`get_num_ready_tpu_slices`, this does NOT check whether the
    slices are idle. A slice is counted as long as every host in it is alive
    and the total chip count matches the topology.

    Args:
        topology: The TPU topology string (e.g. "2x4").
        accelerator_type: The accelerator type string (e.g. "TPU-V6E").
        tpu_resource_per_chip: The number of logical TPU resources per physical chip.
            This scales the total logical resources reserved by each slice.

    Returns:
        The integer count of physically intact TPU slices.
    """
    if tpu_resource_per_chip is None:
        tpu_resource_per_chip = int(
            os.environ.get(RAY_TPU_RESOURCE_PER_CHIP_ENV_VAR, 1)
        )
    return len(
        _get_intact_tpu_slices(topology, accelerator_type, tpu_resource_per_chip)
    )


@PublicAPI(stability="alpha")
class SlicePlacementGroup:
    """
    A handle to a placement group reservation for a TPU slice.

    The following definitions are added for clarity:

    - Accelerator type: A string describing the accelerator type and version (e.g. TPU-V2, TPU-V6E).
    - Accelerator version: The accelerator generation only (e.g. v6e, v5p, v5litepod).
    - Pod type: The TPU accelerator version and the number of chips in a topology. (e.g. v6e-128, v5p-8).
    - Accelerator topology: The physical topology representing the structure (e.g. 2x2x2, 16x16).

    Args:
        topology: The TPU topology string (e.g. "2x2x2").
        accelerator_version: The TPU accelerator generation (e.g. "v6e", "v5p", "v4").
        resources_per_bundle: Optionally specify the resources to include in every worker bundle.
        strategy: PlacementGroup parameter. The strategy to create the placement group. Currently default to "SPREAD"

            - "PACK": Packs Bundles into as few nodes as possible.
            - "SPREAD": Places Bundles across distinct nodes as even as possible.
            - "STRICT_PACK": Packs Bundles into one node. The group is
              not allowed to span multiple nodes.
            - "STRICT_SPREAD": Packs Bundles across distinct nodes.

        name: PlacementGroup parameter. The name of the placement group.
        lifetime: PlacementGroup parameter. Either `None`, which defaults to the placement group
            will fate share with its creator and will be deleted once its
            creator is dead, or "detached", which means the placement group
            will live as a global object independent of the creator.
        num_slices: Number of TPU slices in the SlicePlacementGroup. Defaults to 1 when unspecified.
        chips_per_vm: An optional override for the number of chips per VM. Useful for resolving
            ambiguous topologies (e.g. v6e 2x4) where the slice could physically consist of
            a single 8-chip VM or two 4-chip VMs.
        head_reservation_timeout_s: The maximum time in seconds to wait for each
            TPU head placement group to become ready. Defaults to
            ``DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S``. Pass ``None`` to wait
            indefinitely.
        bundle_label_selector: Optional list of label selectors to apply per bundle. These label
            selectors are applied in addition to dynamic TPU slice name labels, which take precedence.
        pg_per_slice: If False, creates 1 placement group for all slices.
            If True, creates `num_slices` placement groups, 1 per slice.
        tpu_resource_per_chip: The number of logical TPU resources per physical chip. Defaults to 1.
            This scales the total logical resources reserved by each slice.

    Examples:

    .. testcode:: python
        :skipif: True

        import ray
        from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
        from ray.util.tpu import SlicePlacementGroup

        slice_handle = SlicePlacementGroup(topology="4x4", accelerator_version="v6e")
        slice_pg = slice_handle.slice_placement_group
        ray.get(slice_pg.ready(), timeout=10)

        @ray.remote(num_cpus=0, resources={'TPU': 4})
        def spmd_task(world, rank):
            print(f"Current TPU is rank {rank} of {world}")

        tasks = [
            spmd_task.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=slice_pg,
                )
            ).remote(world=4, rank=i)
            for i in range(slice_handle.num_hosts)
        ]
    """

    def __init__(
        self,
        topology: str,
        accelerator_version: str,
        resources_per_bundle: Optional[Dict[str, float]] = None,
        # below are args related to PG
        strategy: str = "SPREAD",
        name: str = "",
        lifetime: Optional[str] = None,
        # default
        num_slices: int = 1,
        chips_per_vm: Optional[int] = None,
        head_reservation_timeout_s: Optional[float] = (
            DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S
        ),
        bundle_label_selector: Optional[List[Dict[str, str]]] = None,
        pg_per_slice: bool = False,
        tpu_resource_per_chip: Optional[int] = None,
    ):
        self._head_pgs: List[PlacementGroup] = []
        self._bundle_label_selector: List[Dict[str, str]] = []
        self._managed_pgs: List[PlacementGroup] = []
        self._pg_per_slice = pg_per_slice
        self._user_bundle_label_selector = bundle_label_selector or []

        self._topology = topology.strip().lower()
        self._accelerator_version = get_tpu_version_from_type(
            accelerator_version.strip()
        )
        self._resources_per_bundle = resources_per_bundle or {}
        self._num_slices = num_slices
        self._head_reservation_timeout_s = head_reservation_timeout_s
        if tpu_resource_per_chip is None:
            tpu_resource_per_chip = int(
                os.environ.get(RAY_TPU_RESOURCE_PER_CHIP_ENV_VAR, 1)
            )
        self._tpu_resource_per_chip = tpu_resource_per_chip

        # Calculate number of bundles and bundle resources for specified TPU topology.
        self._num_bundles, self._bundle_resources = get_tpu_worker_resources(
            topology=self._topology,
            accelerator_type=self._accelerator_version,
            resources_per_worker=resources_per_bundle,
            num_slices=self._num_slices,
            chips_per_vm=chips_per_vm,
            tpu_resource_per_chip=self._tpu_resource_per_chip,
        )

        if chips_per_vm is not None and chips_per_vm <= 0:
            raise ValueError("chips_per_vm must be positive.")

        self._chips_per_host = (
            chips_per_vm
            if chips_per_vm is not None
            else get_chips_per_host(self._topology, self._accelerator_version)
        )
        if self._chips_per_host <= 0:
            raise ValueError(
                f"Resolved chips per host must be positive, got {self._chips_per_host}"
            )

        # Within Ray, a "host" corresponds to a user-visible compute VM.
        # This may differ from the physical hardware host definitions in GCP/GKE docs.
        total_chips = get_num_chips_from_topology(self._topology)

        self._logical_devices_per_host = (
            self._chips_per_host * self._tpu_resource_per_chip
        )
        total_chips *= self._tpu_resource_per_chip

        hosts_per_slice = max(1, total_chips // self._logical_devices_per_host)
        self._num_hosts = hosts_per_slice * self._num_slices

        self._validate_tpu_config()

        # Reserve a TPU slice of the provided accelerator version and topology.
        pgs = self._reserve_slice(
            strategy,
            name,
            lifetime,
        )
        if self._pg_per_slice:
            self._managed_pgs = pgs
        else:
            self._managed_pgs = [pgs]

    def _validate_tpu_config(self):
        # Should validate topology and generation values and return a
        # ValueError if invalid.
        if not TPUAcceleratorManager.is_valid_tpu_accelerator_topology(
            tpu_accelerator_version=self.accelerator_version,
            tpu_topology=self._topology,
        ):
            raise ValueError(
                f"Invalid accelerator topology: '{self._topology}' for "
                f"accelerator version: '{self.accelerator_version}'"
            )

    def _reserve_slice(
        self,
        strategy: str = "SPREAD",
        name: str = "",
        lifetime: Optional[str] = None,
    ) -> Union[PlacementGroup, List[PlacementGroup]]:
        """Performs the two-step scheduling to reserve a TPU slice."""
        if (
            self._user_bundle_label_selector
            and len(self._user_bundle_label_selector) != self._num_bundles
        ):
            raise ValueError(
                f"bundle_label_selector length ({len(self._user_bundle_label_selector)}) must "
                f"match the number of bundles ({self._num_bundles})."
            )

        self._bundle_label_selector = []
        all_bundles = []
        bundles_per_slice = self._num_bundles // self._num_slices

        total_chips = get_num_chips_from_topology(self._topology)
        is_single_host = total_chips <= self._chips_per_host

        try:
            accelerator_type = "TPU-" + self.accelerator_version.upper()

            for slice_idx in range(self.num_slices):
                tpu_slice_name_label = {}

                if not is_single_host:
                    # Reserve a multi-host TPU slice by gang-scheduling using the unique `ray.io/tpu-slice-name`.
                    # Check if user explicitly requested a slice name for this slice
                    user_slice_name = None
                    for bundle_idx in range(bundles_per_slice):
                        global_bundle_idx = slice_idx * bundles_per_slice + bundle_idx
                        user_labels = (
                            self._user_bundle_label_selector[global_bundle_idx]
                            if global_bundle_idx < len(self._user_bundle_label_selector)
                            else {}
                        ) or {}
                        if ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY in user_labels:
                            user_slice_name = user_labels[
                                ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY
                            ]
                            break

                    if user_slice_name:
                        tpu_slice_name_label = {
                            ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: user_slice_name
                        }
                    else:
                        reservation = reserve_tpu_slice(
                            self._topology,
                            accelerator_type,
                            timeout_s=self._head_reservation_timeout_s,
                        )
                        if not reservation:
                            raise RuntimeError(
                                f"Failed to reserve TPU slice. Requested {self.num_slices} "
                                f"slice(s) of topology '{self._topology}' with accelerator type "
                                f"'{accelerator_type}'. Ensure that sufficient TPU resources are "
                                "available in the cluster."
                            )

                        # Store the head placement group for clean-up when un-reserving the slice.
                        slice_name, head_pg = reservation
                        self._head_pgs.append(head_pg)

                        tpu_slice_name_label = {
                            ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name
                        }

                slice_bundle_label_selector = []
                for bundle_idx in range(bundles_per_slice):
                    global_bundle_idx = slice_idx * bundles_per_slice + bundle_idx

                    user_labels = (
                        self._user_bundle_label_selector[global_bundle_idx]
                        if global_bundle_idx < len(self._user_bundle_label_selector)
                        else {}
                    )
                    # TPU slice name label takes precedence; user labels fill in the rest.
                    merged_labels = {**user_labels, **tpu_slice_name_label}
                    self._bundle_label_selector.append(merged_labels)
                    slice_bundle_label_selector.append(merged_labels)

                slice_bundles = [
                    self._bundle_resources.copy() for _ in range(bundles_per_slice)
                ]
                all_bundles += slice_bundles

                if self._pg_per_slice:
                    pg_name = f"{name}_slice_{slice_idx}" if name else ""
                    pg = placement_group(
                        bundles=slice_bundles,
                        strategy=strategy,
                        name=pg_name,
                        lifetime=lifetime,
                        bundle_label_selector=slice_bundle_label_selector,
                    )
                    self._managed_pgs.append(pg)

            if not self._pg_per_slice:
                pg = placement_group(
                    bundles=all_bundles,
                    strategy=strategy,
                    name=name,
                    lifetime=lifetime,
                    bundle_label_selector=self._bundle_label_selector,
                )
                self._managed_pgs.append(pg)
                return pg
            else:
                return self._managed_pgs
        except Exception:
            self.shutdown()
            raise

    @property
    def tpu_resource_per_chip(self) -> int:
        """The logical resource scaling factor per physical TPU chip."""
        return self._tpu_resource_per_chip

    @property
    def slice_placement_group(self) -> Optional[PlacementGroup]:
        """The underlying PlacementGroup object.

        Raises:
            ValueError: If pg_per_slice=True was used.
        """
        if self._pg_per_slice:
            raise ValueError("pg_per_slice=True, use `slice_placement_groups` instead.")
        return self._managed_pgs[0] if self._managed_pgs else None

    @property
    def placement_group(self) -> Optional[PlacementGroup]:
        """Alias for slice_placement_group."""
        return self.slice_placement_group

    @property
    def slice_placement_groups(self) -> List[PlacementGroup]:
        """The list of underlying PlacementGroup objects (one per TPU slice).

        Raises:
            ValueError: If pg_per_slice=False was used.
        """
        if not self._pg_per_slice:
            raise ValueError("pg_per_slice=False, use `slice_placement_group` instead.")
        return self._managed_pgs

    @property
    def chips_per_host(self) -> int:
        """The number of physical chips per host for this TPU slice.

        This returns the physical chip count. If you need the logical resource
        amount to request from Ray (which scales with `tpu_resource_per_chip`),
        use `devices_per_host` instead.
        """
        return self._chips_per_host

    @property
    def devices_per_host(self) -> int:
        """The number of logical TPU devices per host for this TPU slice.

        This value is scaled by `tpu_resource_per_chip`. When scheduling a Ray
        Task or Actor that needs to consume an entire TPU host, you should
        request this value for the "TPU" resource requirement.
        """
        return self._logical_devices_per_host

    @property
    def num_hosts(self) -> int:
        """The total number of hosts in the SlicePlacementGroup."""
        return self._num_hosts

    @property
    def num_bundles(self) -> int:
        """The total number of bundles in the SlicePlacementGroup."""
        return self._num_bundles

    @property
    def topology(self) -> str:
        """The physical topology of the TPU slice."""
        return self._topology

    @property
    def accelerator_version(self) -> str:
        """The TPU accelerator type of the slice."""
        return self._accelerator_version

    @property
    def num_slices(self) -> int:
        """The number of TPU slices this SlicePlacementGroup spans."""
        return self._num_slices

    @property
    def head_placement_groups(self) -> List[PlacementGroup]:
        """The internal head PGs used to reserve the slices."""
        return [pg for pg in self._head_pgs if pg is not None]

    @property
    def bundle_label_selector(self) -> List[Dict[str, str]]:
        """The bundle label selector list for the worker PG."""
        return self._bundle_label_selector

    @property
    def bundle_resources(self) -> Dict[str, float]:
        """The resources that are assigned to each bundle."""
        return self._bundle_resources

    @DeveloperAPI(stability="alpha")
    def release_head_pgs(self, slice_index: Optional[int] = None) -> None:
        """Remove all internal head placement groups or a specific slice's head placement group.

        The head PGs exist only to atomically claim a TPU slice's label during
        the race window between slice selection and worker-PG construction.
        Once the worker PG's bundles are scheduled, the worker PG holds the TPU
        resources on every host in the slice and the head PGs are redundant.

        Callers should invoke this idempotent call after `self.slice_placement_group.ready()`
        resolves successfully (or `self.slice_placement_groups[slice_index].ready()`
        when `pg_per_slice=True`).

        Args:
            slice_index: The index of the slice whose head PG should be released. If None,
                all head PGs are released. If `pg_per_slice=True` and slices may become
                ready independently, it is recommended to release them by index as they
                become ready.
        """
        if slice_index is not None:
            if slice_index < 0 or slice_index >= len(self._head_pgs):
                raise ValueError(f"Invalid slice_index: {slice_index}.")
            head_pg = self._head_pgs[slice_index]
            if head_pg is not None:
                try:
                    remove_placement_group(head_pg)
                except Exception:
                    logger.exception(
                        "Failed to remove TPU head placement group %s; the "
                        "slice reservation marker may leak until the creator "
                        "process exits.",
                        getattr(head_pg, "id", head_pg),
                    )
                self._head_pgs[slice_index] = None
            return

        for idx, head_pg in enumerate(self._head_pgs):
            if head_pg is not None:
                try:
                    remove_placement_group(head_pg)
                except Exception:
                    logger.exception(
                        "Failed to remove TPU head placement group %s; the "
                        "slice reservation marker may leak until the creator "
                        "process exits.",
                        getattr(head_pg, "id", head_pg),
                    )
                self._head_pgs[idx] = None

    def shutdown(self):
        """Remove the worker placement group and all internal head PGs.

        Idempotent. Safe to call on a partially-constructed instance.
        """
        worker_pgs = getattr(self, "_managed_pgs", [])
        self._managed_pgs = []
        for pg in worker_pgs:
            try:
                remove_placement_group(pg)
            except Exception:
                logger.exception(
                    "Failed to remove TPU worker placement group %s.",
                    getattr(pg, "id", pg),
                )
        self.release_head_pgs()


@PublicAPI(stability="alpha")
@client_mode_wrap
def slice_placement_group(
    topology: str,
    accelerator_version: str,
    resources_per_bundle: Optional[Dict[str, float]] = None,
    num_slices: int = 1,
    chips_per_vm: Optional[int] = None,
    pg_per_slice: bool = False,
    tpu_resource_per_chip: Optional[int] = None,
    **kwargs,
) -> SlicePlacementGroup:
    """Asynchronously creates a PlacementGroup for a TPU slice.

    A slice placement group reserves num_slices TPU slice(s) and creates a placement
    group for scheduling tasks or actors.

    Args:
        topology: The desired TPU pod topology (e.g. "4x4", "2x8").
        accelerator_version: The TPU accelerator generation, (e.g. "v4", "v5p", "v6e").
        resources_per_bundle: Specify the number of resources to reserve per bundle.
            When unspecified, SlicePlacementGroup defaults to reserving 1 bundle per TPU host in
            a topology, with the bundle resources set to the number of TPU in a host.
            Ex: Specifying {"TPU": 1} for a 4x4 topology would result in 16 bundles, each with 1 TPU.
            If resources_per_bundle=None for the same topology, there would be 4 bundles with 4 TPU each.
        num_slices: The number of tpu slices within the placement group.
        chips_per_vm: An optional override for the number of chips per TPU VM.
            Useful for ambiguous topologies like v6e 2x4 which have 1 host, but can be provisioned
            as either 1 VM (8 chips) or 2 VMs (4 chips each).
        pg_per_slice: If False, returns a SlicePlacementGroup that manages a single PlacementGroup.
            If True, returns a SlicePlacementGroup that manages a list of per-slice PlacementGroups.
        tpu_resource_per_chip: The number of logical TPU resources per physical chip. Defaults to 1.
            This scales the total logical resources reserved by each slice.
        **kwargs: Additional arguments for the placement group, such as 'name', 'lifetime', or 'strategy'.

    Returns:
        The handle for the created SlicePlacementGroup.
    """

    return SlicePlacementGroup(
        topology=topology,
        accelerator_version=accelerator_version,
        resources_per_bundle=resources_per_bundle,
        num_slices=num_slices,
        chips_per_vm=chips_per_vm,
        pg_per_slice=pg_per_slice,
        tpu_resource_per_chip=tpu_resource_per_chip,
        **kwargs,
    )


@PublicAPI(stability="alpha")
def run_on_slice(
    fn: Any,
    *args: Any,
    topology: Optional[str] = None,
    accelerator_version: Optional[str] = None,
    tpu_slice: Optional[SlicePlacementGroup] = None,
    slice_index: Optional[int] = None,
    num_slices: int = 1,
    chips_per_vm: Optional[int] = None,
    head_reservation_timeout_s: Optional[
        float
    ] = DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S,
    pg_ready_timeout_s: Optional[float] = None,
    **kwargs: Any,
) -> "List[ray.ObjectRef]":
    """Run a remote function on every host in a TPU slice.

    Dispatches one task per host in the slice, pinning each task to its
    corresponding placement-group bundle via
    :class:`~ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy`.
    The function blocks until the underlying placement group is scheduled,
    then returns a list of object references — one per host — that can be
    passed directly to ``ray.get``.

    Resource options (``num_cpus=0``, ``resources={"TPU": N}``, and
    ``scheduling_strategy``) are applied automatically via ``.options()``
    and override any values set in the ``@ray.remote`` decorator.

    Args:
        fn: A ``@ray.remote``-decorated function to run on every host.
        *args: Positional arguments broadcast to every task invocation.
        topology: The TPU topology string (e.g. ``"4x4"``, ``"2x2x2"``). Required
            when ``tpu_slice`` is ``None``; ignored otherwise.
        accelerator_version: The TPU accelerator generation
            (e.g. ``"v4"``, ``"v6e"``). Required when ``tpu_slice`` is ``None``;
            ignored otherwise.
        tpu_slice: An existing :class:`SlicePlacementGroup` to schedule
            onto. When provided, the slice is used directly and
            ``run_on_slice`` does **not** create, modify, or tear down
            any placement groups. When ``None`` (default), a new slice
            is reserved internally and its head placement groups are
            released once the worker placement group becomes ready.
        slice_index: Optional. If ``tpu_slice`` was created with ``pg_per_slice=True``,
            specify a ``slice_index`` to dispatch tasks only to that specific
            TPU slice. If ``None``, tasks are dispatched to all slices.
        num_slices: Number of TPU slices to reserve. Ignored when
            ``tpu_slice`` is provided. Defaults to ``1``.
        chips_per_vm: Optional override for the number of chips per VM.
            Ignored when ``tpu_slice`` is provided.
        head_reservation_timeout_s: Seconds to wait for each head
            placement group during slice reservation. Ignored when
            ``tpu_slice`` is provided. Defaults to
            ``DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S``.
        pg_ready_timeout_s: Seconds to wait for the worker placement
            group to become ready after reservation. Pass ``None`` to
            wait indefinitely (default).
        **kwargs: Keyword arguments broadcast to every task invocation.

    Returns:
        List[ray.ObjectRef]: One object reference per host in the slice.
            Pass the list to ``ray.get`` to retrieve results.

    Raises:
        TypeError: If ``fn`` is not a ``@ray.remote``-decorated function
            (i.e. it has no ``.options()`` method).
        ValueError: If ``tpu_slice`` is ``None`` and either ``topology`` or
            ``accelerator_version`` is not provided.
        TimeoutError: If the placement group does not become ready within
            ``pg_ready_timeout_s`` seconds. When the slice was created
            internally, it is shut down before the error is raised to
            avoid leaking resources.

    Examples:

    .. testcode:: python
        :skipif: True

        import ray
        from ray.util.tpu import run_on_slice, slice_placement_group

        @ray.remote
        def my_tpu_task():
            import jax
            return jax.device_count()

        # One-shot: reserve a v6e 4x4 slice, run on every host, then
        # release automatically when the driver exits.
        results = ray.get(
            run_on_slice(my_tpu_task, topology="4x4", accelerator_version="v6e")
        )

        # Reuse an existing slice across multiple calls.
        slice_handle = slice_placement_group(topology="4x4", accelerator_version="v6e")
        ray.get(slice_handle.slice_placement_group.ready())

        results1 = ray.get(run_on_slice(my_tpu_task, tpu_slice=slice_handle))
        results2 = ray.get(run_on_slice(my_tpu_task, tpu_slice=slice_handle))
        slice_handle.shutdown()
    """

    if not hasattr(fn, "options"):
        raise TypeError(
            f"fn must be a @ray.remote-decorated function, but got "
            f"{type(fn).__name__!r} which has no .options() method."
        )

    _owns_slice = tpu_slice is None
    slice_handle = tpu_slice

    if slice_index is not None:
        if _owns_slice:
            raise ValueError(
                "slice_index can only be used when an existing tpu_slice is provided."
            )
        if not slice_handle._pg_per_slice:
            raise ValueError(
                "slice_index can only be used when tpu_slice was created with pg_per_slice=True."
            )
        if slice_index < 0 or slice_index >= slice_handle.num_slices:
            raise ValueError(
                f"Invalid slice_index {slice_index}. Must be between 0 and {slice_handle.num_slices - 1}."
            )

    if _owns_slice:
        if topology is None or accelerator_version is None:
            raise ValueError(
                "topology and accelerator_version are required when tpu_slice is not provided."
            )
        slice_handle = SlicePlacementGroup(
            topology=topology,
            accelerator_version=accelerator_version,
            num_slices=num_slices,
            chips_per_vm=chips_per_vm,
            head_reservation_timeout_s=head_reservation_timeout_s,
        )

    pgs = (
        slice_handle.slice_placement_groups
        if slice_handle._pg_per_slice
        else [slice_handle.slice_placement_group]
    )

    if not pgs or any(pg is None for pg in pgs):
        raise ValueError(
            "The provided tpu_slice has already been shut down. "
            "Create a new SlicePlacementGroup or pass tpu_slice=None to reserve one automatically."
        )

    if slice_index is not None:
        pgs = [pgs[slice_index]]

    tpu_per_bundle = slice_handle.bundle_resources.get(
        "TPU", slice_handle.devices_per_host
    )

    ready, _ = ray.wait(
        [pg.ready() for pg in pgs], num_returns=len(pgs), timeout=pg_ready_timeout_s
    )
    if len(ready) != len(pgs):
        if _owns_slice:
            slice_handle.shutdown()
        raise TimeoutError(
            f"TPU slice placement group was not ready within {pg_ready_timeout_s}s. "
            "Ensure your cluster has sufficient TPU resources available."
        )

    # ray.wait returns a ref as ready as soon as it resolves, including when
    # it resolves with an exception (e.g. PG removed or failed to schedule).
    # Call ray.get to surface any such error before proceeding.
    try:
        ray.get(ready)
    except Exception:
        if _owns_slice:
            slice_handle.shutdown()
        raise

    if _owns_slice:
        slice_handle.release_head_pgs()

    results = []
    if slice_handle._pg_per_slice:
        bundles_per_slice = slice_handle.num_bundles // slice_handle.num_slices
        for pg in pgs:
            for i in range(bundles_per_slice):
                results.append(
                    fn.options(
                        num_cpus=0,
                        resources={"TPU": tpu_per_bundle},
                        scheduling_strategy=PlacementGroupSchedulingStrategy(
                            placement_group=pg,
                            placement_group_bundle_index=i,
                        ),
                    ).remote(*args, **kwargs)
                )
    else:
        for i in range(slice_handle.num_bundles):
            results.append(
                fn.options(
                    num_cpus=0,
                    resources={"TPU": tpu_per_bundle},
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=pgs[0],
                        placement_group_bundle_index=i,
                    ),
                ).remote(*args, **kwargs)
            )

    return results


# Deprecated alias — ``dispatch`` was the original name of ``run_on_slice``.
# New code should use ``run_on_slice``.
@PublicAPI(stability="alpha")
@Deprecated(
    message="'dispatch' is deprecated and has been renamed to 'run_on_slice'. "
    "Please use 'run_on_slice' instead.",
    warning=True,
)
def dispatch(*args: Any, **kwargs: Any) -> "List[ray.ObjectRef]":
    """Run a remote function on every host in a TPU slice.

    Deprecated, please use ``run_on_slice`` instead.
    """
    return run_on_slice(*args, **kwargs)


@PublicAPI(stability="alpha")
def init_jax_profiler(port: Optional[int] = None) -> None:
    """Setup JAX Profiler server for in-process JAX profiling.

    This opens a background gRPC profiling port inside the current worker process
    and automatically registers the port to GCS internal_kv so that the Ray Dashboard
    can discover the profiling endpoint.

    Args:
        port: The port where JAX profiler server should listen. If None, it reads the
              port from JAX_PROFILER_PORT environment variable (default: 9999).

    Note:
        JAX profiling is inherently an in-process operation. The JAX profiler server
        must run inside the memory space of the target worker process executing the
        JAX/XLA code in order to capture trace events, Python thread stacks, and XLA
        execution times.
    """
    logger = logging.getLogger(__name__)

    try:
        import jax

        if port is None:
            port = int(os.getenv("JAX_PROFILER_PORT", "9999"))
        try:
            # NOTE: We assume there is at most one JAX worker process per host/node
            # (which is typical for multi-host JAX/TPU VM training). Therefore, we attempt
            # to bind directly to a single port without dynamically scanning a range.
            # If this assumption is relaxed in the future (e.g. multiple JAX workers per node),
            # we should consider switching to dynamic port scanning/allocation.
            jax.profiler.start_server(port)
            logger.info(f"Started JAX profiler server on port {port}")

            # Register the JAX profiler port in GCS internal_kv so dashboard head can auto-discover it.
            try:
                worker = ray._private.worker.global_worker
                if worker and hasattr(worker, "node") and worker.node:
                    node_id_hex = worker.node.node_id
                    pid = os.getpid()
                    key = f"jax_profiler_port:{node_id_hex}:{pid}"
                    ray.experimental.internal_kv._internal_kv_put(
                        key,
                        str(port).encode(),
                        namespace=ray._private.ray_constants.KV_NAMESPACE_DASHBOARD,
                    )
                    logger.info(
                        f"Registered JAX profiler port {port} in GCS internal_kv"
                    )

                    atexit.register(_cleanup_jax_profiler_kv, key)
            except Exception as e:
                logger.warning(
                    f"Failed to register JAX profiler port in internal_kv: {e}"
                )

        except Exception as e:
            logger.error(f"Failed to start JAX profiler server on port {port}: {e}")
    except ImportError:
        logger.warning("JAX is not installed, skipping JAX profiler setup")
    except Exception as e:
        logger.error(f"Failed to start JAX profiler server: {e}")


def _cleanup_jax_profiler_kv(key: str) -> None:
    try:
        ray.experimental.internal_kv._internal_kv_del(
            key,
            namespace=ray._private.ray_constants.KV_NAMESPACE_DASHBOARD,
        )
    except Exception:
        pass


# Internal KV namespace for subslice topology data.
_TPU_SUBSLICE_KV_NAMESPACE = "tpu_subslice"

# Runtime cache: {slice_name: {worker_id_label: {label_key: label_value}}}
# worker_id_label is the string value of the ray.io/tpu-worker-id node label.
_tpu_subslice_cache: Dict[str, Dict[str, Dict[str, str]]] = {}

# Guards all reads and writes of _tpu_subslice_cache. Ray drivers are commonly
# multi-threaded (Serve, Train), so concurrent subslice_placement_group() calls
# can otherwise corrupt the dict. Reentrant so nested access on one thread is
# safe.
_tpu_subslice_cache_lock = threading.RLock()


def _get_subslice_kv_key(slice_name: str) -> bytes:
    """Build internal KV key for subslice topology data."""
    return f"tpu_subslice/{slice_name}".encode()


def _find_valid_parent_topologies(
    subslice_topology: str,
    nodes: List[Dict[str, Any]],
) -> List[str]:
    """Return cluster topologies able to parent *subslice_topology*, smallest-first.

    Consults actual node labels (not a static table) so the result reflects
    what is physically present. A topology is a valid parent when its
    worker-grid dimensions are >= the subslice's in every axis.
    """
    sub_worker_dims = _get_worker_dims_for_topology(subslice_topology)

    cluster_topologies: Set[str] = {
        topo
        for node in nodes
        if node.get("Alive")
        and (topo := node.get("Labels", {}).get(ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY))
        is not None
    }

    candidates: List[Tuple[str, Tuple[int, ...]]] = []
    for topo in cluster_topologies:
        if topo == subslice_topology:
            continue
        try:
            topo_worker_dims = _get_worker_dims_for_topology(topo)
        except ValueError:
            continue  # topology not in the known dims map; skip
        if len(topo_worker_dims) != len(sub_worker_dims):
            continue  # dimensionality mismatch
        if all(pd >= sd for pd, sd in zip(topo_worker_dims, sub_worker_dims)):
            candidates.append((topo, topo_worker_dims))

    candidates.sort(key=lambda x: math.prod(x[1]))
    return [topo for topo, _ in candidates]


def _discover_tpu_node_coords(
    mock_coords: Optional[List[Tuple[str, int, List[int]]]] = None,
) -> Dict[str, Any]:
    """Remote function: discover this TPU worker's physical chip coordinates.

    Uses libtpu.sdk to get the (x, y[, z]) coordinate of every chip on this
    worker. Returns ``{"node_id": str, "coords": [(hostname, chip_index,
    [x, y, ...]), ...]}``. *mock_coords* overrides libtpu for testing.
    """
    node_id = ray.get_runtime_context().get_node_id()

    if mock_coords is not None:
        return {"node_id": node_id, "coords": mock_coords}

    try:
        from libtpu import sdk  # type: ignore[import-untyped]
    except ImportError:
        raise RuntimeError(
            "libtpu is required for TPU subslice discovery. "
            "Install libtpu on all TPU worker nodes."
        )

    coords = sdk.slice.get_chip_coordinates()
    return {
        "node_id": node_id,
        "coords": [
            (c.hostname(), c.chip_index(), list(c.coordinates())) for c in coords
        ],
    }


def _discover_and_persist_subslices(
    parent_topology: str,
    accelerator_version: str,
    chips_per_vm: int,
    head_reservation_timeout_s: Optional[float],
    target_slice_name: Optional[str] = None,
) -> Tuple[str, Dict[str, Dict[str, str]]]:
    """Reserve a full slice, run libtpu discovery, persist subslice labels to
    internal KV, then release the slice.

    The head PG reservation serializes concurrent discovery of the same slice:
    the loser reuses the winner's persisted result. The worker PG is scheduled
    onto the reserved slice by name, so it does not reserve a second head.

    Returns ``(slice_name, {worker_id_label: {label_key: label_value}})``.
    """
    logger.info(
        "Running TPU subslice topology discovery for %s (%s)...",
        parent_topology,
        accelerator_version,
    )

    accelerator_type = "TPU-" + accelerator_version.upper()
    reservation = reserve_tpu_slice(
        parent_topology,
        accelerator_type,
        timeout_s=head_reservation_timeout_s,
        slice_name=target_slice_name,
    )
    if not reservation:
        raise RuntimeError(
            f"Failed to reserve TPU slice '{target_slice_name or parent_topology}' "
            f"of topology '{parent_topology}' with accelerator type "
            f"'{accelerator_type}'. Ensure that sufficient TPU resources are "
            "available in the cluster."
        )
    slice_name, head_pg = reservation

    full_slice = None
    try:
        # A concurrent caller may have discovered this slice while we were
        # blocked on the head; persist precedes head release, so any KV entry is
        # complete. Reuse it and skip the libtpu fan-out.
        try:
            existing = ray.experimental.internal_kv._internal_kv_get(
                _get_subslice_kv_key(slice_name),
                namespace=_TPU_SUBSLICE_KV_NAMESPACE,
            )
            if existing:
                worker_labels = json.loads(existing)
                with _tpu_subslice_cache_lock:
                    _tpu_subslice_cache[slice_name] = worker_labels
                logger.info(
                    "Subslice labels for '%s' found in KV after slice "
                    "reservation; skipping libtpu discovery.",
                    slice_name,
                )
                return slice_name, worker_labels
        except Exception:
            logger.warning(
                "KV pre-check for '%s' failed; proceeding with full discovery.",
                slice_name,
            )

        # Schedule the worker PG onto the reserved slice by name.
        num_bundles, _ = get_tpu_worker_resources(
            topology=parent_topology,
            accelerator_type=accelerator_version,
            chips_per_vm=chips_per_vm,
        )
        full_slice = SlicePlacementGroup(
            topology=parent_topology,
            accelerator_version=accelerator_version,
            chips_per_vm=chips_per_vm,
            head_reservation_timeout_s=head_reservation_timeout_s,
            bundle_label_selector=[
                {ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name}
                for _ in range(num_bundles)
            ],
        )
        try:
            ray.get(
                full_slice.placement_group.ready(),
                timeout=head_reservation_timeout_s,
            )
        except ray.exceptions.GetTimeoutError as e:
            raise TimeoutError(
                f"Timed out after {head_reservation_timeout_s}s waiting for the "
                f"full '{parent_topology}' slice to become ready for subslice "
                f"discovery; it may have become busy after being observed idle."
            ) from e

        # Fan out coordinate discovery to every worker in the slice.
        discover_remote = ray.remote(_discover_tpu_node_coords)
        futures = []
        for i in range(full_slice.num_bundles):
            futures.append(
                discover_remote.options(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=full_slice.placement_group,
                        placement_group_bundle_index=i,
                    )
                ).remote()
            )
        results = ray.get(futures)

        # Compute physical positions → subslice labels.
        # The node's tpu-worker-id label is the key (what the scheduler sees).
        # The physical position from libtpu determines subslice membership.
        nodes = ray.nodes()
        node_id_to_info = {n["NodeID"]: n for n in nodes}

        subslice_labels_by_worker_id: Dict[str, Dict[str, str]] = {}

        for result in results:
            if not result or not result.get("coords"):
                continue

            node_id = result["node_id"]
            node_info = node_id_to_info.get(node_id, {})
            worker_id_label = node_info.get("Labels", {}).get(
                ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY
            )

            if worker_id_label is None:
                logger.warning(
                    "Node %s missing tpu-worker-id label; "
                    "skipping subslice label assignment.",
                    node_id,
                )
                continue

            # Compute physical position from chip coordinates.
            # result["coords"] is [(hostname, chip_index, [x, y, ...]), ...]
            # Extract just the coordinate lists.
            coords_list = [c[2] for c in result["coords"]]
            physical_worker = _get_physical_worker_id_from_coords(
                coords_list, parent_topology
            )

            # Build subslice labels based on physical position.
            labels = _build_subslice_labels(physical_worker, parent_topology)
            subslice_labels_by_worker_id[worker_id_label] = labels

        # Validate that every expected worker was labeled. If any worker
        # lacked a tpu-worker-id label or returned no chip coordinates, the
        # mapping is incomplete. Persisting partial data would later produce
        # placement groups with the wrong number of hosts, so we fail fast here.
        #
        # Use full_slice.num_bundles (= total_chips // chips_per_vm) as the
        # expected count rather than the static _VALID_TOPOLOGY_WORKER_DIMS_2D
        # table. The static table assumes chips_per_vm=4 for all 2D topologies,
        # which is wrong for single-host v6e/v5litepod configurations (8
        # chips/VM, 1 bundle). The fan-out itself runs full_slice.num_bundles
        # tasks, so this value is always the correct expected number of results.
        expected_workers = full_slice.num_bundles
        if len(subslice_labels_by_worker_id) < expected_workers:
            raise RuntimeError(
                f"Subslice discovery for '{slice_name}' is incomplete: "
                f"labeled {len(subslice_labels_by_worker_id)} of "
                f"{expected_workers} expected workers. Workers may be missing "
                f"'tpu-worker-id' labels or failed to return chip coordinates."
            )

        # Persist to internal KV.
        ray.experimental.internal_kv._internal_kv_put(
            _get_subslice_kv_key(slice_name),
            json.dumps(subslice_labels_by_worker_id).encode(),
            namespace=_TPU_SUBSLICE_KV_NAMESPACE,
        )

        # Cache in runtime dict.
        with _tpu_subslice_cache_lock:
            _tpu_subslice_cache[slice_name] = subslice_labels_by_worker_id

        logger.info(
            "Subslice discovery complete for slice '%s' (%s). Found %d workers.",
            slice_name,
            parent_topology,
            len(subslice_labels_by_worker_id),
        )

        return slice_name, subslice_labels_by_worker_id

    finally:
        if full_slice is not None:
            full_slice.shutdown()
        try:
            remove_placement_group(head_pg)
        except Exception:
            logger.exception(
                "Failed to remove discovery head PG for slice '%s'", slice_name
            )


def _wait_for_slice_resources_freed(
    slice_name: str,
    timeout_s: Optional[float],
    poll_interval_s: float = 0.5,
) -> None:
    """Block until every node of *slice_name* reports its full TPU capacity as
    available, or *timeout_s* elapses (``None`` waits indefinitely).

    remove_placement_group() is asynchronous, so the discovery reservation's
    bundles can still read as consumed immediately after shutdown(). Without
    this wait the caller would re-read availability, see the slice as busy, and
    wrongly conclude that no subslice is schedulable even though discovery
    succeeded.
    """
    from ray._private.state import available_resources_per_node

    deadline = None if timeout_s is None else time.monotonic() + timeout_s
    while True:
        avail = available_resources_per_node()
        freed = True
        for node in ray.nodes():
            nl = node.get("Labels", {})
            if nl.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY) != slice_name:
                continue
            total = node.get("Resources", {}).get("TPU", 0)
            if avail.get(node["NodeID"], {}).get("TPU", 0) < total:
                freed = False
                break
        if freed:
            return
        if deadline is not None and time.monotonic() >= deadline:
            logger.warning(
                "Timed out after %ss waiting for the discovery reservation on "
                "slice '%s' to be released; proceeding anyway.",
                timeout_s,
                slice_name,
            )
            return
        time.sleep(poll_interval_s)


def _refresh_cache_from_kv(
    parent_topologies: List[str],
    nodes: List[Dict[str, Any]],
) -> None:
    """Load KV-persisted subslice labels into the runtime cache for any
    not-yet-cached slice of a candidate parent topology.

    Isolates the cache-population side effect so that
    :func:`_collect_known_slice_labels` and
    :func:`_find_undiscovered_idle_slice` stay pure reads. Call once before
    them so both observe KV-persisted slices.
    """
    parent_set = set(parent_topologies)

    # Each slice has one node per worker; deduplicate by slice name to avoid
    # redundant GCS round-trips for the same key.
    seen_slice_names: Set[str] = set()
    for node in nodes:
        node_labels = node.get("Labels", {})
        slice_name = node_labels.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY)
        node_topology = node_labels.get(ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY)
        if (
            not slice_name
            or node_topology not in parent_set
            or slice_name in seen_slice_names
        ):
            continue
        seen_slice_names.add(slice_name)

        with _tpu_subslice_cache_lock:
            if slice_name in _tpu_subslice_cache:
                continue
        try:
            existing = ray.experimental.internal_kv._internal_kv_get(
                _get_subslice_kv_key(slice_name),
                namespace=_TPU_SUBSLICE_KV_NAMESPACE,
            )
            worker_labels = json.loads(existing) if existing else None
        except Exception:
            # KV is a best-effort cache; a lookup or decode failure (e.g. a
            # transient GCS error or corrupt persisted value) just means we
            # fall back to fresh discovery. Log at debug to avoid noise since
            # this runs per undiscovered slice per call.
            logger.debug(
                "KV lookup for subslice labels of '%s' failed; "
                "will fall back to discovery.",
                slice_name,
                exc_info=True,
            )
            continue
        if worker_labels is not None:
            with _tpu_subslice_cache_lock:
                _tpu_subslice_cache[slice_name] = worker_labels
            logger.info("Loaded subslice labels for '%s' from KV store.", slice_name)


def _collect_known_slice_labels(
    parent_topology: str,
    nodes: List[Dict[str, Any]],
) -> List[Tuple[str, Dict[str, Dict[str, str]]]]:
    """Return ``(slice_name, worker_labels)`` for every cached slice whose
    nodes match *parent_topology*.

    A pure read of the runtime cache; call :func:`_refresh_cache_from_kv`
    first so KV-persisted slices are present.
    """
    with _tpu_subslice_cache_lock:
        cache_snapshot = dict(_tpu_subslice_cache)

    results: List[Tuple[str, Dict[str, Dict[str, str]]]] = []
    for slice_name, labels in cache_snapshot.items():
        for node in nodes:
            node_labels = node.get("Labels", {})
            if (
                node_labels.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY) == slice_name
                and node_labels.get(ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY)
                == parent_topology
            ):
                results.append((slice_name, labels))
                break

    return results


def _find_available_subslice(
    slice_name: str,
    subslice_topology: str,
    worker_labels: Dict[str, Dict[str, str]],
    avail: Dict[str, Dict[str, float]],
    slice_worker_to_node: Dict[Tuple[str, str], Any],
    subslice_index: Optional[int] = None,
) -> Tuple[Optional[List[str]], Optional[int]]:
    """Find an idle subslice of *subslice_topology* within *slice_name*.

    An idle subslice has all of its workers' full TPU resources available.
    Returns ``(target_worker_ids, subslice_index)`` or ``(None, None)``.
    *slice_worker_to_node* (``(slice_name, worker_id) -> node``) should be
    built once by the caller and reused across calls.
    """
    label_key = f"{TPU_SUBSLICE_LABEL_PREFIX}{subslice_topology}"

    # Build mapping: subslice_index → list of worker_id labels.
    subslice_indices: Dict[str, List[str]] = {}
    for worker_id, labels in worker_labels.items():
        idx = labels.get(label_key)
        if idx is not None:
            if subslice_index is not None and int(idx) != subslice_index:
                continue
            subslice_indices.setdefault(idx, []).append(worker_id)

    if not subslice_indices:
        return None, None

    expected_host_count = math.prod(_get_worker_dims_for_topology(subslice_topology))

    for idx in sorted(subslice_indices.keys(), key=int):
        worker_ids = subslice_indices[idx]

        # Skip subslices with the wrong number of workers — these indicate
        # corrupted or partial cache data and would produce a PG that never
        # becomes ready.
        if len(worker_ids) != expected_host_count:
            logger.warning(
                "Subslice %s of '%s' in '%s' has %d workers but %d are "
                "expected; skipping.",
                idx,
                subslice_topology,
                slice_name,
                len(worker_ids),
                expected_host_count,
            )
            continue

        all_idle = True

        for wid in worker_ids:
            node = slice_worker_to_node.get((slice_name, wid))

            if node is None or not node.get("Alive"):
                all_idle = False
                break

            total_tpus = node.get("Resources", {}).get("TPU", 0)
            avail_tpus = avail.get(node["NodeID"], {}).get("TPU", total_tpus)

            if avail_tpus < total_tpus:
                all_idle = False
                break

        if all_idle:
            # Sort by integer worker-id so bundle index 0 always maps to the
            # numerically-lowest worker, giving deterministic rank assignment.
            return sorted(worker_ids, key=int), int(idx)

    return None, None


@PublicAPI(stability="alpha")
class SubslicePlacementGroup:
    """A handle to a placement group reservation for a TPU subslice.

    Reserves a contiguous subset of workers within a larger TPU slice.
    The selected subset is guaranteed to be a valid slice and TPU topology;
    i.e. the workers are fully connected with ICI.

    Example for a 4x4 v6e slice (4 workers, 4 TPU chips each):

    .. code-block:: text

        Worker grid:    (0,0) --- (1,0)
                          |         |
                        (0,1) --- (1,1)

        Valid 2x4 subslices:
          Subslice 0: workers (0,0) and (0,1)  (left column)
          Subslice 1: workers (1,0) and (1,1)  (right column)

    Args:
        placement_group: The underlying Ray PlacementGroup.
        parent_topology: Full parent TPU topology (e.g. "4x4").
        subslice_topology: Subslice TPU topology (e.g. "2x4").
        subslice_index: Index of this subslice within the parent.
        slice_name: Name of the physical TPU slice.
        num_hosts: Number of hosts (VM workers) in this subslice.
        chips_per_host: TPU chips available per host.
        bundle_resources: Resources per PG bundle.
        head_placement_groups: Internal head PGs for cleanup.
        bundle_label_selectors: Label selectors used per bundle when
            creating the PG.
    """

    def __init__(
        self,
        placement_group: PlacementGroup,
        parent_topology: str,
        subslice_topology: str,
        subslice_index: int,
        slice_name: str,
        num_hosts: int,
        chips_per_host: int,
        bundle_resources: Dict[str, float],
        head_placement_groups: Optional[List[PlacementGroup]] = None,
        bundle_label_selectors: Optional[List[Dict[str, str]]] = None,
    ):
        self._placement_group = placement_group
        self._parent_topology = parent_topology
        self._subslice_topology = subslice_topology
        self._subslice_index = subslice_index
        self._slice_name = slice_name
        self._num_hosts = num_hosts
        self._chips_per_host = chips_per_host
        self._bundle_resources = bundle_resources
        self._head_placement_groups: List[PlacementGroup] = head_placement_groups or []
        self._bundle_label_selectors: List[Dict[str, str]] = (
            bundle_label_selectors or []
        )

    @property
    def placement_group(self) -> PlacementGroup:
        """The underlying PlacementGroup object."""
        return self._placement_group

    @property
    def parent_topology(self) -> str:
        """The full parent TPU topology."""
        return self._parent_topology

    @property
    def subslice_topology(self) -> str:
        """The requested subslice TPU topology."""
        return self._subslice_topology

    @property
    def subslice_index(self) -> int:
        """The subslice index within the parent."""
        return self._subslice_index

    @property
    def slice_name(self) -> str:
        """The name of the physical TPU slice."""
        return self._slice_name

    @property
    def num_hosts(self) -> int:
        """Number of hosts (VM workers) in this subslice."""
        return self._num_hosts

    @property
    def chips_per_host(self) -> int:
        """TPU chips available per host."""
        return self._chips_per_host

    @property
    def bundle_resources(self) -> Dict[str, float]:
        """Resources assigned to each bundle."""
        return self._bundle_resources

    @property
    def head_placement_groups(self) -> List[PlacementGroup]:
        """Internal head PGs used for slice reservation."""
        return self._head_placement_groups

    @property
    def bundle_label_selector(self) -> List[Dict[str, str]]:
        """Label selectors used for each bundle when creating the PG."""
        return self._bundle_label_selectors

    @DeveloperAPI(stability="alpha")
    def release_head_pgs(self) -> None:
        """Remove all internal head placement groups. Idempotent."""
        head_pgs = self._head_placement_groups
        self._head_placement_groups = []
        for pg in head_pgs:
            try:
                remove_placement_group(pg)
            except Exception:
                logger.exception(
                    "Failed to remove TPU head PG %s",
                    getattr(pg, "id", pg),
                )

    def shutdown(self):
        """Remove the worker placement group and all head PGs. Idempotent."""
        if self._placement_group is not None:
            try:
                remove_placement_group(self._placement_group)
            except Exception:
                logger.exception(
                    "Failed to remove subslice placement group %s",
                    getattr(self._placement_group, "id", self._placement_group),
                )
            self._placement_group = None
        self.release_head_pgs()


def _build_slice_worker_to_node(
    nodes: List[Dict[str, Any]],
) -> Dict[Tuple[str, str], Any]:
    """Build a ``(slice_name, worker_id) → node`` lookup from live node dicts."""
    return {
        (
            node_labels.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY),
            node_labels.get(ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY),
        ): node
        for node in nodes
        for node_labels in [node.get("Labels", {})]
        if node_labels.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY)
        and node_labels.get(ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY)
    }


def _slice_head_available(
    slice_nodes: List[Dict[str, Any]],
    avail: Dict[str, Dict[str, float]],
    head_resource: Optional[str],
) -> bool:
    """Return whether the slice's head resource on worker 0 is free.

    Chip idleness alone does not guarantee a slice is reservable: another
    reservation may hold the ``TPU-<pod_type>-head`` resource on worker 0
    while the chips read as free (e.g. between a head reservation and its
    worker-bundle placement, or a leaked head PG). Reserving such a slice
    would then block on the head and time out.

    Conservative: only returns ``False`` when the head resource is explicitly
    reported as unavailable, so an unknown/unreported head never causes a
    genuinely idle slice to be skipped.
    """
    if head_resource is None:
        return True
    for node in slice_nodes:
        if node.get("Labels", {}).get(ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY) != "0":
            continue
        node_avail = avail.get(node["NodeID"], {})
        if head_resource in node_avail:
            return node_avail[head_resource] >= 1
        return True  # head resource not reported; cannot assess, don't reject
    return True  # no worker-0 node found; don't reject


def _find_undiscovered_idle_slice(
    parent_topologies: List[str],
    nodes: List[Dict[str, Any]],
    avail: Dict[str, Dict[str, float]],
    version: str,
) -> Optional[Tuple[str, str]]:
    """Return ``(parent_topology, slice_name)`` for the first undiscovered
    (absent from cache), fully idle slice, scanning *parent_topologies*
    smallest-first; else ``None``.

    A slice is idle only when all its chips are free *and* its head resource
    on worker 0 is free, so the caller can pin discovery to a slice it can
    actually reserve rather than letting an untargeted reservation grab any
    slice's worker 0.

    Must run after :func:`_refresh_cache_from_kv` so the cache already
    reflects KV-persisted labels; otherwise an already-discovered slice may
    be re-discovered.
    """
    parent_set = set(parent_topologies)
    with _tpu_subslice_cache_lock:
        discovered = set(_tpu_subslice_cache)

    # Head resource name (TPU-<pod_type>-head) per parent topology, used to
    # confirm worker 0's head is free before targeting the slice.
    accelerator_type = "TPU-" + version.upper()
    head_resource_by_topo: Dict[str, Optional[str]] = {}
    for topo in parent_set:
        pod_type = infer_tpu_pod_type_from_topology(topo, accelerator_type)
        head_resource_by_topo[topo] = f"TPU-{pod_type}-head" if pod_type else None

    # Group alive nodes by (topology, slice_name).
    topo_slice_nodes: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for node in nodes:
        if not node.get("Alive"):
            continue
        nl = node.get("Labels", {})
        topo = nl.get(ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY)
        sname = nl.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY)
        if topo in parent_set and sname:
            topo_slice_nodes.setdefault((topo, sname), []).append(node)

    for parent_topology in parent_topologies:
        for (topo, sname), sns in topo_slice_nodes.items():
            if topo != parent_topology or sname in discovered:
                continue
            idle = True
            for node in sns:
                total = node.get("Resources", {}).get("TPU", 0)
                if avail.get(node["NodeID"], {}).get("TPU", total) < total:
                    idle = False
                    break
            if idle and _slice_head_available(
                sns, avail, head_resource_by_topo[parent_topology]
            ):
                return parent_topology, sname

    return None


def _find_available_cached_subslice(
    parent_topologies: List[str],
    subslice_topology: str,
    nodes: List[Dict[str, Any]],
    avail: Dict[str, Dict[str, float]],
    slice_worker_to_node: Dict[Tuple[str, str], Any],
    subslice_index: Optional[int] = None,
) -> Optional[Tuple[List[str], int, str, str, Dict[str, Dict[str, str]]]]:
    """Return the first idle subslice across all cached slices of any valid
    parent topology, or ``None``.

    A pure read of the runtime cache (call :func:`_refresh_cache_from_kv`
    first). On success returns ``(worker_ids, subslice_index, slice_name,
    parent_topology, worker_labels)``.
    """
    for parent_topology in parent_topologies:
        for slice_name, worker_labels in _collect_known_slice_labels(
            parent_topology, nodes
        ):
            worker_ids, idx = _find_available_subslice(
                slice_name,
                subslice_topology,
                worker_labels,
                avail,
                slice_worker_to_node,
                subslice_index,
            )
            if worker_ids is not None:
                return worker_ids, idx, slice_name, parent_topology, worker_labels
    return None


def _build_subslice_pg(
    worker_ids: List[str],
    subslice_index: int,
    slice_name: str,
    subslice_topology: str,
    parent_topology: str,
    chips_per_vm: int,
    resources_per_bundle: Optional[Dict[str, float]],
    strategy: str,
    name: str,
    lifetime: Optional[str],
) -> SubslicePlacementGroup:
    """Create a Ray placement group for the selected subslice workers and
    return a :class:`SubslicePlacementGroup` handle.

    *resources_per_bundle* defaults to ``{"CPU": 1, "TPU": chips_per_vm}``.
    """
    if resources_per_bundle is None:
        resources_per_bundle = {"CPU": 1, "TPU": chips_per_vm}

    bundle_label_selectors = [
        {
            ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name,
            ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY: wid,
        }
        for wid in worker_ids
    ]

    pg = placement_group(
        bundles=[resources_per_bundle.copy() for _ in worker_ids],
        strategy=strategy,
        name=name,
        lifetime=lifetime,
        bundle_label_selector=bundle_label_selectors,
    )

    return SubslicePlacementGroup(
        placement_group=pg,
        parent_topology=parent_topology,
        subslice_topology=subslice_topology,
        subslice_index=subslice_index,
        slice_name=slice_name,
        num_hosts=len(worker_ids),
        chips_per_host=chips_per_vm,
        bundle_resources=resources_per_bundle,
        bundle_label_selectors=bundle_label_selectors,
    )


def _resolve_chips_per_vm(
    user_chips_per_vm: Optional[int],
    parent_topology: str,
    version: str,
) -> int:
    """Resolve the effective chips-per-VM for a specific parent topology.

    ``chips_per_vm`` is a property of the parent slice's node type, which
    varies across topologies in a mixed cluster (e.g. v6e single-host 2x4 is
    8 chips/VM while multi-host 4x4 is 4 chips/VM). It must therefore be
    derived from the parent actually being discovered or scheduled, not from
    an arbitrary member of the candidate list. A caller override always wins.
    """
    if user_chips_per_vm is not None:
        return user_chips_per_vm
    return _get_default_chips_per_vm(parent_topology, version)


def _validate_and_resolve(
    subslice_topology: str,
    accelerator_version: str,
    chips_per_vm: Optional[int],
) -> Tuple[str, str, List[str], Optional[int]]:
    """Validate inputs and resolve cluster-dependent parameters, returning
    ``(version, subslice_topology, parent_topologies, chips_per_vm)``.

    Normalises and validates the topology strings and resolves all valid
    parent topologies from live cluster nodes. ``chips_per_vm`` is passed
    through unchanged (validated if given) rather than defaulted here,
    because its correct value depends on the specific parent topology later
    chosen for discovery or scheduling; see :func:`_resolve_chips_per_vm`.

    Raises ``ValueError`` on any validation failure or if no suitable parent
    topology is found in the cluster.
    """
    version = get_tpu_version_from_type(accelerator_version)
    subslice_topology = subslice_topology.strip().lower()

    # Validate the subslice topology string before touching the cluster.
    # Both checks raise ValueError; normalise to a single message format.
    try:
        _parse_topology_dims(subslice_topology)
    except ValueError:
        raise ValueError(
            f"Subslice topology '{subslice_topology}' is not valid for "
            f"accelerator version '{version}'."
        )
    if not TPUAcceleratorManager.is_valid_tpu_accelerator_topology(
        version, subslice_topology
    ):
        raise ValueError(
            f"Subslice topology '{subslice_topology}' is not valid for "
            f"accelerator version '{version}'."
        )

    if chips_per_vm is not None and chips_per_vm <= 0:
        raise ValueError("chips_per_vm must be positive.")

    # Resolve the parent topology from live cluster nodes.
    nodes = ray.nodes()
    parent_topologies = _find_valid_parent_topologies(subslice_topology, nodes)
    if not parent_topologies:
        cluster_topos = sorted(
            {
                topo
                for node in nodes
                if node.get("Alive")
                for topo in [
                    node.get("Labels", {}).get(ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY)
                ]
                if topo is not None
            }
        )
        msg = (
            f"No topology in the cluster can serve as a parent for subslice "
            f"'{subslice_topology}'. Alive TPU topologies found: "
            f"{cluster_topos or ['(none)']}"
        )
        # If the subslice topology itself is present but has no larger parent,
        # direct the user to the correct API.
        if subslice_topology in cluster_topos:
            msg += "  Use slice_placement_group() instead."
        raise ValueError(msg)

    return version, subslice_topology, parent_topologies, chips_per_vm


@PublicAPI(stability="alpha")
@client_mode_wrap
def subslice_placement_group(
    subslice_topology: str,
    accelerator_version: str,
    chips_per_vm: Optional[int] = None,
    resources_per_bundle: Optional[Dict[str, float]] = None,
    strategy: str = "STRICT_SPREAD",
    name: str = "",
    lifetime: Optional[str] = None,
    head_reservation_timeout_s: Optional[
        float
    ] = DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S,
    subslice_index: Optional[int] = None,
) -> SubslicePlacementGroup:
    """Asynchronously creates a PlacementGroup for a TPU subslice.

    A subslice placement group reserves a contiguous subset of workers within
    a larger TPU slice, enabling multiple workloads to share a physical slice
    while maintaining ICI topology alignment.

    On the first call for a given topology this function temporarily reserves
    a full parent slice to discover the physical chip layout, computes
    subslice labels, and releases unused workers. Subsequent calls reuse the
    cached data.

    Args:
        subslice_topology: Desired subslice topology (e.g. ``"2x4"``).
        accelerator_version: TPU accelerator generation (e.g. ``"v6e"``).
        chips_per_vm: Optional override for chips per VM. Useful for
            ambiguous topologies like v6e 2x4 which can be 1 VM (8 chips)
            or 2 VMs (4 chips each).
        resources_per_bundle: Per-bundle resources. Defaults to
            ``{"CPU": 1, "TPU": chips_per_vm}``.
        strategy: Placement group strategy (default ``"STRICT_SPREAD"``).
        name: Optional placement group name.
        lifetime: Placement group lifetime (``None`` or ``"detached"``).
        head_reservation_timeout_s: Maximum seconds to wait for TPU head
            placement groups. Defaults to
            ``DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S``.
        subslice_index: Optional index of the subslice to select. If specified,
            only the subslice at this index within the physical slice will be
            considered. If that subslice is busy, the request will fail even if
            other subslices are idle.

    Returns:
        A :class:`SubslicePlacementGroup` handle.

    Raises:
        ValueError: If the subslice topology is invalid for the accelerator,
            or if no suitable parent topology is found in the cluster.
        RuntimeError: If all slices are occupied, or if libtpu is missing.

    Examples:

    .. testcode:: python
        :skipif: True

        import ray
        from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
        from ray.util.tpu import subslice_placement_group

        sg = subslice_placement_group(
            subslice_topology="2x4",
            accelerator_version="v6e",
        )

        @ray.remote(num_cpus=0, resources={"TPU": 4})
        def train(world, rank):
            ...

        tasks = [
            train.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=sg.placement_group,
                )
            ).remote(world=sg.num_hosts, rank=i)
            for i in range(sg.num_hosts)
        ]
    """
    (
        version,
        subslice_topology,
        parent_topologies,
        user_chips_per_vm,
    ) = _validate_and_resolve(subslice_topology, accelerator_version, chips_per_vm)

    from ray._private.state import available_resources_per_node

    while True:
        nodes = ray.nodes()
        avail = available_resources_per_node()
        slice_worker_to_node = _build_slice_worker_to_node(nodes)

        # Populate the runtime cache from KV first so both the cached-subslice
        # search and the undiscovered-parent check observe persisted slices.
        _refresh_cache_from_kv(parent_topologies, nodes)
        cached_subslice = _find_available_cached_subslice(
            parent_topologies,
            subslice_topology,
            nodes,
            avail,
            slice_worker_to_node,
            subslice_index,
        )
        discoverable = _find_undiscovered_idle_slice(
            parent_topologies, nodes, avail, version
        )

        if cached_subslice is None and discoverable is None:
            raise RuntimeError(
                f"No subslice of '{subslice_topology}' is schedulable across "
                f"any of the candidate parent topologies: {parent_topologies}."
            )

        if cached_subslice is not None:
            worker_ids, subslice_index, slice_name, parent_topology, _ = cached_subslice
            # chips_per_vm depends on the parent's node type, so resolve it
            # against the parent this subslice actually belongs to.
            return _build_subslice_pg(
                worker_ids,
                subslice_index,
                slice_name,
                subslice_topology,
                parent_topology,
                _resolve_chips_per_vm(user_chips_per_vm, parent_topology, version),
                resources_per_bundle,
                strategy,
                name,
                lifetime,
            )

        # No idle cached subslice found — discover the layout of the specific
        # idle slice we found (pinned by name so the head reservation lands on
        # that fully-idle slice) and loop back to claim a subslice from the
        # newly populated cache. chips_per_vm must match the parent discovered.
        assert discoverable is not None  # guaranteed by the check above
        discoverable_parent, discoverable_slice_name = discoverable
        discovered_slice_name, _ = _discover_and_persist_subslices(
            discoverable_parent,
            version,
            _resolve_chips_per_vm(user_chips_per_vm, discoverable_parent, version),
            head_reservation_timeout_s,
            target_slice_name=discoverable_slice_name,
        )
        # remove_placement_group() is async; block until the discovery
        # reservation's TPU is released so the next iteration sees the slice as
        # idle and can claim a subslice instead of wrongly raising.
        _wait_for_slice_resources_freed(
            discovered_slice_name, head_reservation_timeout_s
        )
