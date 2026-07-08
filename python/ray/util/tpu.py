import atexit
import json
import logging
import math
import os
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray._private.accelerators import TPUAcceleratorManager
from ray._private.accelerators.tpu import (
    DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S,
    TPU_SUBSLICE_LABEL_PREFIX,
    VALID_TPU_TYPES,
    _build_subslice_labels,
    _get_default_chips_per_vm,
    _get_physical_worker_id_from_coords,
    _parse_topology_dims,
    _resolve_parent_topology,
    get_chips_per_host,
    get_num_chips_from_topology,
    infer_tpu_pod_type_from_topology,
    reserve_tpu_slice,
)
from ray._private.client_mode_hook import client_mode_wrap
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.placement_group import (
    PlacementGroup,
    placement_group,
    remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logging.getLogger(__name__)


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
) -> int:
    """
    Calculates the number of slices needed to accommodate the specified number of workers.

    Args:
        topology: The TPU topology string.
        accelerator_type: The accelerator type string.
        num_workers: The desired number of workers.
        resources_per_worker: Optional dict of resources per worker.

    Returns:
        The number of slices required. Returns 1 if inputs are invalid or incomplete.
    """
    if not topology or not accelerator_type:
        return 1

    try:
        # Calculate how many workers fit in a single slice (num_slices=1)
        # given the topology and resources per worker.
        workers_per_slice, _ = get_tpu_worker_resources(
            topology=topology,
            accelerator_type=accelerator_type,
            resources_per_unit=resources_per_worker,
            num_slices=1,
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
    resources_per_unit: Optional[Dict[str, float]] = None,
    num_slices: int = 1,
    chips_per_vm: Optional[int] = None,
) -> Tuple[int, Dict[str, float]]:
    """
    Calculates the number of workers and the resources required for each worker
    to run based on a TPU topology.

    Args:
        topology: The TPU topology string.
        accelerator_type: The accelerator string.
        resources_per_unit: Optional manual override for resources per unit. If
            unspecified, the number of TPU chips in a host is assumed.
        num_slices: The number of TPU slices.
        chips_per_vm: An optional override for the number of chips per VM.
            If unspecified, this is inferred automatically from the topology
            and accelerator type.

    Returns:
        A tuple containing:
        - num_workers: Total workers required.
        - unit_resources: The resource dictionary for a single worker.
    """
    accelerator_version = get_tpu_version_from_type(accelerator_type)

    resolved_chips_per_vm = (
        chips_per_vm
        if chips_per_vm is not None
        else get_chips_per_host(topology, accelerator_version)
    )
    if resolved_chips_per_vm <= 0:
        raise ValueError("chips_per_vm must be positive.")

    total_chips_per_slice = get_num_chips_from_topology(topology)
    total_chips_available = total_chips_per_slice * num_slices

    # Calculate the per-unit resources based on the TPU topology.
    final_resources = resources_per_unit.copy() if resources_per_unit else {}

    if "CPU" not in final_resources:
        final_resources["CPU"] = 1

    # If user didn't specify TPU, default to # of chips on 1 host.
    if "TPU" not in final_resources:
        final_resources["TPU"] = resolved_chips_per_vm

    tpus_per_unit = final_resources["TPU"]

    # Validate TPU resource values.
    if tpus_per_unit <= 0:
        raise ValueError("TPU resources must be positive.")

    if total_chips_available % tpus_per_unit != 0:
        raise ValueError(
            f"Total chips ({total_chips_available}) not divisible by "
            f"TPUs requested per unit ({tpus_per_unit})."
        )

    if total_chips_per_slice % tpus_per_unit != 0:
        raise ValueError(
            f"The requested resources per bundle ({tpus_per_unit} TPU chips) do not "
            f"divide evenly into the chips available per slice ({total_chips_per_slice}). "
            "This configuration results in an uneven distribution of workers across slices, "
            "which is not supported."
        )

    num_workers = int(total_chips_available // tpus_per_unit)

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
        The TPU slice name if the node belongs to a slice, otherwise None.
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


@PublicAPI(stability="alpha")
def get_num_ready_tpu_slices(
    topology: str,
    accelerator_type: str,
) -> int:
    """
    Checks the cluster state to determine how many full TPU slices of the
    specified topology are currently intact and available.

    Args:
        topology: The TPU topology string (e.g. "2x4").
        accelerator_type: The accelerator type string (e.g. "TPU-V6E").

    Returns:
        The integer count of fully ready and available TPU slices.
    """
    if not ray.is_initialized():
        return 0

    try:
        pod_type = infer_tpu_pod_type_from_topology(topology, accelerator_type)
        if not pod_type:
            return 0

        total_chips_expected = get_num_chips_from_topology(topology)
        if total_chips_expected <= 0:
            return 0

    except Exception as e:
        logger.warning(f"Failed to parse TPU topology for readiness check: {e}")
        return 0

    # Fetch live resource usage via the State API to ensure slices are idle.
    from ray._private.state import available_resources_per_node

    node_avail_resources = available_resources_per_node()

    slice_to_nodes = {}
    for node in ray.nodes():
        # Build a mapping of currently alive Ray nodes and the TPU slice they belong to.
        if node.get("Alive"):
            labels = node.get("Labels", {})
            if labels.get(ray._raylet.RAY_NODE_TPU_POD_TYPE_KEY) == pod_type:
                slice_name = get_tpu_slice_name_from_node(node)
                if slice_name:
                    slice_to_nodes.setdefault(slice_name, []).append(node)

    ready_and_available_slices = 0
    for slice_name, nodes in slice_to_nodes.items():
        slice_tpu_chips = sum(node.get("Resources", {}).get("TPU", 0) for node in nodes)

        # Validate the slice has all its physical chips.
        if slice_tpu_chips != total_chips_expected:
            continue

        # TPU slices must have a head worker (rank 0).
        has_head = any(
            n.get("Labels", {}).get(ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY) == "0"
            for n in nodes
        )
        if not has_head:
            continue

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

    Returns:
        The integer count of physically intact TPU slices.
    """
    if not ray.is_initialized():
        return 0

    try:
        pod_type = infer_tpu_pod_type_from_topology(topology, accelerator_type)
        total_chips_expected = get_num_chips_from_topology(topology)
    except Exception as e:
        logger.warning(f"Failed to parse TPU topology for integrity check: {e}")
        return 0

    if not pod_type or total_chips_expected <= 0:
        return 0

    slice_to_nodes = {}
    for node in ray.nodes():
        if node.get("Alive"):
            labels = node.get("Labels", {})
            if labels.get(ray._raylet.RAY_NODE_TPU_POD_TYPE_KEY) == pod_type:
                slice_name = get_tpu_slice_name_from_node(node)
                if slice_name:
                    slice_to_nodes.setdefault(slice_name, []).append(node)

    intact_slices = 0
    for slice_name, nodes in slice_to_nodes.items():
        slice_tpu_chips = sum(node.get("Resources", {}).get("TPU", 0) for node in nodes)
        has_head = any(
            n.get("Labels", {}).get(ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY) == "0"
            for n in nodes
        )
        if slice_tpu_chips == total_chips_expected and has_head:
            intact_slices += 1

    return intact_slices


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

    Examples:

    .. testcode:: python
        :skipif: True

        import ray
        from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
        from ray.util.tpu import SlicePlacementGroup

        slice_handle = SlicePlacementGroup(topology="4x4", accelerator_version="v6e")
        slice_pg = slice_handle.placement_group
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
    ):
        self._head_pgs: List[PlacementGroup] = []
        self._bundle_label_selector: List[Dict[str, str]] = []
        self._placement_group: Optional[PlacementGroup] = None
        self._user_bundle_label_selector = bundle_label_selector or []

        self._topology = topology.strip().lower()
        self._accelerator_version = get_tpu_version_from_type(
            accelerator_version.strip()
        )
        self._resources_per_bundle = resources_per_bundle or {}
        self._num_slices = num_slices
        self._head_reservation_timeout_s = head_reservation_timeout_s

        # Calculate number of bundles and bundle resources for specified TPU topology.
        self._num_bundles, self._bundle_resources = get_tpu_worker_resources(
            topology=self._topology,
            accelerator_type=self._accelerator_version,
            resources_per_unit=resources_per_bundle,
            num_slices=self._num_slices,
            chips_per_vm=chips_per_vm,
        )

        self._chips_per_host = (
            chips_per_vm
            if chips_per_vm is not None
            else get_chips_per_host(self._topology, self._accelerator_version)
        )
        if self._chips_per_host <= 0:
            raise ValueError("chips_per_vm must be positive.")

        # Within Ray, a "host" corresponds to a user-visible compute VM.
        # This may differ from the physical hardware host definitions in GCP/GKE docs.
        total_chips = get_num_chips_from_topology(self._topology)
        hosts_per_slice = max(1, total_chips // self._chips_per_host)
        self._num_hosts = hosts_per_slice * self._num_slices

        self._validate_tpu_config()

        # Reserve a TPU slice of the provided accelerator version and topology.
        self._placement_group = self._reserve_slice(
            strategy,
            name,
            lifetime,
        )

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
    ) -> PlacementGroup:
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
        bundles = []
        bundles_per_slice = self._num_bundles // self._num_slices

        # Construct accelerator format for reserve_tpu_slice. e.g. From "v6e" to "TPU-V6E", "v5p" to "TPU-V5P".
        accelerator_type = "TPU-" + self.accelerator_version.upper()

        try:
            for slice_idx in range(self.num_slices):
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

                bundles += [
                    self._bundle_resources.copy() for _ in range(bundles_per_slice)
                ]

            pg = placement_group(
                bundles=bundles,
                strategy=strategy,
                name=name,
                lifetime=lifetime,
                bundle_label_selector=self._bundle_label_selector,
            )

            return pg
        except Exception:
            self.shutdown()
            raise

    @property
    def placement_group(self) -> PlacementGroup:
        """The underlying PlacementGroup object."""
        return self._placement_group

    @property
    def chips_per_host(self) -> int:
        """The number of chips per host for this TPU slice."""
        # This is the same value as resources per worker for TPU.
        return self._chips_per_host

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
        return self._head_pgs

    @property
    def bundle_label_selector(self) -> List[Dict[str, str]]:
        """The bundle label selector list for the worker PG."""
        return self._bundle_label_selector

    @property
    def bundle_resources(self) -> Dict[str, float]:
        """The resources that are assigned to each bundle."""
        return self._bundle_resources

    @DeveloperAPI(stability="alpha")
    def release_head_pgs(self) -> None:
        """Remove all internal head placement groups.

        The head PGs exist only to atomically claim a TPU slice's label during
        the race window between slice selection and worker-PG construction.
        Once the worker PG's bundles are scheduled, the worker PG holds the TPU
        resources on every host in the slice and the head PGs are redundant.

        Callers should invoke this idempotent call after `self.placement_group.ready()`
        resolves successfully.
        """
        head_pgs = getattr(self, "_head_pgs", [])
        self._head_pgs = []
        for head_pg in head_pgs:
            try:
                remove_placement_group(head_pg)
            except Exception:
                logger.exception(
                    "Failed to remove TPU head placement group %s; the "
                    "slice reservation marker may leak until the creator "
                    "process exits.",
                    getattr(head_pg, "id", head_pg),
                )

    def shutdown(self):
        """Remove the worker placement group and all internal head PGs.

        Idempotent. Safe to call on a partially-constructed instance.
        """
        worker_pg = getattr(self, "_placement_group", None)
        if worker_pg is not None:
            self._placement_group = None
            try:
                remove_placement_group(worker_pg)
            except Exception:
                logger.exception(
                    "Failed to remove TPU worker placement group %s.",
                    getattr(worker_pg, "id", worker_pg),
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
        **kwargs,
    )


@PublicAPI(stability="alpha")
def dispatch(
    fn: Any,
    *args: Any,
    topology: Optional[str] = None,
    accelerator_version: Optional[str] = None,
    tpu_slice: Optional[SlicePlacementGroup] = None,
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
            ``dispatch`` does **not** create, modify, or tear down
            any placement groups. When ``None`` (default), a new slice
            is reserved internally and its head placement groups are
            released once the worker placement group becomes ready.
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
        from ray.util.tpu import dispatch, slice_placement_group

        @ray.remote
        def my_tpu_task():
            import jax
            return jax.device_count()

        # One-shot: reserve a v6e 4x4 slice, run on every host, then
        # release automatically when the driver exits.
        results = ray.get(
            dispatch(my_tpu_task, topology="4x4", accelerator_version="v6e")
        )

        # Reuse an existing slice across multiple calls.
        slice_handle = slice_placement_group(topology="4x4", accelerator_version="v6e")
        ray.get(slice_handle.placement_group.ready())

        results1 = ray.get(dispatch(my_tpu_task, tpu_slice=slice_handle))
        results2 = ray.get(dispatch(my_tpu_task, tpu_slice=slice_handle))
        slice_handle.shutdown()
    """

    if not hasattr(fn, "options"):
        raise TypeError(
            f"fn must be a @ray.remote-decorated function, but got "
            f"{type(fn).__name__!r} which has no .options() method."
        )

    _owns_slice = tpu_slice is None
    slice_handle = tpu_slice
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

    pg = slice_handle.placement_group
    if pg is None:
        raise ValueError(
            "The provided tpu_slice has already been shut down. "
            "Create a new SlicePlacementGroup or pass tpu_slice=None to reserve one automatically."
        )

    tpu_per_bundle = slice_handle.bundle_resources.get(
        "TPU", slice_handle.chips_per_host
    )

    ready, _ = ray.wait([pg.ready()], timeout=pg_ready_timeout_s)
    if not ready:
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
        ray.get(ready[0])
    except Exception:
        if _owns_slice:
            slice_handle.shutdown()
        raise

    if _owns_slice:
        slice_handle.release_head_pgs()

    return [
        fn.options(
            num_cpus=0,
            resources={"TPU": tpu_per_bundle},
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg,
                placement_group_bundle_index=i,
            ),
        ).remote(*args, **kwargs)
        for i in range(slice_handle.num_bundles)
    ]


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


def _get_subslice_kv_key(slice_name: str) -> bytes:
    """Build internal KV key for subslice topology data."""
    return f"tpu_subslice/{slice_name}".encode()


def _discover_tpu_node_coords(
    mock_coords: Optional[List] = None,
) -> Dict[str, Any]:
    """Remote function that runs on a single TPU worker to discover chip coordinates.

    Uses libtpu.sdk to get the physical (x, y) or (x, y, z) coordinates of every
    chip on this worker.

    Args:
        mock_coords: For testing only. Overrides real libtpu discovery.

    Returns:
        {"node_id": str, "coords": [(hostname, chip_index, [x, y, ...]), ...]}

    Raises:
        RuntimeError: If libtpu is not importable.
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
) -> Tuple[str, Dict[str, Dict[str, str]]]:
    """Temporarily reserve a full slice, run libtpu discovery, compute subslice
    labels, persist to internal KV, and release the full slice.

    Args:
        parent_topology: Full parent topology (e.g. "4x4").
        accelerator_version: Accelerator version (e.g. "v6e").
        chips_per_vm: Chips per VM.
        head_reservation_timeout_s: Timeout for head PG reservation.

    Returns:
        (slice_name, {worker_id_label: {label_key: label_value}})
    """
    logger.info(
        "Running TPU subslice topology discovery for %s (%s)...",
        parent_topology,
        accelerator_version,
    )

    # Reserve a full slice.
    full_slice = SlicePlacementGroup(
        topology=parent_topology,
        accelerator_version=accelerator_version,
        chips_per_vm=chips_per_vm,
        head_reservation_timeout_s=head_reservation_timeout_s,
    )
    try:
        ray.get(full_slice.placement_group.ready())

        # Extract slice name from the bundle label selectors.
        slice_name: Optional[str] = None
        for selector in full_slice.bundle_label_selector:
            sn = selector.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY)
            if sn:
                slice_name = sn
                break
        if not slice_name:
            raise RuntimeError(
                "Failed to identify TPU slice name during subslice discovery."
            )

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
                coords_list, parent_topology, chips_per_vm
            )

            # Build subslice labels based on physical position.
            labels = _build_subslice_labels(physical_worker, parent_topology)
            subslice_labels_by_worker_id[worker_id_label] = labels

        # Persist to internal KV.
        ray.experimental.internal_kv._internal_kv_put(
            _get_subslice_kv_key(slice_name),
            json.dumps(subslice_labels_by_worker_id).encode(),
            namespace=_TPU_SUBSLICE_KV_NAMESPACE,
        )

        # Cache in runtime dict.
        _tpu_subslice_cache[slice_name] = subslice_labels_by_worker_id

        logger.info(
            "Subslice discovery complete for slice '%s' (%s). " "Found %d workers.",
            slice_name,
            parent_topology,
            len(subslice_labels_by_worker_id),
        )

        return slice_name, subslice_labels_by_worker_id

    finally:
        full_slice.shutdown()


def _get_or_create_subslice_labels(
    parent_topology: str,
    accelerator_version: str,
    chips_per_vm: int,
    head_reservation_timeout_s: Optional[float],
) -> Tuple[str, Dict[str, Dict[str, str]]]:
    """Ensure subslice labels exist for a slice of the given parent topology.

    Physical position of the TPU chips is not known at raylet start time and
    must always be derived from ``libtpu.sdk.slice.get_chip_coordinates()`` as
    a source of truth, and then may be cached for reuse.

    Lookup order:
    1. Runtime cache (fastest — populated by a previous discovery).
    2. Internal KV (persisted across jobs in the same Ray session).
    3. Full-slice reservation + coordinated libtpu discovery, then release.

    Concurrent callers are serialized naturally: step 3 creates a
    ``SlicePlacementGroup`` which acquires the exclusive
    ``TPU-{pod_type}-head`` resource for the target slice. Only one caller
    can hold that resource at a time, so only one discovery run can proceed
    for a given slice simultaneously. When the first caller finishes and
    persists to KV, any blocked caller will find the data in step 2.

    Args:
        parent_topology: Full parent topology (e.g. "4x4").
        accelerator_version: Accelerator version (e.g. "v6e").
        chips_per_vm: Chips per VM.
        head_reservation_timeout_s: Timeout for head PG reservation.

    Returns:
        (slice_name, {worker_id_label: {label_key: label_value}}).
    """
    nodes = ray.nodes()

    # Check runtime cache.
    for slice_name, labels in list(_tpu_subslice_cache.items()):
        for node in nodes:
            node_labels = node.get("Labels", {})
            if (
                node_labels.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY) == slice_name
                and node_labels.get(ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY)
                == parent_topology
            ):
                return slice_name, labels

    # Check internal KV for previously discovered slices. A slice has one
    # entry per worker node, so deduplicate by slice name to avoid N redundant
    # GCS round-trips for the same key.
    seen_slice_names: set = set()
    for node in nodes:
        node_labels = node.get("Labels", {})
        slice_name = node_labels.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY)
        node_topology = node_labels.get(ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY)
        if slice_name and node_topology == parent_topology:
            if slice_name in seen_slice_names:
                continue
            seen_slice_names.add(slice_name)
            try:
                existing = ray.experimental.internal_kv._internal_kv_get(
                    _get_subslice_kv_key(slice_name),
                    namespace=_TPU_SUBSLICE_KV_NAMESPACE,
                )
            except Exception:
                continue
            if existing:
                worker_labels = json.loads(existing)
                _tpu_subslice_cache[slice_name] = worker_labels
                logger.info(
                    "Loaded subslice labels for '%s' from KV store.", slice_name
                )
                return slice_name, worker_labels

    # Coordinated libtpu discovery: temporarily reserve a full slice, fan out
    # get_chip_coordinates() to every worker, compute physical mesh positions,
    # persist results, then release the slice.
    return _discover_and_persist_subslices(
        parent_topology,
        accelerator_version,
        chips_per_vm,
        head_reservation_timeout_s,
    )


def _find_available_subslice(
    slice_name: str,
    subslice_topology: str,
    worker_labels: Dict[str, Dict[str, str]],
) -> Tuple[Optional[List[str]], Optional[int]]:
    """Find an available (idle) subslice of the requested topology.

    Checks that all workers in a candidate subslice have their full TPU
    resources available.

    Args:
        slice_name: Name of the physical TPU slice.
        subslice_topology: Requested subslice topology (e.g. "2x4").
        worker_labels: Mapping of worker_id_label to subslice label dicts.

    Returns:
        (target_worker_ids, subslice_index) or (None, None).
    """
    label_key = f"{TPU_SUBSLICE_LABEL_PREFIX}{subslice_topology}"

    # Build mapping: subslice_index → list of worker_id labels.
    subslice_indices: Dict[str, List[str]] = {}
    for worker_id, labels in worker_labels.items():
        idx = labels.get(label_key)
        if idx is not None:
            subslice_indices.setdefault(idx, []).append(worker_id)

    if not subslice_indices:
        return None, None

    # Check idle resources for each candidate subslice.
    from ray._private.state import available_resources_per_node

    avail = available_resources_per_node()

    # Pre-build a lookup map for O(1) node resolution by (slice_name, worker_id),
    # avoiding an O(N) scan per worker inside the subslice loop.
    slice_worker_to_node: Dict[Tuple[str, str], Any] = {}
    for n in ray.nodes():
        nl = n.get("Labels", {})
        sn = nl.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY)
        wid_label = nl.get(ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY)
        if sn and wid_label:
            slice_worker_to_node[(sn, wid_label)] = n

    # Sort numerically so lower-indexed subslices are preferred over
    # higher-indexed ones. (Indices are stored as strings in labels;
    # lexicographic order would put "10" before "2" for large slices.)
    for idx in sorted(subslice_indices.keys(), key=int):
        worker_ids = subslice_indices[idx]
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
            return worker_ids, int(idx)

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


@PublicAPI(stability="alpha")
@client_mode_wrap
def subslice_placement_group(
    subslice_topology: str,
    accelerator_version: str,
    chips_per_vm: Optional[int] = None,
    subslice_index: Optional[int] = None,
    resources_per_bundle: Optional[Dict[str, float]] = None,
    strategy: str = "STRICT_SPREAD",
    name: str = "",
    lifetime: Optional[str] = None,
    head_reservation_timeout_s: Optional[
        float
    ] = DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S,
) -> SubslicePlacementGroup:
    """Asynchronously creates a PlacementGroup for a TPU subslice.

    A subslice placement group reserves a contiguous subset of workers within
    a larger TPU slice. This enables multiple workloads to share a physical
    TPU slice while maintaining ICI topology alignment.

    On the first call for a given topology, this function will temporarily
    reserve a full slice to discover the physical chip layout, compute
    subslice labels, and release unused workers. Subsequent calls reuse
    the cached topology data.

    Args:
        subslice_topology: Desired subslice TPU topology (e.g. "2x4",
            "2x2x2").
        accelerator_version: TPU accelerator generation (e.g. "v6e", "v4",
            "v5p").
        chips_per_vm: Optional override for chips per VM. Useful for
            ambiguous topologies like v6e 2x4 which can be 1 VM (8 chips)
            or 2 VMs (4 chips each).
        subslice_index: Specific subslice index to request. If ``None``
            (default), an available subslice is selected automatically.
        resources_per_bundle: Per-bundle resources. Defaults to
            ``{"CPU": 1, "TPU": chips_per_vm}``.
        strategy: Placement group strategy (default ``"STRICT_SPREAD"``).
        name: Optional placement group name.
        lifetime: Placement group lifetime (``None`` or ``"detached"``).
        head_reservation_timeout_s: Maximum time in seconds to wait for
            TPU head placement groups. Defaults to
            ``DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S``.

    Note:
        If ``subslice_topology`` equals the largest available topology for
        the accelerator (i.e. no strictly larger parent exists), this
        function falls back to creating a full :class:`SlicePlacementGroup`
        and wraps the result in a :class:`SubslicePlacementGroup`.

    Returns:
        A :class:`SubslicePlacementGroup` handle.

    Raises:
        ValueError: If the subslice topology is invalid for the accelerator.
        RuntimeError: If no available subslice is found or libtpu is missing.

    Examples:

    .. testcode:: python
        :skipif: True

        import ray
        from ray.util.scheduling_strategies import (
            PlacementGroupSchedulingStrategy,
        )
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
    version = get_tpu_version_from_type(accelerator_version)
    subslice_topology = subslice_topology.strip().lower()

    # Validate topology strings are parseable before proceeding.
    try:
        _parse_topology_dims(subslice_topology)
    except ValueError:
        raise ValueError(
            f"Subslice topology '{subslice_topology}' is not valid for "
            f"accelerator version '{version}'."
        )

    # Resolve the parent topology first. chips_per_vm is not used inside
    # _resolve_parent_topology, so it is safe to call before chips_per_vm
    # is finalised. Pass the user-supplied value when present; fall back to
    # a non-zero sentinel otherwise (the value is ignored either way).
    parent_topology = _resolve_parent_topology(
        subslice_topology, version, chips_per_vm if chips_per_vm is not None else 4
    )

    # Derive chips_per_vm from the *parent* topology, not the subslice.
    # All VMs in the physical parent slice share the same chip count; using
    # the subslice topology (e.g. "2x4" on v6e) would incorrectly return the
    # single-host default (8 chips) even though the parent slice (e.g. "4x4")
    # uses 4-chip multi-host VMs.
    if chips_per_vm is None:
        chips_per_vm = _get_default_chips_per_vm(parent_topology, version)
    if chips_per_vm <= 0:
        raise ValueError("chips_per_vm must be positive.")

    # Validate topologies.
    if not TPUAcceleratorManager.is_valid_tpu_accelerator_topology(
        version, subslice_topology
    ):
        raise ValueError(
            f"Subslice topology '{subslice_topology}' is not valid for "
            f"accelerator version '{version}'."
        )
    if not TPUAcceleratorManager.is_valid_tpu_accelerator_topology(
        version, parent_topology
    ):
        raise ValueError(
            f"Parent topology '{parent_topology}' is not valid for "
            f"accelerator version '{version}'."
        )

    sub_dims = _parse_topology_dims(subslice_topology)
    parent_dims = _parse_topology_dims(parent_topology)
    if any(sd > pd for sd, pd in zip(sub_dims, parent_dims)):
        raise ValueError(
            f"Subslice topology '{subslice_topology}' is larger than "
            f"parent topology '{parent_topology}'."
        )

    # If the subslice topology equals the resolved parent, no strictly larger
    # parent slice exists (e.g. requesting "16x16" on v6e). Fall back to a
    # full SlicePlacementGroup and wrap the result for API consistency.
    if parent_topology == subslice_topology:
        if resources_per_bundle is None:
            resources_per_bundle = {"CPU": 1, "TPU": chips_per_vm}
        full_slice = SlicePlacementGroup(
            topology=subslice_topology,
            accelerator_version=version,
            chips_per_vm=chips_per_vm,
            strategy=strategy,
            name=name,
            lifetime=lifetime,
            head_reservation_timeout_s=head_reservation_timeout_s,
        )
        slice_name_fallback = ""
        for sel in full_slice.bundle_label_selector:
            sn = sel.get(ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY)
            if sn:
                slice_name_fallback = sn
                break
        return SubslicePlacementGroup(
            placement_group=full_slice.placement_group,
            parent_topology=subslice_topology,
            subslice_topology=subslice_topology,
            subslice_index=0,
            slice_name=slice_name_fallback,
            num_hosts=full_slice.num_hosts,
            chips_per_host=full_slice.chips_per_host,
            bundle_resources=resources_per_bundle,
            head_placement_groups=full_slice.head_placement_groups,
            bundle_label_selectors=full_slice.bundle_label_selector,
        )

    # Get or discover subslice labels.
    slice_name, worker_labels = _get_or_create_subslice_labels(
        parent_topology, version, chips_per_vm, head_reservation_timeout_s
    )

    # Select target workers by requested index, else automatically selecting
    # the first available.
    if subslice_index is not None:
        label_key = f"{TPU_SUBSLICE_LABEL_PREFIX}{subslice_topology}"
        target_worker_ids = [
            wid
            for wid, labels in worker_labels.items()
            if labels.get(label_key) == str(subslice_index)
        ]
        if not target_worker_ids:
            max_idx = max(
                (
                    int(labels.get(label_key, "-1"))
                    for labels in worker_labels.values()
                    if label_key in labels
                ),
                default=-1,
            )
            raise ValueError(
                f"Subslice index {subslice_index} is not valid for "
                f"subslice '{subslice_topology}' in slice '{slice_name}'. "
                f"Valid indices: 0..{max_idx}."
            )
    else:
        target_worker_ids, selected_index = _find_available_subslice(
            slice_name, subslice_topology, worker_labels
        )
        if target_worker_ids is None:
            raise RuntimeError(
                f"No available subslice of topology '{subslice_topology}' "
                f"found in slice '{slice_name}'."
            )
        subslice_index = selected_index

    # Build bundles.
    if resources_per_bundle is None:
        resources_per_bundle = {"CPU": 1, "TPU": chips_per_vm}

    bundles = [resources_per_bundle.copy() for _ in target_worker_ids]
    bundle_label_selectors = [
        {
            ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY: slice_name,
            ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY: str(wid),
        }
        for wid in target_worker_ids
    ]

    # Create the placement group.
    pg = placement_group(
        bundles=bundles,
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
        num_hosts=len(target_worker_ids),
        chips_per_host=chips_per_vm,
        bundle_resources=resources_per_bundle,
        bundle_label_selectors=bundle_label_selectors,
    )
