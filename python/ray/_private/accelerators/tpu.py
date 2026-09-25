import glob
import logging
import os
import re
from functools import lru_cache
from typing import Dict, List, Optional, Set, Tuple

import requests

import ray
from ray._common.network_utils import parse_address
from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool
from ray.util.placement_group import (
    PlacementGroup,
    placement_group,
    remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logging.getLogger(__name__)


TPU_VALID_CHIP_OPTIONS = (1, 2, 4, 8)
GKE_TPU_ACCELERATOR_TYPE_ENV_VAR = "TPU_ACCELERATOR_TYPE"
GKE_TPU_TOPOLOGY_ENV_VAR = "TPU_TOPOLOGY"
GKE_TPU_WORKER_ID_ENV_VAR = "TPU_WORKER_ID"
GKE_TPU_NAME_ENV_VAR = "TPU_NAME"

# Constants for accessing the `accelerator-type` from TPU VM
# instance metadata.
# See https://cloud.google.com/compute/docs/metadata/overview
# for more details about VM instance metadata.
GCE_TPU_ACCELERATOR_ENDPOINT = (
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/"
)
GCE_TPU_HEADERS = {"Metadata-Flavor": "Google"}
GCE_TPU_ACCELERATOR_KEY = "accelerator-type"
GCE_TPU_ENV_KEY = "tpu-env"
GCE_TPU_INSTANCE_ID_KEY = "instance-id"
GCE_TPU_WORKER_ID_KEY = "agent-worker-number"

TPU_VISIBLE_CHIPS_ENV_VAR = "TPU_VISIBLE_CHIPS"
RAY_TPU_RESOURCE_PER_CHIP_ENV_VAR = "RAY_TPU_RESOURCE_PER_CHIP"

NOSET_TPU_VISIBLE_CHIPS_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"

# The following defines environment variables that allow
# us to access a subset of TPU visible chips and configure
# multi-host LibTPU / JAX meshes.
#
# See: https://github.com/google/jax/issues/14977 for an example/more details.
TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR = "TPU_CHIPS_PER_HOST_BOUNDS"
TPU_CHIPS_PER_HOST_BOUNDS_1_CHIP_CONFIG = "1,1,1"
TPU_CHIPS_PER_HOST_BOUNDS_2_CHIP_CONFIG = "1,2,1"

TPU_HOST_BOUNDS_ENV_VAR = "TPU_HOST_BOUNDS"
TPU_SINGLE_HOST_BOUNDS = "1,1,1"

TPU_CHIPS_PER_PROCESS_BOUNDS_ENV_VAR = "TPU_CHIPS_PER_PROCESS_BOUNDS"
TPU_PROCESS_BOUNDS_ENV_VAR = "TPU_PROCESS_BOUNDS"
TPU_PROCESS_ADDRESSES_ENV_VAR = "TPU_PROCESS_ADDRESSES"
TPU_PROCESS_PORT_ENV_VAR = "TPU_PROCESS_PORT"
TPU_WORKER_HOSTNAMES_ENV_VAR = "TPU_WORKER_HOSTNAMES"
TPU_WORKER_ID_ENV_VAR = "TPU_WORKER_ID"

# By default TPU VMs come with 4 chips per host and 2 tensorcores per chip.
# For more details: https://cloud.google.com/tpu/docs/system-architecture-tpu-vm
DEFAULT_TPU_NUM_CHIPS_PER_HOST = 4
DEFAULT_TPU_NUM_CORES_PER_CHIP = 2

# PCI vendor ID for Google TPUs (used to validate VFIO devices).
# See https://cloud.google.com/tpu/docs/custom-os-image.
TPU_PCI_VENDOR_ID = "0x1ae0"

# TorchTPU (PyTorch/XLA) environment variables and defaults.
TORCH_TPU_TOPOLOGY_ENV_VAR = "TORCH_TPU_TOPOLOGY"
TORCH_TPU_SLICEBUILDER_ADDRESSES_ENV_VAR = "TORCH_TPU_SLICEBUILDER_ADDRESSES"
DEFAULT_TORCH_TPU_SLICEBUILDER_PORT = 8471

# Accelerators that support up to 8 chips per host for single-host topologies: v5e, v6e
TPU_8_CHIPS_PER_HOST_TYPES = ("v5litepod", "v6e")

# Topologies that are always sub-host or single-host
TPU_SINGLE_HOST_TOPOLOGIES = ("1x1", "2x2", "2x4")

# Accelerators that are 2 cores per chip: v2, v3, v4, v5p, v7x
# Accelerators that are 1 core per chip: v5e, v6e
SINGLE_CORE_TPU_TYPES = ("v5litepod", "v6e")

# The valid TPU types.
VALID_TPU_TYPES = ("v2", "v3", "v4", "v5p", "v5litepod", "v6e", "v7x")


# This is only used to construct TPU 3D topologies
def _get_larger_3d_topologies(max_x: int, max_y: int, max_z: int) -> Set[str]:
    """Returns a set of larger 3D TPU topologies given the max x,y,z value. Using DEFAULT_TPU_NUM_CHIPS_PER_HOST as increment"""
    topologies = set()
    for x in range(
        DEFAULT_TPU_NUM_CHIPS_PER_HOST, max_x + 1, DEFAULT_TPU_NUM_CHIPS_PER_HOST
    ):
        for y in range(
            DEFAULT_TPU_NUM_CHIPS_PER_HOST, max_y + 1, DEFAULT_TPU_NUM_CHIPS_PER_HOST
        ):
            for z in range(
                DEFAULT_TPU_NUM_CHIPS_PER_HOST,
                max_z + 1,
                DEFAULT_TPU_NUM_CHIPS_PER_HOST,
            ):
                topologies.add(f"{x}x{y}x{z}")

    return topologies


# The valid TPU topologies for each of the TPU types.
VALID_TPU_TOPOLOGY = {
    "v2": {"4x4", "4x8", "8x8", "8x16", "16x16"},
    "v3": {"4x4", "4x8", "8x8", "8x16", "16x16", "16x32", "32x32"},
    "v4": {"2x2x1", "2x2x2", "2x2x4", "2x4x4"}.union(
        _get_larger_3d_topologies(12, 12, 16)
    ),
    "v5p": {
        "2x2x1",
        "2x2x2",
        "2x2x4",
        "2x4x4",
    }.union(_get_larger_3d_topologies(16, 16, 24)),
    "v5litepod": {"1x1", "2x2", "2x4", "2x8", "4x4", "4x8", "8x8", "8x16", "16x16"},
    "v6e": {"1x1", "2x2", "2x4", "2x8", "4x4", "4x8", "8x8", "8x16", "16x16"},
    "v7x": {
        "2x2x1",
        "2x2x2",
        "2x2x4",
        "2x4x4",
        "4x4x4",
        "4x4x8",
        "4x8x8",
        "8x8x8",
        "8x8x16",
        "8x16x16",
    },
}


# Worker grid dimensions for each valid TPU topology.
# Maps topology -> (worker_x, worker_y) for 2D, (worker_x, worker_y, worker_z) for 3D,
# i.e. the same axis order as the topology string itself.
# Assumes DEFAULT_TPU_NUM_CHIPS_PER_HOST (4) chips per worker for most types.
# For v5e/v6e single-host topologies with 8 chips, the worker count is 1.
#
# Entries are ordered by ascending total worker count (sum of dims). This ordering
# is required by _build_subslice_labels, which relies on it for early termination.
#
# NOTE: Large v3-only topologies ("16x32", "32x32") and the v5litepod-only "2x8"
# topology are intentionally omitted; subslicing those parent sizes is not yet
# supported and will raise ValueError at subslice_placement_group() call time.
_VALID_TOPOLOGY_WORKER_DIMS_2D: Dict[str, Tuple[int, int]] = {
    "2x2": (1, 1),
    "2x4": (1, 2),
    "4x4": (2, 2),
    "4x8": (2, 4),
    "8x8": (4, 4),
    "8x16": (4, 8),
    "16x16": (8, 8),
}

_VALID_TOPOLOGY_WORKER_DIMS_3D: Dict[str, Tuple[int, int, int]] = {
    "2x2x1": (1, 1, 1),
    "2x2x2": (1, 1, 2),
    "2x2x4": (1, 1, 4),
    "2x4x4": (1, 2, 4),
    "4x4x4": (2, 2, 4),
    "4x4x8": (2, 2, 8),
    "4x8x8": (2, 4, 8),
    "8x8x8": (4, 4, 8),
    "8x8x16": (4, 4, 16),
    "8x16x16": (4, 8, 16),
    "16x16x16": (8, 8, 16),
    "16x16x24": (8, 8, 24),
}


def _parse_topology_dims(topology: str) -> Tuple[int, ...]:
    """Parse a topology string (e.g. "2x4", "2x2x2") into a dimension tuple."""
    return tuple(int(d) for d in topology.strip().lower().split("x"))


@lru_cache(maxsize=None)
def _get_worker_dims_for_topology(topology: str) -> Tuple[int, ...]:
    """Return the worker-grid dimensions for *topology*: (x, y) for 2D,
    (x, y, z) for 3D. Raises ``ValueError`` for unknown topologies.

    Cloud TPU topology strings (e.g. "2x4", "2x4x4") specify chip bounds in
    (X, Y, Z) axis order. Dividing each chip axis by per-host bounds (2x2 for 2D,
    2x2x1 for 3D) yields worker dimensions in the same (worker_x, worker_y, worker_z)
    order.
    """
    dims = _parse_topology_dims(topology)
    if len(dims) == 2:
        if topology not in _VALID_TOPOLOGY_WORKER_DIMS_2D:
            raise ValueError(
                f"Unknown 2D topology: '{topology}'. "
                f"Valid: {list(_VALID_TOPOLOGY_WORKER_DIMS_2D.keys())}"
            )
        return _VALID_TOPOLOGY_WORKER_DIMS_2D[topology]
    elif len(dims) == 3:
        if topology in _VALID_TOPOLOGY_WORKER_DIMS_3D:
            return _VALID_TOPOLOGY_WORKER_DIMS_3D[topology]
        if (
            topology in VALID_TPU_TOPOLOGY["v4"]
            or topology in VALID_TPU_TOPOLOGY["v5p"]
            or topology in VALID_TPU_TOPOLOGY["v7x"]
        ):
            return (dims[0] // 2, dims[1] // 2, dims[2])
        raise ValueError(
            f"Unknown 3D topology: '{topology}'. "
            f"Valid: {list(_VALID_TOPOLOGY_WORKER_DIMS_3D.keys())}"
        )
    else:
        raise ValueError(f"Unsupported topology dimensionality for '{topology}'.")


def _strip_endpoint_port(endpoint: Optional[str]) -> str:
    """Strip the port from a network endpoint (e.g. '10.0.0.1:8471' or '[::1]:8471')."""
    if not endpoint or not (s := endpoint.strip()):
        return ""
    parsed = parse_address(s)
    return parsed[0] if parsed is not None else s.strip("[]")


def _get_default_chips_per_vm(topology: str, accelerator_version: str) -> int:
    """Return the default chips-per-VM for *topology* on *accelerator_version*
    (single-host v5e/v6e topologies pack up to 8 chips on one VM).
    """
    accel_lower = accelerator_version.strip().lower()

    # Single-host: return total chips in topology
    if accel_lower in TPU_8_CHIPS_PER_HOST_TYPES:
        total_chips = get_num_chips_from_topology(topology)
        if total_chips <= 8:
            return total_chips

    return DEFAULT_TPU_NUM_CHIPS_PER_HOST


def _get_tpu_metadata(key: str) -> Optional[str]:
    """Poll and get TPU metadata."""
    try:
        accelerator_type_request = requests.get(
            os.path.join(GCE_TPU_ACCELERATOR_ENDPOINT, key),
            headers=GCE_TPU_HEADERS,
        )
        if (
            accelerator_type_request.status_code == 200
            and accelerator_type_request.text
        ):
            return accelerator_type_request.text
        else:
            logging.debug(
                "Unable to poll TPU GCE Metadata. Got "
                f"status code: {accelerator_type_request.status_code} and "
                f"content: {accelerator_type_request.text}"
            )
    except requests.RequestException as e:
        logging.debug("Unable to poll the TPU GCE Metadata: %s", e)
    return None


def _accelerator_type_check(accelerator_type: str):
    if not accelerator_type.startswith(VALID_TPU_TYPES):
        raise ValueError(
            f"Invalid accelerator type: {accelerator_type}. Must start with one of: {VALID_TPU_TYPES}"
        )


def normalize_torchtpu_topology(
    topology: str,
    tpu_resource_per_chip: int = 1,
    accelerator_type: Optional[str] = None,
) -> str:
    """Normalizes TPU topology strings for PyTorch/XLA (e.g. '4x4' -> '4,4,1'; '2x2x4' with tpu_resource_per_chip=2 -> '2,2,4,2')."""
    if type(tpu_resource_per_chip) is not int:
        raise TypeError(
            f"tpu_resource_per_chip must be an integer, got {type(tpu_resource_per_chip)}."
        )
    if tpu_resource_per_chip <= 0:
        raise ValueError("tpu_resource_per_chip must be positive")

    if not isinstance(topology, str):
        raise ValueError(f"Invalid topology string: {topology!r}")

    dims = [d.strip() for d in topology.lower().replace("x", ",").split(",")]
    if len(dims) not in (2, 3, 4) or not all(d.isdigit() and int(d) > 0 for d in dims):
        raise ValueError(f"Invalid topology string: {topology!r}")
    dims = [str(int(d)) for d in dims]

    # 2D topologies (e.g. "2x4") are padded with 1 for the Z dimension ("2,4,1").
    if len(dims) == 2:
        dims.append("1")
    # For dual-device TPUs (or when tpu_resource_per_chip > 1), expand 3D to 4D ("2,4,1,2").
    if len(dims) == 3:
        if tpu_resource_per_chip > 1:
            dims.append(str(tpu_resource_per_chip))
        elif accelerator_type and "v7x" in accelerator_type.lower():
            dims.append("2")
    return ",".join(dims)


def get_total_chips_from_accelerator_type(accelerator_type: str) -> int:
    """Calculates total chips from a GCP accelerator ("pod") type string (e.g. "v6e-16")."""
    _accelerator_type_check(accelerator_type)

    parts = accelerator_type.split("-")
    if len(parts) < 2:
        raise ValueError(
            f"Accelerator type must include size (e.g. 'v6e-8'), got: {accelerator_type}"
        )

    num_cores = int(parts[1])
    cores_per_chip = get_tpu_cores_per_chip(accelerator_type)

    return num_cores // cores_per_chip


def get_num_tpu_visible_chips_per_host(accelerator_type: str) -> int:
    _accelerator_type_check(accelerator_type)

    if accelerator_type.startswith(TPU_8_CHIPS_PER_HOST_TYPES):
        total_chips = get_total_chips_from_accelerator_type(accelerator_type)

        # Sub/single-host topologies return their exact chip count
        if total_chips <= 8:
            return total_chips

    # Multi-host topologies default to 4 visible chips per host
    return DEFAULT_TPU_NUM_CHIPS_PER_HOST


def get_tpu_cores_per_chip(accelerator_type: str) -> int:
    _accelerator_type_check(accelerator_type)
    if accelerator_type.startswith(SINGLE_CORE_TPU_TYPES):
        return 1

    return DEFAULT_TPU_NUM_CORES_PER_CHIP


def get_num_chips_from_topology(topology: str) -> int:
    """
    Calculates the total number of chips in a TPU topology.
    Ex: "2x2x2" -> 8
    """
    total_chips = 1
    for dim in topology.strip().lower().split("x"):
        total_chips *= int(dim)
    return total_chips


def infer_tpu_pod_type_from_topology(
    topology: str, accelerator_type: str
) -> Optional[str]:
    """Infer the TPU pod type (e.g. v4-32) from topology and accelerator type."""
    if not topology or not accelerator_type:
        return None
    try:
        num_chips = get_num_chips_from_topology(topology)
        generation = accelerator_type.lower().replace("tpu-", "")
        num_cores = num_chips * get_tpu_cores_per_chip(generation)

        return f"{generation}-{num_cores}"
    except Exception as e:
        raise ValueError(
            f"Failed to infer pod type from topology '{topology}' "
            f"and type '{accelerator_type}'"
        ) from e


def fetch_tpu_slice_name_from_pg(pg):
    @ray.remote(num_cpus=0)
    def _get_tpu_slice_name():
        return TPUAcceleratorManager.get_current_node_tpu_name()

    tpu_name_ref = _get_tpu_slice_name.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_bundle_index=0
        )
    ).remote()

    return ray.get(tpu_name_ref)


def get_chips_per_host(topology: str, accelerator_version: str) -> int:
    """Get the number of chips per host based on topology and accelerator version.

    Rules for determining the default number of chips per host:
        - Default for most TPU generations (v4, v5p, v7x, etc.) is 4 chips per host.
        - For v5e and v6e:
            - Topologies with <= 8 chips use the exact chip count (e.g. 1x1 -> 1).
              These topologies are always sub or single-host.
            - Multi-host topologies (> 8 chips) default to 4-chip hosts.

    Args:
        topology: The TPU topology string (e.g. "2x2x2", "2x4").
        accelerator_version: The accelerator version string (e.g. "v4", "v6e").

    Returns:
        The default number of chips per host for the given configuration.
    """
    total_chips = get_num_chips_from_topology(topology)

    # Check for 8-chip host types (v5litepod, v6e) for single host setups
    if (
        accelerator_version.strip().lower() in TPU_8_CHIPS_PER_HOST_TYPES
        and topology.strip().lower() in TPU_SINGLE_HOST_TOPOLOGIES
    ):
        return total_chips

    return DEFAULT_TPU_NUM_CHIPS_PER_HOST


# Label prefix for subslice labels set on TPU nodes after discovery.
TPU_SUBSLICE_LABEL_PREFIX = "ray.io/tpu-subslice-"


def _get_physical_worker_id_from_coords(
    coords_list: List[List[int]],
    parent_topology: str,
) -> int:
    """Compute the physical worker position (0-based linear mesh index) from a
    worker's TPU chip coordinates.

    Each worker owns a block of chips sized by the parent topology's chip
    grid divided by its worker grid; the worker's mesh position is the
    minimum 2D (x, y) or 3D (x, y, z) chip coordinate divided by that block size,
    linearized in Z-major, Y-intermediate, X-minor order (``Hz * (By * Bx) + Hy * Bx + Hx``)
    matching JAX ``mesh_utils`` coordinate ordering.
    *coords_list* entries are [x, y] (2D) or [x, y, z] (3D). Raises
    ``ValueError`` if the coordinates don't match the topology.
    """
    if not coords_list:
        raise ValueError("coords_list cannot be empty")
    worker_dims = _get_worker_dims_for_topology(parent_topology)
    chip_dims = _parse_topology_dims(parent_topology)

    min_coords = [
        min((c[i] if len(c) > i else 0) for c in coords_list)
        for i in range(len(worker_dims))
    ]
    worker_pos = tuple(
        m // max(1, t_dim // w_dim)
        for m, t_dim, w_dim in zip(min_coords, chip_dims, worker_dims)
    )
    if any(p >= w_dim for p, w_dim in zip(worker_pos, worker_dims)):
        raise ValueError(
            f"Computed worker position {worker_pos} is out of bounds "
            f"for parent topology '{parent_topology}' with worker dims "
            f"{worker_dims}."
        )

    linear_id = 0
    stride = 1
    for pos, dim in zip(worker_pos, worker_dims):
        linear_id += pos * stride
        stride *= dim
    return linear_id


def _query_local_tpu_chip_coordinates(
    num_hosts: int = 1,
) -> Optional[List[List[int]]]:
    """Query physical 2D (x, y) or 3D (x, y, z) coordinates of local TPU chips via JAX."""
    resolved_hosts = [
        h.strip()
        for h in os.environ.get(TPU_WORKER_HOSTNAMES_ENV_VAR, "").split(",")
        if h.strip()
    ]
    # Guard against an incomplete multi-host environment: without all peer host
    # addresses, PJRT initializes as a standalone host and reports local (0..1, 0..1, 0)
    # coordinates instead of global slice coordinates.
    if num_hosts > 1 and len(resolved_hosts) < num_hosts:
        logger.debug(
            "Skipping TPU chip coordinate discovery: multi-host slice "
            "(num_hosts=%d) requires all worker hostnames in %s, got %d.",
            num_hosts,
            TPU_WORKER_HOSTNAMES_ENV_VAR,
            len(resolved_hosts),
        )
        return None

    try:
        import jax  # type: ignore[import-untyped]

        devices = jax.local_devices(backend="tpu")
        if devices:
            return [list(d.coords) for d in devices]
    except Exception as e:
        logger.debug("Could not query TPU chip coordinates via JAX: %s", e)

    return None


def _build_subslice_labels(
    physical_worker_id: int,
    parent_topology: str,
) -> Dict[str, str]:
    """Compute subslice labels for the worker at *physical_worker_id* in
    *parent_topology*.

    For each valid sub-topology smaller than the parent, determines which
    subslice index this worker belongs to based on its mesh position.
    Returns a dict mapping label keys (e.g. "ray.io/tpu-subslice-2x4") to
    subslice index strings (e.g. "0").
    """
    worker_dims = _get_worker_dims_for_topology(parent_topology)

    if len(worker_dims) == 2:
        dim_x, dim_y = worker_dims
        idx_y = physical_worker_id // dim_x
        idx_x = physical_worker_id % dim_x

        # NOTE: _VALID_TOPOLOGY_WORKER_DIMS_2D must be in ascending order by
        # total worker count. The 'break' below relies on this property: once
        # we reach the parent topology, all subsequent entries are at least as
        # large and need not be examined.
        labels: Dict[str, str] = {}
        for sub_shape, (sub_x, sub_y) in _VALID_TOPOLOGY_WORKER_DIMS_2D.items():
            # Skip shapes that do not tile the parent mesh evenly. This also
            # excludes shapes larger than the parent, since dim % sub == dim.
            if dim_x % sub_x or dim_y % sub_y:
                continue
            if sub_shape == parent_topology:
                break
            subslice_id = (idx_y // sub_y) * (dim_x // sub_x) + (idx_x // sub_x)
            labels[f"{TPU_SUBSLICE_LABEL_PREFIX}{sub_shape}"] = str(subslice_id)
        return labels
    else:
        dim_x, dim_y, dim_z = worker_dims
        wz = physical_worker_id // (dim_y * dim_x)
        remainder = physical_worker_id % (dim_y * dim_x)
        wy = remainder // dim_x
        wx = remainder % dim_x

        # NOTE: _VALID_TOPOLOGY_WORKER_DIMS_3D must be in ascending order by
        # total worker count. The 'break' relies on this property.
        labels = {}
        for sub_shape, (sub_x, sub_y, sub_z) in _VALID_TOPOLOGY_WORKER_DIMS_3D.items():
            if dim_x % sub_x or dim_y % sub_y or dim_z % sub_z:
                continue
            if sub_shape == parent_topology:
                break
            subslice_id = (
                (wz // sub_z) * (dim_y // sub_y) * (dim_x // sub_x)
                + (wy // sub_y) * (dim_x // sub_x)
                + (wx // sub_x)
            )
            labels[f"{TPU_SUBSLICE_LABEL_PREFIX}{sub_shape}"] = str(subslice_id)
        return labels


DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S: float = 100.0


def reserve_tpu_slice(
    topology: str,
    accelerator_type: str,
    timeout_s: Optional[float] = DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S,
    slice_name: Optional[str] = None,
) -> Optional[Tuple[str, PlacementGroup]]:
    """Reserves a TPU slice using its head resource and returns the slice name.
    This enables gang scheduling of training workers with multi-host TPUs.
    This is used by JaxTrainer with TPUs in Ray Train.

    Args:
        topology: The TPU topology string (e.g. "2x2x2").
        accelerator_type: The accelerator type of the node (e.g. "TPU-V4").
        timeout_s: The maximum time in seconds to wait for the TPU head
            placement group to become ready. The head reservation must succeed
            before the slice name can be retrieved, so this call is necessarily
            blocking. Defaults to ``DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S``.
            Pass ``None`` to wait indefinitely.
        slice_name: If provided, target this specific slice by constraining the
            head reservation to that slice's worker 0. Without it, the head can
            land on any matching slice's worker 0 — including one whose other
            workers are busy, which would then fail to gang-schedule.

    Returns:
        A tuple of a string representing a unique TPU slice name and the placement
        group handle reserving the TPU head.

    Raises:
        TimeoutError: If the TPU head placement group does not become ready
            within ``timeout_s`` seconds.
    """
    pod_type = infer_tpu_pod_type_from_topology(topology, accelerator_type)
    if pod_type is None:
        return None

    # Reserve a slice by creating a placement group on the TPU head.
    head_label_selector = {
        "ray.io/tpu-worker-id": "0",
        "ray.io/tpu-pod-type": pod_type,
    }
    if slice_name is not None:
        head_label_selector[ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY] = slice_name
    head_placement_group = placement_group(
        bundles=[{f"TPU-{pod_type}-head": 1}],
        bundle_label_selector=[head_label_selector],
    )

    logger.debug(
        "Waiting up to %s seconds to reserve multi-host slice head.", timeout_s
    )
    ready, _ = ray.wait([head_placement_group.ready()], timeout=timeout_s)

    if not ready:
        # Clean up the pending head reservation so that resources are not
        # held while the caller decides whether to retry.
        try:
            remove_placement_group(head_placement_group)
        except Exception:
            logger.exception(
                "Failed to clean up pending TPU head placement group after timeout."
            )
        raise TimeoutError(
            "Failed to reserve TPU head for slice with shape: {} after {} "
            "seconds. Ensure your cluster has sufficient resources. Requesting "
            "TPU head node with labels: {}. Current resources: {}".format(
                pod_type,
                timeout_s,
                head_label_selector,
                ray.available_resources(),
            )
        )

    # Retrieve the unique slice ID.
    slice_name = fetch_tpu_slice_name_from_pg(head_placement_group)
    if slice_name is None:
        raise RuntimeError(
            "Failed to retrieve TPU slice name after reserving head placement group. "
            "Ensure that TPU slice metadata is available and correctly configured on multi-host nodes."
        )

    return (slice_name, head_placement_group)


def _is_vfio_group_a_tpu(group: int) -> bool:
    """Return True iff the VFIO group is backed by a Google TPU PCI device.

    The VFIO framework exposes any device bound to ``vfio-pci`` as
    ``/dev/vfio/<iommu_group>``. The directory entry alone does not identify
    the underlying device — for example, NVIDIA BlueField-3's SoC Management
    Interface is bound to ``vfio-pci`` by RShim and surfaces as
    ``/dev/vfio/96`` even though the device is not a TPU.

    To distinguish TPU groups from non-TPU groups we inspect
    ``/sys/kernel/iommu_groups/<group>/devices/*/vendor`` and look for
    Google's PCI vendor ID (``0x1ae0``).

    Args:
        group: The numeric IOMMU group id found under ``/dev/vfio``.

    Returns:
        True if the group is backed by a Google TPU PCI device. If the
        sysfs entry is missing or unreadable, returns False — we fail
        closed because a false positive here would prevent the actual
        accelerators (e.g. NVIDIA GPUs) from being registered.
    """
    sysfs_root = f"/sys/kernel/iommu_groups/{group}/devices"
    try:
        devices = os.listdir(sysfs_root)
    except OSError as e:
        logger.debug("Unable to inspect VFIO group %s at %s: %s", group, sysfs_root, e)
        return False
    for device in devices:
        vendor_path = os.path.join(sysfs_root, device, "vendor")
        try:
            with open(vendor_path, encoding="ascii") as f:
                vendor = f.read().strip()
        except (OSError, UnicodeDecodeError) as e:
            logger.debug(
                "Unable to read PCI vendor for VFIO group %s at %s: %s",
                group,
                vendor_path,
                e,
            )
            continue
        if vendor.lower() == TPU_PCI_VENDOR_ID:
            return True
    return False


def normalize_tpu_accelerator_type(accelerator_type: Optional[str]) -> str:
    """Rewrite a TPU generation prefix to its canonical "v{gen}" spelling.

    Only the prefix changes: "TPU-V7X" -> "v7x", "tpu7x-8" -> "v7x-8". Ray's
    topology tables are keyed by that spelling, and its accelerator resource
    names are built from it.
    """
    if not accelerator_type:
        return ""
    return re.sub(r"^tpu-?v?", "v", accelerator_type.strip().lower())


def get_tpu_resource_per_chip() -> int:
    """Return the number of Ray TPU resources per physical chip (defaults to 1).

    Some generations expose 2 logical XLA devices per chip (e.g. v7x). Counting
    per device would change the TPU resource count of existing nodes, so it is
    opt-in via RAY_TPU_RESOURCE_PER_CHIP.
    """
    value = os.environ.get(RAY_TPU_RESOURCE_PER_CHIP_ENV_VAR)
    if value is None:
        return 1
    if not value.isdecimal() or int(value) < 1:
        raise ValueError(
            f"{RAY_TPU_RESOURCE_PER_CHIP_ENV_VAR} must be a positive integer, "
            f"got: {value!r}"
        )
    return int(value)


class TPUAcceleratorManager(AcceleratorManager):
    """Google TPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "TPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return TPU_VISIBLE_CHIPS_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        tpu_visible_chips = os.environ.get(
            TPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )

        if tpu_visible_chips is None:
            return None

        if tpu_visible_chips == "":
            return []

        resource_per_chip = get_tpu_resource_per_chip()
        if resource_per_chip == 1:
            return list(tpu_visible_chips.split(","))

        # Ray allocates one ID per logical device, so expand each physical chip
        # index into the device IDs it hosts.
        return [
            str(int(chip) * resource_per_chip + device)
            for chip in tpu_visible_chips.split(",")
            if chip.strip()
            for device in range(resource_per_chip)
        ]

    @staticmethod
    @lru_cache()
    def get_current_node_num_accelerators() -> int:
        """Attempt to detect the number of TPUs on this machine.

        TPU chips are represented as devices within `/dev/`, either as
        `/dev/accel*` or `/dev/vfio/*`.

        Assumes each TPU-backed IOMMU group contains exactly one TPU device.

        Returns:
            The number of TPUs if any were detected, otherwise 0.
        """
        # Real TPU chips are exposed as character devices at /dev/accel0,
        # /dev/accel1, etc. NVIDIA drivers 570.x and later (Blackwell-class
        # GPUs such as the RTX 5090) instead create /dev/accel as a *directory*
        # containing /dev/accel/accel0, which the non-recursive glob below
        # would otherwise miscount as a TPU chip. Filter directory entries out
        # so both GKE and GCE TPU detection keep working while rejecting the
        # NVIDIA false positive.
        accel_chips = [p for p in glob.glob("/dev/accel*") if not os.path.isdir(p)]
        if accel_chips:
            return len(accel_chips)

        try:
            vfio_entries = os.listdir("/dev/vfio")
            numeric_entries = [int(entry) for entry in vfio_entries if entry.isdigit()]
        except OSError as e:
            logger.debug("Failed to detect number of TPUs: %s", e)
            return 0

        # Preserve the existing VFIO fallback accounting: each validated
        # IOMMU group contributes one TPU resource. Some hardware may expose
        # multiple VFIO groups per physical chip; callers should configure the
        # TPU resource count explicitly when group count and chip count differ.
        tpu_count = 0
        for group in numeric_entries:
            if _is_vfio_group_a_tpu(group):
                tpu_count += 1
        return tpu_count

    @staticmethod
    def is_valid_tpu_accelerator_type(tpu_accelerator_type: str) -> bool:
        """Check whether the tpu accelerator_type is formatted correctly.

        The accelerator_type field typically follows a form of v{generation}-{cores/chips},
        but newer generations like 7x may follow tpu{generation}-{cores/chips}.

        See the following for more information:
        https://cloud.google.com/sdk/gcloud/reference/compute/tpus/tpu-vm/accelerator-types/describe

        Args:
            tpu_accelerator_type: The string representation of the accelerator type
                to be checked for validity.

        Returns:
            True if it's valid, false otherwise.
        """
        # 1. Legacy format: v2-8, v3-32.
        # 2. Newer format with letters in generation: v5litepod-16, v6e-4.
        # 3. Ironwood TPU format which contains a tpu prefix: tpu7x-16.
        expected_pattern = re.compile(r"^(v|tpu)\d+[a-zA-Z]*-\d+$")
        if not expected_pattern.match(tpu_accelerator_type):
            return False
        return True

    @staticmethod
    def is_valid_tpu_accelerator_topology(
        tpu_accelerator_version: str, tpu_topology: str
    ) -> bool:
        """Check whether the tpu topology is valid.

        The accelerator_type field follows a form of v{generation}.
        The accelerator_topology field follows either the form {A}x{B} or {A}x{B}x{C} depending on the v{generation}

        Args:
            tpu_accelerator_version: The string representation of the accelerator version. (e.g. v6e, V5P)
            tpu_topology: The string representation of the accelerator topology
                to be checked for validity

        Returns:
            True if it's a valid topology, False otherwise.
        """
        tpu_version_formatted = normalize_tpu_accelerator_type(
            tpu_accelerator_version
        ).split("-")[0]

        if (
            tpu_version_formatted not in VALID_TPU_TOPOLOGY
            or tpu_topology.strip().lower()
            not in VALID_TPU_TOPOLOGY[tpu_version_formatted]
        ):
            return False
        return True

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        if quantity not in TPU_VALID_CHIP_OPTIONS:
            return (
                False,
                f"The number of requested 'TPU' was set to {quantity} which "
                "is not a supported chip configuration. Supported configs: "
                f"{TPU_VALID_CHIP_OPTIONS}",
            )
        else:
            return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_tpu_chips: List[str],
    ) -> None:
        """Set TPU environment variables based on the provided visible_tpu_chips.

        To access a subset of the TPU visible chips, we must use a combination of
        environment variables that tells the compiler (via ML framework) the:
        - Visible chips
        - The physical bounds of chips per host
        - The host bounds within the context of a TPU pod.

        See: https://github.com/google/jax/issues/14977 for an example/more details.

        TPU_VISIBLE_CHIPS has whole-chip granularity, so an allocation covering
        part of a chip is masked to the whole chip and sees every device on it.
        Frameworks that bind one process per device (TorchTPU) overwrite this
        mask with their own; frameworks that claim every visible device (JAX)
        need whole-chip allocations to avoid contending for the same chip.

        Args:
            visible_tpu_chips: List of str representing TPU chips, or device IDs
                for TPUs with multiple logical devices per chip.
        """
        if env_bool(NOSET_TPU_VISIBLE_CHIPS_ENV_VAR, False):
            return

        num_accelerators_on_node = (
            TPUAcceleratorManager.get_current_node_num_accelerators()
        )
        # When autodetected, resource_and_label_spec caps a node's TPU resources
        # at this count, so an allocation matching it holds the whole node.
        if len(visible_tpu_chips) == num_accelerators_on_node:
            # Let the ML framework use the defaults
            os.environ.pop(TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR, None)
            os.environ.pop(TPU_HOST_BOUNDS_ENV_VAR, None)
            return

        # TPU_VISIBLE_CHIPS masks physical chips, but Ray assigns one ID per
        # logical device when RAY_TPU_RESOURCE_PER_CHIP > 1, so collapse them.
        resource_per_chip = get_tpu_resource_per_chip()
        if resource_per_chip == 1:
            physical_chips = visible_tpu_chips
        else:
            physical_chips = sorted(
                {int(device_id) // resource_per_chip for device_id in visible_tpu_chips}
            )

        os.environ[
            TPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join(str(chip) for chip in physical_chips)
        if len(physical_chips) == 1:
            os.environ[
                TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR
            ] = TPU_CHIPS_PER_HOST_BOUNDS_1_CHIP_CONFIG
            os.environ[TPU_HOST_BOUNDS_ENV_VAR] = TPU_SINGLE_HOST_BOUNDS
        elif len(physical_chips) == 2:
            os.environ[
                TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR
            ] = TPU_CHIPS_PER_HOST_BOUNDS_2_CHIP_CONFIG
            os.environ[TPU_HOST_BOUNDS_ENV_VAR] = TPU_SINGLE_HOST_BOUNDS

    @staticmethod
    def get_current_node_tpu_pod_type() -> Optional[str]:
        """Get the TPU pod type of the current node if applicable.

        Individual TPU VMs within a TPU pod must know what type
        of pod it is a part of. This is necessary for the
        ML framework to work properly.

        The logic is different if the TPU was provisioned via:
        ```
        gcloud tpus tpu-vm create ...
        ```
        (i.e. a GCE VM), vs through GKE:
        - GCE VMs will always have a metadata server to poll this info
        - GKE VMS will have environment variables preset.

        Returns:
            A string representing the current TPU pod type, e.g.
            v4-16.

        """
        # Start with GKE-based check
        accelerator_type = os.getenv(GKE_TPU_ACCELERATOR_TYPE_ENV_VAR, "")
        if not accelerator_type:
            # GCE-based VM check
            accelerator_type = _get_tpu_metadata(key=GCE_TPU_ACCELERATOR_KEY)
        if accelerator_type and TPUAcceleratorManager.is_valid_tpu_accelerator_type(
            tpu_accelerator_type=accelerator_type
        ):
            return normalize_tpu_accelerator_type(accelerator_type)
        logging.debug("Failed to get a valid accelerator type.")
        return None

    @staticmethod
    def get_current_node_tpu_name() -> Optional[str]:
        """Return the name of the TPU pod that this worker node is a part of.

        For instance, if the TPU was created with name "my-tpu", this function
        will return "my-tpu".

        If created through the Ray cluster launcher, the
        name will typically be something like "ray-my-tpu-cluster-worker-aa946781-tpu".

        In case the TPU was created through KubeRay, we currently expect that the
        environment variable TPU_NAME is set per TPU pod slice, in which case
        this function will return the value of that environment variable.

        """
        try:
            # Start with GKE-based check
            tpu_name = os.getenv(GKE_TPU_NAME_ENV_VAR, None)
            if not tpu_name:
                # GCE-based VM check
                tpu_name = _get_tpu_metadata(key=GCE_TPU_INSTANCE_ID_KEY)
            return tpu_name
        except ValueError as e:
            logging.debug("Could not get TPU name: %s", e)
            return None

    @staticmethod
    def get_current_node_tpu_worker_id() -> Optional[int]:
        """Return the worker index of the TPU pod."""
        try:
            # Start with GKE-based check
            worker_id = os.getenv(GKE_TPU_WORKER_ID_ENV_VAR, None)
            if not worker_id:
                # GCE-based VM check
                worker_id = _get_tpu_metadata(key=GCE_TPU_WORKER_ID_KEY)
            if worker_id:
                return int(worker_id)
            else:
                return None
        except ValueError as e:
            logging.debug("Could not get TPU worker id: %s", e)
            return None

    @staticmethod
    def get_num_workers_in_current_tpu_pod() -> Optional[int]:
        """Return the total number of workers in a TPU pod."""
        tpu_pod_type = TPUAcceleratorManager.get_current_node_tpu_pod_type()
        chips_per_host = TPUAcceleratorManager.get_current_node_num_accelerators()
        cores_per_chip = get_tpu_cores_per_chip(tpu_pod_type)  # Hard-coded map.
        cores_per_host = chips_per_host * cores_per_chip
        if tpu_pod_type and cores_per_host > 0:
            num_cores = int(tpu_pod_type.split("-")[1])
            num_workers = num_cores // cores_per_host
            # If the chip count doesn't fill a full host, a sub-host is still treated as a host.
            if num_cores % cores_per_host != 0:
                num_workers += 1
            return num_workers
        else:
            logging.debug("Could not get num workers in TPU pod.")
            return None

    @staticmethod
    def get_current_node_tpu_topology() -> Optional[str]:
        try:
            # Attempt GKE based lookup first
            if topology := os.environ.get(GKE_TPU_TOPOLOGY_ENV_VAR):
                return topology.strip().lower()
            # GCE-based VM check using TPU env string.
            tpu_env = _get_tpu_metadata(key=GCE_TPU_ENV_KEY)
            if tpu_env:
                topology = re.search(r"TOPOLOGY:\s*'([^']+)'", tpu_env)
                if topology:
                    return topology.group(1).strip().lower()
        except ValueError as e:
            logging.debug("Could not get TPU topology: %s", e)
            return None

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Attempt to detect the TPU accelerator type.

        The output of this function will return the "ray accelerator type"
        resource (e.g. TPU-V4) that indicates the TPU version.

        We also expect that our TPU nodes contain a "TPU pod type"
        resource, which indicates information about the topology of
        the TPU pod slice.

        We expect that the "TPU pod type" resource to be used when
        running multi host workers, i.e. when TPU units are pod slices.

        We expect that the "ray accelerator type" resource to be used when
        running single host workers, i.e. when TPU units are single hosts.

        Returns:
            A string representing the TPU accelerator type,
            e.g. "TPU-V2", "TPU-V3", "TPU-V4" if applicable, else None.

        """

        tpu_pod_type = TPUAcceleratorManager.get_current_node_tpu_pod_type()
        if tpu_pod_type is None:
            logging.info("Failed to auto-detect TPU type.")
            return None

        tpu_version = normalize_tpu_accelerator_type(tpu_pod_type).split("-")[0]
        return f"TPU-{tpu_version.upper()}"

    @staticmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        """Get additional resources required for TPU nodes.

        This will populate the TPU pod type and the TPU name which
        is used for TPU pod execution.

        When running workloads on a TPU pod, we need a way to run
        the same binary on every worker in the TPU pod.

        See https://jax.readthedocs.io/en/latest/multi_process.html
        for more information.

        To do this in ray, we take advantage of custom resources. We
        mark worker 0 of the TPU pod as a "coordinator" that identifies
        the other workers in the TPU pod. We therefore need:
        - worker 0 to be targetable.
        - all workers in the TPU pod to have a unique identifier consistent
        within a TPU pod.

        So assuming we want to run the following workload:

        @ray.remote
        def my_jax_fn():
            import jax
            return jax.device_count()

        We could broadcast this on a TPU pod (e.g. a v4-16) as follows:

        @ray.remote(resources={"TPU-v4-16-head"})
        def run_jax_fn(executable):
            # Note this will execute on worker 0
            tpu_name = ray.util.tpu.get_current_pod_name()
            num_hosts = ray.util.tpu.get_current_pod_worker_count()
            tpu_executable = executable.options(resources={"TPU": 4, tpu_name: 1})
            return [tpu_executable.remote() for _ in range(num_hosts)]

        Returns:
            A dictionary representing additional resources that may be
            necessary for a particular accelerator type.

        """
        resources = {}
        tpu_name = TPUAcceleratorManager.get_current_node_tpu_name()
        worker_id = TPUAcceleratorManager.get_current_node_tpu_worker_id()
        tpu_pod_type = TPUAcceleratorManager.get_current_node_tpu_pod_type()

        if tpu_name and worker_id is not None and tpu_pod_type:
            pod_head_resource_name = f"TPU-{tpu_pod_type}-head"
            # Add the name of the TPU to the resource.
            resources[tpu_name] = 1
            # Only add in the TPU pod type resource to worker 0.
            if worker_id == 0:
                resources[pod_head_resource_name] = 1
        else:
            logging.info(
                "Failed to configure TPU pod. Got: "
                "tpu_name: %s, worker_id: %s, accelerator_type: %s",
                tpu_name,
                worker_id,
                tpu_pod_type,
            )
        if resources:
            return resources
        return None

    @staticmethod
    def get_current_node_accelerator_labels() -> Dict[str, str]:
        """Get default TPU-specific Ray node labels for the current node.

        For TPUs, these labels include:
        - ray.io/tpu-slice-name: the name of the TPU Pod or slice
        - ray.io/tpu-worker-id: the integer worker ID within the slice
        - ray.io/tpu-topology: the TPU topology (e.g. 4x4)
        - ray.io/tpu-pod-type: the TPU pod type (e.g. v4-8)

        Returns:
            A dictionary of TPU label keys and resolved values.
        """
        tpu_labels = {}

        tpu_name = TPUAcceleratorManager.get_current_node_tpu_name()
        if tpu_name:
            tpu_labels[ray._raylet.RAY_NODE_TPU_SLICE_NAME_KEY] = tpu_name

        worker_id = TPUAcceleratorManager.get_current_node_tpu_worker_id()
        if worker_id is not None:
            tpu_labels[ray._raylet.RAY_NODE_TPU_WORKER_ID_KEY] = str(worker_id)

        tpu_topology = TPUAcceleratorManager.get_current_node_tpu_topology()
        if tpu_topology:
            tpu_labels[ray._raylet.RAY_NODE_TPU_TOPOLOGY_KEY] = tpu_topology

        pod_type = TPUAcceleratorManager.get_current_node_tpu_pod_type()
        if pod_type:
            tpu_labels[ray._raylet.RAY_NODE_TPU_POD_TYPE_KEY] = pod_type

        return tpu_labels
