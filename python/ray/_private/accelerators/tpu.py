import glob
import logging
import os
import re
from functools import lru_cache
from typing import Dict, List, Optional, Set, Tuple

import requests

import ray
from ray._private.accelerators.accelerator import AcceleratorManager
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

NOSET_TPU_VISIBLE_CHIPS_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"

# The following defines environment variables that allow
# us to access a subset of TPU visible chips.
#
# See: https://github.com/google/jax/issues/14977 for an example/more details.
TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR = "TPU_CHIPS_PER_HOST_BOUNDS"
TPU_CHIPS_PER_HOST_BOUNDS_1_CHIP_CONFIG = "1,1,1"
TPU_CHIPS_PER_HOST_BOUNDS_2_CHIP_CONFIG = "1,2,1"

TPU_HOST_BOUNDS_ENV_VAR = "TPU_HOST_BOUNDS"
TPU_SINGLE_HOST_BOUNDS = "1,1,1"

# By default TPU VMs come with 4 chips per host and 2 tensorcores per chip.
# For more details: https://cloud.google.com/tpu/docs/system-architecture-tpu-vm
DEFAULT_TPU_NUM_CHIPS_PER_HOST = 4
DEFAULT_TPU_NUM_CORES_PER_CHIP = 2

# Accelerators that are 4 chips per host: v2, v3, v4, v5p
# Accelerators that are 8 chips per host: v5e, v6e
SINGLE_HOST_8_CHIPS_TPU_TYPES = ("v5litepod", "v6e")

# Accelerators that are 2 cores per chip: v2, v3, v4, v5p
# Accelerators that are 1 core per chip: v5e, v6e
SINGLE_CORE_TPU_TYPES = ("v5litepod", "v6e")

# The valid TPU types.
VALID_TPU_TYPES = ("v2", "v3", "v4", "v5p", "v5litepod", "v6e")

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


# The valid TPU topologies for each of the TPU types
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
    "v5litepod": {"2x8", "4x4", "4x8", "8x8", "8x16", "16x16"},
    "v6e": {"2x8", "4x4", "4x8", "8x8", "8x16", "16x16"},
}


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


def get_num_tpu_visible_chips_per_host(accelerator_type: str) -> int:
    _accelerator_type_check(accelerator_type)
    if accelerator_type.startswith(SINGLE_HOST_8_CHIPS_TPU_TYPES):
        return 8

    return DEFAULT_TPU_NUM_CHIPS_PER_HOST


def get_tpu_cores_per_chip(accelerator_type: str) -> int:
    _accelerator_type_check(accelerator_type)
    if accelerator_type.startswith(SINGLE_CORE_TPU_TYPES):
        return 1

    return DEFAULT_TPU_NUM_CORES_PER_CHIP


def infer_tpu_pod_type_from_topology(
    topology: str, accelerator_type: str
) -> Optional[str]:
    """Infer the TPU pod type (e.g. v4-32) from topology and accelerator type."""
    if not topology or not accelerator_type:
        return None
    try:
        num_chips = 1
        for value in topology.strip().lower().split("x"):
            num_chips *= int(value)
        generation = accelerator_type.lower().replace("tpu-", "")
        return f"{generation}-{num_chips}"
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
    """Get the number of chips per host (aka VMs) based on topology and accelerator version.
    The current rule is as follows:
        Default chips per host is 4.
        If accelerator_version is v5e or v6e AND topology product <= 8, the chips per host will just be the proudct. i.e. 1, 4, or 8
        If accelerator_version is v5e or v6e AND topology product > 8, the chips per host will be 4
        If accelerator_version is v5p or other versions, the chips per host will be 4

    Args:
        topology: The TPU topology string (e.g. "2x2x2").
        accelerator_version: The accelerator version of the node (e.g. "V4", "v4").

    Returns:
        A int representing the number of chips per host (aka VM)
    """
    chips_per_host = DEFAULT_TPU_NUM_CHIPS_PER_HOST
    total_chips = 1
    for value in topology.strip().lower().split("x"):
        total_chips *= int(value)

    if (
        total_chips <= 8
        and accelerator_version.strip().lower() in SINGLE_HOST_8_CHIPS_TPU_TYPES
    ):
        return total_chips

    return chips_per_host


def reserve_tpu_slice(
    topology: str,
    accelerator_type: str,
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
    pod_type = infer_tpu_pod_type_from_topology(topology, accelerator_type)
    if pod_type is None:
        return None

    # Reserve a slice by creating a placement group on the TPU head.
    head_label_selector = {
        "ray.io/tpu-worker-id": "0",
        "ray.io/tpu-pod-type": pod_type,
    }
    head_placement_group = ray.util.placement_group(
        bundles=[{f"TPU-{pod_type}-head": 1}],
        bundle_label_selector=[head_label_selector],
    )

    logger.debug("Waiting to reserve multi-host slice head.")
    timeout = 100
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

    # TODO: return both the slice name and reference to the PG reservation.
    return slice_name


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

        return list(tpu_visible_chips.split(","))

    @staticmethod
    @lru_cache()
    def get_current_node_num_accelerators() -> int:
        """Attempt to detect the number of TPUs on this machine.

        TPU chips are represented as devices within `/dev/`, either as
        `/dev/accel*` or `/dev/vfio/*`.

        Returns:
            The number of TPUs if any were detected, otherwise 0.
        """
        accel_files = glob.glob("/dev/accel*")
        if accel_files:
            return len(accel_files)

        try:
            vfio_entries = os.listdir("/dev/vfio")
            numeric_entries = [int(entry) for entry in vfio_entries if entry.isdigit()]
            return len(numeric_entries)
        except FileNotFoundError as e:
            logger.debug("Failed to detect number of TPUs: %s", e)
            return 0

    @staticmethod
    def is_valid_tpu_accelerator_type(tpu_accelerator_type: str) -> bool:
        """Check whether the tpu accelerator_type is formatted correctly.

        The accelerator_type field follows a form of v{generation}-{cores/chips}.

        See the following for more information:
        https://cloud.google.com/sdk/gcloud/reference/compute/tpus/tpu-vm/accelerator-types/describe

        Args:
            tpu_accelerator_type: The string representation of the accelerator type
                to be checked for validity.

        Returns:
            True if it's valid, false otherwise.
        """
        expected_pattern = re.compile(r"^v\d+[a-zA-Z]*-\d+$")
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
            True if it's valid topology, false othrwise
        """
        tpu_version_formatted = tpu_accelerator_version.strip().lower().split("-")[0]
        if (
            tpu_version_formatted.lower() not in VALID_TPU_TOPOLOGY
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

        Args:
            visible_tpu_chips (List[str]): List of int representing TPU chips.
        """
        if os.environ.get(NOSET_TPU_VISIBLE_CHIPS_ENV_VAR):
            return

        num_visible_tpu_chips = len(visible_tpu_chips)
        num_accelerators_on_node = (
            TPUAcceleratorManager.get_current_node_num_accelerators()
        )
        if num_visible_tpu_chips == num_accelerators_on_node:
            # Let the ML framework use the defaults
            os.environ.pop(TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR, None)
            os.environ.pop(TPU_HOST_BOUNDS_ENV_VAR, None)
            return
        os.environ[
            TPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_tpu_chips])
        if num_visible_tpu_chips == 1:
            os.environ[
                TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR
            ] = TPU_CHIPS_PER_HOST_BOUNDS_1_CHIP_CONFIG
            os.environ[TPU_HOST_BOUNDS_ENV_VAR] = TPU_SINGLE_HOST_BOUNDS
        elif num_visible_tpu_chips == 2:
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
            return accelerator_type
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
                return topology
            # GCE-based VM check using TPU env string.
            tpu_env = _get_tpu_metadata(key=GCE_TPU_ENV_KEY)
            if tpu_env:
                topology = re.search(r"TOPOLOGY:\s*'([^']+)'", tpu_env)
                if topology:
                    return topology.group(1)
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

        def tpu_pod_type_to_ray_accelerator_type(
            tpu_pod_type: str,
        ) -> Optional[str]:
            return "TPU-" + str(tpu_pod_type.split("-")[0].upper())

        ray_accelerator_type = None
        tpu_pod_type = TPUAcceleratorManager.get_current_node_tpu_pod_type()

        if tpu_pod_type is not None:
            ray_accelerator_type = tpu_pod_type_to_ray_accelerator_type(
                tpu_pod_type=tpu_pod_type
            )
            if ray_accelerator_type is None:
                logger.info(
                    "While trying to autodetect a TPU type, "
                    f"received malformed accelerator_type: {tpu_pod_type}"
                )

        if ray_accelerator_type is None:
            logging.info("Failed to auto-detect TPU type.")

        return ray_accelerator_type

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
            tpu_name = ray.util.tpu.get_tpu_pod_name()
            num_workers = ray.util.tpu.get_tpu_num_workers()
            tpu_executable = executable.options(resources={"TPU": 4, tpu_name: 1})
            return [tpu_executable.remote() for _ in range(num_workers)]

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
