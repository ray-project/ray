import os
import re
import glob
import requests
import logging
from functools import lru_cache
from typing import Dict, Optional, List, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)


TPU_VALID_CHIP_OPTIONS = (1, 2, 4, 8)
GKE_TPU_ACCELERATOR_TYPE_ENV_VAR = "TPU_ACCELERATOR_TYPE"
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
GCE_TPU_INSTANCE_ID_KEY = "instance-id"
GCE_TPU_WORKER_ID_KEY = "agent-worker-number"

TPU_VISIBLE_CHIPS_ENV_VAR = "TPU_VISIBLE_CHIPS"
TPU_VERSIONS_WITH_MULTIPLE_CORES_PER_CHIP = {"v2", "v3", "v4"}

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
    def _get_current_node_tpu_pod_type() -> Optional[str]:
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
    def _get_current_node_tpu_worker_id() -> Optional[int]:
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
        tpu_pod_type = TPUAcceleratorManager._get_current_node_tpu_pod_type()
        if tpu_pod_type:
            version = tpu_pod_type.split("-")[0]
            num_chips_or_cores = int(tpu_pod_type.split("-")[1])
            if version in TPU_VERSIONS_WITH_MULTIPLE_CORES_PER_CHIP:
                return num_chips_or_cores // 8
            else:
                return num_chips_or_cores // 4
        else:
            logging.debug("Could not get num workers in TPU pod.")
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
        tpu_pod_type = TPUAcceleratorManager._get_current_node_tpu_pod_type()

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
            tpu_name = ray.util.accelerators.tpu.get_tpu_pod_name()
            num_workers = ray.util.accelerators.tpu.get_tpu_num_workers()
            tpu_executable = executable.options(resources={"TPU": 4, tpu_name: 1})
            return [tpu_executable.remote() for _ in range(num_workers)]

        Returns:
            A dictionary representing additional resources that may be
            necessary for a particular accelerator type.

        """
        resources = {}
        tpu_name = TPUAcceleratorManager.get_current_node_tpu_name()
        worker_id = TPUAcceleratorManager._get_current_node_tpu_worker_id()
        tpu_pod_type = TPUAcceleratorManager._get_current_node_tpu_pod_type()

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
