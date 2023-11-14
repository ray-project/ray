import os
import re
import glob
import requests
import logging
from typing import Optional, List, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)


TPU_VALID_CHIP_OPTIONS = (1, 2, 4)
GKE_TPU_ACCELERATOR_TYPE_ENV_VAR = "TPU_ACCELERATOR_TYPE"

# Constants for accessing the `accelerator-type` from TPU VM
# instance metadata.
# See https://cloud.google.com/compute/docs/metadata/overview
# for more details about VM instance metadata.
GCE_TPU_ACCELERATOR_ENDPOINT = (
    "http://metadata.google.internal/computeMetadata/"
    "v1/instance/attributes/accelerator-type"
)
GCE_TPU_HEADERS = {"Metadata-Flavor": "Google"}

TPU_VISIBLE_CHIPS_ENV_VAR = "TPU_VISIBLE_CHIPS"

NOSET_TPU_VISIBLE_CHIPS_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS"

# TPU VMs come with 4 chips per host and 2 tensorcores per chip.
# For more details: https://cloud.google.com/tpu/docs/system-architecture-tpu-vm
TPU_NUM_CHIPS_PER_HOST = 4
TPU_CORES_PER_CHIP = 2

# The following defines environment variables that allow
# us to access a subset of TPU visible chips.
#
# See: https://github.com/google/jax/issues/14977 for an example/more details.
TPU_CHIPS_PER_HOST_BOUNDS_ENV_VAR = "TPU_CHIPS_PER_HOST_BOUNDS"
TPU_CHIPS_PER_HOST_BOUNDS_1_CHIP_CONFIG = "1,1,1"
TPU_CHIPS_PER_HOST_BOUNDS_2_CHIP_CONFIG = "1,2,1"

TPU_HOST_BOUNDS_ENV_VAR = "TPU_HOST_BOUNDS"
TPU_SINGLE_HOST_BOUNDS = "1,1,1"


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
    def get_current_node_accelerator_type() -> Optional[str]:
        """Attempt to detect the TPU accelerator type.

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
            A string representing the TPU accelerator type,
            e.g. "TPU-V2", "TPU-V3", "TPU-V4" if applicable, else None.
        """

        def tpu_accelerator_type_to_ray_accelerator_type(
            tpu_accelerator_type: str,
        ) -> Optional[str]:
            if TPUAcceleratorManager.is_valid_tpu_accelerator_type(
                tpu_accelerator_type
            ):
                return "TPU-" + str(tpu_accelerator_type.split("-")[0]).upper()
            else:
                return None

        ray_accelerator_type = None
        # GKE-based check
        tpu_accelerator_type = os.getenv(GKE_TPU_ACCELERATOR_TYPE_ENV_VAR, None)
        if tpu_accelerator_type is not None:
            ray_accelerator_type = tpu_accelerator_type_to_ray_accelerator_type(
                tpu_accelerator_type
            )
            if ray_accelerator_type is None:
                logger.info(
                    "While trying to autodetect a TPU type and "
                    f"parsing {GKE_TPU_ACCELERATOR_TYPE_ENV_VAR}, "
                    f"received malformed accelerator_type: {tpu_accelerator_type}"
                )
        else:
            # GCE-based VM check
            try:
                tpu_accelerator_type_request = requests.get(
                    GCE_TPU_ACCELERATOR_ENDPOINT,
                    headers=GCE_TPU_HEADERS,
                    timeout=30,
                )
                if (
                    tpu_accelerator_type_request.status_code == 200
                    and tpu_accelerator_type_request.text
                ):
                    ray_accelerator_type = tpu_accelerator_type_to_ray_accelerator_type(
                        tpu_accelerator_type_request.text
                    )
                    if ray_accelerator_type is None:
                        logger.info(
                            "While trying to autodetect a TPU type, "
                            "the TPU GCE metadata "
                            "returned a malformed accelerator type: "
                            f"{tpu_accelerator_type_request.text}."
                        )
                else:
                    logger.info(
                        "While trying to autodetect a TPU type, "
                        "unable to poll TPU GCE metadata. Got "
                        f"status code: {tpu_accelerator_type_request.status_code} and "
                        f"content: {tpu_accelerator_type_request.text}"
                    )
            except requests.RequestException as e:
                logger.info(
                    "While trying to autodetect a TPU type, "
                    " unable to poll TPU GCE metadata: %s",
                    e,
                )

        if ray_accelerator_type is None:
            logging.info("Failed to auto-detect TPU type.")
        return ray_accelerator_type

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
        if num_visible_tpu_chips == TPU_NUM_CHIPS_PER_HOST:
            # Let the ML framework use the defaults
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
