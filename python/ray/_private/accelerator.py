import json
import os
import glob
import subprocess
import sys
import requests
import logging
from typing import Optional
from ray._private import ray_constants


def update_resources_with_accelerator_type(resources: dict):
    """Update the resources dictionary with the accelerator type and custom
    resources.

    Currently, we support AWS NeuronCore (neuron_cores /
    accelerator_type:aws-neuron-core) detection and configuration.

    Args:
        resources: Resources dictionary to be updated with
        accelerator type and custom resources.
    """
    _detect_and_configure_aws_neuron_core(resources)


def _detect_and_configure_aws_neuron_core(resources: dict):
    """Configuration and auto-detection of AWS NeuronCore accelerator type
    and number of NeuronCore (neuron_cores).

    If the number of NeuronCore is not specified in the resources, this
    function will try to detect the number of NeuronCore.

    If the number of NeuronCore is specified in the resources, this
    function will check if the number of NeuronCore is greater than the
    number of visible NeuronCore and raise an error if it is true.

    If the number of NeuronCore is greater than the number of visible
    NeuronCore, this function will raise an error.

    Lastly, update accelerator_type and neuron_cores in resources.

    Args:
        resources: Resources dictionary to be updated with
        NeuronCore accelerator type and custom resources(neuron_cores).

    Raises:
        ValueError: If the number of NeuronCore is greater than the number of
            visible NeuronCore.
    """
    import ray._private.ray_constants as ray_constants
    import ray._private.utils as utils

    # AWS NeuronCore detection and configuration
    # 1. Check if the user specified neuron_cores in resources
    neuron_cores = resources.get(ray_constants.NEURON_CORES, None)
    # 2. Check if the user specified NEURON_RT_VISIBLE_CORES
    neuron_core_ids = utils.get_aws_neuron_core_visible_ids()
    if (
        neuron_cores is not None
        and neuron_core_ids is not None
        and neuron_cores > len(neuron_core_ids)
    ):
        raise ValueError(
            f"Attempting to start raylet with {neuron_cores} "
            f"neuron cores, but NEURON_RT_VISIBLE_CORES contains "
            f"{neuron_core_ids}."
        )
    # 3. Auto-detect neuron_cores if not specified in resources
    if neuron_cores is None:
        neuron_cores = _autodetect_aws_neuron_cores()
        # Don't use more neuron cores than allowed by NEURON_RT_VISIBLE_CORES.
        if neuron_cores is not None and neuron_core_ids is not None:
            neuron_cores = min(neuron_cores, len(neuron_core_ids))
    if neuron_cores is not None:
        # 4. Update accelerator_type and neuron_cores with
        # number of neuron cores detected or configured.
        resources.update(
            {
                ray_constants.NEURON_CORES: neuron_cores,
                utils.get_neuron_core_constraint_name(): neuron_cores,
            }
        )


def _autodetect_aws_neuron_cores() -> Optional[int]:
    """
    Attempt to detect the number of Neuron cores on this machine.

    Returns:
        The number of Neuron cores if any were detected, otherwise None.
    """
    result = None
    if sys.platform.startswith("linux") and os.path.isdir("/opt/aws/neuron/bin/"):
        result = _get_neuron_core_count()
    return result


def _get_neuron_core_count() -> int:
    """Get the number of Neuron cores on a machine based on neuron_path.

    Returns:
        The number of Neuron cores on this machine (Default to 0).
    """
    neuron_path = "/opt/aws/neuron/bin/"
    nc_count: int = 0
    result = subprocess.run(
        [os.path.join(neuron_path, "neuron-ls"), "--json-output"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode == 0 and result.stdout:
        json_out = json.loads(result.stdout)
        for neuron_device in json_out:
            nc_count += neuron_device.get("nc_count", 0)
    return nc_count


def autodetect_num_tpus() -> int:
    """Attempt to detect the number of TPUs on this machine.

    TPU chips are represented as devices within `/dev/`, either as
    `/dev/accel*` or `/dev/vfio/*`.

    Returns:
        The number of TPUs if any were detected, otherwise 0.
    """
    accel_files = glob.glob("/dev/accel*")
    if accel_files:
        return len(accel_files)

    vfio_entries = os.listdir("/dev/vfio")
    numeric_entries = [int(entry) for entry in vfio_entries if entry.isdigit()]
    return len(numeric_entries)


def autodetect_tpu_version() -> Optional[str]:
    """Attempt to detect the TPU version.

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
        A string representing the TPU version,
        e.g. "V2", "V3", "V4" if applicable, else None.

    """

    def accelerator_type_to_version(accelerator_type: str) -> str:
        return str(accelerator_type.split("-")[0]).upper()

    # GKE-based check
    accelerator_type = os.getenv(
        ray_constants.RAY_GKE_TPU_ACCELERATOR_TYPE_ENV_VAR, None
    )
    if accelerator_type is not None:
        return accelerator_type_to_version(accelerator_type)

    # GCE-based VM check
    try:
        accelerator_type_request = requests.get(
            ray_constants.RAY_GCE_TPU_ACCELERATOR_ENDPOINT,
            headers=ray_constants.RAY_GCE_TPU_HEADERS,
        )
        if accelerator_type_request.status_code == 200:
            return accelerator_type_to_version(accelerator_type_request.text)
    except requests.RequestException as e:
        logging.info("Unable to poll TPU GCE metadata: %s", e)

    return None
