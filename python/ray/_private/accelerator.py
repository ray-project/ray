import json
import os
import glob
import subprocess
import sys
import requests
import logging
import ray._private.ray_constants as ray_constants
import ray._private.utils as utils
import re
from typing import Callable, Iterable, Optional


def update_resources_with_accelerator_type(resources: dict):
    """Update the resources dictionary with the accelerator type and custom
    resources.

    Currently, we support detection and configuration of:
    - AWS NeuronCore (neuron_cores / accelerator_type:aws-neuron-core)
    - Google Cloud TPUs (TPU / accelerator_type:TPU-V*)

    Args:
        resources: Resources dictionary to be updated with
        accelerator type and custom resources.
    """
    # Autodetect AWS NeuronCore
    _detect_and_configure_custom_accelerator(
        resources=resources,
        accelerator_key=ray_constants.NEURON_CORES,
        get_accelerator_type=utils.get_neuron_core_constraint_name,
        get_visible_ids=utils.get_aws_neuron_core_visible_ids,
        autodetect_accelerators=_autodetect_aws_neuron_cores,
        visible_devices_env_variable=ray_constants.NEURON_RT_VISIBLE_CORES_ENV_VAR,
    )
    # Autodetect Google Cloud TPUs
    _detect_and_configure_custom_accelerator(
        resources=resources,
        accelerator_key=ray_constants.TPU,
        get_accelerator_type=_autodetect_tpu_version,
        get_visible_ids=utils.get_tpu_visible_chips,
        autodetect_accelerators=_autodetect_num_tpus,
        visible_devices_env_variable=ray_constants.TPU_VISIBLE_CHIPS_ENV_VAR,
    )
    if ray_constants.TPU in resources:
        _detect_and_configure_tpu_pod(resources=resources)


def _detect_and_configure_custom_accelerator(
    resources: dict,
    accelerator_key: str,
    get_accelerator_type: Callable[[None], str],
    get_visible_ids: Callable[[None], Optional[Iterable[str]]],
    autodetect_accelerators: Callable[[None], int],
    visible_devices_env_variable: str,
):
    """Configure and autodetect custom accelerators counts and types.

    If the number of accelerators is not specified in the resources, this
    function will try to detect the number of accelerators.

    If the number of accelerators is specified in the resources, this
    function will check if the number of accelerators is greater than the
    number of visible devices and raise an error if it is true.

    If the number of accelerators is greater than the number of visible
    devices, this function will raise an error.

    Lastly, update accelerator_type and number of accelerators in resources.

    Args:
        resources: Resources dictionary to be updated with the custom
            accelerator type and resource count.
        accelerator_key: The key used to access the number of accelerators
            within `resources`. This can be:
            ray_constants.NEURON_CORES or ray_constants.TPU
        get_accelerator_type: A function that returns the name of the accelerator
            type. This is the unique identifier of the accelerator version, e.g.
            ray_constants.AWS_NEURON_CORE or ray_constants.GOOGLE_TPU_V4.
        get_visible_ids: A function that returns the visible IDs specified by the user.
            This is typically controlled by an environment variable, e.g.
            NEURON_RT_VISIBLE_CORES or TPU_VISIBLE_CHIPS.
        autodetected_accelerators: A function that returns the number of
            accelerators autodetected on the machine.
        visible_devices_env_variable: The environment variable a user uses
            to specify which devices are visible.

    Raises:
        ValueError: If the number of requested accelerator chips is greater
            than the number of visible accelerator chips.
    """
    # Custom accelerator detection and configuration
    # 1. Check if the user specified accelerator_count in resources
    accelerator_count = resources.get(accelerator_key, None)
    # 2. Check if the user specified visible cores/chips (within `visible_ids`)
    visible_ids = get_visible_ids()
    if (
        accelerator_count is not None
        and visible_ids is not None
        and accelerator_count > len(visible_ids)
    ):
        raise ValueError(
            f"Attempting to start raylet with {accelerator_count} "
            f"{accelerator_key}, but f{visible_devices_env_variable} "
            f"contains {visible_ids}."
        )
    # 3. Auto-detect accelerator_count if not specified in resources
    if accelerator_count is None:
        accelerator_count = autodetect_accelerators()
        # Don't use more resources than allowed by the user's pre-set values.
        if accelerator_count is not None and visible_ids is not None:
            accelerator_count = min(accelerator_count, len(visible_ids))
    if accelerator_count is not None and accelerator_count > 0:
        # 4. Update accelerator_type and accelerator_count with
        # number of accelerators detected or configured.
        resources.update(
            {
                accelerator_key: accelerator_count,
                get_accelerator_type(): accelerator_count,
            }
        )


def _detect_and_configure_tpu_pod(resources: dict):
    """Configure resources specific to TPU pods.

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

    @ray.remote(resources={"tpu-v4-16"})
    def run_jax_fn(executable):
        # Note this will execute on worker 0
        tpu_name = ray.util.accelerators.tpu.get_tpu_pod_name()
        num_workers = ray.util.accelerators.tpu.get_tpu_num_workers()

        tpu_executable = executable.options(resources={"TPU": 4, tpu_name: 1})
        return [tpu_executable.remote() for _ in range(num_workers)]

    """
    tpu_id = get_tpu_id()
    worker_id = get_tpu_worker_id()
    accelerator_type = get_tpu_accelerator_type()
    if tpu_id and worker_id is not None and accelerator_type:
        pod_resource_name = f"TPU-{accelerator_type}"
        # Add the name of the TPU ID to the resource.
        resources[tpu_id] = 1
        # Only add in the TPU pod resource type to worker 0.
        if worker_id == 0:
            resources[pod_resource_name] = 1
        else:
            if pod_resource_name in resources:
                resources.pop(pod_resource_name)
    else:
        logging.info(
            "Failed to configure TPU pod. Got: "
            "tpu_id: %s, worker_id: %s, accelerator_type: %s",
            tpu_id,
            worker_id,
            accelerator_type,
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


def _autodetect_num_tpus() -> int:
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
        logging.info("Failed to detect number of TPUs: %s", e)
        return 0


def _autodetect_tpu_version() -> Optional[str]:
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
        e.g. "TPU-V2", "TPU-V3", "TPU-V4" if applicable, else None.

    """
    accelerator_type = get_tpu_accelerator_type()
    if accelerator_type:
        return "TPU-" + str(accelerator_type.split("-")[0]).upper()
    else:
        logging.info("Failed to auto-detect TPU type.")
        return None


def valid_tpu_accelerator_type(accelerator_type: str):
    """Assert that the inputed accelerator_type is formatted correctly.

    The accelerator_type field follows a form of v{generation}-{cores/chips}.

    See the following for more information:
    https://cloud.google.com/sdk/gcloud/reference/compute/tpus/tpu-vm/accelerator-types/describe

    Args:
        accelerator_type: The string representation of the accelerator type
            to be asserted for validity.

    Raises:
        ValueError: If the provided accelerator_type is malformed.

    """
    expected_pattern = re.compile(r"^v\d+[a-zA-Z]*-\d+$")
    if not expected_pattern.match(accelerator_type):
        return False
    return True


def _get_tpu_metadata(key: str) -> str:
    """Poll and get TPU metadata."""
    try:
        accelerator_type_request = requests.get(
            os.path.join(ray_constants.RAY_GCE_TPU_METADATA_ENDPOINT, key),
            headers=ray_constants.RAY_GCE_TPU_HEADERS,
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
    return ""


def get_tpu_accelerator_type() -> str:
    """Get the TPU accelerator type if applicable, e.g. v4-16."""
    # GKE-based check
    accelerator_type = os.getenv(ray_constants.RAY_GKE_TPU_ACCELERATOR_TYPE_ENV_VAR, "")
    if not accelerator_type:
        # GCE-based VM check
        accelerator_type = _get_tpu_metadata(
            key=ray_constants.RAY_GCE_TPU_ACCELERATOR_KEY
        )
    if valid_tpu_accelerator_type(accelerator_type=accelerator_type):
        return accelerator_type
    logging.debug("Failed to get a valid accelerator type.")
    return ""


def get_tpu_id() -> str:
    """Return the name of the TPU pod that this worker node is a part of."""
    return _get_tpu_metadata(key=ray_constants.RAY_GCE_TPU_INSTANCE_ID_KEY)


def get_tpu_worker_id() -> Optional[int]:
    """Return the worker index of the TPU pod."""
    try:
        worker_id = _get_tpu_metadata(key=ray_constants.RAY_GCE_TPU_WORKER_ID_KEY)
        return int(worker_id)
    except ValueError as e:
        logging.debug("Could not get TPU worker id: %s", e)
        return None


def num_workers_in_tpu_pod() -> Optional[int]:
    """Return the total number of workers in a TPU pod."""
    accelerator_type = get_tpu_accelerator_type()
    if accelerator_type:
        version = accelerator_type.split("-")[0]
        num_chips_or_cores = int(accelerator_type.split("-")[1])
        if version in ray_constants.TPU_VERSIONS_WITH_MULTIPLE_CORES_PER_CHIP:
            return num_chips_or_cores // 8
        else:
            return num_chips_or_cores // 4
    else:
        logging.debug("Could not get num workers in TPU pod.")
        return None
