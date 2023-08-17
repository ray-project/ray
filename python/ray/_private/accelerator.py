import json
import os
import subprocess
import sys
from typing import Optional


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
