import os
import sys
import json
import logging
import subprocess
from typing import Optional, List

from ray._private.accelerators.accelerator import Accelerator

logger = logging.getLogger(__name__)

NEURON_RT_VISIBLE_CORES_ENV_VAR = "NEURON_RT_VISIBLE_CORES"
NOSET_AWS_NEURON_RT_VISIBLE_CORES_ENV_VAR = (
    "RAY_EXPERIMENTAL_NOSET_NEURON_RT_VISIBLE_CORES"
)


class NeuronAccelerator(Accelerator):
    """AWS Inferentia and Trainium accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "neuron_cores"

    @staticmethod
    def is_available() -> bool:
        return NeuronAccelerator.get_num_acclerators() > 0

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return NEURON_RT_VISIBLE_CORES_ENV_VAR

    @staticmethod
    def get_visible_accelerator_ids() -> Optional[List[str]]:
        neuron_visible_cores = os.environ.get(NEURON_RT_VISIBLE_CORES_ENV_VAR, None)

        if neuron_visible_cores is None:
            return None

        if neuron_visible_cores == "":
            return []

        return list(neuron_visible_cores.split(","))

    @staticmethod
    def get_num_acclerators() -> int:
        """
        Attempt to detect the number of Neuron cores on this machine.

        Returns:
            The number of Neuron cores if any were detected, otherwise 0.
        """
        nc_count: int = 0
        neuron_path = "/opt/aws/neuron/bin/"
        if sys.platform.startswith("linux") and os.path.isdir(neuron_path):
            result = subprocess.run(
                [os.path.join(neuron_path, "neuron-ls"), "--json-output"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if result.returncode == 0 and result.stdout:
                neuron_devices = json.loads(result.stdout)
                for neuron_device in neuron_devices:
                    nc_count += neuron_device.get("nc_count", 0)
        return nc_count

    @staticmethod
    def get_accelerator_type() -> Optional[str]:
        import ray

        return ray.util.accelerators.accelerators.AWS_NEURON_CORE

    @staticmethod
    def validate_resource_request_quantity(quantity: float) -> Optional[str]:
        if isinstance(quantity, float) and not quantity.is_integer():
            return (
                f"{NeuronAccelerator.get_resource_name()} resource quantity"
                " must be whole numbers. "
                f"The specified quantity {quantity} is invalid."
            )
        else:
            return None

    @staticmethod
    def set_visible_accelerator_ids(visible_neuron_core_ids: List[str]) -> None:
        """Set the NEURON_RT_VISIBLE_CORES environment variable based on
        given visible_neuron_core_ids.

        Args:
            visible_neuron_core_ids (List[str]): List of int representing core IDs.
        """
        if os.environ.get(NOSET_AWS_NEURON_RT_VISIBLE_CORES_ENV_VAR):
            return

        os.environ[NEURON_RT_VISIBLE_CORES_ENV_VAR] = ",".join(
            [str(i) for i in visible_neuron_core_ids]
        )
