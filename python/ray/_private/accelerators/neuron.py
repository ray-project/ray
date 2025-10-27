import json
import logging
import os
import subprocess
import sys
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

NEURON_RT_VISIBLE_CORES_ENV_VAR = "NEURON_RT_VISIBLE_CORES"
NOSET_AWS_NEURON_RT_VISIBLE_CORES_ENV_VAR = (
    "RAY_EXPERIMENTAL_NOSET_NEURON_RT_VISIBLE_CORES"
)

# https://awsdocs-neuron.readthedocs-hosted.com/en/latest/general/arch/neuron-hardware/inf2-arch.html#aws-inf2-arch
# https://awsdocs-neuron.readthedocs-hosted.com/en/latest/general/arch/neuron-hardware/trn1-arch.html#aws-trn1-arch
# Subject to removal after the information is available via public API
AWS_NEURON_INSTANCE_MAP = {
    "trn1.2xlarge": 2,
    "trn1.32xlarge": 32,
    "trn1n.32xlarge": 32,
    "inf2.xlarge": 2,
    "inf2.8xlarge": 2,
    "inf2.24xlarge": 12,
    "inf2.48xlarge": 24,
}


class NeuronAcceleratorManager(AcceleratorManager):
    """AWS Inferentia and Trainium accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "neuron_cores"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return NEURON_RT_VISIBLE_CORES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        neuron_visible_cores = os.environ.get(
            NeuronAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )

        if neuron_visible_cores is None:
            return None

        if neuron_visible_cores == "":
            return []

        return list(neuron_visible_cores.split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
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
    def get_current_node_accelerator_type() -> Optional[str]:
        from ray.util.accelerators import AWS_NEURON_CORE

        return AWS_NEURON_CORE

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        if isinstance(quantity, float) and not quantity.is_integer():
            return (
                False,
                f"{NeuronAcceleratorManager.get_resource_name()} resource quantity"
                " must be whole numbers. "
                f"The specified quantity {quantity} is invalid.",
            )
        else:
            return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_neuron_core_ids: List[str],
    ) -> None:
        """Set the NEURON_RT_VISIBLE_CORES environment variable based on
        given visible_neuron_core_ids.

        Args:
            visible_neuron_core_ids (List[str]): List of int representing core IDs.
        """
        if os.environ.get(NOSET_AWS_NEURON_RT_VISIBLE_CORES_ENV_VAR):
            return

        os.environ[
            NeuronAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_neuron_core_ids])

    @staticmethod
    def get_ec2_instance_num_accelerators(
        instance_type: str, instances: dict
    ) -> Optional[int]:
        # TODO: AWS SDK (public API) doesn't yet expose the NeuronCore
        # information. It will be available (work-in-progress)
        # as xxAcceleratorInfo in InstanceTypeInfo.
        # https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_InstanceTypeInfo.html
        # See https://github.com/ray-project/ray/issues/38473
        return AWS_NEURON_INSTANCE_MAP.get(instance_type.lower(), None)

    @staticmethod
    def get_ec2_instance_accelerator_type(
        instance_type: str, instances: dict
    ) -> Optional[str]:
        from ray.util.accelerators import AWS_NEURON_CORE

        return AWS_NEURON_CORE
