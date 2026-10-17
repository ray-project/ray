from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

# https://github.com/ray-project/ray/issues/54868
# Ray no longer overrides accelerator ids environment variables when the number
# of accelerators is zero. For example, if a user sets `num_gpus=0` in
# `ray.init()`, the environment variable `CUDA_VISIBLE_DEVICES` will not be
# set to an empty string.
#
# Set RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=1 to restore the old behavior where
# Ray would override accelerator env vars even when zero accelerators are assigned.
#
RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO_ENV_VAR = "RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"


class AcceleratorManager(ABC):
    """This class contains all the functions needed for supporting
    an accelerator family in Ray."""

    @staticmethod
    @abstractmethod
    def get_resource_name() -> str:
        """Get the name of the resource representing this accelerator family.

        Returns:
            The resource name: e.g., the resource name for NVIDIA GPUs is "GPU"
        """

    @staticmethod
    @abstractmethod
    def get_visible_accelerator_ids_env_var() -> Optional[str]:
        """Get the env var that sets the ids of visible accelerators of this family.

        Returns:
            The env var for setting visible accelerator ids: e.g.,
                CUDA_VISIBLE_DEVICES for NVIDIA GPUs. Returns None for accelerator
                families that have no visible-devices env var (e.g. Apple Silicon
                MPS, which exposes a single device that cannot be selected or hidden).
        """

    @staticmethod
    @abstractmethod
    def get_current_node_num_accelerators() -> int:
        """Get the total number of accelerators of this family on the current node.

        Returns:
            The detected total number of accelerators of this family.
            Return 0 if the current node doesn't contain accelerators of this family.
        """

    @staticmethod
    @abstractmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Get the type of the accelerator of this family on the current node.

        Currently Ray only supports single accelerator type of
        an accelerator family on each node.

        The result should only be used when get_current_node_num_accelerators() > 0.

        Returns:
            The detected accelerator type of this family: e.g., H100 for NVIDIA GPU.
            Return None if it's unknown or the node doesn't have
            accelerators of this family.
        """

    @staticmethod
    @abstractmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        """Get any additional resources required for the current node.

        In case a particular accelerator type requires considerations for
        additional resources (e.g. for TPUs, providing the TPU pod type and
        TPU name), this function can be used to provide the
        additional logical resources.

        Returns:
            A dictionary representing additional resources that may be
            necessary for a particular accelerator type.
        """

    @staticmethod
    @abstractmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        """Validate the resource request quantity of this accelerator resource.

        Args:
            quantity: The resource request quantity to be validated.

        Returns:
            (valid, error_message) tuple: the first element of the tuple
                indicates whether the given quantity is valid or not,
                the second element is the error message
                if the given quantity is invalid.
        """

    @staticmethod
    @abstractmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        """Get the ids of accelerators of this family that are visible to the current process.

        Returns:
            The list of visiable accelerator ids.
            Return None if all accelerators are visible.
        """

    @staticmethod
    @abstractmethod
    def set_current_process_visible_accelerator_ids(ids: List[str]) -> None:
        """Set the ids of accelerators of this family that are visible to the current process.

        Args:
            ids: The ids of visible accelerators of this family.
        """

    @staticmethod
    def get_ec2_instance_num_accelerators(
        instance_type: str, instances: dict
    ) -> Optional[int]:
        """Get the number of accelerators of this family on ec2 instance with given type.

        Args:
            instance_type: The ec2 instance type.
            instances: Map from ec2 instance type to instance metadata returned by
                ec2 `describe-instance-types`.

        Returns:
            The number of accelerators of this family on the ec2 instance
            with given type.
            Return None if it's unknown.
        """
        return None

    @staticmethod
    def get_ec2_instance_accelerator_type(
        instance_type: str, instances: dict
    ) -> Optional[str]:
        """Get the accelerator type of this family on ec2 instance with given type.

        Args:
            instance_type: The ec2 instance type.
            instances: Map from ec2 instance type to instance metadata returned by
                ec2 `describe-instance-types`.

        Returns:
            The accelerator type of this family on the ec2 instance with given type.
            Return None if it's unknown.
        """
        return None

    @staticmethod
    def get_current_node_accelerator_labels() -> Optional[Dict[str, str]]:
        """Get accelerator related Ray node labels of the curent node.

        Returns:
            A dictionary mapping accelerator related label keys to values.
        """
        return None

    @staticmethod
    def get_k8s_node_group_accelerator_labels(
        node_selector: Dict[str, str],
        num_accelerators: int,
        num_of_hosts: int = 1,
    ) -> Optional[Dict[str, str]]:
        """Get accelerator-related Ray node labels for a Kubernetes node group
        from its pod nodeSelector, accelerator count, and host count.

        Args:
            node_selector: The Kubernetes pod spec nodeSelector dictionary.
            num_accelerators: The number of accelerators of this family per pod.
            num_of_hosts: The number of hosts (workers) per replica in the node group.

        Returns:
            A dictionary mapping accelerator-related label keys to static or
            placeholder values, or None if not applicable.
        """
        return None

    @staticmethod
    def get_multi_host_domain_label_key() -> Optional[str]:
        """Get the Ray node label key that identifies a multi-host accelerator
        domain or slice replica (e.g., "ray.io/tpu-slice-name" for TPUs).

        Returns:
            The domain label key string, or None if not applicable.
        """
        return None

    @staticmethod
    def get_multi_host_worker_id_label_key() -> Optional[str]:
        """Get the Ray node label key that identifies the 0-indexed host rank
        within a multi-host accelerator domain (e.g., "ray.io/tpu-worker-id"
        for TPUs).

        Returns:
            The worker ID label key string, or None if not applicable.
        """
        return None
