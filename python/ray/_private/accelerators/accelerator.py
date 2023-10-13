from abc import ABC, abstractmethod
from typing import Optional, List

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class Accelerator(ABC):
    @staticmethod
    @abstractmethod
    def is_available() -> bool:
        """Detect if the accelerator is available on this node."""

    @staticmethod
    @abstractmethod
    def get_resource_name() -> str:
        """Get the name of the resource representing this accelerator."""

    @staticmethod
    @abstractmethod
    def get_visible_accelerator_ids_env_var() -> str:
        """Get the env var that sets the ids of visible accelerators."""

    @staticmethod
    @abstractmethod
    def get_num_accelerators() -> int:
        """Get the total number of accelerators on this node."""

    @staticmethod
    @abstractmethod
    def get_visible_accelerator_ids() -> Optional[List[str]]:
        """Get the ids of accelerators visible to Ray."""

    @staticmethod
    @abstractmethod
    def get_accelerator_type() -> Optional[str]:
        """Get the type of the accelerator on this node.

        The result should only be used when get_num_accelerators() > 0.

        Return None if it's unknown.
        """

    @staticmethod
    @abstractmethod
    def validate_resource_request_quantity(quantity: float) -> Optional[str]:
        """Validate the resource request quantity of this accelerator resource.

        Return error message if the quantity is invalid.
        """

    @staticmethod
    @abstractmethod
    def set_visible_accelerator_ids(ids: List[str]) -> None:
        """Set the ids of accelerators visible to the Ray task or actor."""

    @staticmethod
    def get_ec2_num_accelerators(instance_type: str, instances: dict) -> Optional[int]:
        """Get the number of accelerators of this aws ec2 instance type.

        Return None if it's unknown.
        """
        return None

    @staticmethod
    def get_ec2_accelerator_type(instance_type: str, instances: dict) -> Optional[str]:
        """Get the accelerator type of this aws ec2 instance type.

        Return None if it's unknown.
        """
        return None
