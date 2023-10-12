from abc import ABC, abstractmethod
from typing import Optional, List

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class Accelerator(ABC):
    @staticmethod
    @abstractmethod
    def is_available() -> bool:
        """Detect if the hardware is available."""

    @staticmethod
    @abstractmethod
    def get_resource_name() -> str:
        """Get the name of the resource represents this accelerator."""

    @staticmethod
    @abstractmethod
    def get_visible_accelerator_ids_env_var() -> str:
        """Get the env var that sets the ids of visible accelerators."""

    @staticmethod
    @abstractmethod
    def get_num_acclerators() -> int:
        """Auto detect the number of accelerators on this machine."""

    @staticmethod
    @abstractmethod
    def get_visible_accelerator_ids() -> Optional[List[str]]:
        """Auto detect the ids of accelerators visible to Ray."""

    @staticmethod
    @abstractmethod
    def get_accelerator_type() -> Optional[str]:
        """Auto detect the type of the accelerator on this machine."""

    @staticmethod
    @abstractmethod
    def validate_resource_request_quantity(quantity: float) -> Optional[str]:
        """Validate the resource request quantity of this accelerator resource."""

    @staticmethod
    @abstractmethod
    def set_visible_accelerator_ids(ids: List[str]) -> None:
        """Set the ids of accelerators visible to the Ray task or actor."""

    @staticmethod
    def get_ec2_num_accelerators(instance_type: str, instances: dict) -> Optional[int]:
        """Get the number of accelerators of this aws ec2 instance type."""
        return None

    @staticmethod
    def get_ec2_accelerator_type(instance_type: str, instances: dict) -> Optional[str]:
        """Get the accelerator type of this aws ec2 instance type."""
        return None
