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
    def detect_num_accelerators() -> int:
        """Auto detect the number of accelerators on this machine."""

    @staticmethod
    @abstractmethod
    def detect_visible_accelerator_ids() -> Optional[List[str]]:
        """Auto detect the ids of accelerators visible to Ray."""

    @staticmethod
    @abstractmethod
    def detect_accelerator_type() -> Optional[str]:
        """Auto detect the type of the accelerator on this machine."""

    @staticmethod
    @abstractmethod
    def validate_resource_request_quantity(quantity: float) -> Optional[str]:
        """Validate the resource request quantity of this accelerator resource."""

    @staticmethod
    @abstractmethod
    def set_visible_accelerator_ids(ids: List[str]) -> None:
        """Set the ids of accelerators visible to the Ray task or actor."""
