import enum
import os
from typing import TYPE_CHECKING, Optional

from .ranker import DefaultRanker, Ranker

if TYPE_CHECKING:
    from ... import DataContext
    from .resource_manager import OpResourceAllocator, ResourceManager


DEFAULT_USE_OP_RESOURCE_ALLOCATOR_VERSION = os.environ.get(
    "RAY_DATA_USE_OP_RESOURCE_ALLOCATOR_VERSION", "V1"
)


class OpResourceAllocatorVersion(str, enum.Enum):
    V1 = "V1"  # ReservationOpResourceAllocator


def create_resource_allocator(
    resource_manager: "ResourceManager",
    data_context: "DataContext",
) -> Optional["OpResourceAllocator"]:
    """Creates ``OpResourceAllocator`` instances.``"""

    if not data_context.op_resource_reservation_enabled:
        # This is a historical kill-switch to disable resource allocator, that
        # will be soon deprecated and removed.
        return None

    if DEFAULT_USE_OP_RESOURCE_ALLOCATOR_VERSION == OpResourceAllocatorVersion.V1:
        from .resource_manager import ReservationOpResourceAllocator

        return ReservationOpResourceAllocator(
            resource_manager,
            reservation_ratio=data_context.op_resource_reservation_ratio,
        )

    else:
        raise ValueError(
            "Resource allocator version of "
            f"'{DEFAULT_USE_OP_RESOURCE_ALLOCATOR_VERSION}' is not supported"
        )


def create_ranker() -> Ranker:
    """Create a ranker instance based on environment and configuration."""
    return DefaultRanker()
