import enum
import logging
import os
from typing import TYPE_CHECKING, Optional

from .ranker import DefaultRanker, Ranker

if TYPE_CHECKING:
    from ... import DataContext
    from .resource_manager import OpResourceAllocator, ResourceManager


logger = logging.getLogger(__name__)

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

    logger.debug(
        f"Using op resource allocator version: "
        f"{DEFAULT_USE_OP_RESOURCE_ALLOCATOR_VERSION!r}"
    )

    if DEFAULT_USE_OP_RESOURCE_ALLOCATOR_VERSION == OpResourceAllocatorVersion.V1:
        from .resource_manager import ReservationOpResourceAllocator

        reservation_ratio = resolve_reservation_ratio(data_context)
        if reservation_ratio != data_context.op_resource_reservation_ratio:
            logger.info(
                "Using CBO-derived op resource reservation ratio "
                f"{reservation_ratio:.2f} "
                f"(default {data_context.op_resource_reservation_ratio:.2f})"
            )
        return ReservationOpResourceAllocator(
            resource_manager,
            reservation_ratio=reservation_ratio,
            object_store_reservation_overshoot_ratio=(
                data_context.object_store_reservation_overshoot_ratio
            ),
            object_store_memory_pressure_fraction=(
                data_context.object_store_memory_pressure_fraction
            ),
        )

    else:
        raise ValueError(
            "Resource allocator version of "
            f"'{DEFAULT_USE_OP_RESOURCE_ALLOCATOR_VERSION}' is not supported"
        )


def resolve_reservation_ratio(data_context: "DataContext") -> float:
    """Returns the effective ``op_resource_reservation_ratio``.

    Precedence: explicit user configuration > CBO-derived ratio (only when
    ``enable_cost_based_optimization`` is on) > default.
    """
    ratio = data_context.op_resource_reservation_ratio
    if getattr(data_context, "_user_set_reservation_ratio", True):
        return ratio
    if not getattr(data_context, "enable_cost_based_optimization", False):
        return ratio
    derived = getattr(data_context, "_cbo_derived_reservation_ratio", None)
    if derived is None or not 0.0 <= derived <= 1.0:
        return ratio
    return derived


def create_ranker() -> Ranker:
    """Create a ranker instance based on environment and configuration."""
    return DefaultRanker()
