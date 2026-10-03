from __future__ import annotations

from .base import (
    BaseBundleQueue,
    QueueWithRemoval,
)
from .bundler import (
    EstimateBytes,
    EstimateSize,
    ExactMultipleSize,
    PendingBundleState,
    RebundleQueue,
)
from .fifo import FIFOBundleQueue
from .hash_link import HashLinkedQueue
from .reordering import ReorderingBundleQueue
from .resident_first import ResidentFirstBundleQueue
from .thread_safe import ThreadSafeBundleQueue


def create_bundle_queue(*, preserve_order: bool = False) -> QueueWithRemoval:
    """Create the queue used to buffer bundles between and inside operators.

    Args:
        preserve_order: Whether the executor must keep bundles in input order.
            Pass the executor's ``ExecutionOptions.preserve_order`` rather than
            reading ``DataContext``.

    Returns:
        An ``ResidentFirstBundleQueue`` unless disabled or order must be
        preserved, in which case a ``HashLinkedQueue``.
    """
    from ray._common.utils import env_bool

    if (
        env_bool("RAY_DATA_ENABLE_RESIDENT_FIRST_BUNDLE_QUEUES", True)
        # The resident-first queue reorders inputs to serve bundles still
        # resident in the object store first, which breaks order preservation.
        and not preserve_order
    ):
        return ResidentFirstBundleQueue()
    return HashLinkedQueue()


__all__ = [
    "BaseBundleQueue",
    "create_bundle_queue",
    "HashLinkedQueue",
    "ResidentFirstBundleQueue",
    "RebundleQueue",
    "EstimateBytes",
    "EstimateSize",
    "ReorderingBundleQueue",
    "FIFOBundleQueue",
    "ExactMultipleSize",
    "PendingBundleState",
    "QueueWithRemoval",
    "ThreadSafeBundleQueue",
]
