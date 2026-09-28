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
from .object_store_aware import ObjectStoreAwareBundleQueue
from .reordering import ReorderingBundleQueue
from .thread_safe import ThreadSafeBundleQueue


def create_bundle_queue() -> QueueWithRemoval:
    from ray._common.utils import env_bool
    from ray.data.context import DataContext

    if (
        env_bool("RAY_DATA_ENABLE_OBJECT_STORE_AWARE_BUNDLE_QUEUES", True)
        # The object-store-aware queue reorders inputs to serve bundles still
        # resident in the object store first, which breaks order preservation.
        and not DataContext.get_current().execution_options.preserve_order
    ):
        return ObjectStoreAwareBundleQueue()
    return HashLinkedQueue()


__all__ = [
    "BaseBundleQueue",
    "create_bundle_queue",
    "HashLinkedQueue",
    "ObjectStoreAwareBundleQueue",
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
