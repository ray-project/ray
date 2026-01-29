from __future__ import annotations

from .base import (
    BaseBundleQueue,
    QueueWithRemoval,
)
from .bundler import EstimateSize, ExactMultipleSize, RebundleQueue
from .fifo import FIFOBundleQueue
from .hash_link import HashLinkedQueue
from .reordering import ReorderingBundleQueue


def create_bundle_queue() -> QueueWithRemoval:
    return HashLinkedQueue()


__all__ = [
    "BaseBundleQueue",
    "create_bundle_queue",
    "SupportsRemoval",
    "HashLinkedQueue",
    "RebundleQueue",
    "EstimateSize",
    "ReorderingBundleQueue",
    "FIFOBundleQueue",
    "ExactMultipleSize",
    "QueueWithRemoval",
]
