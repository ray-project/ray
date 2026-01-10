from __future__ import annotations

from .base import (
    BaseBundleQueue,
    SupportsRemoval,
)
from .fifo import FIFOBundleQueue
from .hash_link import HashLinkedQueue
from .ordered import OrderedBundleQueue


class QueueWithRemoval(BaseBundleQueue, SupportsRemoval):
    pass


def create_bundle_queue() -> QueueWithRemoval:
    return HashLinkedQueue()


__all__ = [
    "BaseBundleQueue",
    "create_bundle_queue",
    "SupportsRemoval",
    "HashLinkedQueue",
    "OrderedBundleQueue",
    "FIFOBundleQueue",
    "QueueWithRemoval",
]
