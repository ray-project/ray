from __future__ import annotations

from .base import (
    BaseBundleQueue,
    QueueWithRemoval,
)
from .fifo import FIFOBundleQueue
from .hash_link import HashLinkedQueue
from .ordered import ReorderingBundleQueue


def create_bundle_queue() -> QueueWithRemoval:
    return HashLinkedQueue()


__all__ = [
    "BaseBundleQueue",
    "create_bundle_queue",
    "ReorderingBundleQueue",
    "FIFOBundleQueue",
    "QueueWithRemoval",
    "HashLinkedQueue",
]
