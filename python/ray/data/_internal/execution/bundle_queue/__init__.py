from __future__ import annotations

from .base import (
    BaseBundleQueue,
    QueueWithRemoval,
)
from .fifo import FIFOBundleQueue
from .hash_link import HashLinkedQueue
from .reordering import ReorderingBundleQueue
from .thread_safe import ThreadSafeBundleQueue


def create_bundle_queue() -> QueueWithRemoval:
    return HashLinkedQueue()


__all__ = [
    "BaseBundleQueue",
    "create_bundle_queue",
    "ReorderingBundleQueue",
    "FIFOBundleQueue",
    "QueueWithRemoval",
    "HashLinkedQueue",
    "ThreadSafeBundleQueue",
]
