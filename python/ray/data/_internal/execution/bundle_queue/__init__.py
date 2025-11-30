from __future__ import annotations

from .base import (
    BaseBundleQueue,
    SupportsDeque,
    SupportsIndexing,
    SupportsRebundling,
)
from .bundler import BlockRefBundler
from .bundler_exact import StreamingRepartitionRefBundler
from .fifo import FIFOBundleQueue
from .hash_link import HashLinkedQueue
from .ordered import OrderedBundleQueue


class QueueWithIndexing(BaseBundleQueue, SupportsIndexing):
    pass


class QueueWithRebundling(BaseBundleQueue, SupportsRebundling):
    pass


def create_bundle_queue() -> QueueWithIndexing:
    return HashLinkedQueue()


__all__ = [
    "BaseBundleQueue",
    "create_bundle_queue",
    "SupportsIndexing",
    "HashLinkedQueue",
    "BlockRefBundler",
    "StreamingRepartitionRefBundler",
    "OrderedBundleQueue",
    "FIFOBundleQueue",
    "SupportsDeque",
    "SupportsRebundling",
    "QueueWithIndexing",
    "QueueWithRebundling",
    "DequeWithIndexing",
]
