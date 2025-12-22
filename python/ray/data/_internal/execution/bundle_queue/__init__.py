from __future__ import annotations

from .base import (
    BaseBundleQueue,
    SupportsDequeue,
    SupportsRebundling,
    SupportsRemoval,
)
from .bundler import BlockRefBundler
from .bundler_exact import StreamingRepartitionRefBundler
from .fifo import FIFOBundleQueue
from .hash_link import HashLinkedQueue
from .ordered import OrderedBundleQueue


class QueueWithRemoval(BaseBundleQueue, SupportsRemoval):
    pass


class QueueWithRebundling(BaseBundleQueue, SupportsRebundling):
    pass


def create_bundle_queue() -> QueueWithRemoval:
    return HashLinkedQueue()


__all__ = [
    "BaseBundleQueue",
    "create_bundle_queue",
    "SupportsRemoval",
    "HashLinkedQueue",
    "BlockRefBundler",
    "StreamingRepartitionRefBundler",
    "OrderedBundleQueue",
    "FIFOBundleQueue",
    "SupportsDequeue",
    "SupportsRebundling",
    "QueueWithRemoval",
    "QueueWithRebundling",
]
