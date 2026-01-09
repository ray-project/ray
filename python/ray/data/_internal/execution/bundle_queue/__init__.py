from __future__ import annotations

from .base import (
    BaseBundleQueue,
    SupportsRemoval,
)
from .hash_link import HashLinkedQueue


class QueueWithRemoval(BaseBundleQueue, SupportsRemoval):
    pass


def create_bundle_queue() -> QueueWithRemoval:
    return HashLinkedQueue()


__all__ = [
    "BaseBundleQueue",
    "create_bundle_queue",
    "SupportsRemoval",
    "HashLinkedQueue",
    "QueueWithRemoval",
]
