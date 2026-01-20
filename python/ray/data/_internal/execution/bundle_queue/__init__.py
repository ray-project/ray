from __future__ import annotations

from .base import (
    BaseBundleQueue,
    QueueWithRemoval,
)
from .hash_link import HashLinkedQueue


def create_bundle_queue() -> QueueWithRemoval:
    return HashLinkedQueue()


__all__ = [
    "BaseBundleQueue",
    "create_bundle_queue",
    "QueueWithRemoval",
    "HashLinkedQueue",
]
