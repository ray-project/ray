"""Streaming utilities for Ray Data.

This module provides utilities for streaming datasources to integrate better
with Ray Data's streaming execution engine.
"""

from ray.data._internal.streaming.block_coalescer import BlockCoalescer
from ray.data._internal.streaming.partition_coordinator import (
    PartitionCoordinatorActor,
    PartitionLease,
)
from ray.data._internal.streaming.reader_actor import StreamingReaderActor

__all__ = [
    "BlockCoalescer",
    "PartitionCoordinatorActor",
    "PartitionLease",
    "StreamingReaderActor",
]
