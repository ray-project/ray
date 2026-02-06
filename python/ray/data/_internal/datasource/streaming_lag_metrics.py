"""Lag metrics for streaming datasources.

This module provides a standardized interface for reporting lag metrics from
streaming datasources (Kafka, Kinesis, etc.) to enable lag-aware autoscaling.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass
class LagMetrics:
    """Lag metrics for streaming datasources.

    Used by datasources to report consumer lag to enable lag-aware autoscaling
    in UnboundedDataOperator.

    Attributes:
        total_lag: Total lag across all partitions (e.g., sum of Kafka offset lag).
        fetch_rate: Current fetch rate in records/second (optional).
        partitions: Number of partitions/shards (optional).
        per_partition_lag: Optional dict mapping partition_id -> lag (optional).
    """

    total_lag: int
    fetch_rate: Optional[float] = None
    partitions: Optional[int] = None
    per_partition_lag: Optional[dict[str, int]] = None
