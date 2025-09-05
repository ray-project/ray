"""Unbound metrics collection.

Simple metrics collection for unbound datasources without overcomplicating.
"""

from datetime import datetime
from typing import Any, Dict

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class UnboundMetrics:
    """Simple metrics for unbound sources."""

    def __init__(self):
        """Initialize unbound metrics."""
        self.records_read = 0
        self.bytes_read = 0
        self.read_errors = 0
        self.start_time = datetime.now()

    def record_read(self, record_count: int, byte_count: int) -> None:
        """Record successful read."""
        self.records_read += record_count
        self.bytes_read += byte_count

    def record_error(self) -> None:
        """Record read error."""
        self.read_errors += 1

    def get_throughput(self) -> Dict[str, float]:
        """Calculate throughput metrics."""
        duration = (datetime.now() - self.start_time).total_seconds()
        if duration > 0:
            return {
                "records_per_second": self.records_read / duration,
                "bytes_per_second": self.bytes_read / duration,
                "error_rate": self.read_errors / max(1, self.records_read),
            }
        return {"records_per_second": 0, "bytes_per_second": 0, "error_rate": 0}

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        stats = self.get_throughput()
        stats.update(
            {
                "total_records": self.records_read,
                "total_bytes": self.bytes_read,
                "total_errors": self.read_errors,
                "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
            }
        )
        return stats
