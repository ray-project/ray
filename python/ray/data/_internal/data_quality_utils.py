"""Data quality utilities for Ray Data check operations.

This module provides utilities for violation tracking, error handling, and metadata
management for data quality checks.
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Union

import ray

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Exception raised for data quality violations."""

    def __init__(
        self, message: str, violation_details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(message)
        self.violation_details = violation_details or {}
        self.timestamp = time.time()


class ViolationTracker:
    """Thread-safe violation tracker for distributed data quality checks."""

    def __init__(self, check_id: str):
        self.check_id = check_id
        self._local_violations = 0
        self._lock = threading.Lock()
        self._global_actor = None

    def add_violation(self, count: int = 1):
        """Add violation count to local tracker."""
        with self._lock:
            self._local_violations += count

    def get_local_violations(self) -> int:
        """Get local violation count."""
        with self._lock:
            return self._local_violations

    def get_total_violations(self) -> int:
        """Get total violations across all workers (may be approximate)."""
        # For now, return local violations
        # In a full implementation, this would query a global actor
        return self.get_local_violations()

    def reset(self):
        """Reset local violation count."""
        with self._lock:
            self._local_violations = 0


# Global violation trackers per check
_violation_trackers: Dict[str, ViolationTracker] = {}
_tracker_lock = threading.Lock()


def _get_violation_tracker(ctx, check_id: Optional[str] = None) -> ViolationTracker:
    """Get or create violation tracker for a check."""
    if check_id is None:
        check_id = getattr(ctx, "check_id", "default_check")

    with _tracker_lock:
        if check_id not in _violation_trackers:
            _violation_trackers[check_id] = ViolationTracker(check_id)
        return _violation_trackers[check_id]


def _write_batch_metadata(metadata_path: str, batch_stats: Dict[str, Any], ctx):
    """Write batch metadata to specified path."""
    try:
        import json
        import os

        # Create metadata entry
        metadata_entry = {
            "timestamp": time.time(),
            "batch_id": getattr(ctx, "batch_id", "unknown"),
            "worker_id": getattr(ctx, "worker_id", "unknown"),
            "stats": batch_stats,
        }

        # Ensure directory exists
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

        # Append to metadata file
        with open(metadata_path, "a") as f:
            f.write(json.dumps(metadata_entry) + "\n")

    except Exception as e:
        logger.warning(f"Failed to write batch metadata: {e}")


def validate_quarantine_path(path: str) -> bool:
    """Validate that quarantine path is writable."""
    try:
        import os
        import tempfile

        # Check if directory exists or can be created
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        # Try to create a temporary file to test write permissions
        with tempfile.NamedTemporaryFile(dir=dir_path or ".", delete=True):
            pass

        return True
    except (OSError, PermissionError, IOError):
        return False


class CheckMetrics:
    """Metrics collector for data quality checks."""

    def __init__(self, check_id: str):
        self.check_id = check_id
        self.start_time = time.time()
        self.processed_rows = 0
        self.violations = 0
        self.batches_processed = 0
        self.quarantined_rows = 0
        self._lock = threading.Lock()

    def update_batch_stats(self, batch_stats: Dict[str, Any]):
        """Update metrics with batch statistics."""
        with self._lock:
            self.processed_rows += batch_stats.get("processed_rows", 0)
            self.violations += batch_stats.get("violations", 0)
            self.batches_processed += 1
            self.quarantined_rows += batch_stats.get("quarantined_rows", 0)

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        with self._lock:
            elapsed_time = time.time() - self.start_time
            return {
                "check_id": self.check_id,
                "processed_rows": self.processed_rows,
                "violations": self.violations,
                "quarantined_rows": self.quarantined_rows,
                "batches_processed": self.batches_processed,
                "violation_rate": (
                    self.violations / self.processed_rows
                    if self.processed_rows > 0
                    else 0
                ),
                "processing_time": elapsed_time,
                "throughput": (
                    self.processed_rows / elapsed_time if elapsed_time > 0 else 0
                ),
            }


# Global metrics collectors per check
_metrics_collectors: Dict[str, CheckMetrics] = {}
_metrics_lock = threading.Lock()


def get_check_metrics(check_id: str) -> CheckMetrics:
    """Get or create metrics collector for a check."""
    with _metrics_lock:
        if check_id not in _metrics_collectors:
            _metrics_collectors[check_id] = CheckMetrics(check_id)
        return _metrics_collectors[check_id]


def get_all_check_metrics() -> Dict[str, Dict[str, Any]]:
    """Get summary of all check metrics."""
    with _metrics_lock:
        return {
            check_id: collector.get_summary()
            for check_id, collector in _metrics_collectors.items()
        }


def reset_check_metrics(check_id: Optional[str] = None):
    """Reset metrics for a specific check or all checks."""
    with _metrics_lock:
        if check_id:
            if check_id in _metrics_collectors:
                del _metrics_collectors[check_id]
        else:
            _metrics_collectors.clear()


class ExpressionCache:
    """Cache for compiled expressions to improve performance."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._cache: Dict[str, Any] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = threading.Lock()

    def get(self, expression: str) -> Optional[Any]:
        """Get cached compiled expression."""
        with self._lock:
            if expression in self._cache:
                self._access_times[expression] = time.time()
                return self._cache[expression]
            return None

    def put(self, expression: str, compiled_expr: Any):
        """Cache compiled expression."""
        with self._lock:
            # Evict least recently used if cache is full
            if len(self._cache) >= self.max_size:
                lru_key = min(self._access_times, key=self._access_times.get)
                del self._cache[lru_key]
                del self._access_times[lru_key]

            self._cache[expression] = compiled_expr
            self._access_times[expression] = time.time()

    def clear(self):
        """Clear the cache."""
        with self._lock:
            self._cache.clear()
            self._access_times.clear()


# Global expression cache
_expression_cache = ExpressionCache()


def get_expression_cache() -> ExpressionCache:
    """Get the global expression cache."""
    return _expression_cache


def create_informative_error_message(
    error: Exception, context: Dict[str, Any], suggestions: Optional[List[str]] = None
) -> str:
    """Create an informative error message with context and suggestions."""
    message_parts = [f"Data quality check failed: {str(error)}"]

    if context:
        message_parts.append("Context:")
        for key, value in context.items():
            message_parts.append(f"  {key}: {value}")

    if suggestions:
        message_parts.append("Suggestions:")
        for suggestion in suggestions:
            message_parts.append(f"  - {suggestion}")

    return "\n".join(message_parts)


def handle_check_error(
    error: Exception, on_violation: str, context: Dict[str, Any]
) -> Union[Exception, None]:
    """Handle check errors based on violation policy."""
    if on_violation == "fail":
        # Create informative error with suggestions
        suggestions = [
            "Check your data for unexpected values",
            "Verify your check expression is correct",
            "Consider using 'warn' or 'quarantine' for non-critical checks",
        ]
        message = create_informative_error_message(error, context, suggestions)
        return DataQualityError(message, context)
    elif on_violation == "warn":
        logger.warning(f"Data quality check warning: {error}")
        return None
    else:  # quarantine
        logger.info(f"Data quality violation quarantined: {error}")
        return None


def graceful_degradation(func, fallback_value=None, error_message="Operation failed"):
    """Decorator for graceful degradation when operations fail."""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"{error_message}: {e}")
            return fallback_value

    return wrapper


# Security and compliance utilities


def sanitize_quarantine_data(
    data: Dict[str, Any], gdpr_fields: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Sanitize quarantine data for compliance (e.g., GDPR)."""
    if not gdpr_fields:
        return data

    sanitized = data.copy()
    for field in gdpr_fields:
        if field in sanitized:
            # Replace with placeholder or hash
            sanitized[field] = f"<REDACTED_{field.upper()}>"

    return sanitized


def audit_log_check_operation(
    check_id: str,
    operation: str,
    details: Dict[str, Any],
    user_id: Optional[str] = None,
):
    """Log check operations for audit trail."""
    audit_entry = {
        "timestamp": time.time(),
        "check_id": check_id,
        "operation": operation,
        "details": details,
        "user_id": user_id or "system",
    }

    # In a full implementation, this would write to a secure audit log
    logger.info(f"AUDIT: {audit_entry}")


# Performance optimization utilities


@graceful_degradation
def optimize_batch_size(dataset_size: int, available_memory: int) -> int:
    """Optimize batch size based on dataset size and available memory."""
    # Simple heuristic - in practice this would be more sophisticated
    if dataset_size < 1000:
        return min(dataset_size, 100)
    elif dataset_size < 100000:
        return min(dataset_size // 10, 1000)
    else:
        # For large datasets, consider memory constraints
        max_batch_size = max(1000, available_memory // (1024 * 1024))  # Rough estimate
        return min(dataset_size // 100, max_batch_size)


def estimate_memory_usage(row_count: int, avg_row_size: int) -> int:
    """Estimate memory usage for a batch of rows."""
    # Add overhead for processing and quarantine storage
    base_memory = row_count * avg_row_size
    overhead_factor = 2.5  # Account for processing overhead
    return int(base_memory * overhead_factor)


def should_enable_streaming(dataset_size: int, memory_limit: int) -> bool:
    """Determine if streaming processing should be enabled."""
    estimated_memory = estimate_memory_usage(dataset_size, 1024)  # Assume 1KB per row
    return estimated_memory > memory_limit * 0.8  # Use 80% of memory limit as threshold
