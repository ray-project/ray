from ray.data._internal.telemetry.collector import (
    record_execution_result,
    record_workload,
)

__all__ = ["record_workload", "record_execution_result"]
