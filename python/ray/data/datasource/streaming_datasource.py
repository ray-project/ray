# Re-export streaming components for backward compatibility
from ray.data._internal.datasource.streaming.base import StreamingDatasource
from ray.data._internal.datasource.streaming.position import StreamingPosition
from ray.data._internal.datasource.streaming.metrics import StreamingMetrics
from ray.data._internal.datasource.streaming.read_task import create_streaming_read_task

__all__ = [
    "StreamingDatasource",
    "StreamingPosition",
    "StreamingMetrics",
    "create_streaming_read_task",
]
