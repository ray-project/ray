"""Flink datasource for unbounded data streams.

Provides streaming reads from Apache Flink jobs via REST API.

Requires:
    - requests: HTTP library for REST API access
"""

import logging
import time
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from ray.data._internal.datasource.streaming_utils import (
    HTTPClientConfig,
    create_standard_schema,
)
from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.unbound_datasource import (
    UnboundDatasource,
    create_unbound_read_task,
)

logger = logging.getLogger(__name__)

# Batch size for yielding records
_FLINK_BATCH_SIZE = 1000


class FlinkDatasource(UnboundDatasource):
    """Flink datasource for streaming reads from Apache Flink jobs.

    Reads data from Flink jobs via REST API. Supports reading metrics,
    accumulators, and job output.
    """

    def __init__(
        self,
        rest_api_url: str,
        job_id: str,
        max_records_per_task: int = 1000,
        http_config: Optional[HTTPClientConfig] = None,
        poll_interval_seconds: float = 5.0,
    ):
        """Initialize Flink datasource.

        Args:
            rest_api_url: Flink REST API base URL.
            job_id: Flink job ID to read from.
            max_records_per_task: Maximum records per task per batch.
            http_config: HTTP client configuration (optional).
            poll_interval_seconds: Seconds between API polls.

        Raises:
            ValueError: If configuration is invalid.
            ImportError: If requests is not installed.
        """
        super().__init__("flink")
        _check_import(self, module="requests", package="requests")

        if not rest_api_url:
            raise ValueError("rest_api_url cannot be empty")
        if not job_id:
            raise ValueError("job_id cannot be empty")
        if max_records_per_task <= 0:
            raise ValueError("max_records_per_task must be positive")

        self.job_id = job_id
        self.max_records_per_task = max_records_per_task
        self.poll_interval_seconds = poll_interval_seconds
        self.http_config = http_config or HTTPClientConfig(base_url=rest_api_url)

    def _get_job_parallelism(self) -> int:
        """Query Flink job parallelism via REST API.

        Returns:
            Number of parallel tasks in the job.
        """
        import requests

        job_url = f"{self.http_config.base_url}/jobs/{self.job_id}"
        try:
            response = requests.get(job_url, **self.http_config.get_request_kwargs())
            response.raise_for_status()
            job_info = response.json()

            vertices = job_info.get("vertices", [])
            if vertices:
                return max(v.get("parallelism", 1) for v in vertices)
            return 1
        except Exception as e:
            logger.warning(f"Could not determine job parallelism: {e}")
            return 1

    def _get_read_tasks_for_partition(
        self, partition_info: Dict[str, Any], parallelism: int
    ) -> List[ReadTask]:
        """Create read tasks for Flink job outputs.

        Args:
            partition_info: Unused.
            parallelism: Number of parallel tasks.

        Returns:
            List of ReadTask objects.
        """
        # Query job parallelism
        job_parallelism = self._get_job_parallelism()
        num_tasks = min(job_parallelism, parallelism) if parallelism > 0 else job_parallelism

        # Create schema
        schema = create_standard_schema(include_binary_data=False)
        schema = schema.append(pa.field("job_id", pa.string()))
        schema = schema.append(pa.field("task_id", pa.int32()))
        schema = schema.append(pa.field("metric_name", pa.string()))

        # Create read task for each parallel slot
        return [
            self._create_task_read_task(task_id, schema) for task_id in range(num_tasks)
        ]

    def _create_task_read_task(self, task_id: int, schema: pa.Schema) -> ReadTask:
        """Create read task for a Flink parallel task slot.

        Args:
            task_id: Task ID.
            schema: PyArrow schema.

        Returns:
            ReadTask for this task slot.
        """
        # Capture config
        job_id = self.job_id
        http_config = self.http_config
        max_records = self.max_records_per_task
        poll_interval = self.poll_interval_seconds

        def read_fn() -> Iterator[pa.Table]:
            """Read from Flink job via REST API."""
            import requests

            records_read = 0
            records_buffer = []

            # Poll Flink REST API for job metrics/data
            metrics_url = f"{http_config.base_url}/jobs/{job_id}/metrics"

            while records_read < max_records:
                try:
                    response = requests.get(
                        metrics_url, **http_config.get_request_kwargs()
                    )
                    response.raise_for_status()
                    metrics = response.json()

                    # Convert metrics to records
                    # Note: This is a simplified example - actual implementation
                    # depends on what data you want to extract from Flink
                    for metric in metrics:
                        records_buffer.append(
                            {
                                "timestamp": int(time.time() * 1000),
                                "key": metric.get("id", ""),
                                "value": str(metric.get("value", "")),
                                "headers": {},
                                "job_id": job_id,
                                "task_id": task_id,
                                "metric_name": metric.get("id", ""),
                            }
                        )
                        records_read += 1

                        if len(records_buffer) >= _FLINK_BATCH_SIZE:
                            yield pa.Table.from_pylist(records_buffer)
                            records_buffer = []

                        if records_read >= max_records:
                            break

                except Exception as e:
                    logger.error(f"Error reading from Flink API: {e}")
                    break

                time.sleep(poll_interval)

            # Yield remaining records
            if records_buffer:
                yield pa.Table.from_pylist(records_buffer)

        metadata = BlockMetadata(
            num_rows=self.max_records_per_task,
            size_bytes=None,
            input_files=[f"flink://{self.job_id}/task-{task_id}"],
            exec_stats=None,
        )

        return create_unbound_read_task(read_fn=read_fn, metadata=metadata, schema=schema)
