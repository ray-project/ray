"""Flink datasource for unbounded data streams.

Provides streaming reads from Apache Flink jobs via REST API.

Requires:
    - requests: https://requests.readthedocs.io/
"""

import logging
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pyarrow as pa

from ray.data._internal.datasource.streaming_lag_metrics import LagMetrics
from ray.data._internal.datasource.streaming_utils import (
    HTTPClientConfig,
    TwoPhaseCommitMixin,
    compute_budget_deadline_s,
    create_block_coalescer,
    create_standard_schema,
    yield_coalesced_blocks,
    yield_coalesced_blocks_with_progress,
)
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.unbound_datasource import (
    TriggerBudget,
    UnboundDatasource,
    create_unbound_read_task,
)

logger = logging.getLogger(__name__)

# Batch size for yielding records
_FLINK_BATCH_SIZE = 1000


class FlinkDatasource(UnboundDatasource, TwoPhaseCommitMixin):
    """Flink datasource for streaming reads from Apache Flink jobs.

    Reads data from Flink jobs via REST API. Supports reading metrics,
    accumulators, and job output.

    Uses requests for HTTP API access. See:
    https://requests.readthedocs.io/

    Flink REST API documentation:
    https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/
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

        # Track checkpoint state for streaming
        self._current_checkpoint: Optional[Dict[str, Any]] = None
        self._pending_commit_token: Optional[Any] = None

    def supports_exactly_once(self) -> bool:
        # REST polling is at-least-once. Exactly-once requires Flink checkpoint integration.
        return False

    def _get_job_parallelism(self) -> int:
        """Query Flink job parallelism via REST API.

        Returns:
            Number of parallel tasks in the job.
        """
        # requests: https://requests.readthedocs.io/
        import requests

        job_url = f"{self.http_config.base_url}/jobs/{self.job_id}"
        response = requests.get(job_url, **self.http_config.get_request_kwargs())
        response.raise_for_status()
        job_info = response.json()

        vertices = job_info.get("vertices", [])
        if vertices:
            return max(v.get("parallelism", 1) for v in vertices)
        return 1

    def initial_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Get initial checkpoint state for Flink job.

        Returns:
            Checkpoint dict mapping task_id -> last_read_timestamp or None.
        """
        # For Flink, we can use timestamps as checkpoints
        # In production, this might query Flink's checkpoint store
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        *,
        checkpoint: Optional[Dict[str, Any]] = None,
        trigger: Optional[Any] = None,
        batch_id: Optional[int] = None,
        budget: Optional[TriggerBudget] = None,
        max_records_per_trigger: Optional[int] = None,  # back-compat
        max_bytes_per_trigger: Optional[int] = None,    # back-compat
        max_splits_per_trigger: Optional[int] = None,   # back-compat
    ) -> Tuple[List[ReadTask], Optional[Dict[str, Any]]]:
        """Create read tasks for Flink job outputs with checkpoint support.

        NOTE: This datasource polls Flink REST endpoints and is at-least-once.
        It is NOT equivalent to structured streaming over Flink records unless
        the endpoint provides record-level semantics.
        """
        # Store checkpoint for this microbatch
        self._current_checkpoint = checkpoint or {}
        batch_id = batch_id or 0

        # Query job parallelism
        job_parallelism = self._get_job_parallelism()
        num_tasks = min(job_parallelism, parallelism) if parallelism > 0 else job_parallelism

        # Determine microbatch bounds
        max_splits = (budget.max_splits if budget and budget.max_splits is not None else max_splits_per_trigger)
        if max_splits and max_splits > 0:
            num_tasks = min(num_tasks, max_splits)

        poll_timeout_s = getattr(trigger, "poll_timeout_seconds", self.poll_interval_seconds) if trigger else self.poll_interval_seconds
        deadline_s = budget.deadline_s if budget and budget.deadline_s is not None else compute_budget_deadline_s(poll_timeout_seconds=float(poll_timeout_s))
        max_records = budget.max_records if budget and budget.max_records is not None else (max_records_per_trigger or self.max_records_per_task)

        # Create schema
        schema = create_standard_schema(include_binary_data=False)
        schema = schema.append(pa.field("job_id", pa.string()))
        schema = schema.append(pa.field("task_id", pa.int32()))
        schema = schema.append(pa.field("metric_name", pa.string()))

        def _make_task(task_id: int) -> ReadTask:
            # requests is imported on worker
            def read_fn() -> Iterator[Tuple[Block, BlockMetadata]]:
                import requests

                deadline = time.time() + float(deadline_s)
                coalescer = create_block_coalescer()
                small_tables: List[pa.Table] = []
                emitted = 0
                last_ts: Optional[float] = None

                while emitted < int(max_records) and time.time() < deadline:
                    # Example REST call (adjust endpoint to the actual API you intend)
                    url = f"{self.http_config.base_url}/jobs/{self.job_id}"
                    resp = requests.get(url, **self.http_config.get_request_kwargs())
                    resp.raise_for_status()

                    # Convert to table (existing helper would be better; keep simple here)
                    now = time.time()
                    last_ts = now
                    rows = [
                        {"job_id": self.job_id, "task_id": task_id, "metric_name": "job_info", "timestamp": now}
                    ]
                    table = pa.Table.from_pylist(rows, schema=schema.append(pa.field("timestamp", pa.float64())))
                    small_tables.append(table)
                    emitted += table.num_rows

                    # Yield in bounded batches
                    if len(small_tables) >= 1:
                        progress = {str(task_id): last_ts} if last_ts is not None else {}
                        yield from yield_coalesced_blocks_with_progress(
                            coalescer,
                            small_tables,
                            input_file=f"flink://{self.job_id}/{task_id}",
                            batch_id=batch_id or 0,
                            split_id=str(task_id),
                            progress_delta=progress,
                        )
                        small_tables.clear()
                    break  # bounded: one poll per task per microbatch by default

                # Flush any remaining
                if small_tables:
                    progress = {str(task_id): last_ts} if last_ts is not None else {}
                    yield from yield_coalesced_blocks_with_progress(
                        coalescer,
                        small_tables,
                        input_file=f"flink://{self.job_id}/{task_id}",
                        batch_id=batch_id or 0,
                        split_id=str(task_id),
                        progress_delta=progress,
                    )

            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                input_files=[f"flink://{self.job_id}/{task_id}"],
                exec_stats=None,
            )
            return create_unbound_read_task(read_fn, metadata, schema=schema)

        read_tasks = [_make_task(task_id) for task_id in range(num_tasks)]

        # Planned checkpoint is an upper bound/intent; actual is derived from per-block metadata.
        planned = dict(self._current_checkpoint or {})
        for task_id in range(num_tasks):
            planned.setdefault(str(task_id), None)
        return read_tasks, planned

    def _get_read_tasks_for_partition(
        self, partition_info: Dict[str, Any], parallelism: int
    ) -> List[ReadTask]:
        """Backward-compatible method for old interface.

        Args:
            partition_info: Unused.
            parallelism: Number of parallel tasks.

        Returns:
            List of ReadTask objects.
        """
        tasks, _ = self.get_read_tasks(parallelism=parallelism)
        return tasks

    def _create_task_read_task(
        self,
        task_id: int,
        schema: pa.Schema,
        checkpoint: Optional[Any] = None,
        max_records: Optional[int] = None,
        max_bytes: Optional[int] = None,
        last_timestamps_dict: Optional[Dict[int, float]] = None,
    ) -> ReadTask:
        """Create read task for a Flink parallel task slot.

        Args:
            task_id: Task ID.
            schema: PyArrow schema.
            checkpoint: Optional checkpoint (last_read_timestamp for this task).
            max_records: Maximum records to read.
            max_bytes: Maximum bytes to read (optional).
            last_timestamps_dict: Optional shared dict to track last read timestamps.

        Returns:
            ReadTask for this task slot.
        """
        # Capture config
        job_id = self.job_id
        http_config = self.http_config
        max_records = max_records or self.max_records_per_task
        poll_interval = self.poll_interval_seconds

        # Get starting timestamp from checkpoint
        start_timestamp = checkpoint if checkpoint else None

        # Initialize block coalescer for well-sized blocks
        coalescer = create_block_coalescer()

        def read_fn() -> Iterator[Tuple[Block, BlockMetadata]]:
            """Read from Flink job via REST API."""
            # requests: https://requests.readthedocs.io/
            import requests

            records_read = 0
            bytes_read = 0
            records_buffer = []
            small_tables = []
            last_timestamp = start_timestamp

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
                        # Extract metric timestamp if available, otherwise use current time
                        metric_timestamp = metric.get("timestamp")
                        if metric_timestamp is None:
                            metric_timestamp = time.time()
                        elif isinstance(metric_timestamp, (int, float)):
                            # Convert from milliseconds if needed
                            if metric_timestamp > 1e10:
                                metric_timestamp = metric_timestamp / 1000.0
                        else:
                            metric_timestamp = time.time()

                        # Skip metrics older than checkpoint timestamp
                        if start_timestamp and metric_timestamp < start_timestamp:
                            continue

                        record = {
                            "timestamp": int(metric_timestamp * 1000),
                            "key": metric.get("id", ""),
                            "value": str(metric.get("value", "")),
                            "headers": {},
                            "job_id": job_id,
                            "task_id": task_id,
                            "metric_name": metric.get("id", ""),
                        }

                        records_buffer.append(record)
                        records_read += 1
                        last_timestamp = metric_timestamp

                        # Update last timestamp in shared dict
                        if last_timestamps_dict is not None:
                            last_timestamps_dict[task_id] = last_timestamp

                        # Estimate bytes
                        value_str = str(metric.get("value", ""))
                        bytes_read += len(value_str.encode())

                        # Check max_bytes limit
                        if max_bytes and bytes_read >= max_bytes:
                            if records_buffer:
                                small_tables.append(pa.Table.from_pylist(records_buffer))
                                records_buffer = []
                            # Yield coalesced blocks
                            yield from yield_coalesced_blocks(coalescer, small_tables, f"flink://{job_id}/task-{task_id}")
                            return

                        # Yield small table for coalescing when batch size reached
                        if len(records_buffer) >= min(max_records, _FLINK_BATCH_SIZE):
                            small_tables.append(pa.Table.from_pylist(records_buffer))
                            records_buffer = []

                        if records_read >= max_records:
                            if records_buffer:
                                small_tables.append(pa.Table.from_pylist(records_buffer))
                                records_buffer = []
                            # Yield coalesced blocks
                            yield from yield_coalesced_blocks(coalescer, small_tables, f"flink://{job_id}/task-{task_id}")
                            return

                except Exception as e:
                    logger.error(f"Error reading from Flink API: {e}")
                    raise

                time.sleep(poll_interval)

            # Yield remaining records
            if records_buffer:
                small_tables.append(pa.Table.from_pylist(records_buffer))
            yield from yield_coalesced_blocks(coalescer, small_tables, f"flink://{job_id}/task-{task_id}")

        metadata = BlockMetadata(
            num_rows=max_records,
            size_bytes=None,
            input_files=[f"flink://{self.job_id}/task-{task_id}"],
            exec_stats=None,
        )

        return create_unbound_read_task(read_fn=read_fn, metadata=metadata, schema=schema)

    def commit_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        """Commit Flink checkpoint (no-op for now).

        Flink doesn't have built-in checkpointing like Kafka consumer groups.
        In production, this would write to Flink's checkpoint store or external storage.

        Args:
            checkpoint: Dictionary mapping task_id -> last_read_timestamp.
        """
        # No-op for now - in production would write to Flink checkpoint store
        logger.debug(f"Flink checkpoint commit (no-op): {len(checkpoint)} tasks")

    def abort_commit(self, commit_token: Dict[str, Any]) -> None:
        """Abort prepared commit (best-effort).

        Args:
            commit_token: Commit token to abort.
        """
        # Flink doesn't support aborting commits, but we can clear the pending token
        super().abort_commit(commit_token)
        logger.debug("Aborted Flink commit (no-op, timestamps not committed)")

    def get_lag_metrics(self) -> Optional[LagMetrics]:
        """Get Flink job lag metrics for lag-aware autoscaling.

        Returns:
            LagMetrics object with total lag and task count.
        """
        # Get job parallelism as a proxy for lag
        job_parallelism = self._get_job_parallelism()

        # Flink doesn\'t expose consumer lag directly like Kafka
        # In production, this would query Flink metrics or checkpoint store
        # For now, return basic metrics
        return LagMetrics(
            total_lag=0,  # Unknown without checkpoint store
            partitions=job_parallelism,
        )

    def supports_reader_actors(self) -> bool:
        """Check if Flink datasource supports reader actors.

        Returns:
            True if reader actors are supported.
        """
        # Flink REST API can be used with reader actors
        return True
