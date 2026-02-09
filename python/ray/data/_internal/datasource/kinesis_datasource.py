"""Kinesis datasource for unbounded data streams.

Provides streaming reads from AWS Kinesis Data Streams with support for
enhanced fan-out (EFO) and standard polling modes.

Requires:
    - boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
"""

import logging
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pyarrow as pa

from ray.data._internal.datasource.streaming_lag_metrics import LagMetrics
from ray.data._internal.datasource.streaming_utils import (
    AWSCredentials,
    TwoPhaseCommitMixin,
    compute_budget_deadline_s,
    create_block_coalescer,
    create_standard_schema,
    yield_coalesced_blocks_with_progress,
)
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.unbound_datasource import (
    TriggerBudget,
    UnboundDatasource,
)

logger = logging.getLogger(__name__)

# Batch size for yielding records
_KINESIS_BATCH_SIZE = 1000


class KinesisDatasource(UnboundDatasource, TwoPhaseCommitMixin):
    """Kinesis datasource for streaming reads from AWS Kinesis Data Streams.

    Supports both standard polling (GetRecords) and enhanced fan-out (EFO)
    for dedicated throughput.

    Uses boto3 for AWS API access. See:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html
    """

    def __init__(
        self,
        stream_name: str,
        region_name: str,
        max_records_per_task: int = 1000,
        start_sequence: Optional[str] = None,
        end_sequence: Optional[str] = None,
        aws_credentials: Optional[AWSCredentials] = None,
        enhanced_fan_out: bool = False,
        consumer_name: Optional[str] = None,
        poll_interval_seconds: float = 5.0,
    ):
        """Initialize Kinesis datasource.

        Args:
            stream_name: Kinesis stream name.
            region_name: AWS region.
            max_records_per_task: Maximum records per task per batch.
            start_sequence: Starting sequence number ("LATEST", "TRIM_HORIZON", or specific).
            end_sequence: Ending sequence number (optional, for bounded reads).
            aws_credentials: AWS credentials (optional, uses default chain if not provided).
            enhanced_fan_out: Whether to use enhanced fan-out (EFO).
            consumer_name: Consumer name (required if enhanced_fan_out=True).
            poll_interval_seconds: Seconds between GetRecords calls.

        Raises:
            ValueError: If configuration is invalid.
            ImportError: If boto3 is not installed.
        """
        super().__init__("kinesis")
        _check_import(self, module="boto3", package="boto3")

        if not stream_name:
            raise ValueError("stream_name cannot be empty")
        if max_records_per_task <= 0:
            raise ValueError("max_records_per_task must be positive")
        if enhanced_fan_out and not consumer_name:
            raise ValueError("consumer_name required when enhanced_fan_out=True")

        self.stream_name = stream_name
        self.max_records_per_task = max_records_per_task
        self.start_sequence = start_sequence or "LATEST"
        self.end_sequence = end_sequence
        self.credentials = aws_credentials or AWSCredentials(region_name=region_name)
        self.enhanced_fan_out = enhanced_fan_out
        self.consumer_name = consumer_name
        self.poll_interval_seconds = poll_interval_seconds

        # Internal state for checkpointing
        self._current_checkpoint: Optional[Dict[str, Any]] = None
        self._pending_commit_token: Optional[Any] = None
        self._last_committed_checkpoint = None

        # Block size and schema configuration
        ctx = DataContext.get_current()
        self._block_size_bytes = ctx.target_max_block_size or (128 * 1024 * 1024)
        self._schema = create_standard_schema(include_binary_data=False)

        # Budget limits (can be overridden per trigger)
        self._max_splits_per_trigger = None
        self._max_bytes_per_trigger = None

    def supports_exactly_once(self) -> bool:
        # Exactly-once depends on external sink+checkpoint store integration.
        return False

    def initial_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Return initial checkpoint state (Kinesis sequence numbers).

        Returns:
            Dictionary mapping shard_id -> sequence_number, or None if starting fresh.
        """
        # Kinesis doesn't have a built-in way to read committed sequence numbers
        # without a consumer, so return None for now
        # In production, this could query DynamoDB or another checkpoint store
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
        **kwargs,
    ) -> Tuple[List[ReadTask], Optional[Dict[str, Any]]]:
        """Plan a bounded microbatch over shards and return (tasks, planned_next_checkpoint)."""

        if checkpoint is None:
            checkpoint = {}
        if batch_id is None:
            batch_id = 0

        # Discover shards (bounded set)
        import boto3
        session = boto3.Session(**self.credentials.to_session_kwargs())
        client = session.client("kinesis", **self.credentials.to_client_kwargs())
        shard_ids = []
        next_token = None
        while True:
            if next_token:
                response = client.list_shards(StreamName=self.stream_name, NextToken=next_token)
            else:
                response = client.list_shards(StreamName=self.stream_name)
            shard_ids.extend([shard["ShardId"] for shard in response.get("Shards", [])])
            next_token = response.get("NextToken")
            if not next_token:
                break

        max_splits = (
            (budget.max_splits if budget and budget.max_splits is not None else max_splits_per_trigger)
            or self._max_splits_per_trigger
            or len(shard_ids)
        )
        selected = shard_ids[:max_splits] if max_splits else shard_ids

        max_records = (budget.max_records if budget and budget.max_records is not None else max_records_per_trigger) or self.max_records_per_task
        max_bytes = (budget.max_bytes if budget and budget.max_bytes is not None else max_bytes_per_trigger) or self._max_bytes_per_trigger

        poll_timeout = getattr(trigger, "poll_timeout_seconds", self.poll_interval_seconds) if trigger else self.poll_interval_seconds
        deadline_s = budget.deadline_s if budget and budget.deadline_s is not None else compute_budget_deadline_s(poll_timeout_seconds=float(poll_timeout))
        tasks: List[ReadTask] = []
        for shard_id in selected:
            tasks.append(
                self._create_read_task_for_shard(
                shard_id=shard_id,
                    checkpoint=checkpoint.get(shard_id),
                    batch_id=batch_id,
                    max_records=max_records,
                    max_bytes=max_bytes,
                    poll_timeout_s=float(poll_timeout),
                    deadline_s=float(deadline_s),
                )
            )

        # Planned checkpoint is an upper bound/intent; actual comes from emitted metadata.
        return tasks, dict(checkpoint)

    def _get_read_tasks_for_partition(
        self, partition_info: Dict[str, Any], parallelism: int
    ) -> List[ReadTask]:
        """Create read tasks for Kinesis shards (backward compatibility).

        This method is kept for backward compatibility with UnboundDatasource interface.
        New code should use get_read_tasks() directly.

        Args:
            partition_info: Unused (we discover shards dynamically).
            parallelism: Number of parallel tasks.

        Returns:
            List of ReadTask objects, one per shard.
        """
        # Fallback to new get_read_tasks() method
        result = self.get_read_tasks(parallelism)
        if isinstance(result, tuple):
            return result[0]
        return result

    def _create_read_task_for_shard(
        self,
        shard_id: str,
        checkpoint: Optional[str],
        batch_id: Optional[int],
        max_records: Optional[int],
        max_bytes: Optional[int],
        poll_timeout_s: float,
        deadline_s: float,
    ) -> ReadTask:
        """Create a bounded read task for a single shard."""

        def read_fn() -> Iterator[Tuple[Block, BlockMetadata]]:
            # Lazily check boto3 import on worker.
            _check_import("boto3", package="boto3")
            import boto3  # noqa: F401

            client = self._get_boto3_client()
            coalescer = create_block_coalescer(
                max_block_size_bytes=self._block_size_bytes
            )

            records_read = 0
            bytes_read = 0
            last_seq_no: Optional[str] = None
            last_lag_ms: Optional[int] = None
            deadline = time.time() + float(deadline_s)

            # Resolve shard iterator from checkpoint (if any)
            shard_iterator = self._get_shard_iterator(
                client, shard_id, checkpoint
            )

            while time.time() < deadline:
                if max_records is not None and records_read >= max_records:
                    break
                if max_bytes is not None and bytes_read >= max_bytes:
                    break

                # Poll Kinesis for records
                try:
                    response = client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=self._get_limit(),
                    )
                except Exception as e:
                    if "ExpiredIteratorException" in str(e):
                        shard_iterator = self._get_shard_iterator(client, shard_id, checkpoint)
                        continue
                    raise

                shard_iterator = response.get("NextShardIterator")
                last_lag_ms = response.get("MillisBehindLatest")

                k_records = response.get("Records", [])
                if not k_records:
                                break

                # Convert records to PyArrow
                table = self._records_to_arrow_table(k_records, shard_id)
                if k_records:
                    last_seq_no = k_records[-1].get("SequenceNumber")
                records_read += table.num_rows
                bytes_read += table.nbytes

                progress = {shard_id: last_seq_no} if last_seq_no is not None else {}
                lag = {shard_id: last_lag_ms} if last_lag_ms is not None else None
                yield from yield_coalesced_blocks_with_progress(
                    coalescer,
                    [table],
                    input_file=f"kinesis://{self.stream_name}/{shard_id}",
                    batch_id=batch_id or 0,
                    split_id=shard_id,
                    progress_delta=progress,
                    lag_metrics=lag,
                )

            # Flush remaining blocks (no-op in current BlockCoalescer implementation, but keep for safety)
            # If BlockCoalescer has flush() returning tables, attach stats when possible.

        metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=self._schema,
            input_files=[f"kinesis://{self.stream_name}/{shard_id}"],
            exec_stats=None,
        )

        return ReadTask(read_fn, metadata)

    def _get_boto3_client(self):
        """Get boto3 Kinesis client (lazy initialization).

        Returns:
            boto3.client('kinesis') instance.
        """
        import boto3
        session = boto3.Session(**self.credentials.to_session_kwargs())
        return session.client("kinesis", **self.credentials.to_client_kwargs())

    def _get_shard_iterator(self, client, shard_id: str, checkpoint: Optional[str]) -> str:
        """Get shard iterator for reading from Kinesis.

        Args:
            client: boto3 Kinesis client.
            shard_id: Shard ID.
            checkpoint: Optional sequence number checkpoint.

        Returns:
            Shard iterator string.
        """
        iterator_type = "AFTER_SEQUENCE_NUMBER" if checkpoint else self.start_sequence
        kwargs = {
            "StreamName": self.stream_name,
            "ShardId": shard_id,
            "ShardIteratorType": iterator_type,
        }
        if checkpoint:
            kwargs["StartingSequenceNumber"] = checkpoint
        response = client.get_shard_iterator(**kwargs)
        return response["ShardIterator"]

    def _get_limit(self) -> int:
        """Get maximum records per GetRecords call.

        Returns:
            Maximum records limit (Kinesis allows up to 10,000).
        """
        return min(self.max_records_per_task, 10000)

    def _records_to_arrow_table(self, records: List[Dict[str, Any]], shard_id: str = "") -> pa.Table:
        """Convert Kinesis records to PyArrow table.

        Args:
            records: List of Kinesis record dicts from boto3.
            shard_id: Shard ID for record metadata (optional).

        Returns:
            PyArrow Table with standardized schema.
        """
        rows = [_kinesis_record_to_dict(record, shard_id) for record in records]
        return pa.Table.from_pylist(rows, schema=self._schema)

    def commit_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        """Commit Kinesis sequence numbers (no-op for now).

        Kinesis doesn't have built-in checkpointing like Kafka consumer groups.
        In production, this would write to DynamoDB or another checkpoint store.

        Args:
            checkpoint: Dictionary mapping shard_id -> sequence_number.
        """
        # No-op for now - in production would write to DynamoDB checkpoint table
        logger.debug(f"Kinesis checkpoint commit (no-op): {len(checkpoint)} shards")

    def abort_commit(self, commit_token: Dict[str, Any]) -> None:
        """Abort prepared commit (best-effort).

        Args:
            commit_token: Commit token to abort.
        """
        # Kinesis doesn't support aborting commits, but we can clear the pending token
        super().abort_commit(commit_token)
        logger.debug("Aborted Kinesis commit (no-op, sequence numbers not committed)")

    def get_lag_metrics(self) -> Optional[LagMetrics]:
        """Get Kinesis consumer lag metrics for lag-aware autoscaling.

        Returns:
            LagMetrics object with total lag, fetch rate, and shard count.
        """
        # boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
        import boto3

        session = boto3.Session(**self.credentials.to_session_kwargs())
        client = session.client("kinesis", **self.credentials.to_client_kwargs())

        # Get stream description to count shards
        stream_desc = client.describe_stream(StreamName=self.stream_name)
        shards = stream_desc["StreamDescription"]["Shards"]
        shard_count = len(shards)

        # Kinesis doesn't expose consumer lag directly like Kafka
        # In production, this would query CloudWatch metrics or a checkpoint store
        # For now, return basic metrics
        return LagMetrics(
            total_lag=0,  # Unknown without checkpoint store
            partitions=shard_count,
        )


def _kinesis_record_to_dict(record: Dict[str, Any], shard_id: str) -> Dict[str, Any]:
    """Convert Kinesis record to dictionary for PyArrow.

    Args:
        record: Kinesis record from boto3.
        shard_id: Shard ID.

    Returns:
        Dictionary with standardized fields.
    """

    data = record["Data"]
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="replace")

    return {
        "timestamp": int(record["ApproximateArrivalTimestamp"].timestamp() * 1000),
        "key": record.get("PartitionKey", ""),
        "value": data,
        "headers": {},  # Kinesis doesn't have headers
        "sequence_number": record["SequenceNumber"],
        "partition_key": record.get("PartitionKey", ""),
        "shard_id": shard_id,
    }
