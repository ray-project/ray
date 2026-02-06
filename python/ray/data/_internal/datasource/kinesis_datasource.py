"""Kinesis datasource for unbounded data streams.

Provides streaming reads from AWS Kinesis Data Streams with support for
enhanced fan-out (EFO) and standard polling modes.

Requires:
    - boto3: AWS SDK for Python
"""

import logging
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import pyarrow as pa

from ray.data._internal.datasource.streaming_utils import (
    AWSCredentials,
    create_block_coalescer,
    create_standard_schema,
    yield_coalesced_blocks,
)
from ray.data._internal.streaming.streaming_lag_metrics import LagMetrics
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.unbound_datasource import (
    UnboundDatasource,
    create_unbound_read_task,
)

logger = logging.getLogger(__name__)

# Batch size for yielding records
_KINESIS_BATCH_SIZE = 1000


class KinesisDatasource(UnboundDatasource):
    """Kinesis datasource for streaming reads from AWS Kinesis Data Streams.

    Supports both standard polling (GetRecords) and enhanced fan-out (EFO)
    for dedicated throughput.
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
        *,
        checkpoint: Optional[Dict[str, Any]] = None,
        trigger: Optional[Any] = None,
        batch_id: Optional[int] = None,
        max_records_per_trigger: Optional[int] = None,
        max_bytes_per_trigger: Optional[int] = None,
        max_splits_per_trigger: Optional[int] = None,
        **kwargs,
    ) -> Union[List[ReadTask], Tuple[List[ReadTask], Optional[Dict[str, Any]]]]:
        """Get read tasks with checkpointing and budget support.

        Args:
            parallelism: Desired parallelism level.
            checkpoint: Optional checkpoint dict (shard_id -> sequence_number).
            trigger: Optional StreamingTrigger (for budget hints).
            batch_id: Optional microbatch ID.
            max_records_per_trigger: Maximum records per microbatch.
            max_bytes_per_trigger: Maximum bytes per microbatch.
            max_splits_per_trigger: Maximum splits/shards per microbatch.

        Returns:
            List of ReadTask objects, or tuple of (tasks, next_checkpoint).
        """
        # Store checkpoint for commit later
        self._current_checkpoint = checkpoint

        # Use budget from trigger if provided, otherwise use instance default
        effective_max_records = (
            max_records_per_trigger
            if max_records_per_trigger is not None
            else self.max_records_per_task
        )

        # Discover shards
        import boto3

        session = boto3.Session(**self.credentials.to_session_kwargs())
        client = session.client("kinesis", **self.credentials.to_client_kwargs())
        shard_ids = []
        next_token = None

        while True:
            if next_token:
                response = client.list_shards(
                    StreamName=self.stream_name, NextToken=next_token
                )
            else:
                response = client.list_shards(StreamName=self.stream_name)

            shard_ids.extend([shard["ShardId"] for shard in response.get("Shards", [])])

            next_token = response.get("NextToken")
            if not next_token:
                break

        # Limit shards if max_splits_per_trigger is set
        if max_splits_per_trigger and len(shard_ids) > max_splits_per_trigger:
            shard_ids = shard_ids[:max_splits_per_trigger]
            logger.debug(
                f"Limited shards to {max_splits_per_trigger} "
                f"(from {len(shard_ids)})"
            )

        # Create schema
        schema = create_standard_schema(include_binary_data=False)
        schema = schema.append(pa.field("sequence_number", pa.string()))
        schema = schema.append(pa.field("partition_key", pa.string()))
        schema = schema.append(pa.field("shard_id", pa.string()))

        # Get target block size for coalescing
        ctx = DataContext.get_current()
        target_max_block_size = ctx.target_max_block_size or (128 * 1024 * 1024)

        # Create read task for each shard
        tasks = []
        next_checkpoint = checkpoint.copy() if checkpoint else {}
        last_sequence_numbers: Dict[str, str] = {}

        for shard_id in shard_ids:
            task = self._create_shard_read_task(
                shard_id=shard_id,
                schema=schema,
                checkpoint=checkpoint,
                max_records=effective_max_records,
                max_bytes=max_bytes_per_trigger,
                target_max_block_size=target_max_block_size,
                last_sequence_numbers_dict=last_sequence_numbers,
            )
            tasks.append(task)
            next_checkpoint[shard_id] = (
                checkpoint.get(shard_id) if checkpoint else None
            )
            last_sequence_numbers[shard_id] = (
                checkpoint.get(shard_id) if checkpoint else None
            )

        return tasks, next_checkpoint

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

    def _create_shard_read_task(
        self,
        shard_id: str,
        schema: pa.Schema,
        checkpoint: Optional[Dict[str, Any]],
        max_records: int,
        max_bytes: Optional[int],
        target_max_block_size: int,
        last_sequence_numbers_dict: Optional[Dict[str, str]] = None,
    ) -> ReadTask:
        """Create read task for a single Kinesis shard.

        Args:
            shard_id: Kinesis shard ID.
            schema: PyArrow schema.
            checkpoint: Optional checkpoint dict (shard_id -> sequence_number).
            max_records: Maximum records to read in this batch.
            max_bytes: Maximum bytes to read in this batch (optional).
            target_max_block_size: Target block size for coalescing.
            last_sequence_numbers_dict: Optional shared dict to track last sequence numbers.

        Returns:
            ReadTask for this shard.
        """
        # Capture config
        stream_name = self.stream_name
        credentials = self.credentials
        start_sequence = self.start_sequence
        end_sequence = self.end_sequence
        enhanced_fan_out = self.enhanced_fan_out
        consumer_name = self.consumer_name
        poll_interval = self.poll_interval_seconds

        # Get starting sequence from checkpoint if available
        checkpoint_sequence = (
            checkpoint.get(shard_id) if checkpoint else None
        )

        # Initialize block coalescer for well-sized blocks
        coalescer = create_block_coalescer(target_max_block_size)

        # Track last sequence number for checkpoint updates
        if last_sequence_numbers_dict is not None:
            last_sequence_numbers_dict[shard_id] = checkpoint_sequence or ""

        def read_fn() -> Iterator[Tuple[Block, BlockMetadata]]:
            """Read from Kinesis shard."""
            import boto3

            session = boto3.Session(**credentials.to_session_kwargs())
            client = session.client("kinesis", **credentials.to_client_kwargs())

            records_read = 0
            bytes_read = 0
            records_buffer = []
            small_tables = []

            if enhanced_fan_out and consumer_name:
                # Enhanced Fan-Out mode
                # Get or create consumer and retrieve proper ConsumerARN
                stream_arn = client.describe_stream(StreamName=stream_name)["StreamDescription"]["StreamARN"]

                # Check if consumer exists
                try:
                    consumer_response = client.describe_stream_consumer(
                        StreamARN=stream_arn,
                        ConsumerName=consumer_name,
                    )
                    consumer_arn = consumer_response["ConsumerDescription"]["ConsumerARN"]
                except client.exceptions.ResourceNotFoundException:
                    # Consumer doesn't exist, create it
                    register_response = client.register_stream_consumer(
                        StreamARN=stream_arn,
                        ConsumerName=consumer_name,
                    )
                    consumer_arn = register_response["Consumer"]["ConsumerARN"]

                response = client.subscribe_to_shard(
                    ConsumerARN=consumer_arn,
                    ShardId=shard_id,
                    StartingPosition={"Type": start_sequence},
                )

                for event in response["EventStream"]:
                    if "SubscribeToShardEvent" in event:
                        shard_event = event["SubscribeToShardEvent"]
                        for record in shard_event["Records"]:
                            if (
                                end_sequence
                                and record["SequenceNumber"] >= end_sequence
                            ):
                                if records_buffer:
                                    small_tables.append(pa.Table.from_pylist(records_buffer))
                                    records_buffer = []
                                # Yield coalesced blocks
                                yield from yield_coalesced_blocks(coalescer, small_tables, f"kinesis://{stream_name}/{shard_id}")

                            records_buffer.append(_kinesis_record_to_dict(record, shard_id))
                            records_read += 1
                            last_sequence = record["SequenceNumber"]
                            # Update last sequence in shared dict
                            if last_sequence_numbers_dict is not None:
                                last_sequence_numbers_dict[shard_id] = last_sequence

                            # Estimate bytes
                            if record.get("Data"):
                                data = record["Data"]
                                bytes_read += len(data) if isinstance(data, bytes) else len(str(data).encode())

                            # Check max_bytes limit
                            if max_bytes and bytes_read >= max_bytes:
                                if records_buffer:
                                    small_tables.append(pa.Table.from_pylist(records_buffer))
                                    records_buffer = []
                                # Yield coalesced blocks
                                yield from yield_coalesced_blocks(coalescer, small_tables, f"kinesis://{stream_name}/{shard_id}")

                            # Yield small table for coalescing when batch size reached
                            if len(records_buffer) >= min(max_records or _KINESIS_BATCH_SIZE, _KINESIS_BATCH_SIZE):
                                small_tables.append(pa.Table.from_pylist(records_buffer))
                                records_buffer = []

                            if records_read >= max_records:
                                if records_buffer:
                                    small_tables.append(pa.Table.from_pylist(records_buffer))
                                    records_buffer = []
                                # Yield coalesced blocks
                                yield from yield_coalesced_blocks(coalescer, small_tables, f"kinesis://{stream_name}/{shard_id}")
            else:
                # Standard polling mode
                # Use checkpoint sequence if available
                shard_iterator_type = (
                    "AFTER_SEQUENCE_NUMBER" if checkpoint_sequence else start_sequence
                )
                iterator_kwargs = {
                    "StreamName": stream_name,
                    "ShardId": shard_id,
                    "ShardIteratorType": shard_iterator_type,
                }
                if checkpoint_sequence:
                    iterator_kwargs["StartingSequenceNumber"] = checkpoint_sequence

                iterator_response = client.get_shard_iterator(**iterator_kwargs)
                shard_iterator = iterator_response["ShardIterator"]

                while shard_iterator:
                    response = client.get_records(ShardIterator=shard_iterator)
                    records = response["Records"]
                    shard_iterator = response.get("NextShardIterator")

                    for record in records:
                        if end_sequence and record["SequenceNumber"] >= end_sequence:
                            if records_buffer:
                                small_tables.append(pa.Table.from_pylist(records_buffer))
                            # Yield coalesced blocks
                            yield from yield_coalesced_blocks(coalescer, small_tables, f"kinesis://{stream_name}/{shard_id}")

                        records_buffer.append(_kinesis_record_to_dict(record, shard_id))
                        records_read += 1
                        last_sequence = record["SequenceNumber"]
                        # Update last sequence in shared dict
                        if last_sequence_numbers_dict is not None:
                            last_sequence_numbers_dict[shard_id] = last_sequence

                        # Estimate bytes
                        if record.get("Data"):
                            data = record["Data"]
                            bytes_read += len(data) if isinstance(data, bytes) else len(str(data).encode())

                        # Check max_bytes limit
                        if max_bytes and bytes_read >= max_bytes:
                            if records_buffer:
                                small_tables.append(pa.Table.from_pylist(records_buffer))
                                records_buffer = []
                            # Yield coalesced blocks
                            yield from yield_coalesced_blocks(coalescer, small_tables, f"kinesis://{stream_name}/{shard_id}")

                        # Yield small table for coalescing when batch size reached
                        if len(records_buffer) >= min(max_records, _KINESIS_BATCH_SIZE):
                            small_tables.append(pa.Table.from_pylist(records_buffer))
                            records_buffer = []

                        if records_read >= max_records:
                            if records_buffer:
                                small_tables.append(pa.Table.from_pylist(records_buffer))
                            # Yield coalesced blocks
                            yield from yield_coalesced_blocks(coalescer, small_tables, f"kinesis://{stream_name}/{shard_id}")

                    if not records:
                        # Yield any pending coalesced blocks before sleeping
                        if small_tables:
                            yield from yield_coalesced_blocks(coalescer, small_tables, f"kinesis://{stream_name}/{shard_id}")
                        time.sleep(poll_interval)

            # Yield remaining records
            if records_buffer:
                small_tables.append(pa.Table.from_pylist(records_buffer))
            yield from yield_coalesced_blocks(coalescer, small_tables, f"kinesis://{stream_name}/{shard_id}")
        metadata = BlockMetadata(
            num_rows=max_records,  # Estimated
            size_bytes=None,
            input_files=[f"kinesis://{self.stream_name}/{shard_id}"],
            exec_stats=None,
        )

        return create_unbound_read_task(read_fn=read_fn, metadata=metadata, schema=schema)

    def commit_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        """Commit Kinesis sequence numbers (no-op for now).

        Kinesis doesn't have built-in checkpointing like Kafka consumer groups.
        In production, this would write to DynamoDB or another checkpoint store.

        Args:
            checkpoint: Dictionary mapping shard_id -> sequence_number.
        """
        # No-op for now - in production would write to DynamoDB checkpoint table
        logger.debug(f"Kinesis checkpoint commit (no-op): {len(checkpoint)} shards")

    def prepare_commit(self, checkpoint: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare commit token for two-phase commit.

        Args:
            checkpoint: Checkpoint dict to prepare.

        Returns:
            Commit token (same as checkpoint for Kinesis).
        """
        self._pending_commit_token = checkpoint
        return checkpoint

    def commit(self, commit_token: Dict[str, Any]) -> None:
        """Commit prepared token (two-phase commit).

        Args:
            commit_token: Commit token from prepare_commit().
        """
        self.commit_checkpoint(commit_token)
        self._pending_commit_token = None

    def abort_commit(self, commit_token: Dict[str, Any]) -> None:
        """Abort prepared commit (best-effort).

        Args:
            commit_token: Commit token to abort.
        """
        # Kinesis doesn't support aborting commits, but we can clear the pending token
        self._pending_commit_token = None
        logger.debug("Aborted Kinesis commit (no-op, sequence numbers not committed)")

    def get_lag_metrics(self) -> Optional[LagMetrics]:
        """Get Kinesis consumer lag metrics for lag-aware autoscaling.

        Returns:
            LagMetrics object with total lag, fetch rate, and shard count.
        """
        try:
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
        except Exception as e:
            logger.warning(f"Failed to get Kinesis lag metrics: {e}")
            return None


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
