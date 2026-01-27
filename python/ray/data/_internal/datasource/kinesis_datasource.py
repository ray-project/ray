"""Kinesis datasource for unbounded data streams.

Provides streaming reads from AWS Kinesis Data Streams with support for
enhanced fan-out (EFO) and standard polling modes.

Requires:
    - boto3: AWS SDK for Python
"""

import logging
import time
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from ray.data._internal.datasource.streaming_utils import (
    AWSCredentials,
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

    def _get_read_tasks_for_partition(
        self, partition_info: Dict[str, Any], parallelism: int
    ) -> List[ReadTask]:
        """Create read tasks for Kinesis shards.

        Args:
            partition_info: Unused (we discover shards dynamically).
            parallelism: Number of parallel tasks.

        Returns:
            List of ReadTask objects, one per shard.
        """
        import boto3

        # Discover shards
        session = boto3.Session(**self.credentials.to_session_kwargs())
        client = session.client("kinesis", **self.credentials.to_client_kwargs())
        response = client.list_shards(StreamName=self.stream_name)
        shard_ids = [shard["ShardId"] for shard in response["Shards"]]

        if parallelism > 0:
            shard_ids = shard_ids[:parallelism]

        # Create schema
        schema = create_standard_schema(include_binary_data=False)
        schema = schema.append(pa.field("sequence_number", pa.string()))
        schema = schema.append(pa.field("partition_key", pa.string()))
        schema = schema.append(pa.field("shard_id", pa.string()))

        # Create read task for each shard
        return [
            self._create_shard_read_task(shard_id, schema) for shard_id in shard_ids
        ]

    def _create_shard_read_task(self, shard_id: str, schema: pa.Schema) -> ReadTask:
        """Create read task for a single shard.

        Args:
            shard_id: Kinesis shard ID.
            schema: PyArrow schema.

        Returns:
            ReadTask for this shard.
        """
        # Capture config
        stream_name = self.stream_name
        credentials = self.credentials
        max_records = self.max_records_per_task
        start_sequence = self.start_sequence
        end_sequence = self.end_sequence
        enhanced_fan_out = self.enhanced_fan_out
        consumer_name = self.consumer_name
        poll_interval = self.poll_interval_seconds

        def read_fn() -> Iterator[pa.Table]:
            """Read from Kinesis shard."""
            import boto3

            session = boto3.Session(**credentials.to_session_kwargs())
            client = session.client("kinesis", **credentials.to_client_kwargs())

            records_read = 0
            records_buffer = []

            if enhanced_fan_out and consumer_name:
                # Enhanced Fan-Out mode
                response = client.subscribe_to_shard(
                    ConsumerARN=f"arn:aws:kinesis:{credentials.region_name}:{stream_name}:{consumer_name}",
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
                                    yield pa.Table.from_pylist(records_buffer)
                                return

                            records_buffer.append(_kinesis_record_to_dict(record, shard_id))
                            records_read += 1

                            if len(records_buffer) >= _KINESIS_BATCH_SIZE:
                                yield pa.Table.from_pylist(records_buffer)
                                records_buffer = []

                            if records_read >= max_records:
                                if records_buffer:
                                    yield pa.Table.from_pylist(records_buffer)
                                return
            else:
                # Standard polling mode
                iterator_response = client.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType=start_sequence,
                )
                shard_iterator = iterator_response["ShardIterator"]

                while shard_iterator:
                    response = client.get_records(ShardIterator=shard_iterator)
                    records = response["Records"]
                    shard_iterator = response.get("NextShardIterator")

                    for record in records:
                        if end_sequence and record["SequenceNumber"] >= end_sequence:
                            if records_buffer:
                                yield pa.Table.from_pylist(records_buffer)
                            return

                        records_buffer.append(_kinesis_record_to_dict(record, shard_id))
                        records_read += 1

                        if len(records_buffer) >= _KINESIS_BATCH_SIZE:
                            yield pa.Table.from_pylist(records_buffer)
                            records_buffer = []

                        if records_read >= max_records:
                            if records_buffer:
                                yield pa.Table.from_pylist(records_buffer)
                            return

                    if not records:
                        time.sleep(poll_interval)

            # Yield remaining records
            if records_buffer:
                yield pa.Table.from_pylist(records_buffer)

        metadata = BlockMetadata(
            num_rows=self.max_records_per_task,
            size_bytes=None,
            input_files=[f"kinesis://{self.stream_name}/{shard_id}"],
            exec_stats=None,
        )

        return create_unbound_read_task(read_fn=read_fn, metadata=metadata, schema=schema)


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
