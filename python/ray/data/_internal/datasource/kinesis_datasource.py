import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from ray.data._internal.util import _check_import
from ray.data.datasource import ReadTask
from ray.data.datasource.streaming_datasource import (
    StreamingDatasource,
    StreamingMetrics,
    create_streaming_read_task,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


def _parse_kinesis_position(position: str) -> Optional[str]:
    """Parse Kinesis sequence number from position string.

    Args:
        position: Position string in format "sequence:123" or just "123".

    Returns:
        Parsed sequence number as string, or None if invalid.
    """
    if position:
        try:
            if position.startswith("sequence:"):
                return position.split(":", 1)[1]
            else:
                return position
        except (ValueError, IndexError):
            logger.warning(f"Invalid Kinesis sequence number: {position}")
    return None


def _create_kinesis_reader(
    stream_name: str,
    shard_id: str,
    kinesis_config: Dict[str, Any],
    start_sequence: Optional[str] = None,
    end_sequence: Optional[str] = None,
    max_records: int = 1000,
) -> tuple[callable, callable]:
    """Create a Kinesis reader function with encapsulated state.

    Args:
        stream_name: Kinesis stream name.
        shard_id: Shard identifier.
        kinesis_config: Kinesis client configuration.
        start_sequence: Starting sequence number.
        end_sequence: Ending sequence number.
        max_records: Maximum records to read per call.

    Returns:
        Tuple of (read_function, get_position_function).

    Yields:
        Dictionary records from Kinesis.
    """
    _check_import(module="boto3", package="boto3")
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    # State variables encapsulated in closure
    current_sequence_number = start_sequence
    kinesis_client = None
    shard_iterator = None
    metrics = StreamingMetrics()

    # Initialize max_records with default if not provided
    if max_records is None:
        max_records = 1000

    def read_shard() -> Iterator[Dict[str, Any]]:
        """Read records from Kinesis shard, maintaining position state."""
        nonlocal current_sequence_number, kinesis_client, shard_iterator

        try:
            # Initialize client if needed
            if kinesis_client is None:
                kinesis_client = boto3.client("kinesis", **kinesis_config)

            # Initialize shard iterator if needed
            if shard_iterator is None:
                if current_sequence_number:
                    iterator_type = "AT_SEQUENCE_NUMBER"
                    starting_sequence_number = current_sequence_number
                else:
                    iterator_type = "LATEST"
                    starting_sequence_number = None

                response = kinesis_client.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType=iterator_type,
                    StartingSequenceNumber=starting_sequence_number,
                )
                shard_iterator = response["ShardIterator"]

            # Read records from shard with optimized batching
            if shard_iterator:
                try:
                    # Optimize Kinesis read parameters for better performance
                    response = kinesis_client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=min(max_records, 10000),  # Kinesis max is 10k
                        StreamARN=None,  # Use stream name instead
                    )

                    records = response["Records"]
                    shard_iterator = response.get("NextShardIterator")

                    for record in records:
                        # Check if we've reached the end sequence
                        if (
                            end_sequence is not None
                            and record["SequenceNumber"] >= end_sequence
                        ):
                            logger.info(
                                f"Reached end sequence {end_sequence} for "
                        f"{stream_name}:{shard_id}"
                            )
                            return  # Stop reading

                        # Update current position
                        current_sequence_number = record["SequenceNumber"]

                        # Convert Kinesis record to dict
                        record_dict = {
                            "SequenceNumber": record["SequenceNumber"],
                            "Data": record["Data"].decode("utf-8"),
                            "PartitionKey": record["PartitionKey"],
                            "ApproximateArrivalTimestamp": record.get(
                                "ApproximateArrivalTimestamp", datetime.now()
                            ),
                            "stream_name": stream_name,
                            "shard_id": shard_id,
                        }

                        # Track metrics
                        metrics.record_read(1, len(record["Data"]))

                        yield record_dict

                except (BotoCoreError, ClientError) as e:
                    logger.error(f"Error reading from Kinesis shard {shard_id}: {e}")
                    metrics.record_error()

                    # Handle specific Kinesis errors with appropriate retry logic
                    if "ExpiredIteratorException" in str(e):
                        logger.warning(
                            f"Shard iterator expired for {stream_name}:{shard_id}, "
                            "refreshing..."
                        )
                        # Refresh shard iterator
                        try:
                            response = kinesis_client.get_shard_iterator(
                                StreamName=stream_name,
                                ShardId=shard_id,
                                ShardIteratorType="LATEST",
                            )
                            shard_iterator = response["ShardIterator"]
                            # Note: Retry logic is handled by the streaming framework
                        except Exception as refresh_error:
                            logger.error(
                                f"Failed to refresh shard iterator: "
                                f"{refresh_error}"
                            )
                            raise
                    elif "ProvisionedThroughputExceededException" in str(e):
                        logger.warning(
                            f"Throughput exceeded for {stream_name}:{shard_id}, "
                            "backing off..."
                        )
                        import time
                        time.sleep(1)  # Back off for 1 second
                        # Note: Retry logic is handled by the streaming framework

                    # Don't yield anything on error, let the retry logic handle it

        except Exception as e:
            logger.error(f"Unexpected error in Kinesis reader: {e}")
            metrics.record_error()
            raise

    def get_current_position() -> str:
        """Get current sequence number position."""
        return f"sequence:{current_sequence_number or 'LATEST'}"

    return read_shard, get_current_position


@PublicAPI(stability="alpha")
class KinesisDatasource(StreamingDatasource):
    """Kinesis datasource for reading from Amazon Kinesis data streams.

    This datasource provides structured streaming capabilities for Amazon Kinesis,
    supporting real-time data ingestion with configurable triggers and automatic
    position tracking.

    Examples:
        Basic Kinesis stream reading:

        .. testcode::
            :skipif: True

            import ray
            from ray.data._internal.datasource.kinesis_datasource import (
                KinesisDatasource,
            )

            # Read from Kinesis stream with default configuration
            ds = ray.data.read_kinesis(
                stream_name="my-stream",
                kinesis_config={
                    "region_name": "us-west-2"
                }
            )

        Advanced configuration with custom triggers:

        .. testcode::
            :skipif: True

            import ray
            from ray.data._internal.logical.operators.streaming_data_operator import (
                StreamingTrigger,
            )

            # Read with fixed interval trigger
            trigger = StreamingTrigger.fixed_interval("10s")
            ds = ray.data.read_kinesis(
                stream_name="my-stream",
                kinesis_config={
                    "region_name": "us-west-2",
                    "aws_access_key_id": "your-key",
                    "aws_secret_access_key": "your-secret"
                },
                trigger=trigger,
                max_records_per_task=500
            )
    """

    def __init__(
        self,
        stream_name: str,
        kinesis_config: Dict[str, Any],
        max_records_per_task: int = 1000,
        start_sequence: Optional[str] = None,
        end_sequence: Optional[str] = None,
        **kwargs,
    ):
        """Initialize Kinesis datasource.

        Args:
            stream_name: Name of the Kinesis stream to read from.
            kinesis_config: Kinesis client configuration (region, credentials, etc.).
            max_records_per_task: Maximum records per shard per task per batch.
            start_sequence: Starting sequence number for reading.
            end_sequence: Ending sequence number for reading.
            **kwargs: Additional arguments passed to StreamingDatasource.
        """
        self.stream_name = stream_name
        self.kinesis_config = kinesis_config

        # Create streaming config
        streaming_config = {
            "stream_name": self.stream_name,
            "kinesis_config": self.kinesis_config,
            "source_identifier": (
                f"kinesis://{kinesis_config.get('region_name', 'unknown')}/"
                f"{stream_name}"
            ),
        }

        super().__init__(
            max_records_per_task=max_records_per_task,
            start_position=start_sequence,
            end_position=end_sequence,
            streaming_config=streaming_config,
            **kwargs,
        )

    def _validate_config(self) -> None:
        """Validate Kinesis configuration.

        Raises:
            ValueError: If required configuration is missing or invalid.
        """
        if not self.stream_name:
            raise ValueError("stream_name is required for Kinesis datasource")

        if not isinstance(self.kinesis_config, dict):
            raise ValueError("kinesis_config must be a dictionary")

        if "region_name" not in self.kinesis_config:
            raise ValueError("region_name is required in kinesis_config")

    def get_name(self) -> str:
        """Return datasource name."""
        return f"kinesis://{self.stream_name}"

    def get_streaming_partitions(self) -> List[Dict[str, Any]]:
        """Get partitions (shards) for the Kinesis stream.

        Returns:
            List of partition info dictionaries, one per shard.
        """
        _check_import("boto3")
        import boto3

        try:
            kinesis_client = boto3.client("kinesis", **self.kinesis_config)

            # Get stream description to find shards
            response = kinesis_client.describe_stream(StreamName=self.stream_name)
            stream_description = response["StreamDescription"]

            partitions = []
            for shard in stream_description["Shards"]:
                shard_id = shard["ShardId"]
                partitions.append(
                    {
                        "stream_name": self.stream_name,
                        "shard_id": shard_id,
                        "partition_id": f"{self.stream_name}-{shard_id}",
                        "start_sequence": self.start_position,
                        "end_sequence": self.end_position,
                    }
                )

            logger.info(
                f"Found {len(partitions)} shards for Kinesis stream {self.stream_name}"
            )
            return partitions

        except Exception as e:
            logger.error(f"Error listing Kinesis shards: {e}")
            raise RuntimeError(f"Failed to get Kinesis partitions: {e}") from e

    def _create_streaming_read_task(self, partition_info: Dict[str, Any]) -> ReadTask:
        """Create a read task for a Kinesis shard.

        Args:
            partition_info: Partition information containing shard details.

        Returns:
            ReadTask for reading from the shard.
        """
        stream_name = partition_info["stream_name"]
        shard_id = partition_info["shard_id"]
        partition_id = partition_info["partition_id"]
        start_sequence = partition_info.get("start_sequence")
        end_sequence = partition_info.get("end_sequence")

        # Create stateful reader functions
        read_shard_fn, get_position_fn = _create_kinesis_reader(
            stream_name=stream_name,
            shard_id=shard_id,
            kinesis_config=self.kinesis_config,
            start_sequence=_parse_kinesis_position(start_sequence)
            if start_sequence
            else None,
            end_sequence=_parse_kinesis_position(end_sequence)
            if end_sequence
            else None,
            max_records=self.max_records_per_task,
        )

        def get_schema() -> pa.Schema:
            """Return schema for Kinesis records."""
            return pa.schema(
                [
                    ("SequenceNumber", pa.string()),
                    ("Data", pa.string()),
                    ("PartitionKey", pa.string()),
                    ("ApproximateArrivalTimestamp", pa.timestamp("us")),
                    ("stream_name", pa.string()),
                    ("shard_id", pa.string()),
                ]
            )

        return create_streaming_read_task(
            partition_id=partition_id,
            streaming_config=self.streaming_config,
            read_source_fn=read_shard_fn,
            get_position_fn=get_position_fn,
            get_schema_fn=get_schema,
            start_position=self.start_position,
            end_position=self.end_position,
            max_records=self.max_records_per_task,
        )

    def get_streaming_schema(self) -> Optional[pa.Schema]:
        """Return the schema for Kinesis streaming data.

        Returns:
            PyArrow schema for Kinesis records.
        """
        return pa.schema(
            [
                ("SequenceNumber", pa.string()),
                ("Data", pa.string()),
                ("PartitionKey", pa.string()),
                ("ApproximateArrivalTimestamp", pa.timestamp("us")),
                ("stream_name", pa.string()),
                ("shard_id", pa.string()),
                ("partition_id", pa.string()),
                ("read_timestamp", pa.timestamp("us")),
                ("current_position", pa.string()),
            ]
        )
