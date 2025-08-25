import logging
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
    max_records: Optional[int] = None,
    batch_size: int = 1000,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> tuple[callable, callable]:
    """Create a Kinesis reader function with encapsulated state.

    This function creates a closure that encapsulates all state needed for
    reading from a Kinesis shard, avoiding the use of global variables.

    Args:
        stream_name: Kinesis stream name.
        shard_id: Shard identifier.
        kinesis_config: Kinesis client configuration.
        start_sequence: Starting sequence number.
        end_sequence: Ending sequence number.
        max_records: Maximum records to read per call.
        batch_size: Number of records to fetch per API call.
        max_retries: Maximum number of retries for failed requests.
        retry_delay: Delay between retries in seconds.

    Returns:
        Tuple of (read_function, get_position_function).

    Examples:
        Creating a Kinesis reader:

        .. testcode::

            read_fn, get_pos_fn = _create_kinesis_reader(
                stream_name="my-stream",
                shard_id="shardId-000000000000",
                kinesis_config={"region_name": "us-west-2"},
                start_sequence="1234567890",
                max_records=100
            )
    """
    _check_import(module="boto3", package="boto3")
    import time

    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    # State variables encapsulated in closure
    current_sequence = start_sequence
    records_read = 0
    metrics = StreamingMetrics()

    # Enhanced configuration with security and performance defaults
    enhanced_config = {
        # Security defaults
        "region_name": "us-east-1",  # Can be overridden
        "aws_access_key_id": None,  # Can be overridden
        "aws_secret_access_key": None,  # Can be overridden
        "aws_session_token": None,  # Can be overridden
        # Performance defaults
        "max_attempts": 3,
        "retry_mode": "adaptive",
        "connect_timeout": 60,
        "read_timeout": 60,
        # Override with user config
        **kinesis_config,
    }

    # Create Kinesis client with enhanced config
    kinesis_client = boto3.client("kinesis", **enhanced_config)

    # Initialize max_records with default if not provided
    if max_records is None:
        max_records = 1000

    def read_shard() -> Iterator[Dict[str, Any]]:
        """Read records from Kinesis shard, maintaining position state."""
        nonlocal current_sequence, records_read

        try:
            # Parse starting position
            if start_sequence:
                if start_sequence.startswith("AT_SEQUENCE_NUMBER:"):
                    starting_position = start_sequence
                else:
                    starting_position = f"AT_SEQUENCE_NUMBER:{start_sequence}"
            else:
                starting_position = "TRIM_HORIZON"  # Start from oldest record

            # Get shard iterator
            try:
                response = kinesis_client.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType=starting_position,
                )
                shard_iterator = response["ShardIterator"]
            except ClientError as e:
                if e.response["Error"]["Code"] == "InvalidArgumentException":
                    # Try with LATEST if AT_SEQUENCE_NUMBER fails
                    response = kinesis_client.get_shard_iterator(
                        StreamName=stream_name,
                        ShardId=shard_id,
                        ShardIteratorType="LATEST",
                    )
                    shard_iterator = response["ShardIterator"]
                    logger.warning(f"Using LATEST position for shard {shard_id}")
                else:
                    raise

            # Read records with retry logic
            retry_count = 0
            while records_read < max_records and retry_count < max_retries:
                try:
                    # Get records from Kinesis
                    response = kinesis_client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=min(batch_size, max_records - records_read),
                    )

                    records = response.get("Records", [])
                    next_shard_iterator = response.get("NextShardIterator")

                    if not records:
                        # No more records available
                        break

                    # Process records
                    for record in records:
                        if records_read >= max_records:
                            break

                        # Check if we've reached the end sequence
                        if end_sequence and record["SequenceNumber"] >= end_sequence:
                            logger.info(
                                f"Reached end sequence {end_sequence} for shard {shard_id}"
                            )
                            return

                        # Create record with enhanced metadata
                        record_dict = {
                            "SequenceNumber": record["SequenceNumber"],
                            "Data": (
                                record["Data"].decode("utf-8")
                                if record["Data"]
                                else None
                            ),
                            "PartitionKey": record["PartitionKey"],
                            "ApproximateArrivalTimestamp": record[
                                "ApproximateArrivalTimestamp"
                            ],
                            "stream_name": stream_name,
                            "shard_id": shard_id,
                        }

                        # Update current position
                        current_sequence = record["SequenceNumber"]

                        # Record metrics
                        data_size = len(record["Data"]) if record["Data"] else 0
                        metrics.record_read(1, data_size)
                        records_read += 1

                        yield record_dict

                    # Update shard iterator for next batch
                    if next_shard_iterator:
                        shard_iterator = next_shard_iterator
                    else:
                        # Shard is closed
                        logger.info(f"Shard {shard_id} is closed")
                        break

                    # Reset retry count on success
                    retry_count = 0

                except ClientError as e:
                    error_code = e.response["Error"]["Code"]
                    if error_code == "ExpiredIteratorException":
                        # Get new shard iterator
                        try:
                            response = kinesis_client.get_shard_iterator(
                                StreamName=stream_name,
                                ShardId=shard_id,
                                ShardIteratorType="AT_SEQUENCE_NUMBER",
                                StartingSequenceNumber=current_sequence or "0",
                            )
                            shard_iterator = response["ShardIterator"]
                            logger.info(f"Refreshed shard iterator for {shard_id}")
                            continue
                        except Exception:
                            retry_count += 1
                            time.sleep(retry_delay)
                    elif error_code in [
                        "ProvisionedThroughputExceededException",
                        "ThrottlingException",
                    ]:
                        # Rate limiting, wait and retry
                        retry_count += 1
                        wait_time = retry_delay * (
                            2**retry_count
                        )  # Exponential backoff
                        logger.warning(
                            f"Rate limited, waiting {wait_time}s before retry {retry_count}"
                        )
                        time.sleep(wait_time)
                    else:
                        # Other client errors
                        logger.error(f"Kinesis client error: {e}")
                        retry_count += 1
                        time.sleep(retry_delay)

                except BotoCoreError as e:
                    # Network or configuration errors
                    logger.error(f"Boto core error: {e}")
                    retry_count += 1
                    time.sleep(retry_delay)

                except Exception as e:
                    # Unexpected errors
                    logger.error(f"Unexpected error reading from Kinesis: {e}")
                    retry_count += 1
                    time.sleep(retry_delay)

            if retry_count >= max_retries:
                raise RuntimeError(
                    f"Failed to read from Kinesis after {max_retries} retries"
                )

        except Exception as e:
            logger.error(f"Error reading from Kinesis shard {shard_id}: {e}")
            metrics.record_error()
            raise

    def get_position() -> str:
        """Get current position as sequence number string."""
        return (
            f"AT_SEQUENCE_NUMBER:{current_sequence}"
            if current_sequence
            else "TRIM_HORIZON"
        )

    return read_shard, get_position


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
            start_sequence=(
                _parse_kinesis_position(start_sequence) if start_sequence else None
            ),
            end_sequence=(
                _parse_kinesis_position(end_sequence) if end_sequence else None
            ),
            max_records=self.max_records_per_task,
        )

        def get_schema() -> pa.Schema:
            """Return schema for Kinesis records with optional dynamic inference.

            This provides a balance between functionality and maintainability by
            attempting to infer schema from data when possible, with a reliable
            fallback to the standard schema.
            """
            try:
                # Try to get a sample record for schema inference
                sample_reader = read_shard_fn()
                sample_record = None

                # Get just one sample record for efficiency
                for record in sample_reader:
                    sample_record = record
                    break

                if sample_record:
                    # Use PyArrow's schema inference on the sample
                    inferred_schema = pa.infer_schema([sample_record])

                    # Ensure we have the required streaming fields
                    required_fields = {
                        "partition_id": pa.string(),
                        "read_timestamp": pa.string(),
                        "current_position": pa.string(),
                    }

                    # Merge inferred schema with required fields
                    final_fields = list(inferred_schema)
                    existing_names = {field.name for field in final_fields}

                    for name, field_type in required_fields.items():
                        if name not in existing_names:
                            final_fields.append(pa.field(name, field_type))

                    return pa.schema(final_fields)

            except Exception as e:
                logger.debug(f"Schema inference failed, using standard schema: {e}")

            # Standard schema fallback
            return pa.schema(
                [
                    ("SequenceNumber", pa.string()),
                    ("Data", pa.string()),
                    ("PartitionKey", pa.string()),
                    ("ApproximateArrivalTimestamp", pa.timestamp("us")),
                    ("stream_name", pa.string()),
                    ("shard_id", pa.string()),
                    ("partition_id", pa.string()),
                    ("read_timestamp", pa.string()),
                    ("current_position", pa.string()),
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
                ("read_timestamp", pa.string()),
                ("current_position", pa.string()),
            ]
        )
