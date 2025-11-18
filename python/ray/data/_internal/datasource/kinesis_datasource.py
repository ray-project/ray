"""Kinesis datasource for unbound data streams.

This module provides a Kinesis datasource implementation for Ray Data that works
with the UnboundedDataOperator.

Requires:
    - boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
"""

import logging
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.unbound_datasource import (
    UnboundDatasource,
    create_unbound_read_task,
)

logger = logging.getLogger(__name__)


def _check_boto3_available():
    """Check if boto3 is available."""
    try:
        import boto3  # noqa: F401

        return True
    except ImportError:
        raise ImportError(
            "boto3 is required for Kinesis datasource. "
            "Install with: pip install boto3"
        )


class KinesisDatasource(UnboundDatasource):
    """Kinesis datasource for reading from Kinesis streams."""

    def __init__(
        self,
        stream_name: str,
        kinesis_config: Dict[str, Any],
        max_records_per_task: int = 1000,
        start_sequence: Optional[str] = None,
        end_sequence: Optional[str] = None,
        enhanced_fan_out: bool = False,
        consumer_name: Optional[str] = None,
        poll_interval_seconds: float = 5.0,
    ):
        """Initialize Kinesis datasource.

        Args:
            stream_name: Kinesis stream name to read from
            kinesis_config: Kinesis configuration dictionary
            max_records_per_task: Maximum records per task
            start_sequence: Starting sequence number for reading
            end_sequence: Ending sequence number for reading
            enhanced_fan_out: Whether to use enhanced fan-out (EFO) consumers
            consumer_name: Consumer name for enhanced fan-out
            poll_interval_seconds: Seconds between Kinesis GetRecords calls (default 5.0s)

        Raises:
            ValueError: If required configuration is missing
            ImportError: If boto3 is not installed
        """
        super().__init__("kinesis")
        _check_boto3_available()

        # Validate required configuration
        if not stream_name:
            raise ValueError("stream_name cannot be empty")

        if not kinesis_config.get("region_name") and not kinesis_config.get(
            "aws_region"
        ):
            raise ValueError("region_name or aws_region is required in kinesis_config")

        if max_records_per_task <= 0:
            raise ValueError("max_records_per_task must be positive")

        if enhanced_fan_out and not consumer_name:
            raise ValueError(
                "consumer_name is required when enhanced_fan_out is enabled"
            )

        self.stream_name = stream_name
        self.kinesis_config = kinesis_config
        self.max_records_per_task = max_records_per_task
        self.start_sequence = start_sequence or "LATEST"
        self.end_sequence = end_sequence
        self.enhanced_fan_out = enhanced_fan_out
        self.consumer_name = consumer_name
        self.poll_interval_seconds = poll_interval_seconds

    def _get_read_tasks_for_partition(
        self,
        partition_info: Dict[str, Any],
        parallelism: int,
    ) -> List[ReadTask]:
        """Create read tasks for Kinesis shards.

        Args:
            partition_info: Partition information (not used for Kinesis, we use shards)
            parallelism: Number of parallel read tasks to create

        Returns:
            List of ReadTask objects for Kinesis shards
        """
        import boto3

        # Build session kwargs for credential management
        session_kwargs = {
            "region_name": self.kinesis_config.get("region_name")
            or self.kinesis_config.get("aws_region")
        }

        if "aws_access_key_id" in self.kinesis_config:
            session_kwargs["aws_access_key_id"] = self.kinesis_config[
                "aws_access_key_id"
            ]
        if "aws_secret_access_key" in self.kinesis_config:
            session_kwargs["aws_secret_access_key"] = self.kinesis_config[
                "aws_secret_access_key"
            ]
        if "aws_session_token" in self.kinesis_config:
            session_kwargs["aws_session_token"] = self.kinesis_config[
                "aws_session_token"
            ]

        session = boto3.Session(**session_kwargs)
        client_kwargs = {"region_name": session_kwargs["region_name"]}
        if "endpoint_url" in self.kinesis_config:
            client_kwargs["endpoint_url"] = self.kinesis_config["endpoint_url"]

        kinesis_client = session.client("kinesis", **client_kwargs)

        # Get shard IDs from stream using list_shards (more efficient than describe_stream)
        response = kinesis_client.list_shards(StreamName=self.stream_name)
        shard_ids = [shard["ShardId"] for shard in response["Shards"]]

        # Limit to parallelism if specified
        if parallelism > 0:
            shard_ids = shard_ids[:parallelism]

        tasks = []

        # Store config for use in read functions (avoid serialization issues)
        kinesis_config = self.kinesis_config
        stream_name = self.stream_name
        max_records_per_task = self.max_records_per_task
        start_sequence = self.start_sequence
        end_sequence = self.end_sequence
        enhanced_fan_out = self.enhanced_fan_out
        consumer_name = self.consumer_name
        poll_interval_seconds = self.poll_interval_seconds

        for shard_id_val in shard_ids:

            def create_kinesis_read_fn(
                shard_id_val: str = shard_id_val,
                kinesis_config: Dict[str, Any] = kinesis_config,
                stream_name: str = stream_name,
                max_records_per_task: int = max_records_per_task,
                start_sequence: str = start_sequence,
                end_sequence: Optional[str] = end_sequence,
                enhanced_fan_out: bool = enhanced_fan_out,
                consumer_name: Optional[str] = consumer_name,
                poll_interval_seconds: float = poll_interval_seconds,
            ):
                """Create a Kinesis read function with captured variables.

                This factory function captures all needed configuration as default arguments
                to avoid serialization issues. When Ray executes this remotely, it won't
                need to serialize the parent datasource object.

                Args:
                    shard_id_val: The Kinesis shard ID to read from
                    kinesis_config: Configuration dictionary for Kinesis client
                    stream_name: Name of the Kinesis stream
                    max_records_per_task: Maximum records to read in this task
                    start_sequence: Starting sequence number or position type
                    end_sequence: Optional ending sequence number
                    enhanced_fan_out: Whether to use Enhanced Fan-Out
                    consumer_name: Consumer name for Enhanced Fan-Out
                """

                def kinesis_read_fn() -> Iterator[pa.Table]:
                    """Read function for Kinesis shard using boto3.

                    This function runs remotely in a Ray task. It supports two reading modes:
                    1. Enhanced Fan-Out (subscribe_to_shard) - dedicated throughput, lower latency
                    2. Standard polling (get_records) - shared throughput, cost-effective

                    Yields PyArrow tables incrementally for efficient streaming processing.
                    """
                    import base64
                    from datetime import datetime

                    import boto3

                    # Recreate AWS session in the remote task
                    # This ensures credentials are properly available in the Ray worker
                    session_kwargs = {
                        "region_name": kinesis_config.get("region_name")
                        or kinesis_config.get("aws_region")
                    }

                    # Add explicit credentials if provided
                    # Otherwise boto3 will use the default credential chain (IAM role, env vars, etc.)
                    if "aws_access_key_id" in kinesis_config:
                        session_kwargs["aws_access_key_id"] = kinesis_config[
                            "aws_access_key_id"
                        ]
                    if "aws_secret_access_key" in kinesis_config:
                        session_kwargs["aws_secret_access_key"] = kinesis_config[
                            "aws_secret_access_key"
                        ]
                    if "aws_session_token" in kinesis_config:
                        session_kwargs["aws_session_token"] = kinesis_config[
                            "aws_session_token"
                        ]

                    session = boto3.Session(**session_kwargs)

                    client_kwargs = {"region_name": session_kwargs["region_name"]}
                    # Support custom endpoints (e.g., LocalStack for testing)
                    if "endpoint_url" in kinesis_config:
                        client_kwargs["endpoint_url"] = kinesis_config["endpoint_url"]

                    kinesis_client = session.client("kinesis", **client_kwargs)

                    try:
                        records = []
                        records_read = 0

                        # For streaming execution: yield blocks incrementally (in batches of 1000)
                        # This allows Ray Data to process data as it arrives rather than waiting
                        # for all data to be read, improving memory efficiency and throughput
                        batch_size = min(max_records_per_task, 1000)

                        # Choose reading mode: Enhanced Fan-Out vs Standard Polling
                        # Enhanced Fan-Out provides dedicated 2MB/sec throughput per consumer
                        # Standard polling shares 2MB/sec across all consumers
                        if enhanced_fan_out and consumer_name:
                            # Enhanced Fan-Out mode using subscribe_to_shard API
                            # Provides push-based delivery with HTTP/2 streaming

                            # Get the stream ARN needed for Enhanced Fan-Out APIs
                            stream_desc = kinesis_client.describe_stream(
                                StreamName=stream_name
                            )
                            stream_arn = stream_desc["StreamDescription"]["StreamARN"]

                            try:
                                # Check if consumer already exists
                                consumer_response = (
                                    kinesis_client.describe_stream_consumer(
                                        StreamARN=stream_arn, ConsumerName=consumer_name
                                    )
                                )
                                consumer_arn = consumer_response["ConsumerDescription"][
                                    "ConsumerARN"
                                ]
                            except kinesis_client.exceptions.ResourceNotFoundException:
                                # Consumer doesn't exist yet - register it
                                # This creates a dedicated Enhanced Fan-Out consumer
                                consumer_response = (
                                    kinesis_client.register_stream_consumer(
                                        StreamARN=stream_arn, ConsumerName=consumer_name
                                    )
                                )
                                consumer_arn = consumer_response["Consumer"][
                                    "ConsumerARN"
                                ]

                            # Build starting position for subscription
                            # Supports: LATEST, TRIM_HORIZON, AT_SEQUENCE_NUMBER, AT_TIMESTAMP
                            starting_position = {"Type": start_sequence}
                            if start_sequence not in [
                                "LATEST",
                                "TRIM_HORIZON",
                                "AT_TIMESTAMP",
                            ]:
                                # User provided a specific sequence number
                                starting_position = {
                                    "Type": "AT_SEQUENCE_NUMBER",
                                    "SequenceNumber": start_sequence,
                                }

                            # Subscribe to the shard with Enhanced Fan-Out
                            # This returns an event stream that pushes records as they arrive
                            response = kinesis_client.subscribe_to_shard(
                                ConsumerARN=consumer_arn,
                                ShardId=shard_id_val,
                                StartingPosition=starting_position,
                            )

                            # Process the HTTP/2 event stream from Enhanced Fan-Out
                            # Events are pushed to us as they arrive (low latency)
                            event_stream = response["EventStream"]
                            for event in event_stream:
                                # Each event can contain multiple types, we care about SubscribeToShardEvent
                                if "SubscribeToShardEvent" in event:
                                    shard_event = event["SubscribeToShardEvent"]
                                    # Process all records in this event batch
                                    for record in shard_event["Records"]:
                                        # Check if we've reached the end sequence (for bounded reads)
                                        if (
                                            end_sequence
                                            and record["SequenceNumber"] >= end_sequence
                                        ):
                                            # Yield any accumulated records and stop
                                            if records:
                                                yield pa.Table.from_pylist(records)
                                            return

                                        # Decode the record data from bytes
                                        # Decode data as UTF-8 string or base64 for binary data
                                        # Users can parse JSON themselves using map_batches if needed
                                        data = record["Data"]
                                        if isinstance(data, bytes):
                                            try:
                                                # Decode as UTF-8 string (works for JSON, plain text, etc.)
                                                data = data.decode("utf-8")
                                            except UnicodeDecodeError:
                                                # Binary data - encode as base64 string for safety
                                                data = base64.b64encode(data).decode(
                                                    "ascii"
                                                )

                                        # Build record with all Kinesis metadata
                                        records.append(
                                            {
                                                "sequence_number": record[
                                                    "SequenceNumber"
                                                ],  # Unique per shard
                                                "partition_key": record[
                                                    "PartitionKey"
                                                ],  # Used for shard assignment
                                                "data": data,  # Decoded payload
                                                "stream_name": stream_name,
                                                "shard_id": shard_id_val,
                                                "timestamp": record[
                                                    "ApproximateArrivalTimestamp"
                                                ].isoformat(),
                                            }
                                        )
                                        records_read += 1

                                        # Yield incrementally when batch is full
                                        # This allows downstream processing to start before all data is read
                                        if len(records) >= batch_size:
                                            yield pa.Table.from_pylist(records)
                                            records = []  # Clear for next batch

                                        # Check if we've hit our total record limit
                                        if records_read >= max_records_per_task:
                                            if records:
                                                yield pa.Table.from_pylist(records)
                                            return
                        else:
                            # Standard polling with get_records
                            iterator_kwargs = {
                                "StreamName": stream_name,
                                "ShardId": shard_id_val,
                                "ShardIteratorType": start_sequence,
                            }

                            # If specific sequence number provided, use it
                            if start_sequence and start_sequence not in [
                                "LATEST",
                                "TRIM_HORIZON",
                                "AT_TIMESTAMP",
                            ]:
                                iterator_kwargs[
                                    "ShardIteratorType"
                                ] = "AT_SEQUENCE_NUMBER"
                                iterator_kwargs[
                                    "StartingSequenceNumber"
                                ] = start_sequence

                            response = kinesis_client.get_shard_iterator(
                                **iterator_kwargs
                            )
                            shard_iterator = response["ShardIterator"]

                            # Read records from shard with proper pagination
                            while (
                                shard_iterator and records_read < max_records_per_task
                            ):
                                # Respect Kinesis limits: max 10000 records or 10MB per call
                                response = kinesis_client.get_records(
                                    ShardIterator=shard_iterator,
                                    Limit=min(
                                        batch_size,
                                        max_records_per_task - records_read,
                                        kinesis_config.get(
                                            "max_records_per_request", 10000
                                        ),
                                    ),
                                )

                                for record in response["Records"]:
                                    # Check if we've reached end sequence
                                    if (
                                        end_sequence
                                        and record["SequenceNumber"] >= end_sequence
                                    ):
                                        if records:
                                            yield pa.Table.from_pylist(records)
                                        return

                                    # Decode data as UTF-8 string or base64 for binary data
                                    # Users can parse JSON themselves using map_batches if needed
                                    data = record["Data"]
                                    if isinstance(data, bytes):
                                        try:
                                            # Decode as UTF-8 string (works for JSON, plain text, etc.)
                                            data = data.decode("utf-8")
                                        except UnicodeDecodeError:
                                            # Binary data - encode as base64 string for safety
                                            data = base64.b64encode(data).decode(
                                                "ascii"
                                            )

                                    records.append(
                                        {
                                            "sequence_number": record["SequenceNumber"],
                                            "partition_key": record["PartitionKey"],
                                            "data": data,
                                            "stream_name": stream_name,
                                            "shard_id": shard_id_val,
                                            "timestamp": record[
                                                "ApproximateArrivalTimestamp"
                                            ].isoformat()
                                            if "ApproximateArrivalTimestamp" in record
                                            else datetime.utcnow().isoformat(),
                                        }
                                    )
                                    records_read += 1

                                # Yield incrementally for better streaming performance
                                if records and len(records) >= batch_size:
                                    yield pa.Table.from_pylist(records)
                                    records = []

                                if records_read >= max_records_per_task:
                                    if records:
                                        yield pa.Table.from_pylist(records)
                                    return

                                # Get next iterator
                                shard_iterator = response.get("NextShardIterator")

                                # Respect Kinesis rate limits - if MillisBehindLatest is 0, add delay
                                # This prevents busy-waiting when caught up to latest data
                                if response.get("MillisBehindLatest", 0) == 0:
                                    import time

                                    time.sleep(poll_interval_seconds)

                                # If no more records and we haven't hit our limit, break
                                if not response["Records"]:
                                    if records:
                                        yield pa.Table.from_pylist(records)
                                    break

                        # Yield any remaining records
                        if records:
                            yield pa.Table.from_pylist(records)

                    except Exception as e:
                        # Log error but allow task to complete gracefully
                        logger.error(
                            f"Error reading from Kinesis shard {shard_id_val}: {e}"
                        )
                        # Yield any records we successfully read before the error
                        if records:
                            yield pa.Table.from_pylist(records)

                return kinesis_read_fn

            # Create metadata for this task
            metadata = BlockMetadata(
                num_rows=self.max_records_per_task,
                size_bytes=None,
                input_files=[f"kinesis://{self.stream_name}/shard-{shard_id_val}"],
                exec_stats=None,
            )

            # Create schema
            schema = pa.schema(
                [
                    ("sequence_number", pa.string()),
                    ("partition_key", pa.string()),
                    ("data", pa.string()),
                    ("stream_name", pa.string()),
                    ("shard_id", pa.string()),
                    ("timestamp", pa.string()),
                ]
            )

            # Create read task
            task = create_unbound_read_task(
                read_fn=create_kinesis_read_fn(shard_id_val),
                metadata=metadata,
                schema=schema,
            )
            tasks.append(task)

        return tasks

    def get_name(self) -> str:
        """Get name of this datasource."""
        return "kinesis_unbound_datasource"

    def get_unbound_schema(
        self, kinesis_config: Dict[str, Any]
    ) -> Optional["pa.Schema"]:
        """Get schema for Kinesis records.

        Args:
            kinesis_config: Kinesis configuration

        Returns:
            PyArrow schema for Kinesis records
        """
        # Standard Kinesis record schema
        return pa.schema(
            [
                ("sequence_number", pa.string()),
                ("partition_key", pa.string()),
                ("data", pa.string()),
                ("stream_name", pa.string()),
                ("shard_id", pa.string()),
                ("timestamp", pa.string()),
            ]
        )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size for Kinesis streams.

        Returns:
            None for unbounded streams
        """
        return None
