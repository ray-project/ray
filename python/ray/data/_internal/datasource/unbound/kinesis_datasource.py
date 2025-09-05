"""Kinesis datasource for unbound data streams.

This module provides a simplified Kinesis datasource implementation for Ray Data.
"""

from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from ray.data._internal.datasource.datasource import ReadTask
from ray.data._internal.datasource.unbound.unbound_datasource import UnboundDatasource
from ray.data._internal.datasource.unbound.position import UnboundPosition
from ray.data._internal.datasource.unbound.read_task import create_unbound_read_task
from ray.data._internal.datasource.unbound.utils import validate_unbound_config


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
    ):
        """Initialize Kinesis datasource.

        Args:
            stream_name: Name of the Kinesis stream
            kinesis_config: Kinesis configuration dictionary
            max_records_per_task: Maximum records per task
            start_sequence: Starting sequence number
            end_sequence: Ending sequence number
            enhanced_fan_out: Use Enhanced Fan-Out
            consumer_name: Consumer name for Enhanced Fan-Out
        """
        super().__init__("kinesis")
        self.stream_name = stream_name
        self.kinesis_config = kinesis_config
        self.max_records_per_task = max_records_per_task
        self.start_sequence = start_sequence
        self.end_sequence = end_sequence
        self.enhanced_fan_out = enhanced_fan_out
        self.consumer_name = consumer_name

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Validate Kinesis configuration.

        Args:
            config: Kinesis configuration

        Raises:
            ValueError: If configuration is invalid
        """
        validate_unbound_config(config)

        # Check required Kinesis parameters
        if "region_name" not in config:
            raise ValueError("region_name is required for Kinesis")

    def get_unbound_partitions(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get Kinesis stream shards.

        Args:
            config: Kinesis configuration

        Returns:
            List of partition configurations
        """
        self._validate_config(config)

        # For simplicity, create one partition per shard
        # In a real implementation, this would query Kinesis for actual shards
        partitions = []

        # Create a single partition for the stream
        partitions.append(
            {
                "partition_id": f"{self.stream_name}_shard_0",
                "stream_name": self.stream_name,
                "shard_id": "shardId-000000000000",
                "region_name": config["region_name"],
                "aws_access_key_id": config.get("aws_access_key_id"),
                "aws_secret_access_key": config.get("aws_secret_access_key"),
                "aws_session_token": config.get("aws_session_token"),
            }
        )

        return partitions

    def _create_unbound_read_task(
        self,
        partition_config: Dict[str, Any],
        position: Optional[UnboundPosition] = None,
    ) -> ReadTask:
        """Create a read task for a Kinesis shard.

        Args:
            partition_config: Partition configuration
            position: Starting position

        Returns:
            ReadTask for this partition
        """

        def read_kinesis_shard() -> Iterator[Dict[str, Any]]:
            """Read from Kinesis shard."""
            try:
                import boto3
            except ImportError:
                raise ImportError("boto3 is required for Kinesis datasource")

            # Create Kinesis client
            kinesis_client = boto3.client(
                "kinesis",
                region_name=partition_config["region_name"],
                aws_access_key_id=partition_config.get("aws_access_key_id"),
                aws_secret_access_key=partition_config.get("aws_secret_access_key"),
                aws_session_token=partition_config.get("aws_session_token"),
            )

            # Get shard iterator
            shard_iterator = kinesis_client.get_shard_iterator(
                StreamName=partition_config["stream_name"],
                ShardId=partition_config["shard_id"],
                ShardIteratorType="LATEST",
            )["ShardIterator"]

            try:
                while True:
                    response = kinesis_client.get_records(
                        ShardIterator=shard_iterator, Limit=100
                    )

                    for record in response["Records"]:
                        yield {
                            "stream_name": partition_config["stream_name"],
                            "shard_id": partition_config["shard_id"],
                            "sequence_number": record["SequenceNumber"],
                            "partition_key": record["PartitionKey"],
                            "data": record["Data"].decode("utf-8"),
                            "approximate_arrival_timestamp": record[
                                "ApproximateArrivalTimestamp"
                            ],
                        }

                    shard_iterator = response.get("NextShardIterator")
                    if not shard_iterator:
                        break

            except Exception as e:
                raise RuntimeError(f"Kinesis read error: {e}") from e

        def get_current_position() -> str:
            """Get current position in the stream."""
            return f"sequence:{position.value if position else 'latest'}"

        def get_kinesis_schema() -> pa.Schema:
            """Get Kinesis record schema."""
            return pa.schema(
                [
                    ("stream_name", pa.string()),
                    ("shard_id", pa.string()),
                    ("sequence_number", pa.string()),
                    ("partition_key", pa.string()),
                    ("data", pa.string()),
                    ("approximate_arrival_timestamp", pa.timestamp("s")),
                ]
            )

        return create_unbound_read_task(
            partition_id=partition_config["partition_id"],
            unbound_config=partition_config,
            read_source_fn=read_kinesis_shard,
            get_position_fn=get_current_position,
            get_schema_fn=get_kinesis_schema,
        )

    def get_unbound_schema(self, config: Dict[str, Any]) -> Optional[pa.Schema]:
        """Get schema for Kinesis records.

        Args:
            config: Kinesis configuration

        Returns:
            PyArrow schema for Kinesis records
        """
        return pa.schema(
            [
                ("stream_name", pa.string()),
                ("shard_id", pa.string()),
                ("sequence_number", pa.string()),
                ("partition_key", pa.string()),
                ("data", pa.string()),
                ("approximate_arrival_timestamp", pa.timestamp("s")),
            ]
        )

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """Get read tasks for this datasource.

        Args:
            parallelism: Desired parallelism level

        Returns:
            List of read tasks
        """
        # Get partitions using the stored config
        partitions = self.get_unbound_partitions(self.kinesis_config)

        # Create read tasks for each partition
        read_tasks = []
        for partition_config in partitions:
            read_task = self._create_unbound_read_task(partition_config)
            read_tasks.append(read_task)

        return read_tasks
