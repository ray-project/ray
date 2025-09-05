"""Kafka datasource for unbound data streams.

This module provides a simplified Kafka datasource implementation for Ray Data.
"""

from typing import Any, Dict, Iterator, List, Optional, Union

import pyarrow as pa

from ray.data._internal.datasource.datasource import ReadTask
from ray.data._internal.datasource.unbound.unbound_datasource import UnboundDatasource
from ray.data._internal.datasource.unbound.position import UnboundPosition
from ray.data._internal.datasource.unbound.read_task import create_unbound_read_task
from ray.data._internal.datasource.unbound.utils import validate_unbound_config


class KafkaDatasource(UnboundDatasource):
    """Kafka datasource for reading from Kafka topics."""

    def __init__(
        self,
        topics: Union[str, List[str]],
        kafka_config: Dict[str, Any],
        max_records_per_task: int = 1000,
        start_offset: Optional[str] = None,
        end_offset: Optional[str] = None,
    ):
        """Initialize Kafka datasource.

        Args:
            topics: Kafka topic name(s) to read from
            kafka_config: Kafka configuration dictionary
            max_records_per_task: Maximum records per task
            start_offset: Starting offset for reading
            end_offset: Ending offset for reading
        """
        super().__init__("kafka")
        self.topics = topics if isinstance(topics, list) else [topics]
        self.kafka_config = kafka_config
        self.max_records_per_task = max_records_per_task
        self.start_offset = start_offset
        self.end_offset = end_offset

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Validate Kafka configuration.

        Args:
            config: Kafka configuration

        Raises:
            ValueError: If configuration is invalid
        """
        validate_unbound_config(config)

        # Check required Kafka parameters
        if "bootstrap_servers" not in config:
            raise ValueError("bootstrap_servers is required for Kafka")

    def get_unbound_partitions(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get Kafka topic partitions.

        Args:
            config: Kafka configuration

        Returns:
            List of partition configurations
        """
        self._validate_config(config)

        # For simplicity, create one partition per topic
        # In a real implementation, this would query Kafka for actual partitions
        partitions = []
        for topic in self.topics:
            partitions.append(
                {
                    "partition_id": f"{topic}_partition_0",
                    "topic": topic,
                    "bootstrap_servers": config["bootstrap_servers"],
                    "group_id": config.get("group_id", "ray_data_consumer"),
                    "auto_offset_reset": config.get("auto_offset_reset", "latest"),
                }
            )

        return partitions

    def _create_unbound_read_task(
        self,
        partition_config: Dict[str, Any],
        position: Optional[UnboundPosition] = None,
    ) -> ReadTask:
        """Create a read task for a Kafka partition.

        Args:
            partition_config: Partition configuration
            position: Starting position

        Returns:
            ReadTask for this partition
        """

        def read_kafka_partition() -> Iterator[Dict[str, Any]]:
            """Read from Kafka partition."""
            try:
                from kafka import KafkaConsumer
            except ImportError:
                raise ImportError("kafka-python is required for Kafka datasource")

            # Create consumer
            consumer = KafkaConsumer(
                partition_config["topic"],
                bootstrap_servers=partition_config["bootstrap_servers"],
                group_id=partition_config["group_id"],
                auto_offset_reset=partition_config["auto_offset_reset"],
                enable_auto_commit=True,
                value_deserializer=lambda m: m.decode("utf-8") if m else None,
            )

            try:
                for message in consumer:
                    yield {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key.decode("utf-8") if message.key else None,
                        "value": message.value,
                        "timestamp": message.timestamp,
                    }
            except Exception as e:
                raise RuntimeError(f"Kafka read error: {e}") from e

        def get_current_position() -> str:
            """Get current position in the stream."""
            return f"offset:{position.value if position else 'latest'}"

        def get_kafka_schema() -> pa.Schema:
            """Get Kafka record schema."""
            return pa.schema(
                [
                    ("topic", pa.string()),
                    ("partition", pa.int32()),
                    ("offset", pa.int64()),
                    ("key", pa.string()),
                    ("value", pa.string()),
                    ("timestamp", pa.timestamp("ms")),
                ]
            )

        return create_unbound_read_task(
            partition_id=partition_config["partition_id"],
            unbound_config=partition_config,
            read_source_fn=read_kafka_partition,
            get_position_fn=get_current_position,
            get_schema_fn=get_kafka_schema,
        )

    def get_unbound_schema(self, config: Dict[str, Any]) -> Optional[pa.Schema]:
        """Get schema for Kafka records.

        Args:
            config: Kafka configuration

        Returns:
            PyArrow schema for Kafka records
        """
        return pa.schema(
            [
                ("topic", pa.string()),
                ("partition", pa.int32()),
                ("offset", pa.int64()),
                ("key", pa.string()),
                ("value", pa.string()),
                ("timestamp", pa.timestamp("ms")),
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
        partitions = self.get_unbound_partitions(self.kafka_config)

        # Create read tasks for each partition
        read_tasks = []
        for partition_config in partitions:
            read_task = self._create_unbound_read_task(partition_config)
            read_tasks.append(read_task)

        return read_tasks
