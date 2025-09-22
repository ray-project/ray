"""Kafka datasource for unbound data streams.

This module provides a Kafka datasource implementation for Ray Data that works
with the UnboundedDataOperator.
"""

from typing import Any, Dict, Iterator, List, Optional, Union

import pyarrow as pa

from ray.data.datasource.datasource import ReadTask
from ray.data.block import BlockMetadata
from ray.data.datasource.unbound_datasource import (
    UnboundDatasource,
    create_unbound_read_task,
)


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

        Raises:
            ValueError: If required configuration is missing
        """
        super().__init__("kafka")

        # Validate required configuration
        if not kafka_config.get("bootstrap_servers"):
            raise ValueError("bootstrap_servers is required in kafka_config")

        if not topics:
            raise ValueError("topics cannot be empty")

        if max_records_per_task <= 0:
            raise ValueError("max_records_per_task must be positive")

        self.topics = topics if isinstance(topics, list) else [topics]
        self.kafka_config = kafka_config
        self.max_records_per_task = max_records_per_task
        self.start_offset = start_offset
        self.end_offset = end_offset

    def _get_read_tasks_for_partition(
        self,
        partition_info: Dict[str, Any],
        parallelism: int,
    ) -> List[ReadTask]:
        """Create read tasks for Kafka topics.

        Args:
            partition_info: Partition information (not used for Kafka, we use topics)
            parallelism: Number of parallel read tasks to create

        Returns:
            List of ReadTask objects for Kafka topics
        """
        tasks = []

        # Create one task per topic, up to parallelism limit
        topics_to_process = (
            self.topics[:parallelism] if parallelism > 0 else self.topics
        )

        for topic in topics_to_process:

            def create_kafka_read_fn(topic_name: str):
                def kafka_read_fn() -> Iterator[pa.Table]:
                    """Read function for Kafka topic."""
                    # Mock Kafka data generation for testing
                    # In production, this would use kafka-python or confluent-kafka
                    records = []
                    for i in range(self.max_records_per_task):
                        records.append(
                            {
                                "offset": i,
                                "key": f"key_{i}",
                                "value": f"message_{i}_from_{topic_name}",
                                "topic": topic_name,
                                "partition": 0,
                                "timestamp": "2024-01-01T00:00:00Z",
                            }
                        )

                    # Convert to PyArrow table
                    if records:
                        table = pa.Table.from_pylist(records)
                        yield table

                return kafka_read_fn

            # Create metadata for this task
            metadata = BlockMetadata(
                num_rows=self.max_records_per_task,
                size_bytes=None,
                input_files=[f"kafka://{topic}"],
                exec_stats=None,
            )

            # Create schema
            schema = pa.schema(
                [
                    ("offset", pa.int64()),
                    ("key", pa.string()),
                    ("value", pa.string()),
                    ("topic", pa.string()),
                    ("partition", pa.int32()),
                    ("timestamp", pa.string()),
                ]
            )

            # Create read task
            task = create_unbound_read_task(
                read_fn=create_kafka_read_fn(topic),
                metadata=metadata,
                schema=schema,
            )
            tasks.append(task)

        return tasks

    def get_name(self) -> str:
        """Get name of this datasource."""
        return "kafka_unbound_datasource"

    def get_unbound_schema(self, kafka_config: Dict[str, Any]) -> Optional["pa.Schema"]:
        """Get schema for Kafka messages.

        Args:
            kafka_config: Kafka configuration

        Returns:
            PyArrow schema for Kafka messages
        """
        # Standard Kafka message schema
        return pa.schema(
            [
                ("offset", pa.int64()),
                ("key", pa.string()),
                ("value", pa.string()),
                ("topic", pa.string()),
                ("partition", pa.int32()),
                ("timestamp", pa.string()),
            ]
        )

    def supports_distributed_reads(self) -> bool:
        """Kafka datasource supports distributed reads."""
        return True

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size for Kafka streams.

        Returns:
            None for unbounded streams
        """
        return None
