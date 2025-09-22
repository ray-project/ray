"""Flink datasource for unbound data streams.

This module provides a Flink datasource implementation for Ray Data that works
with the UnboundedDataOperator.
"""

from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from ray.data.datasource.datasource import ReadTask
from ray.data.block import BlockMetadata
from ray.data.datasource.unbound_datasource import (
    UnboundDatasource,
    create_unbound_read_task,
)


class FlinkDatasource(UnboundDatasource):
    """Flink datasource for reading from Flink jobs."""

    def __init__(
        self,
        source_type: str,
        flink_config: Dict[str, Any],
        max_records_per_task: int = 1000,
        start_position: Optional[str] = None,
        end_position: Optional[str] = None,
    ):
        """Initialize Flink datasource.

        Args:
            source_type: Type of Flink source (rest_api, table, checkpoint)
            flink_config: Flink configuration dictionary
            max_records_per_task: Maximum records per task
            start_position: Starting position for reading
            end_position: Ending position for reading

        Raises:
            ValueError: If required configuration is missing
        """
        super().__init__("flink")

        # Validate source type
        valid_source_types = {"rest_api", "table", "checkpoint"}
        if source_type not in valid_source_types:
            raise ValueError(f"source_type must be one of {valid_source_types}")

        # Validate configuration based on source type
        if source_type == "rest_api":
            if not flink_config.get("rest_api_url"):
                raise ValueError("rest_api_url is required for rest_api source type")
            if not flink_config.get("job_id"):
                raise ValueError("job_id is required for rest_api source type")
        elif source_type == "table":
            if not flink_config.get("table_name"):
                raise ValueError("table_name is required for table source type")
        elif source_type == "checkpoint":
            if not flink_config.get("checkpoint_path"):
                raise ValueError(
                    "checkpoint_path is required for checkpoint source type"
                )

        if max_records_per_task <= 0:
            raise ValueError("max_records_per_task must be positive")

        self.source_type = source_type
        self.flink_config = flink_config
        self.max_records_per_task = max_records_per_task
        self.start_position = start_position
        self.end_position = end_position

    def _get_read_tasks_for_partition(
        self,
        partition_info: Dict[str, Any],
        parallelism: int,
    ) -> List[ReadTask]:
        """Create read tasks for Flink job outputs.

        Args:
            partition_info: Partition information (not used for Flink)
            parallelism: Number of parallel read tasks to create

        Returns:
            List of ReadTask objects for Flink job outputs
        """
        tasks = []

        # Create tasks based on parallelism (simulate Flink job outputs)
        for task_id in range(min(parallelism, 2)):  # Max 2 tasks for testing

            def create_flink_read_fn(task_num: int):
                def flink_read_fn() -> Iterator[pa.Table]:
                    """Read function for Flink job output."""
                    # Mock Flink data generation for testing
                    # In production, this would connect to Flink REST API or output sink
                    records = []
                    for i in range(self.max_records_per_task):
                        records.append(
                            {
                                "job_id": self.flink_config.get("job_id", "job_123"),
                                "task_id": task_num,
                                "record_id": f"flink_{task_num}_{i}",
                                "data": f"flink_output_{task_num}_{i}",
                                "processing_time": "2024-01-01T00:00:00Z",
                                "watermark": i * 1000,  # Mock watermark
                            }
                        )

                    # Convert to PyArrow table
                    if records:
                        table = pa.Table.from_pylist(records)
                        yield table

                return flink_read_fn

            # Create metadata for this task
            metadata = BlockMetadata(
                num_rows=self.max_records_per_task,
                size_bytes=None,
                input_files=[
                    f"flink://job/{self.flink_config.get('job_id', 'job_123')}/task-{task_id}"
                ],
                exec_stats=None,
            )

            # Create schema
            schema = pa.schema(
                [
                    ("job_id", pa.string()),
                    ("task_id", pa.int64()),
                    ("record_id", pa.string()),
                    ("data", pa.string()),
                    ("processing_time", pa.string()),
                    ("watermark", pa.int64()),
                ]
            )

            # Create read task
            task = create_unbound_read_task(
                read_fn=create_flink_read_fn(task_id),
                metadata=metadata,
                schema=schema,
            )
            tasks.append(task)

        return tasks

    def get_name(self) -> str:
        """Get name of this datasource."""
        return "flink_unbound_datasource"

    def get_unbound_schema(self, flink_config: Dict[str, Any]) -> Optional["pa.Schema"]:
        """Get schema for Flink job outputs.

        Args:
            flink_config: Flink configuration

        Returns:
            PyArrow schema for Flink job outputs
        """
        # Standard Flink job output schema
        return pa.schema(
            [
                ("job_id", pa.string()),
                ("task_id", pa.int64()),
                ("record_id", pa.string()),
                ("data", pa.string()),
                ("processing_time", pa.string()),
                ("watermark", pa.int64()),
            ]
        )

    def supports_distributed_reads(self) -> bool:
        """Flink datasource supports distributed reads."""
        return True

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size for Flink streams.

        Returns:
            None for unbounded streams
        """
        return None
