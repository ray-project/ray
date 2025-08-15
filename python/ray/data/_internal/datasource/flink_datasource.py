"""Flink datasource implementation for Ray Data streaming.

This module provides FlinkDatasource for reading from Apache Flink sources 
as part of Ray Data's streaming capabilities.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import ReadTask
from ray.data.datasource.streaming_datasource import (
    StreamingDatasource,
    StreamingMetrics,
    create_streaming_read_task,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


def _parse_flink_position(position: str) -> Optional[str]:
    """Parse Flink position from position string.

    Args:
        position: Position string in format "checkpoint:123" or just "123".

    Returns:
        Parsed position as string, or None if invalid.
    """
    if position:
        try:
            if position.startswith("checkpoint:"):
                return position.split(":", 1)[1]
            elif position.startswith("timestamp:"):
                return position.split(":", 1)[1]
            else:
                return position
        except (ValueError, IndexError):
            logger.warning(f"Invalid Flink position: {position}")
    return None


def _create_flink_reader(
    partition_id: str,
    source_type: str,
    flink_config: Dict[str, Any],
    start_position: Optional[str] = None,
    end_position: Optional[str] = None,
    max_records: int = 1000,
) -> tuple[callable, callable]:
    """Create a stateful Flink reader with position tracking.

    This function creates a closure that encapsulates all state needed for
    reading from a Flink source, avoiding the use of global variables.

    Args:
        partition_id: Partition identifier.
        source_type: Type of Flink source.
        flink_config: Flink configuration.
        start_position: Starting position.
        end_position: Ending position. If None, reads indefinitely.
        max_records: Maximum records per read.

    Returns:
        Tuple of (read_function, get_position_function).
    """
    # State variables encapsulated in closure
    current_position = start_position
    metrics = StreamingMetrics()

    def read_partition() -> Iterator[Dict[str, Any]]:
        """Read records from Flink source, maintaining position state."""
        nonlocal current_position

        try:
            if source_type == "rest_api":
                yield from _read_from_rest_api(
                    flink_config, max_records, current_position, end_position, metrics
                )
            elif source_type == "sql_query":
                yield from _read_from_sql_query(
                    flink_config, max_records, current_position, end_position, metrics
                )
            elif source_type == "checkpoint":
                yield from _read_from_checkpoint(
                    flink_config, max_records, current_position, end_position, metrics
                )
            elif source_type == "table":
                yield from _read_from_table(
                    flink_config, max_records, current_position, end_position, metrics
                )
            else:
                raise ValueError(f"Unsupported Flink source type: {source_type}")

        except Exception as e:
            logger.error(f"Unexpected error in Flink reader: {e}")
            metrics.record_error()
            raise

    def _read_from_rest_api(
        config: Dict[str, Any],
        max_records: int,
        position: Optional[str],
        end_position: Optional[str],
        metrics: StreamingMetrics,
    ) -> Iterator[Dict[str, Any]]:
        """Read from Flink REST API."""
        _check_import("requests")
        import requests

        rest_api_url = config.get("rest_api_url")
        job_id = config.get("job_id")

        if not rest_api_url or not job_id:
            raise ValueError("rest_api_url and job_id are required for REST API source")

        try:
            # Query Flink REST API for job data
            url = f"{rest_api_url}/jobs/{job_id}/vertices"
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            vertices_data = response.json()
            records_processed = 0

            for vertex in vertices_data.get("vertices", []):
                if records_processed >= max_records:
                    break

                vertex_id = vertex.get("id", "unknown")

                # Check if we've reached the end position
                if end_position is not None and f"vertex:{vertex_id}" >= end_position:
                    logger.info(
                        f"Reached end position {end_position} for Flink REST API"
                    )
                    return  # Stop reading

                record = {
                    "job_id": job_id,
                    "vertex_id": vertex_id,
                    "vertex_name": vertex.get("name"),
                    "parallelism": vertex.get("parallelism"),
                    "status": vertex.get("status"),
                    "start_time": vertex.get("start-time"),
                    "end_time": vertex.get("end-time"),
                    "duration": vertex.get("duration"),
                    "tasks": vertex.get("tasks"),
                    "metrics": vertex.get("metrics", {}),
                    "source_type": "rest_api",
                    "partition_id": partition_id,
                }

                # Update position (use vertex ID as position marker)
                nonlocal current_position
                current_position = f"vertex:{vertex_id}"

                metrics.record_read(1, len(str(record)))
                records_processed += 1
                yield record

        except requests.RequestException as e:
            logger.error(f"Error querying Flink REST API: {e}")
            metrics.record_error()

    def _read_from_sql_query(
        config: Dict[str, Any],
        max_records: int,
        position: Optional[str],
        end_position: Optional[str],
        metrics: StreamingMetrics,
    ) -> Iterator[Dict[str, Any]]:
        """Read from Flink SQL query."""
        sql_query = config.get("sql_query")

        if not sql_query:
            raise ValueError("sql_query is required for SQL query source")

        try:
            # For now, simulate SQL query execution
            # In a real implementation, this would connect to Flink SQL Gateway
            logger.info(f"Executing Flink SQL query: {sql_query}")

            # Simulate query results
            for i in range(min(max_records, 10)):  # Limit simulation
                # Check if we've reached the end position
                if end_position is not None and f"row:{i}" >= end_position:
                    logger.info(
                        f"Reached end position {end_position} for Flink SQL query"
                    )
                    return  # Stop reading

                record = {
                    "query": sql_query,
                    "row_number": i,
                    "result_data": f"simulated_data_{i}",
                    "execution_time": datetime.now(),
                    "source_type": "sql_query",
                    "partition_id": partition_id,
                }

                # Update position
                nonlocal current_position
                current_position = f"row:{i}"

                metrics.record_read(1, len(str(record)))
                yield record

        except Exception as e:
            logger.error(f"Error executing Flink SQL query: {e}")
            metrics.record_error()

    def _read_from_checkpoint(
        config: Dict[str, Any],
        max_records: int,
        position: Optional[str],
        end_position: Optional[str],
        metrics: StreamingMetrics,
    ) -> Iterator[Dict[str, Any]]:
        """Read from Flink checkpoint."""
        checkpoint_path = config.get("checkpoint_path")

        if not checkpoint_path:
            raise ValueError("checkpoint_path is required for checkpoint source")

        try:
            # Simulate reading from checkpoint
            # In a real implementation, this would parse Flink checkpoint files
            logger.info(f"Reading from Flink checkpoint: {checkpoint_path}")

            for i in range(min(max_records, 5)):  # Limit simulation
                # Check if we've reached the end position
                if end_position is not None and f"state:{i}" >= end_position:
                    logger.info(
                        f"Reached end position {end_position} for Flink checkpoint"
                    )
                    return  # Stop reading

                record = {
                    "checkpoint_path": checkpoint_path,
                    "state_id": f"state_{i}",
                    "state_data": f"checkpoint_data_{i}",
                    "checkpoint_timestamp": datetime.now(),
                    "source_type": "checkpoint",
                    "partition_id": partition_id,
                }

                # Update position
                nonlocal current_position
                current_position = f"state:{i}"

                metrics.record_read(1, len(str(record)))
                yield record

        except Exception as e:
            logger.error(f"Error reading from Flink checkpoint: {e}")
            metrics.record_error()

    def _read_from_table(
        config: Dict[str, Any],
        max_records: int,
        position: Optional[str],
        end_position: Optional[str],
        metrics: StreamingMetrics,
    ) -> Iterator[Dict[str, Any]]:
        """Read from Flink table."""
        table_name = config.get("table_name")

        if not table_name:
            raise ValueError("table_name is required for table source")

        try:
            # Simulate reading from Flink table
            # In a real implementation, this would use PyFlink to read from tables
            logger.info(f"Reading from Flink table: {table_name}")

            for i in range(min(max_records, 8)):  # Limit simulation
                # Check if we've reached the end position
                if end_position is not None and f"row_id:{i}" >= end_position:
                    logger.info(f"Reached end position {end_position} for Flink table")
                    return  # Stop reading

                record = {
                    "table_name": table_name,
                    "row_id": i,
                    "column_a": f"value_a_{i}",
                    "column_b": f"value_b_{i}",
                    "row_timestamp": datetime.now(),
                    "source_type": "table",
                    "partition_id": partition_id,
                }

                # Update position
                nonlocal current_position
                current_position = f"row_id:{i}"

                metrics.record_read(1, len(str(record)))
                yield record

        except Exception as e:
            logger.error(f"Error reading from Flink table: {e}")
            metrics.record_error()

    def get_current_position() -> str:
        """Get current position."""
        return f"position:{current_position or 'start'}"

    return read_partition, get_current_position


@PublicAPI(stability="alpha")
class FlinkDatasource(StreamingDatasource):
    """Flink datasource for reading from Apache Flink data streams and sources.

    This datasource supports multiple Flink source types including REST API, SQL queries,
    checkpoints, and tables in both batch and streaming modes.

    Examples:
        REST API source:

        .. testcode::
            :skipif: True

            from ray.data._internal.datasource.flink_datasource import FlinkDatasource

            # Create Flink REST API datasource
            datasource = FlinkDatasource(
                source_type="rest_api",
                flink_config={
                    "rest_api_url": "http://localhost:8081",
                    "job_id": "my-job-id"
                }
            )

        SQL query source:

        .. testcode::
            :skipif: True

            from ray.data._internal.datasource.flink_datasource import FlinkDatasource

            # Create Flink SQL query datasource
            datasource = FlinkDatasource(
                source_type="sql_query",
                flink_config={
                    "sql_query": "SELECT * FROM events WHERE event_time > NOW() - INTERVAL '1' HOUR"
                }
            )

        Checkpoint source:

        .. testcode::
            :skipif: True

            from ray.data._internal.datasource.flink_datasource import FlinkDatasource

            # Create Flink checkpoint datasource
            datasource = FlinkDatasource(
                source_type="checkpoint",
                flink_config={
                    "checkpoint_path": "/path/to/checkpoint"
                }
            )

        Table source:

        .. testcode::
            :skipif: True

            from ray.data._internal.datasource.flink_datasource import FlinkDatasource

            # Create Flink table datasource
            datasource = FlinkDatasource(
                source_type="table",
                flink_config={
                    "table_name": "events_table"
                }
            )
    """

    def __init__(
        self,
        source_type: str,
        flink_config: Dict[str, Any],
        max_records_per_task: int = 1000,
        start_position: Optional[str] = None,
        end_position: Optional[str] = None,
        **kwargs,
    ):
        """Initialize Flink datasource.

        Args:
            source_type: Type of Flink source (rest_api, sql_query, checkpoint, table).
            flink_config: Flink configuration dictionary.
            max_records_per_task: Maximum records per task per batch.
            start_position: Starting position.
            end_position: Ending position.
            **kwargs: Additional arguments passed to StreamingDatasource.
        """
        self.source_type = source_type
        self.flink_config = flink_config

        # Create streaming config
        streaming_config = {
            "source_type": self.source_type,
            "flink_config": self.flink_config,
            "source_identifier": f"flink://{source_type}:{flink_config.get('job_id', flink_config.get('table_name', flink_config.get('checkpoint_path', 'unknown')))}",
        }

        super().__init__(
            max_records_per_task=max_records_per_task,
            start_position=start_position,
            end_position=end_position,
            streaming_config=streaming_config,
            **kwargs,
        )

    def _validate_config(self) -> None:
        """Validate Flink configuration.

        Raises:
            ValueError: If required configuration is missing or invalid.
        """
        if not self.source_type:
            raise ValueError("source_type is required for Flink datasource")

        if self.source_type not in ["rest_api", "sql_query", "checkpoint", "table"]:
            raise ValueError(
                f"Unsupported source_type: {self.source_type}. "
                "Must be one of: rest_api, sql_query, checkpoint, table"
            )

        if not isinstance(self.flink_config, dict):
            raise ValueError("flink_config must be a dictionary")

        # Validate required config for each source type
        if self.source_type == "rest_api":
            if "rest_api_url" not in self.flink_config:
                raise ValueError("rest_api_url is required for REST API source")
            if "job_id" not in self.flink_config:
                raise ValueError("job_id is required for REST API source")

        elif self.source_type == "sql_query":
            if "sql_query" not in self.flink_config:
                raise ValueError("sql_query is required for SQL query source")

        elif self.source_type == "checkpoint":
            if "checkpoint_path" not in self.flink_config:
                raise ValueError("checkpoint_path is required for checkpoint source")

        elif self.source_type == "table":
            if "table_name" not in self.flink_config:
                raise ValueError("table_name is required for table source")

    def get_name(self) -> str:
        """Return datasource name."""
        source_id = (
            self.flink_config.get("job_id")
            or self.flink_config.get("table_name")
            or self.flink_config.get("checkpoint_path")
            or "unknown"
        )
        return f"flink://{self.source_type}:{source_id}"

    def get_streaming_partitions(self) -> List[Dict[str, Any]]:
        """Get the list of partitions to read from based on source type.

        This method discovers the available parallelism from Flink and creates
        partitions accordingly to enable distributed reading.

        Returns:
            List of partition dictionaries containing partition metadata.
        """
        partitions = []

        try:
            # Discover parallelism based on source type
            parallelism = self._discover_source_parallelism()

            # Generate partitions based on source type and discovered parallelism
            if self.source_type == "rest_api":
                # For REST API, query jobs and create partitions for each job/vertex
                jobs = self._get_flink_jobs()
                for job in jobs:
                    vertices = self._get_job_vertices(job.get("id", "default"))
                    for i, vertex in enumerate(vertices):
                        partitions.append(
                            {
                                "partition_id": f"rest_api_{job.get('id', 'default')}_{i}",
                                "source_type": self.source_type,
                                "job_id": job.get("id"),
                                "vertex_id": vertex.get("id"),
                                "subtask_index": i,
                                "start_position": self.start_position,
                                "end_position": self.end_position,
                            }
                        )

            elif self.source_type == "sql_query":
                # For SQL query, create partitions based on expected parallelism
                # Each partition will execute the same query but process different data ranges
                for i in range(parallelism):
                    partitions.append(
                        {
                            "partition_id": f"sql_query_{i}",
                            "source_type": self.source_type,
                            "subtask_index": i,
                            "total_parallelism": parallelism,
                            "query": self.flink_config.get("sql_query"),
                            "start_position": self.start_position,
                            "end_position": self.end_position,
                        }
                    )

            elif self.source_type == "checkpoint":
                # For checkpoint, discover operator states and create partitions
                checkpoint_metadata = self._get_checkpoint_metadata()
                operator_states = checkpoint_metadata.get("operator_states", [])

                for i, operator_state in enumerate(operator_states):
                    partitions.append(
                        {
                            "partition_id": f"checkpoint_{operator_state.get('operator_id', i)}",
                            "source_type": self.source_type,
                            "operator_id": operator_state.get("operator_id"),
                            "subtask_index": i,
                            "checkpoint_path": self.flink_config.get("checkpoint_path"),
                            "start_position": self.start_position,
                            "end_position": self.end_position,
                        }
                    )

            elif self.source_type == "table":
                # For table, create partitions based on table parallelism
                table_info = self._get_table_info()
                table_parallelism = table_info.get("parallelism", parallelism)

                for i in range(table_parallelism):
                    partitions.append(
                        {
                            "partition_id": f"table_{self.flink_config.get('table_name', 'default')}_{i}",
                            "source_type": self.source_type,
                            "table_name": self.flink_config.get("table_name"),
                            "subtask_index": i,
                            "total_parallelism": table_parallelism,
                            "start_position": self.start_position,
                            "end_position": self.end_position,
                        }
                    )

            else:
                raise ValueError(f"Unsupported Flink source type: {self.source_type}")

        except Exception as e:
            # Fallback to single partition if discovery fails
            logger.warning(
                f"Failed to discover Flink parallelism for {self.source_type}: {e}. "
                "Falling back to single partition."
            )
            partitions.append(
                {
                    "partition_id": f"{self.source_type}_0",
                    "source_type": self.source_type,
                    "subtask_index": 0,
                    "total_parallelism": 1,
                    "start_position": self.start_position,
                    "end_position": self.end_position,
                }
            )

        return partitions

    def _discover_source_parallelism(self) -> int:
        """Discover the parallelism available from the Flink source.

        Returns:
            Number of parallel subtasks available for this source.
        """
        try:
            if self.source_type == "rest_api":
                # Query Flink REST API for job parallelism
                rest_url = self.flink_config.get(
                    "rest_api_url", "http://localhost:8081"
                )
                jobs = self._get_flink_jobs()
                if jobs:
                    # Use the parallelism of the first job as default
                    return jobs[0].get("parallelism", 1)

            elif self.source_type == "table":
                # Query table metadata for parallelism
                table_info = self._get_table_info()
                return table_info.get("parallelism", 1)

            elif self.source_type == "checkpoint":
                # Count operator states in checkpoint
                checkpoint_metadata = self._get_checkpoint_metadata()
                operator_states = checkpoint_metadata.get("operator_states", [])
                return max(1, len(operator_states))

            elif self.source_type == "sql_query":
                # Default parallelism for SQL queries
                return self.flink_config.get("parallelism", 2)

        except Exception as e:
            logger.warning(f"Failed to discover parallelism: {e}")

        # Default fallback
        return 1

    def _get_flink_jobs(self) -> List[Dict[str, Any]]:
        """Get list of Flink jobs from REST API."""
        try:
            import requests

            rest_url = self.flink_config.get("rest_api_url", "http://localhost:8081")
            response = requests.get(f"{rest_url}/jobs", timeout=5)
            response.raise_for_status()
            return response.json().get("jobs", [])
        except Exception:
            return []

    def _get_job_vertices(self, job_id: str) -> List[Dict[str, Any]]:
        """Get vertices for a specific Flink job."""
        try:
            import requests

            rest_url = self.flink_config.get("rest_api_url", "http://localhost:8081")
            response = requests.get(f"{rest_url}/jobs/{job_id}", timeout=5)
            response.raise_for_status()
            return response.json().get("vertices", [])
        except Exception:
            return []

    def _get_checkpoint_metadata(self) -> Dict[str, Any]:
        """Get checkpoint metadata."""
        try:
            checkpoint_path = self.flink_config.get("checkpoint_path")
            if checkpoint_path:
                # This would normally read checkpoint metadata
                # For now, return mock data
                return {
                    "operator_states": [
                        {"operator_id": f"operator_{i}"} for i in range(2)
                    ]
                }
        except Exception:
            pass
        return {"operator_states": []}

    def _get_table_info(self) -> Dict[str, Any]:
        """Get table information."""
        try:
            table_name = self.flink_config.get("table_name")
            if table_name:
                # This would normally query table catalog
                # For now, return mock data with reasonable parallelism
                return {"parallelism": 2}
        except Exception:
            pass
        return {"parallelism": 1}

    def _create_streaming_read_task(self, partition_info: Dict[str, Any]) -> ReadTask:
        """Create a read task for a Flink partition.

        Args:
            partition_info: Partition information containing source details.

        Returns:
            ReadTask for reading from the partition.
        """
        partition_id = partition_info["partition_id"]
        source_type = partition_info["source_type"]
        flink_config = partition_info["flink_config"]
        start_position = partition_info.get("start_position")
        end_position = partition_info.get("end_position")

        # Create stateful reader functions
        read_partition_fn, get_position_fn = _create_flink_reader(
            partition_id=partition_id,
            source_type=source_type,
            flink_config=flink_config,
            start_position=_parse_flink_position(start_position)
            if start_position
            else None,
            end_position=_parse_flink_position(end_position) if end_position else None,
            max_records=self.max_records_per_task,
        )

        def get_schema() -> pa.Schema:
            """Return schema for Flink records."""
            # Base schema common to all source types
            base_schema = [
                ("source_type", pa.string()),
                ("partition_id", pa.string()),
            ]

            # Add source-specific fields
            if source_type == "rest_api":
                base_schema.extend(
                    [
                        ("job_id", pa.string()),
                        ("vertex_id", pa.string()),
                        ("vertex_name", pa.string()),
                        ("parallelism", pa.int32()),
                        ("status", pa.string()),
                        ("start_time", pa.int64()),
                        ("end_time", pa.int64()),
                        ("duration", pa.int64()),
                        ("tasks", pa.string()),  # JSON string
                        ("metrics", pa.string()),  # JSON string
                    ]
                )
            elif source_type == "sql_query":
                base_schema.extend(
                    [
                        ("query", pa.string()),
                        ("row_number", pa.int32()),
                        ("result_data", pa.string()),
                        ("execution_time", pa.timestamp("us")),
                    ]
                )
            elif source_type == "checkpoint":
                base_schema.extend(
                    [
                        ("checkpoint_path", pa.string()),
                        ("state_id", pa.string()),
                        ("state_data", pa.string()),
                        ("checkpoint_timestamp", pa.timestamp("us")),
                    ]
                )
            elif source_type == "table":
                base_schema.extend(
                    [
                        ("table_name", pa.string()),
                        ("row_id", pa.int32()),
                        ("column_a", pa.string()),
                        ("column_b", pa.string()),
                        ("row_timestamp", pa.timestamp("us")),
                    ]
                )

            return pa.schema(base_schema)

        return create_streaming_read_task(
            partition_id=partition_id,
            streaming_config=self.streaming_config,
            read_source_fn=read_partition_fn,
            get_position_fn=get_position_fn,
            get_schema_fn=get_schema,
            start_position=self.start_position,
            end_position=self.end_position,
            max_records=self.max_records_per_task,
        )

    def get_streaming_schema(self) -> Optional[pa.Schema]:
        """Return the schema for Flink streaming data.

        Returns:
            PyArrow schema for Flink records (varies by source type).
        """
        # Return a generic schema that covers all source types
        return pa.schema(
            [
                ("source_type", pa.string()),
                ("partition_id", pa.string()),
                ("read_timestamp", pa.timestamp("us")),
                ("current_position", pa.string()),
                # Generic data field that can hold source-specific data
                ("data", pa.string()),  # JSON string representation
            ]
        )
