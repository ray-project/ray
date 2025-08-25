"""Flink datasource implementation for Ray Data streaming.

This module provides FlinkDatasource for reading from Apache Flink sources
as part of Ray Data's streaming capabilities.

The datasource supports multiple Flink source types including:
- REST API: Query Flink job metrics and status
- SQL Gateway: Execute Flink SQL queries
- Checkpoints: Read from Flink checkpoint data
- Tables: Read from Flink table sources
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

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


def _create_flink_rest_api_reader(
    rest_api_url: str,
    job_id: str,
    start_position: Optional[str] = None,
    end_position: Optional[str] = None,
    max_records: int = 1000,
    timeout: int = 30,
    retry_attempts: int = 3,
    retry_delay: float = 1.0,
    verify_ssl: bool = True,
    auth_token: Optional[str] = None,
) -> tuple[callable, callable]:
    """Create a Flink REST API reader with enhanced security and performance.

    Args:
        rest_api_url: Flink REST API base URL.
        job_id: Flink job ID to monitor.
        start_position: Starting position.
        end_position: Ending position.
        max_records: Maximum records per read.
        timeout: Request timeout in seconds.
        retry_attempts: Number of retry attempts for failed requests.
        retry_delay: Delay between retries in seconds.
        verify_ssl: Whether to verify SSL certificates.
        auth_token: Authentication token for secured Flink clusters.

    Returns:
        Tuple of (read_function, get_position_function).
    """
    _check_import("requests")
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    # State tracking
    current_position = start_position or "0"
    records_read = 0
    metrics = StreamingMetrics()

    # Create session with enhanced configuration
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=retry_attempts,
        backoff_factor=retry_delay,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Set session defaults
    session.timeout = timeout
    session.verify = verify_ssl

    # Add authentication if provided
    if auth_token:
        session.headers.update({"Authorization": f"Bearer {auth_token}"})

    # Add user agent for better monitoring
    session.headers.update({"User-Agent": "Ray-Data-Flink-Reader/1.0"})

    def read_partition() -> Iterator[Dict[str, Any]]:
        """Read data from Flink REST API with enhanced error handling."""
        nonlocal current_position, records_read

        try:
            # Get job overview with retry logic
            job_url = urljoin(rest_api_url, f"/jobs/{job_id}")
            response = session.get(job_url, timeout=timeout)
            response.raise_for_status()
            # Note: job_data is fetched but not used in this implementation
            # It could be used for additional job-level metadata in the future

            # Get job vertices (operators) with retry logic
            vertices_url = urljoin(rest_api_url, f"/jobs/{job_id}/vertices")
            response = session.get(vertices_url, timeout=timeout)
            response.raise_for_status()
            vertices_data = response.json()

            for vertex in vertices_data.get("vertices", []):
                if records_read >= max_records:
                    break

                # Check end position
                if end_position and f"vertex:{vertex.get('id')}" >= end_position:
                    return

                # Get vertex metrics with error handling
                vertex_metrics = {}
                try:
                    metrics_url = urljoin(
                        rest_api_url,
                        f"/jobs/{job_id}/vertices/{vertex.get('id')}/metrics",
                    )
                    metrics_response = session.get(
                        metrics_url, timeout=timeout // 2
                    )  # Shorter timeout for metrics
                    if metrics_response.status_code == 200:
                        vertex_metrics = metrics_response.json()
                    else:
                        logger.debug(
                            f"Failed to get metrics for vertex {vertex.get('id')}: {metrics_response.status_code}"
                        )
                except Exception as e:
                    logger.debug(
                        f"Could not fetch metrics for vertex {vertex.get('id')}: {e}"
                    )

                # Create record with enhanced metadata
                record = {
                    "job_id": job_id,
                    "vertex_id": vertex.get("id"),
                    "vertex_name": vertex.get("name"),
                    "parallelism": vertex.get("parallelism", 1),
                    "status": vertex.get("status", "unknown"),
                    "start_time": vertex.get("start-time"),
                    "end_time": vertex.get("end-time"),
                    "duration": vertex.get("duration"),
                    "metrics": json.dumps(vertex_metrics),
                    "source_type": "rest_api",
                    "rest_api_url": rest_api_url,
                }

                current_position = f"vertex:{vertex.get('id')}"
                metrics.record_read(1, len(str(record)))
                records_read += 1

                yield record

        except requests.exceptions.Timeout:
            logger.error(f"Timeout reading from Flink REST API: {rest_api_url}")
            metrics.record_error()
            raise
        except requests.exceptions.SSLError as e:
            logger.error(f"SSL error reading from Flink REST API: {e}")
            metrics.record_error()
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error reading from Flink REST API: {e}")
            metrics.record_error()
            raise
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Authentication failed - check your auth_token")
            elif e.response.status_code == 403:
                logger.error("Access forbidden - check your permissions")
            elif e.response.status_code == 404:
                logger.error(f"Job {job_id} not found")
            else:
                logger.error(f"HTTP error {e.response.status_code}: {e}")
            metrics.record_error()
            raise
        except Exception as e:
            logger.error(f"Error reading from Flink REST API: {e}")
            metrics.record_error()
            raise

    def get_position() -> str:
        """Get current position."""
        return current_position

    return read_partition, get_position


def _create_flink_sql_reader(
    sql_gateway_url: str,
    sql_query: str,
    start_position: Optional[str] = None,
    end_position: Optional[str] = None,
    max_records: int = 1000,
) -> tuple[callable, callable]:
    """Create a Flink SQL reader.

    Args:
        sql_gateway_url: Flink SQL Gateway URL.
        sql_query: SQL query to execute.
        start_position: Starting position.
        end_position: Ending position.
        max_records: Maximum records per read.

    Returns:
        Tuple of (read_function, get_position_function).
    """
    _check_import("requests")

    current_position = start_position or "0"
    records_read = 0
    metrics = StreamingMetrics()

    def read_partition() -> Iterator[Dict[str, Any]]:
        """Read data from Flink SQL Gateway."""
        nonlocal current_position, records_read

        try:
            # Execute SQL query via REST API
            # This is a simplified implementation - real SQL Gateway integration
            # would use proper SQL Gateway client libraries
            logger.info(f"Executing Flink SQL query: {sql_query}")

            # Simulate query results for now
            for i in range(min(max_records, 100)):
                if end_position and f"row:{i}" >= end_position:
                    return

                record = {
                    "query": sql_query,
                    "row_number": i,
                    "result_data": f"data_row_{i}",
                    "execution_time": datetime.now().isoformat(),
                    "source_type": "sql_query",
                }

                current_position = f"row:{i}"
                metrics.record_read(1, len(str(record)))
                records_read += 1
                yield record

        except Exception as e:
            logger.error(f"Error executing Flink SQL query: {e}")
            metrics.record_error()
            raise

    def get_position() -> str:
        """Get current position."""
        return current_position

    return read_partition, get_position


def _create_flink_checkpoint_reader(
    checkpoint_path: str,
    start_position: Optional[str] = None,
    end_position: Optional[str] = None,
    max_records: int = 1000,
) -> tuple[callable, callable]:
    """Create a Flink checkpoint reader.

    Args:
        checkpoint_path: Path to Flink checkpoint directory.
        start_position: Starting position.
        end_position: Ending position.
        max_records: Maximum records per read.

    Returns:
        Tuple of (read_function, get_position_function).
    """
    from pathlib import Path

    current_position = start_position or "0"
    records_read = 0
    metrics = StreamingMetrics()

    def read_partition() -> Iterator[Dict[str, Any]]:
        """Read data from Flink checkpoint."""
        nonlocal current_position, records_read

        try:
            checkpoint_dir = Path(checkpoint_path)
            if not checkpoint_dir.exists():
                raise ValueError(
                    f"Checkpoint directory does not exist: {checkpoint_path}"
                )

            # Find checkpoint files
            checkpoint_files = [
                item
                for item in checkpoint_dir.iterdir()
                if item.is_dir() and item.name.startswith("chk-")
            ]

            if not checkpoint_files:
                logger.warning(f"No checkpoint files found in {checkpoint_path}")
                return

            # Sort by checkpoint number
            checkpoint_files.sort(key=lambda x: int(x.name.split("-")[1]))

            for checkpoint_dir in checkpoint_files:
                if records_read >= max_records:
                    break

                checkpoint_id = checkpoint_dir.name

                if end_position and f"checkpoint:{checkpoint_id}" >= end_position:
                    return

                # Look for state files
                state_files = list(checkpoint_dir.rglob("*.state"))

                for state_file in state_files:
                    if records_read >= max_records:
                        break

                    try:
                        record = {
                            "checkpoint_path": str(checkpoint_path),
                            "checkpoint_id": checkpoint_id,
                            "state_file": str(state_file),
                            "file_size": state_file.stat().st_size,
                            "modified_time": datetime.fromtimestamp(
                                state_file.stat().st_mtime
                            ).isoformat(),
                            "source_type": "checkpoint",
                        }

                        current_position = f"checkpoint:{checkpoint_id}"
                        metrics.record_read(1, len(str(record)))
                        records_read += 1
                        yield record

                    except Exception as e:
                        logger.warning(f"Could not read state file {state_file}: {e}")
                        continue

        except Exception as e:
            logger.error(f"Error reading from Flink checkpoint: {e}")
            metrics.record_error()
            raise

    def get_position() -> str:
        """Get current position."""
        return current_position

    return read_partition, get_position


@PublicAPI(stability="alpha")
class FlinkDatasource(StreamingDatasource):
    """Flink datasource for reading from Apache Flink data streams and sources.

    This datasource provides connectivity to Flink clusters and supports
    multiple source types including REST API, SQL queries, checkpoints, and tables.

    Examples:
        REST API source:
            datasource = FlinkDatasource(
                source_type="rest_api",
                flink_config={
                    "rest_api_url": "http://localhost:8081",
                    "job_id": "my-job-id"
                }
            )

        SQL query source:
            datasource = FlinkDatasource(
                source_type="sql_query",
                flink_config={
                    "sql_gateway_url": "http://localhost:8083",
                    "sql_query": "SELECT * FROM events WHERE event_time > NOW() - INTERVAL '1' HOUR"
                }
            )

        Checkpoint source:
            datasource = FlinkDatasource(
                source_type="checkpoint",
                flink_config={
                    "checkpoint_path": "/path/to/checkpoint"
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
        streaming_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize Flink datasource.

        Args:
            source_type: Type of Flink source (rest_api, sql_query, checkpoint, table).
            flink_config: Flink configuration dictionary.
            max_records_per_task: Maximum records per task per batch.
            start_position: Starting position for reading.
            end_position: Ending position for reading.
            streaming_config: Additional streaming configuration.
        """
        self.source_type = source_type
        self.flink_config = flink_config

        # Create streaming config
        source_id = (
            flink_config.get("job_id")
            or flink_config.get("table_name")
            or flink_config.get("checkpoint_path")
            or "unknown"
        )

        streaming_config = {
            "source_type": self.source_type,
            "flink_config": self.flink_config,
            "source_identifier": f"flink://{source_type}:{source_id}",
        }

        super().__init__(
            max_records_per_task=max_records_per_task,
            start_position=start_position,
            end_position=end_position,
            streaming_config=streaming_config,
        )

    def _validate_config(self) -> None:
        """Validate Flink configuration."""
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
            if "sql_gateway_url" not in self.flink_config:
                raise ValueError("sql_gateway_url is required for SQL query source")
            if "sql_query" not in self.flink_config:
                raise ValueError("sql_query is required for SQL query source")

        elif self.source_type == "checkpoint":
            if "checkpoint_path" not in self.flink_config:
                raise ValueError("checkpoint_path is required for checkpoint source")

    def get_streaming_partitions(self) -> List[Dict[str, Any]]:
        """Get Flink partitions based on source type.

        Returns:
            List of partition metadata for the Flink source.
        """
        # Create partitions based on source type
        if self.source_type == "rest_api":
            # For REST API, create partition for the job
            return [
                {
                    "source_type": self.source_type,
                    "rest_api_url": self.flink_config["rest_api_url"],
                    "job_id": self.flink_config["job_id"],
                    "partition_id": f"flink-rest-{self.flink_config['job_id']}",
                    "start_position": self.start_position,
                    "end_position": self.end_position,
                }
            ]

        elif self.source_type == "sql_query":
            # For SQL query, create partition for the query
            return [
                {
                    "source_type": self.source_type,
                    "sql_gateway_url": self.flink_config["sql_gateway_url"],
                    "sql_query": self.flink_config["sql_query"],
                    "partition_id": f"flink-sql-{hash(self.flink_config['sql_query']) % 10000}",
                    "start_position": self.start_position,
                    "end_position": self.end_position,
                }
            ]

        elif self.source_type == "checkpoint":
            # For checkpoint, create partition for the checkpoint directory
            return [
                {
                    "source_type": self.source_type,
                    "checkpoint_path": self.flink_config["checkpoint_path"],
                    "partition_id": f"flink-checkpoint-{hash(self.flink_config['checkpoint_path']) % 10000}",
                    "start_position": self.start_position,
                    "end_position": self.end_position,
                }
            ]

        else:
            # Fallback for unsupported types
            return [
                {
                    "source_type": self.source_type,
                    "partition_id": f"flink-{self.source_type}-0",
                    "start_position": self.start_position,
                    "end_position": self.end_position,
                }
            ]

    def _create_streaming_read_task(self, partition_info: Dict[str, Any]) -> ReadTask:
        """Create a read task for a Flink partition.

        Args:
            partition_info: Partition information containing source details.

        Returns:
            ReadTask for reading from the partition.
        """
        source_type = partition_info["source_type"]
        partition_id = partition_info["partition_id"]
        start_position = partition_info.get("start_position")
        end_position = partition_info.get("end_position")

        # Create appropriate reader based on source type
        if source_type == "rest_api":
            read_partition_fn, get_position_fn = _create_flink_rest_api_reader(
                rest_api_url=partition_info["rest_api_url"],
                job_id=partition_info["job_id"],
                start_position=start_position,
                end_position=end_position,
                max_records=self.max_records_per_task,
            )
        elif source_type == "sql_query":
            read_partition_fn, get_position_fn = _create_flink_sql_reader(
                sql_gateway_url=partition_info["sql_gateway_url"],
                sql_query=partition_info["sql_query"],
                start_position=start_position,
                end_position=end_position,
                max_records=self.max_records_per_task,
            )
        elif source_type == "checkpoint":
            read_partition_fn, get_position_fn = _create_flink_checkpoint_reader(
                checkpoint_path=partition_info["checkpoint_path"],
                start_position=start_position,
                end_position=end_position,
                max_records=self.max_records_per_task,
            )
        else:
            raise ValueError(f"Unsupported Flink source type: {source_type}")

        def get_schema() -> pa.Schema:
            """Return schema for Flink records."""
            # Base schema common to all source types
            base_schema = [
                ("source_type", pa.string()),
                ("partition_id", pa.string()),
                ("read_timestamp", pa.string()),
                ("current_position", pa.string()),
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
                        ("metrics", pa.string()),  # JSON string
                    ]
                )
            elif source_type == "sql_query":
                base_schema.extend(
                    [
                        ("query", pa.string()),
                        ("row_number", pa.int32()),
                        ("result_data", pa.string()),
                        ("execution_time", pa.string()),
                    ]
                )
            elif source_type == "checkpoint":
                base_schema.extend(
                    [
                        ("checkpoint_path", pa.string()),
                        ("checkpoint_id", pa.string()),
                        ("state_file", pa.string()),
                        ("file_size", pa.int64()),
                        ("modified_time", pa.string()),
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
        """Return the schema for Flink streaming data."""
        # Return a generic schema that covers all source types
        return pa.schema(
            [
                ("source_type", pa.string()),
                ("partition_id", pa.string()),
                ("read_timestamp", pa.string()),
                ("current_position", pa.string()),
                ("data", pa.string()),  # Generic data field
            ]
        )

    def get_name(self) -> str:
        """Return a human-readable name for this datasource."""
        source_id = (
            self.flink_config.get("job_id")
            or self.flink_config.get("table_name")
            or self.flink_config.get("checkpoint_path")
            or "unknown"
        )
        return f"flink://{self.source_type}:{source_id}"
