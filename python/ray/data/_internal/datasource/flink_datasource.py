"""Flink datasource for unbound data streams.

This module provides a Flink datasource implementation for Ray Data that works
with the UnboundedDataOperator.

Requires:
    - requests: https://requests.readthedocs.io/ (for REST API access)
    - pyflink: https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/python/overview/ (optional, for table source type)
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


def _check_requests_available():
    """Check if requests library is available."""
    try:
        import requests  # noqa: F401

        return True
    except ImportError:
        raise ImportError(
            "requests is required for Flink datasource. "
            "Install with: pip install requests"
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
        poll_interval_seconds: float = 5.0,
    ):
        """Initialize Flink datasource.

        Args:
            source_type: Type of Flink source (rest_api, table, checkpoint)
            flink_config: Flink configuration dictionary
            max_records_per_task: Maximum records per task
            start_position: Starting position for reading
            end_position: Ending position for reading
            poll_interval_seconds: Seconds between polling for new data (default 5.0s)

        Raises:
            ValueError: If required configuration is missing
            ImportError: If required libraries are not installed
        """
        super().__init__("flink")
        _check_requests_available()

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

        self.source_type_val = source_type
        self.flink_config = flink_config
        self.max_records_per_task = max_records_per_task
        self.start_position = start_position
        self.end_position = end_position
        self.poll_interval_seconds = poll_interval_seconds

    def _get_job_parallelism(self) -> int:
        """Query Flink job parallelism via REST API."""
        import requests

        base_url = self.flink_config["rest_api_url"].rstrip("/")
        job_id = self.flink_config["job_id"]

        headers = {}
        if "auth_token" in self.flink_config:
            auth_type = self.flink_config.get("auth_type", "bearer")
            if auth_type.lower() == "bearer":
                headers["Authorization"] = f"Bearer {self.flink_config['auth_token']}"

        verify_ssl = self.flink_config.get("verify_ssl", True)
        timeout = self.flink_config.get("timeout", 30)

        try:
            # Get job configuration to determine parallelism
            job_url = f"{base_url}/jobs/{job_id}"
            response = requests.get(
                job_url, headers=headers, verify=verify_ssl, timeout=timeout
            )
            response.raise_for_status()
            job_info = response.json()

            # Get parallelism from job vertices
            vertices = job_info.get("vertices", [])
            if vertices:
                # Use max parallelism across all vertices
                return max(v.get("parallelism", 1) for v in vertices)
            return 1
        except Exception:
            # Default to 1 if cannot determine
            return 1

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

        # Determine number of tasks based on source type
        if self.source_type_val == "rest_api":
            # For REST API, query parallelism from job
            num_tasks = self._get_job_parallelism()
            num_tasks = min(num_tasks, parallelism) if parallelism > 0 else num_tasks
        else:
            # For table/checkpoint, use requested parallelism
            num_tasks = parallelism if parallelism > 0 else 1

        # Store config for use in read functions (avoid serialization issues)
        source_type_val = self.source_type_val
        flink_config = self.flink_config
        max_records_per_task = self.max_records_per_task
        poll_interval_seconds = self.poll_interval_seconds

        for task_id in range(num_tasks):

            def create_flink_read_fn(
                task_num: int = task_id,
                source_type_val: str = source_type_val,
                flink_config: Dict[str, Any] = flink_config,
                max_records_per_task: int = max_records_per_task,
                poll_interval_seconds: float = poll_interval_seconds,
            ):
                def flink_read_fn() -> Iterator[pa.Table]:
                    """Read function for Flink job output via REST API."""
                    import json
                    from datetime import datetime

                    import requests

                    if source_type_val == "rest_api":
                        # Read from Flink REST API
                        base_url = flink_config["rest_api_url"].rstrip("/")
                        job_id = flink_config["job_id"]

                        # Set up authentication
                        headers = {}

                        if "auth_token" in flink_config:
                            auth_type = flink_config.get("auth_type", "bearer")
                            if auth_type.lower() == "bearer":
                                headers[
                                    "Authorization"
                                ] = f"Bearer {flink_config['auth_token']}"
                            elif auth_type.lower() == "basic":
                                import base64

                                credentials = f"{flink_config.get('username', '')}:{flink_config.get('password', '')}"
                                encoded = base64.b64encode(
                                    credentials.encode()
                                ).decode()
                                headers["Authorization"] = f"Basic {encoded}"

                        # SSL verification
                        verify_ssl = flink_config.get("verify_ssl", True)
                        cert = None
                        if "ssl_cert" in flink_config:
                            cert = (
                                flink_config["ssl_cert"],
                                flink_config.get("ssl_key"),
                            )

                        timeout = flink_config.get("timeout", 30)

                        try:
                            # Get job details
                            job_url = f"{base_url}/jobs/{job_id}"
                            response = requests.get(
                                job_url,
                                headers=headers,
                                verify=verify_ssl,
                                cert=cert,
                                timeout=timeout,
                            )
                            response.raise_for_status()
                            job_info = response.json()

                            # Get task-specific metrics/accumulators
                            # Note: Actual implementation depends on what data you want to read
                            # This is a simplified example reading job metrics
                            metrics_url = f"{base_url}/jobs/{job_id}/metrics"
                            response = requests.get(
                                metrics_url,
                                headers=headers,
                                verify=verify_ssl,
                                cert=cert,
                                timeout=timeout,
                            )
                            response.raise_for_status()
                            metrics = response.json()

                            records = []

                            # Extract records from metrics/accumulators
                            # This is a template - actual implementation depends on your Flink job structure
                            for metric in metrics[:max_records_per_task]:
                                records.append(
                                    {
                                        "job_id": job_id,
                                        "job_name": job_info.get("name", "unknown"),
                                        "task_id": task_num,
                                        "record_id": metric.get(
                                            "id", f"metric_{task_num}"
                                        ),
                                        "data": json.dumps(metric),
                                        "processing_time": datetime.utcnow().isoformat(),
                                        "watermark": int(
                                            datetime.utcnow().timestamp() * 1000
                                        ),
                                    }
                                )

                            if records:
                                table = pa.Table.from_pylist(records)
                                yield table

                        except requests.exceptions.RequestException as e:
                            # Log error but don't fail completely
                            logger.error(f"Error reading from Flink REST API: {e}")

                    elif source_type_val == "table":
                        # Read from Flink table - requires pyflink
                        try:
                            from pyflink.table import (
                                EnvironmentSettings,
                                TableEnvironment,
                            )

                            # Create table environment
                            env_settings = EnvironmentSettings.in_streaming_mode()
                            table_env = TableEnvironment.create(env_settings)

                            # Execute query
                            table_name = flink_config["table_name"]
                            result = table_env.execute_sql(
                                f"SELECT * FROM {table_name} LIMIT {max_records_per_task}"
                            )

                            records = []
                            for row in result.collect():
                                # Convert Flink row to dict
                                records.append(
                                    {
                                        "job_id": "table_read",
                                        "job_name": table_name,
                                        "task_id": task_num,
                                        "record_id": f"table_{task_num}_{len(records)}",
                                        "data": str(row),
                                        "processing_time": datetime.utcnow().isoformat(),
                                        "watermark": int(
                                            datetime.utcnow().timestamp() * 1000
                                        ),
                                    }
                                )

                                if len(records) >= max_records_per_task:
                                    break

                            if records:
                                table = pa.Table.from_pylist(records)
                                yield table

                        except ImportError:
                            raise ImportError(
                                "pyflink is required for table source type. "
                                "Install with: pip install apache-flink"
                            )

                    elif source_type_val == "checkpoint":
                        # Read from Flink checkpoint/savepoint
                        # This would require parsing checkpoint metadata and state
                        raise NotImplementedError(
                            "Checkpoint reading is not yet implemented. "
                            "Use rest_api or table source types instead."
                        )

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
                    ("job_name", pa.string()),
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
                ("job_name", pa.string()),
                ("task_id", pa.int64()),
                ("record_id", pa.string()),
                ("data", pa.string()),
                ("processing_time", pa.string()),
                ("watermark", pa.int64()),
            ]
        )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size for Flink streams.

        Returns:
            None for unbounded streams
        """
        return None
