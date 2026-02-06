"""Shared utilities for streaming datasources.

This module provides common utilities for Kafka, Kinesis, Flink, and other
streaming datasources to reduce code duplication.

Uses:
    - PyArrow: https://arrow.apache.org/docs/python/
    - boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
    - requests: https://requests.readthedocs.io/
"""

from dataclasses import dataclass, fields
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pyarrow as pa  # https://arrow.apache.org/docs/python/

from ray.data._internal.streaming.block_coalescer import BlockCoalescer
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext


@dataclass
class AWSCredentials:
    """AWS credentials configuration for Kinesis and other AWS services.

    Attributes:
        region_name: AWS region (required).
        aws_access_key_id: AWS access key ID (optional, uses credential chain if not provided).
        aws_secret_access_key: AWS secret access key (optional).
        aws_session_token: AWS session token (optional).
        endpoint_url: Custom endpoint URL (optional, for testing with LocalStack).
    """

    region_name: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    endpoint_url: Optional[str] = None

    def to_session_kwargs(self) -> Dict[str, Any]:
        """Convert to boto3 Session kwargs.

        Returns:
            Dictionary of session kwargs for boto3.Session().

        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
        """
        kwargs = {"region_name": self.region_name}
        if self.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if self.aws_session_token:
            kwargs["aws_session_token"] = self.aws_session_token
        return kwargs

    def to_client_kwargs(self) -> Dict[str, Any]:
        """Convert to boto3 client kwargs.

        Returns:
            Dictionary of client kwargs for session.client().
        """
        kwargs = {"region_name": self.region_name}
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        return kwargs

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "AWSCredentials":
        """Create from configuration dictionary.

        Args:
            config: Configuration dict with AWS parameters.

        Returns:
            AWSCredentials instance.

        Raises:
            ValueError: If region_name is missing.
        """
        region = config.get("region_name") or config.get("aws_region")
        if not region:
            raise ValueError("region_name or aws_region is required")

        return cls(
            region_name=region,
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
            aws_session_token=config.get("aws_session_token"),
            endpoint_url=config.get("endpoint_url"),
        )


@dataclass
class HTTPClientConfig:
    """HTTP client configuration for REST API-based datasources.

    Attributes:
        base_url: Base URL for API (required).
        auth_token: Authentication token (optional).
        auth_type: Authentication type - "bearer" or "basic" (default: "bearer").
        username: Username for basic auth (optional).
        password: Password for basic auth (optional).
        verify_ssl: Whether to verify SSL certificates (default: True).
        ssl_cert: Path to SSL certificate (optional).
        ssl_key: Path to SSL key (optional).
        timeout: Request timeout in seconds (default: 30).
        headers: Additional HTTP headers (optional).
    """

    base_url: str
    auth_token: Optional[str] = None
    auth_type: str = "bearer"
    username: Optional[str] = None
    password: Optional[str] = None
    verify_ssl: bool = True
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    timeout: int = 30
    headers: Optional[Dict[str, str]] = None

    def get_request_kwargs(self) -> Dict[str, Any]:
        """Get kwargs for requests library calls.

        Returns:
            Dictionary of kwargs for requests.get/post/etc.

        See: https://requests.readthedocs.io/en/latest/api/
        """
        kwargs = {
            "verify": self.verify_ssl,
            "timeout": self.timeout,
        }

        # Add authentication headers
        headers = dict(self.headers) if self.headers else {}
        if self.auth_type.lower() == "bearer" and self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        elif self.auth_type.lower() == "basic" and self.username and self.password:
            import base64

            credentials = f"{self.username}:{self.password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"

        if headers:
            kwargs["headers"] = headers

        # Add SSL cert if provided
        if self.ssl_cert:
            kwargs["cert"] = (
                (self.ssl_cert, self.ssl_key) if self.ssl_key else self.ssl_cert
            )

        return kwargs

    @classmethod
    def from_config(cls, config: Dict[str, Any], base_url_key: str = "base_url") -> "HTTPClientConfig":
        """Create from configuration dictionary.

        Args:
            config: Configuration dict with HTTP parameters.
            base_url_key: Key name for base URL in config (default: "base_url").

        Returns:
            HTTPClientConfig instance.

        Raises:
            ValueError: If base_url is missing.
        """
        base_url = config.get(base_url_key)
        if not base_url:
            raise ValueError(f"{base_url_key} is required")

        return cls(
            base_url=base_url.rstrip("/"),
            auth_token=config.get("auth_token"),
            auth_type=config.get("auth_type", "bearer"),
            username=config.get("username"),
            password=config.get("password"),
            verify_ssl=config.get("verify_ssl", True),
            ssl_cert=config.get("ssl_cert"),
            ssl_key=config.get("ssl_key"),
            timeout=config.get("timeout", 30),
            headers=config.get("headers"),
        )


def create_standard_schema(
    include_binary_data: bool = False,
    extra_fields: Optional[List[tuple]] = None,
) -> pa.Schema:
    """Create a standard PyArrow schema for streaming messages.

    Args:
        include_binary_data: If True, use binary type for key/value fields;
            if False, use string type.
        extra_fields: Additional fields to include as list of (name, type) tuples.

    Returns:
        PyArrow schema for streaming messages.
    """
    data_type = pa.binary() if include_binary_data else pa.string()
    header_type = pa.binary() if include_binary_data else pa.string()

    fields = [
        ("timestamp", pa.int64()),
        ("key", data_type),
        ("value", data_type),
        ("headers", pa.map_(pa.string(), header_type)),
    ]

    if extra_fields:
        fields.extend(extra_fields)

    return pa.schema(fields)


def apply_dataclass_to_dict(
    target: Dict[str, Any],
    dataclass_instance: Any,
) -> None:
    """Apply non-None fields from dataclass to dictionary in-place.

    Args:
        target: Dictionary to modify.
        dataclass_instance: Dataclass instance with configuration.
    """
    for field in fields(dataclass_instance):
        value = getattr(dataclass_instance, field.name)
        if value is not None:
            target[field.name] = value


def create_block_coalescer(target_max_block_size: Optional[int] = None) -> BlockCoalescer:
    """Create a BlockCoalescer with appropriate target size.

    Args:
        target_max_block_size: Target block size in bytes. If None, uses DataContext default.

    Returns:
        BlockCoalescer instance configured with target block size.
    """
    if target_max_block_size is None:
        ctx = DataContext.get_current()
        target_max_block_size = ctx.target_max_block_size or (128 * 1024 * 1024)
    return BlockCoalescer(target_max_bytes=target_max_block_size)


def create_block_metadata(
    table: pa.Table,
    input_file: str,
) -> BlockMetadata:
    """Create BlockMetadata from a PyArrow table.

    Args:
        table: PyArrow table to create metadata for.
        input_file: Input file identifier (e.g., "kafka://topic/partition").

    Returns:
        BlockMetadata instance.
    """
    return BlockMetadata(
        num_rows=table.num_rows,
        size_bytes=table.nbytes if hasattr(table, "nbytes") else None,
        input_files=[input_file],
        exec_stats=None,
    )


def yield_coalesced_blocks(
    coalescer: BlockCoalescer,
    small_tables: List[pa.Table],
    input_file: str,
) -> Iterator[Tuple[Block, BlockMetadata]]:
    """Yield coalesced blocks with metadata.

    Args:
        coalescer: BlockCoalescer instance.
        small_tables: List of small PyArrow tables to coalesce.
        input_file: Input file identifier for metadata.

    Yields:
        Tuples of (coalesced_block, metadata).
    """
    for coalesced_table in coalescer.coalesce_tables(small_tables):
        meta = create_block_metadata(coalesced_table, input_file)
        yield coalesced_table, meta


class TwoPhaseCommitMixin:
    """Mixin providing default two-phase commit implementation for streaming datasources.

    This mixin provides standard implementations of prepare_commit, commit, and
    abort_commit methods that work with the commit_checkpoint pattern used by
    streaming datasources.

    Subclasses should:
    1. Initialize `_pending_commit_token: Optional[Any] = None` in __init__
    2. Implement `commit_checkpoint(checkpoint: Dict[str, Any]) -> None`
    3. Optionally override `abort_commit` to customize logging messages

    Example:
        .. testcode::
            :skipif: True

            from ray.data._internal.datasource.streaming_utils import TwoPhaseCommitMixin
            from ray.data.datasource.unbound_datasource import UnboundDatasource

            class MyDatasource(UnboundDatasource, TwoPhaseCommitMixin):
                def __init__(self):
                    super().__init__("my_source")
                    self._pending_commit_token = None

                def commit_checkpoint(self, checkpoint):
                    # Implement checkpoint commit logic
                    pass
    """

    def prepare_commit(self, checkpoint: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare commit token for two-phase commit.

        Args:
            checkpoint: Checkpoint dict to prepare.

        Returns:
            Commit token (same as checkpoint by default).
        """
        self._pending_commit_token = checkpoint
        return checkpoint

    def commit(self, commit_token: Dict[str, Any]) -> None:
        """Commit prepared token (two-phase commit).

        Args:
            commit_token: Commit token from prepare_commit().
        """
        self.commit_checkpoint(commit_token)
        self._pending_commit_token = None

    def abort_commit(self, commit_token: Dict[str, Any]) -> None:
        """Abort prepared commit (best-effort).

        Args:
            commit_token: Commit token to abort.
        """
        # Clear pending token - actual abort behavior depends on datasource
        self._pending_commit_token = None


__all__ = [
    "AWSCredentials",
    "HTTPClientConfig",
    "create_standard_schema",
    "apply_dataclass_to_dict",
    "create_block_coalescer",
    "create_block_metadata",
    "yield_coalesced_blocks",
    "TwoPhaseCommitMixin",
]
