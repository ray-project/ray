"""Utilities for unbound datasources.

This module provides shared utility functions for unbound data sources
to reduce code duplication and ensure consistency.
"""

from typing import Any, Dict, List, Optional, Union

import pyarrow as pa


def infer_unbound_schema_with_fallback(
    sample_data: List[Dict[str, Any]], default_schema: Optional[pa.Schema] = None
) -> pa.Schema:
    """Infer schema from sample data with fallback.

    Args:
        sample_data: Sample records to infer schema from
        default_schema: Default schema to use if inference fails

    Returns:
        Inferred or default PyArrow schema
    """
    if not sample_data:
        if default_schema:
            return default_schema
        # Return a basic schema with common fields
        return pa.schema(
            [
                ("timestamp", pa.timestamp("us")),
                ("data", pa.string()),
            ]
        )

    try:
        # Try to infer schema from sample data
        table = pa.Table.from_pylist(sample_data)
        return table.schema
    except Exception:
        if default_schema:
            return default_schema

        # Fallback to string schema
        if sample_data:
            fields = []
            for key in sample_data[0].keys():
                fields.append((key, pa.string()))
            return pa.schema(fields)

        return pa.schema([("data", pa.string())])


def validate_unbound_config(config: Dict[str, Any]) -> None:
    """Validate basic unbound configuration.

    Args:
        config: Configuration to validate

    Raises:
        ValueError: If configuration is invalid
    """
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a dictionary")

    # Check for required trigger
    if "trigger" not in config:
        raise ValueError("Unbound configuration must include a trigger")


def standardize_unbound_position(position: Union[str, int, Dict[str, Any]]) -> str:
    """Standardize position representation.

    Args:
        position: Position in various formats

    Returns:
        Standardized position string
    """
    if isinstance(position, str):
        return position
    elif isinstance(position, int):
        return str(position)
    elif isinstance(position, dict):
        # Convert dict to string representation
        return str(position)
    else:
        return str(position)


def parse_unbound_position(position_str: str) -> Dict[str, Any]:
    """Parse position string into structured format.

    Args:
        position_str: Position string to parse

    Returns:
        Parsed position as dictionary
    """
    try:
        # Try to parse as integer (offset)
        offset = int(position_str)
        return {"type": "offset", "value": offset}
    except ValueError:
        pass

    # Check for special positions
    if position_str.lower() in ["earliest", "latest", "beginning", "end"]:
        return {"type": "special", "value": position_str.lower()}

    # Default to treating as string
    return {"type": "string", "value": position_str}


def create_unbound_record_enricher(source_type: str):
    """Create a record enricher function for unbound data.

    Args:
        source_type: Type of unbound source

    Returns:
        Function that enriches records with metadata
    """

    def enrich_record(
        record: Dict[str, Any], partition_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enrich a record with unbound metadata.

        Args:
            record: Original record
            partition_info: Partition information

        Returns:
            Enriched record
        """
        enriched = record.copy()

        # Add unbound metadata
        enriched["_unbound_source_type"] = source_type
        enriched["_unbound_partition"] = partition_info.get("partition_id")

        return enriched

    return enrich_record


def retry_unbound_operation(
    operation, max_retries: int = 3, backoff_factor: float = 1.0
):
    """Retry an unbound operation with exponential backoff.

    Args:
        operation: Function to retry
        max_retries: Maximum number of retries
        backoff_factor: Backoff multiplier

    Returns:
        Result of the operation

    Raises:
        Exception: If all retries are exhausted
    """
    import time

    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return operation()
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                wait_time = backoff_factor * (2**attempt)
                time.sleep(wait_time)

    raise last_exception


class UnboundError(Exception):
    """Base exception for unbound datasource errors."""

    pass


class UnboundConnectionError(UnboundError):
    """Connection-related unbound error."""

    pass


class UnboundAuthenticationError(UnboundError):
    """Authentication-related unbound error."""

    pass


class UnboundConfigurationError(UnboundError):
    """Configuration-related unbound error."""

    pass


def handle_unbound_error(error: Exception, operation: str) -> UnboundError:
    """Convert generic exceptions to unbound-specific ones.

    Args:
        error: Original exception
        operation: Operation that failed

    Returns:
        Unbound-specific exception
    """
    error_msg = f"Operation '{operation}' failed: {error}"

    # Map common error types
    if "connection" in str(error).lower():
        return UnboundConnectionError(error_msg)
    elif "auth" in str(error).lower() or "permission" in str(error).lower():
        return UnboundAuthenticationError(error_msg)
    elif "config" in str(error).lower() or "invalid" in str(error).lower():
        return UnboundConfigurationError(error_msg)
    else:
        return UnboundError(error_msg)
