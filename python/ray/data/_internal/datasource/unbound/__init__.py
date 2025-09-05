"""Unbound datasources for Ray Data.

This package provides datasources for reading unbounded data streams
like Kafka topics, Kinesis streams, and Flink jobs.
"""

# Core components
from ray.data._internal.datasource.unbound.unbound_datasource import UnboundDatasource
from ray.data._internal.datasource.unbound.position import UnboundPosition
from ray.data._internal.datasource.unbound.metrics import UnboundMetrics
from ray.data._internal.datasource.unbound.read_task import create_unbound_read_task

# Datasource implementations
from ray.data._internal.datasource.unbound.kafka_datasource import KafkaDatasource
from ray.data._internal.datasource.unbound.kinesis_datasource import KinesisDatasource
from ray.data._internal.datasource.unbound.flink_datasource import FlinkDatasource

# Utilities
from ray.data._internal.datasource.unbound.utils import (
    infer_unbound_schema_with_fallback,
    validate_unbound_config,
    standardize_unbound_position,
    parse_unbound_position,
    create_unbound_record_enricher,
    retry_unbound_operation,
    UnboundError,
    UnboundConnectionError,
    UnboundAuthenticationError,
    UnboundConfigurationError,
    handle_unbound_error,
)

__all__ = [
    # Core components
    "UnboundDatasource",
    "UnboundPosition",
    "UnboundMetrics",
    "create_unbound_read_task",
    # Datasource implementations
    "KafkaDatasource",
    "KinesisDatasource",
    "FlinkDatasource",
    # Utilities
    "infer_unbound_schema_with_fallback",
    "validate_unbound_config",
    "standardize_unbound_position",
    "parse_unbound_position",
    "create_unbound_record_enricher",
    "retry_unbound_operation",
    "UnboundError",
    "UnboundConnectionError",
    "UnboundAuthenticationError",
    "UnboundConfigurationError",
    "handle_unbound_error",
]
