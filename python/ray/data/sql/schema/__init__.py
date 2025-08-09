"""
Schema management module for Ray Data SQL API.

This module provides comprehensive schema management and dataset registry
functionality for the SQL engine. It handles the mapping between SQL table
concepts and Ray Dataset objects, along with automatic schema inference
and metadata management.

Key Components:
- DatasetRegistry: Central registry mapping table names to Ray Datasets
- SchemaManager: Automatic schema inference and metadata storage

The schema system enables SQL-like table operations on Ray Datasets by:
1. Registering datasets under table names for SQL reference
2. Automatically inferring column types and schemas from sample data
3. Providing metadata for query optimization and validation
4. Managing table lifecycle (registration, lookup, cleanup)

Schema inference uses sample-based analysis to determine SQL types from
Python values, supporting efficient type checking without full dataset scans.
This enables proper SQL semantics while maintaining Ray's distributed
processing performance.
"""

from ray.data.sql.schema.manager import SchemaManager
from ray.data.sql.schema.registry import DatasetRegistry

__all__ = [
    "SchemaManager",
    "DatasetRegistry",
]
