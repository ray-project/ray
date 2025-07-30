"""
Schema management module for Ray Data SQL API.

This module provides schema management and dataset registry functionality
for the SQL engine.
"""

from ray.data.sql.schema.manager import SchemaManager
from ray.data.sql.schema.registry import DatasetRegistry

__all__ = [
    "SchemaManager",
    "DatasetRegistry",
] 