"""Table registry module for Ray Data SQL API.

This module provides table registration and management functionality for
the SQL API, allowing users to register Ray Datasets as SQL tables.
"""

from ray.data.experimental.sql.registry.base import TableRegistry

__all__ = [
    "TableRegistry",
]
