"""Table registry module for Ray Data SQL API.

This module provides table registration and management functionality for
the SQL API, allowing users to register Ray Datasets as SQL tables.
"""

from ray.data.sql.registry.base import TableRegistry
from ray.data.sql.registry.manager import TableManager

__all__ = [
    "TableRegistry",
    "TableManager",
]
