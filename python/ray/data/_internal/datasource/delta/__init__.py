"""
Delta Lake datasource package for Ray Data.

This package provides Delta Lake functionality including:
- Read/write with ACID transactions
- Time travel and partition filtering
- Multi-cloud storage support (S3, GCS, Azure, HDFS)
- Unity Catalog compatibility
"""

# Configuration classes
from .config import WriteMode

# Core datasources and datasink
from .delta_datasink import DeltaDatasink
from .delta_datasource import DeltaDatasource

# Utilities and table operations
from .utilities import get_storage_options, try_get_deltatable

__all__ = [
    # Core classes
    "DeltaDatasink",
    "DeltaDatasource",
    # Enums
    "WriteMode",
    # Helper utilities
    "get_storage_options",
    "try_get_deltatable",
]
