"""
Delta Lake datasource package for Ray Data.

This package provides Delta Lake functionality including:
- Read/write with ACID transactions
- Change Data Feed (CDF) for incremental processing
- Time travel and partition filtering
- Multi-cloud storage support (S3, GCS, Azure, HDFS)
- Unity Catalog compatibility
"""

# Configuration classes
from .config import (
    DeltaJSONEncoder,
    DeltaWriteConfig,
    WriteMode,
)

# CDF reading
from .delta_cdf import read_delta_cdf_distributed

# Core datasource and datasink
from .delta_datasink import DeltaDatasink
from .delta_datasource import DeltaDatasource

# Utilities and table operations
from .utilities import (
    AWSUtilities,
    AzureUtilities,
    DeltaUtilities,
    GCPUtilities,
    try_get_deltatable,
)

__all__ = [
    # Core classes
    "DeltaDatasink",
    "DeltaDatasource",
    # Configuration classes
    "DeltaJSONEncoder",
    "DeltaWriteConfig",
    # Enums
    "WriteMode",
    # Cloud utilities
    "AWSUtilities",
    "AzureUtilities",
    "DeltaUtilities",
    "GCPUtilities",
    # Helper utilities
    "try_get_deltatable",
    # CDF reading
    "read_delta_cdf_distributed",
]
