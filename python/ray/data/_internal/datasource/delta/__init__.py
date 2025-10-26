"""
Delta Lake datasource package for Ray Data.

This package provides Delta Lake functionality including:
- Read/write with ACID transactions
- Change Data Feed (CDF) for incremental processing with streaming execution
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

# Core datasources and datasink
from .delta_cdf_datasource import DeltaCDFDatasource
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
    "DeltaCDFDatasource",
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
]
