"""Delta Lake datasource package for Ray Data.

This package provides basic Delta Lake functionality including:
- Standard write modes (append, overwrite, error, ignore)
- Partitioned writes
- Schema validation
- Advanced deltalake options (target_file_size, writer_properties, etc.)
"""

from ray.data._internal.datasource.delta.config import (
    DeltaSinkWriteResult,
    DeltaWriteConfig,
)
from ray.data._internal.datasource.delta.delta_datasink import DeltaDatasink
from ray.data._internal.datasource.delta.utilities import (
    create_bloom_filter_properties,
    create_column_properties,
    create_writer_properties,
    get_storage_options_for_path,
    try_get_deltatable,
)

__all__ = [
    # Main classes
    "DeltaDatasink",
    # Configuration classes
    "DeltaSinkWriteResult",
    "DeltaWriteConfig",
    # Utility functions
    "get_storage_options_for_path",
    "try_get_deltatable",
    "create_writer_properties",
    "create_bloom_filter_properties",
    "create_column_properties",
]
