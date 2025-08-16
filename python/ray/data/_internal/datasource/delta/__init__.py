"""
Delta Lake datasource package for Ray Data.

This package provides comprehensive Delta Lake functionality including:
- Standard write modes (append, overwrite, error, ignore)
- Advanced merge operations with DeltaMergeBuilder syntax
- SCD (Slowly Changing Dimensions) Types 1, 2, and 3
- Scalable microbatch processing
- Multi-cloud support with automatic credential detection
- Table optimization (compaction, Z-ordering, vacuum)
"""

from ray.data._internal.datasource.delta.config import (
    AddAction,
    DeltaSinkWriteResult,
    DeltaWriteConfig,
    MergeConditions,
    MergeConfig,
    MergeMode,
    OptimizationConfig,
    OptimizationMode,
    SCDConfig,
    WriteMode,
)
from ray.data._internal.datasource.delta.delta_datasink import DeltaDatasink
from ray.data._internal.datasource.delta.merger import DeltaTableMerger
from ray.data._internal.datasource.delta.optimizer import DeltaTableOptimizer
from ray.data._internal.datasource.delta.utilities import (
    AWSUtilities,
    AzureUtilities,
    DeltaJSONEncoder,
    DeltaUtilities,
    GCPUtilities,
    compact_delta_table,
    try_get_deltatable,
    vacuum_delta_table,
    z_order_delta_table,
)

__all__ = [
    # Main classes
    "DeltaDatasink",
    "DeltaTableMerger",
    "DeltaTableOptimizer",
    # Configuration classes
    "AddAction",
    "DeltaSinkWriteResult",
    "DeltaWriteConfig",
    "MergeConditions",
    "MergeConfig",
    "OptimizationConfig",
    "SCDConfig",
    # Enums
    "MergeMode",
    "OptimizationMode",
    "WriteMode",
    # Utilities
    "AWSUtilities",
    "AzureUtilities",
    "DeltaJSONEncoder",
    "DeltaUtilities",
    "GCPUtilities",
    # Standalone functions
    "compact_delta_table",
    "try_get_deltatable",
    "vacuum_delta_table",
    "z_order_delta_table",
]
