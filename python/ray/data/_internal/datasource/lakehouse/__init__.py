"""Generic lakehouse datasink framework.

This module is the shared abstraction sitting under the Iceberg and Delta
datasinks. It owns the distributed write lifecycle and delegates every
format-specific concern (table loading, per-block file writing, transactional
commit) to a ``LakehouseAdapter``.

Supporting a new lakehouse format reduces to implementing a new adapter.
"""

from .adapter import LakehouseAdapter
from .file_writer import DataFileWriter, ParquetFileWriter
from .lakehouse_datasink import LakehouseDatasink
from .modes import SaveMode, UpsertSemantics
from .result import LakehouseWriteTaskResult

__all__ = [
    "DataFileWriter",
    "LakehouseAdapter",
    "LakehouseDatasink",
    "LakehouseWriteTaskResult",
    "ParquetFileWriter",
    "SaveMode",
    "UpsertSemantics",
]
