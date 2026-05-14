"""Generic table datasink framework.

This module is the shared abstraction sitting under the Iceberg and Delta
datasinks. It owns the distributed write lifecycle and delegates every
format-specific concern (table loading, per-block file writing, transactional
commit) to a ``TableAdapter``.

Supporting a new table format reduces to implementing a new adapter.
"""

from .adapter import SupportsUpserts, TableAdapter
from .file_writer import DataFileWriter, ParquetFileWriter
from .modes import SaveMode, UpsertSemantics
from .result import TableWriteTaskResult
from .table_datasink import TableDatasink

__all__ = [
    "DataFileWriter",
    "ParquetFileWriter",
    "SaveMode",
    "SupportsUpserts",
    "TableAdapter",
    "TableDatasink",
    "TableWriteTaskResult",
    "UpsertSemantics",
]
