from dataclasses import dataclass

from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class DeltaScanner(ParquetScanner):
    """Scanner for a Delta Lake table.

    A Delta table's data files are Parquet files, so everything is inherited
    from :class:`ParquetScanner`: filter, column, limit and partition pushdown,
    and the Parquet reader. Partition values come from the table's Hive-style
    partition directories, through ``partitioning``.
    """
