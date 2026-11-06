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
    the Parquet reader and the generated ID column. Partition values come from
    the table's Hive-style partition directories, through ``partitioning``.
    """

    #: The table URI, for messages.
    table_uri: str = ""
    #: Whether the caller passed the version to read. Otherwise the read is
    #: of the version that was latest when the dataset was created, which a
    #: resumed job reads anew.
    version_pinned_by_caller: bool = False
