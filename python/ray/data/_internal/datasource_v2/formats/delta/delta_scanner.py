from dataclasses import dataclass
from typing import ClassVar, Dict, Iterator, Optional, Type

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    PARTITION_VALUES_COLUMN,
    DeltaPartitionPruner,
    PartitionValues,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.util.annotations import DeveloperAPI


class DeltaFileReader(ParquetFileReader):
    """Parquet reader that takes partition values from the listing.

    A Delta file's path needn't hold its partition values, so the listing puts
    the ones the log records in each manifest (``PARTITION_VALUES_COLUMN``).
    The reader casts them to the columns' types as it does a value parsed from
    a path.
    """

    # Replaced, never changed in place, by each ``read``.
    _partition_values_by_path: Dict[str, PartitionValues] = {}

    @override
    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        # A read task reads its manifests one after another.
        extra_columns = input_split.extra_columns
        self._partition_values_by_path = (
            dict(
                zip(
                    input_split.paths.tolist(),
                    extra_columns[PARTITION_VALUES_COLUMN].to_pylist(),
                )
            )
            if extra_columns is not None
            and PARTITION_VALUES_COLUMN in extra_columns.column_names
            else {}
        )
        yield from super().read(input_split)

    @override
    def _get_partition_values(self, path: str) -> Dict[str, Optional[str]]:
        # ``ParquetFileReader`` keeps its partitioning here, not in the base
        # class's parser; ``None`` for an unpartitioned table.
        if self._partition_value_parser is None:
            return {}
        values = self._partition_values_by_path.get(path)
        if values is None:
            raise ValueError(
                f"The listing gave no partition values for the Delta data file {path!r}."
            )
        return values


@DeveloperAPI
@dataclass(frozen=True)
class DeltaScanner(ParquetScanner):
    """Scanner for a Delta Lake table.

    A Delta table's data files are Parquet files, so filter, column and limit
    pushdown and the generated ID column are inherited from
    :class:`ParquetScanner`. Partition values come from the log: the listing
    prunes files by them and carries them to :class:`DeltaFileReader`.
    ``partitioning`` only names the partition columns.
    """

    #: The table URI, for messages.
    table_uri: str = ""
    #: Whether the caller passed the version to read. Otherwise the read is
    #: of the version that was latest when the dataset was created, which a
    #: resumed job reads anew.
    version_pinned_by_caller: bool = False

    reader_class: ClassVar[Type[ParquetFileReader]] = DeltaFileReader

    @override
    def _create_partition_pruner(self) -> Optional[FilePruner]:
        if self.partitioning is None or self.partition_predicate is None:
            return None
        return DeltaPartitionPruner(self.partition_predicate, self.schema)

    @override
    def prune_input_split(self, input_split: FileManifest) -> FileManifest:
        # The listing already left out the files whose partition values in the
        # log fail the predicate, and the reader filters rows by it.
        return input_split
