from dataclasses import dataclass
from typing import ClassVar, Dict, Iterator, Optional, Type

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    NUM_RECORDS_COLUMN,
    PARTITION_VALUES_COLUMN,
    DeltaPartitionPruner,
    PartitionValues,
)
from ray.data._internal.datasource_v2.formats.delta.delta_filesystem import (
    add_known_file_sizes,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data.block import BlockMetadata
from ray.util.annotations import DeveloperAPI


class DeltaFileReader(ParquetFileReader):
    """Parquet reader that takes partition values from the listing.

    A Delta file's path needn't hold its partition values, so the listing puts
    the ones the log records in each manifest (``PARTITION_VALUES_COLUMN``).
    The reader casts them to the columns' types as it does a value parsed from
    a path. It also hands the files' sizes from the log to the table's
    filesystem, which then opens them without asking the storage for them.
    """

    # Replaced, never changed in place, by each ``read``.
    _partition_values_by_path: Dict[str, PartitionValues] = {}

    @override
    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        # A read task reads its manifests one after another.
        self._add_known_file_sizes(input_split)
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
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Yield each file's row count, from the log where it has one.

        The whole-file listing puts the log's counts in ``NUM_RECORDS_COLUMN``.
        Only the files without one have their Parquet footer read. Tables with
        deletion vectors, whose log counts include deleted rows, aren't read
        with this reader.
        """
        extra_columns = file_manifest.extra_columns
        if (
            extra_columns is not None
            and NUM_RECORDS_COLUMN in extra_columns.column_names
        ):
            num_records = extra_columns[NUM_RECORDS_COLUMN].to_pylist()
            for count in num_records:
                if count is not None:
                    yield BlockMetadata(
                        num_rows=count,
                        size_bytes=None,
                        exec_stats=None,
                        input_files=None,
                    )
            unknown = [count is None for count in num_records]
            if not any(unknown):
                return
            block = file_manifest.as_block()
            # Listing manifests are always built as Arrow tables.
            assert isinstance(block, pa.Table)
            file_manifest = FileManifest(block.filter(pa.array(unknown)))
        self._add_known_file_sizes(file_manifest)
        yield from super().read_metadata(file_manifest)

    def _add_known_file_sizes(self, manifest: FileManifest) -> None:
        add_known_file_sizes(
            self._filesystem,
            dict(zip(manifest.paths.tolist(), manifest.known_file_sizes)),
        )

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
