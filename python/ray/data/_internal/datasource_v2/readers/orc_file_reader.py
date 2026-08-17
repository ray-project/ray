from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterator, List, Optional, Set

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.orc as orc
from pyarrow.fs import FileSystem, LocalFileSystem
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    OrcFileChunkMetadata,
)
from ray.data._internal.datasource_v2.chunkers.orc_file_chunking_utils import (
    stripe_range_from_chunk_metadata,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
    FileFormat,
    FileReader,
    _FragmentReadUnit,
)
from ray.data._internal.datasource_v2.readers.supports_metadata import (
    MetadataType,
    SupportsMetadata,
)
from ray.data.block import BlockMetadata
from ray.data.datasource.partitioning import Partitioning
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


def _pick_carrier_column(schema: pa.Schema) -> str:
    """Choose a cheap physical column when only the row count is needed."""
    for field in schema:
        if (
            pa.types.is_integer(field.type)
            or pa.types.is_boolean(field.type)
            or pa.types.is_floating(field.type)
            or pa.types.is_temporal(field.type)
        ):
            return field.name
    return schema.field(0).name


def _iter_stripe_indices(
    orc_file: orc.ORCFile,
    chunk_metadata: Optional[OrcFileChunkMetadata],
) -> Iterator[int]:
    if chunk_metadata is None:
        stripe_range = (0, orc_file.nstripes)
    else:
        stripe_range = stripe_range_from_chunk_metadata(
            chunk_metadata, orc_file.nstripes
        )
    if stripe_range is not None:
        yield from range(*stripe_range)


@dataclass(frozen=True)
class _OrcReadUnit(_FragmentReadUnit):
    chunk_metadata: Optional[OrcFileChunkMetadata] = None


@DeveloperAPI
class OrcFileReader(FileReader, SupportsMetadata):
    """ORC reader that maps manifest chunks onto ORC stripes."""

    _COUNT_ROWS_BATCH_SIZE = env_integer(
        "RAY_DATA_ORC_READER_COUNT_ROWS_BATCH_SIZE", 16
    )

    def __init__(
        self,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        predicate: Optional[Expr] = None,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[Partitioning] = None,
        include_paths: bool = False,
        schema: Optional[pa.Schema] = None,
    ):
        super().__init__(
            format=FileFormat.ORC,
            batch_size=batch_size or _ARROW_DEFAULT_BATCH_SIZE,
            columns=columns,
            predicate=predicate,
            limit=limit,
            filesystem=filesystem,
            partitioning=partitioning,
            include_paths=include_paths,
            schema=schema,
        )

    @override
    def _get_fragments_to_read(
        self,
        dataset: pds.Dataset,
        manifest: FileManifest,
    ) -> List[_FragmentReadUnit]:
        path_to_fragment = {
            fragment.path: fragment for fragment in dataset.get_fragments()
        }
        return [
            _OrcReadUnit(
                fragment=path_to_fragment[path],
                chunk_metadata=chunk_metadata,
            )
            for path, chunk_metadata in zip(
                manifest.paths, manifest.file_chunk_metadatas
            )
        ]

    @override
    def _iter_fragment_tables(
        self,
        read_unit: _FragmentReadUnit,
        scanner_kwargs: dict,
    ) -> Iterator[pa.Table]:
        """Read ORC stripes against the unified dataset schema.

        First resolve the logical columns needed by the projection and filter.
        Read only the intersection with the file's physical schema, then
        null-fill logical columns missing from this file. Finally, apply the
        filter, restore the requested projection order, and align the result to
        the unified schema.
        """
        from ray.data._internal.arrow_ops.transform_pyarrow import _align_struct_fields

        assert isinstance(read_unit, _OrcReadUnit)
        filesystem = self._filesystem or LocalFileSystem()
        with filesystem.open_input_file(read_unit.path) as handle:
            orc_file = orc.ORCFile(handle)
            physical_columns = orc_file.schema.names
            file_dataset_schema = self._file_dataset_schema
            unified_schema_columns = (
                physical_columns
                if file_dataset_schema is None
                else file_dataset_schema.names
            )

            columns = scanner_kwargs.get("columns")
            filter_expr = scanner_kwargs.get("filter")
            filter_columns = self._filter_columns or []

            output_columns = columns if columns is not None else unified_schema_columns
            logical_read_columns = list(dict.fromkeys(output_columns + filter_columns))

            physical_read_columns = [
                c for c in logical_read_columns if c in physical_columns
            ]
            if not physical_read_columns:
                if not physical_columns:
                    raise ValueError(
                        "ORC file has no physical columns; projection pushdown "
                        "cannot preserve row count without at least one carrier."
                    )
                physical_read_columns = [_pick_carrier_column(orc_file.schema)]

            if file_dataset_schema is not None and columns is not None:
                align_schema = pa.schema(
                    [
                        file_dataset_schema.field(c)
                        for c in columns
                        if file_dataset_schema.get_field_index(c) != -1
                    ]
                )
            else:
                align_schema = file_dataset_schema

            columns_to_null_fill = [
                c for c in logical_read_columns if c not in physical_columns
            ]
            null_fill_type_by_column = {
                column_name: (
                    file_dataset_schema.field(column_name).type
                    if file_dataset_schema is not None
                    and file_dataset_schema.get_field_index(column_name) != -1
                    else pa.null()
                )
                for column_name in columns_to_null_fill
            }

            for stripe_idx in _iter_stripe_indices(orc_file, read_unit.chunk_metadata):
                batch = orc_file.read_stripe(stripe_idx, columns=physical_read_columns)
                table = pa.Table.from_batches([batch])

                for column_name in columns_to_null_fill:
                    if column_name not in table.column_names:
                        table = table.append_column(
                            column_name,
                            pa.nulls(
                                table.num_rows,
                                type=null_fill_type_by_column[column_name],
                            ),
                        )

                if filter_expr is not None:
                    table = table.filter(filter_expr)
                    if table.num_rows == 0:
                        continue
                if columns is not None:
                    table = table.select(
                        [c for c in columns if c in table.column_names]
                    )

                if align_schema is not None:
                    table = _align_struct_fields([table], align_schema)[0].cast(
                        align_schema
                    )

                batch_size = scanner_kwargs.get("batch_size")
                if (
                    batch_size is None
                    or batch_size <= 0
                    or table.num_rows <= batch_size
                ):
                    yield table
                    continue
                for offset in range(0, table.num_rows, batch_size):
                    yield table.slice(offset, min(batch_size, table.num_rows - offset))

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        from ray.data._internal.util import call_with_retry
        from ray.data.context import DataContext

        filesystem = self._filesystem or LocalFileSystem()
        retried_io_errors = DataContext.get_current().retried_io_errors

        def _num_rows(path: str) -> int:
            def _read_num_rows() -> int:
                with filesystem.open_input_file(path) as handle:
                    return orc.ORCFile(handle).nrows

            return call_with_retry(
                _read_num_rows,
                description=f"read Orc metadata for {path}",
                match=retried_io_errors,
            )

        paths = [str(p) for p in file_manifest.paths]
        with ThreadPoolExecutor() as executor:
            for num_rows in executor.map(_num_rows, paths):
                yield BlockMetadata(
                    num_rows=num_rows,
                    size_bytes=None,
                    exec_stats=None,
                    input_files=None,
                )

    @override
    def available_metadata(self) -> Set[MetadataType]:
        if self._predicate is not None:
            return set()
        return {MetadataType.NUM_ROWS, MetadataType.NUM_BYTES}

    @override
    def get_target_metadata_batch_size(self) -> Optional[int]:
        return self._COUNT_ROWS_BATCH_SIZE
