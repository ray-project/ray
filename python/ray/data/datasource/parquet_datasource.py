import logging
import itertools
from typing import Any, Callable, Dict, Optional, List, Union, Iterator, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pyarrow

from ray.data.block import Block, BlockAccessor
from ray.data.context import DatasetContext
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource,
    _resolve_paths_and_filesystem,
    _resolve_kwargs,
    _wrap_s3_serialization_workaround,
    _S3FileSystemWrapper,
)
from ray.data.impl.block_list import BlockMetadata
from ray.data.impl.output_buffer import BlockOutputBuffer
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.remote_fn import cached_remote_fn
from ray.data.impl.util import _check_pyarrow_version

logger = logging.getLogger(__name__)

PIECES_PER_META_FETCH = 6
PARALLELIZE_META_FETCH_THRESHOLD = 24

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
PARQUET_READER_ROW_BATCH_SIZE = 100000


class PartitionedFile:
    """Represents a remote parquet file with partitioning info."""

    def __init__(self, path: str, partition_keys: dict):
        self.path = path
        self.partition_keys = partition_keys


class ParquetDatasource(FileBasedDatasource):
    """Parquet datasource, for reading and writing Parquet files.

    Examples:
        >>> source = ParquetDatasource()
        >>> ray.data.read_datasource(source, paths="/path/to/dir").take()
        ... [{"a": 1, "b": "foo"}, ...]
    """

    def prepare_read(
        self,
        parallelism: int,
        paths: Union[str, List[str]],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        columns: Optional[List[str]] = None,
        schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None,
        _block_udf: Optional[Callable[[Block], Block]] = None,
        **reader_args,
    ) -> List[ReadTask]:
        """Creates and returns read tasks for a Parquet file-based datasource."""
        # NOTE: We override the base class FileBasedDatasource.prepare_read
        # method in order to leverage pyarrow's ParquetDataset abstraction,
        # which simplifies partitioning logic. We still use
        # FileBasedDatasource's write side (do_write), however.
        _check_pyarrow_version()
        import pyarrow as pa
        import pyarrow.parquet as pq
        from pyarrow.dataset import _get_partition_keys
        import numpy as np

        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        if len(paths) == 1:
            paths = paths[0]

        dataset_kwargs = reader_args.pop("dataset_kwargs", {})
        pq_ds = pq.ParquetDataset(
            paths,
            **dataset_kwargs,
            filesystem=filesystem,
            use_legacy_dataset=False,
            split_row_groups=False,
        )
        if schema is None:
            schema = pq_ds.schema
        if columns:
            schema = pa.schema(
                [schema.field(column) for column in columns], schema.metadata
            )

        # Mapping from file path to partitioning keys.
        paths_seen = set()
        files_to_read: List[PartitionedFile] = []
        for frag in pq_ds.pieces:
            if frag.path not in paths_seen:
                paths_seen.add(frag.path)
                files_to_read.append(
                    PartitionedFile(
                        frag.path, _get_partition_keys(frag.partition_expression)
                    )
                )

        # Prepare local variables for serialization in remote tasks.
        # Partitioning isn't relevant when reading individual files, drop it now.
        if "partitioning" in dataset_kwargs:
            dataset_kwargs = dataset_kwargs.copy()
            del dataset_kwargs["partitioning"]
        filesystem = _wrap_s3_serialization_workaround(filesystem)

        def read_files(
            files: List[PartitionedFile], filesystem, **dataset_kwargs
        ) -> Iterator[pa.Table]:
            if isinstance(filesystem, _S3FileSystemWrapper):
                filesystem = filesystem.unwrap()

            # Ensure that we're reading at least one file.
            assert len(files) > 0

            ctx = DatasetContext.get_current()
            output_buffer = BlockOutputBuffer(
                block_udf=_block_udf, target_max_block_size=ctx.target_max_block_size
            )

            logger.debug(f"Reading {len(files)} parquet files")
            use_threads = reader_args.pop("use_threads", False)
            for f in files:
                ds = pq.ParquetDataset(
                    f.path,
                    **dataset_kwargs,
                    filesystem=filesystem,
                    use_legacy_dataset=False,
                    split_row_groups=False,
                )
                for fragment in ds.pieces:
                    batches = fragment.to_batches(
                        use_threads=use_threads,
                        columns=columns,
                        schema=schema,
                        batch_size=PARQUET_READER_ROW_BATCH_SIZE,
                        **reader_args,
                    )
                    for batch in batches:
                        table = pa.Table.from_batches([batch], schema=schema)
                        # TODO(ekl) we have to manually apply the filter on partitioned
                        # columns after the read. This is an artifact of the way we
                        # work around file fragment serialization issues.
                        if f.partition_keys and "filter" in reader_args:
                            raise NotImplementedError(
                                "The `filter` reader arg is not implemented "
                                "for partitioned datasets."
                            )
                        if f.partition_keys:
                            for col, value in f.partition_keys.items():
                                table = table.set_column(
                                    table.schema.get_field_index(col),
                                    col,
                                    pa.array([value] * len(table)),
                                )
                        # If the table is empty, drop it.
                        if table.num_rows > 0:
                            output_buffer.add_block(table)
                            if output_buffer.has_next():
                                yield output_buffer.next()
            output_buffer.finalize()
            if output_buffer.has_next():
                yield output_buffer.next()

        if _block_udf is not None:
            # Try to infer dataset schema by passing dummy table through UDF.
            dummy_table = schema.empty_table()
            try:
                inferred_schema = _block_udf(dummy_table).schema
                inferred_schema = inferred_schema.with_metadata(schema.metadata)
            except Exception:
                logger.debug(
                    "Failed to infer schema of dataset by passing dummy table "
                    "through UDF due to the following exception:",
                    exc_info=True,
                )
                inferred_schema = schema
        else:
            inferred_schema = schema
        read_tasks = []
        if len(files_to_read) > PARALLELIZE_META_FETCH_THRESHOLD:
            metadata = _fetch_metadata_remotely(
                files_to_read, filesystem, **dataset_kwargs
            )
        else:
            metadata = _fetch_metadata(files_to_read, filesystem, **dataset_kwargs)
        for file_data in np.array_split(
            list(zip(files_to_read, metadata)), parallelism
        ):
            if len(file_data) == 0:
                continue
            files, metadata = zip(*file_data)
            meta = _build_block_metadata(files, metadata, inferred_schema)
            read_tasks.append(
                ReadTask(
                    lambda files=files: read_files(files, filesystem, **dataset_kwargs),
                    meta,
                )
            )

        return read_tasks

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args,
    ):
        import pyarrow.parquet as pq

        writer_args = _resolve_kwargs(writer_args_fn, **writer_args)
        pq.write_table(block.to_arrow(), f, **writer_args)

    def _file_format(self) -> str:
        return "parquet"


def _fetch_metadata_remotely(
    files: List[PartitionedFile],
    filesystem,
    **dataset_kwargs,
) -> List["pyarrow.parquet.FileMetaData"]:

    remote_fetch_metadata = cached_remote_fn(_fetch_metadata)
    metas = []
    parallelism = max(1, min(len(files) // PIECES_PER_META_FETCH, 100))
    meta_fetch_bar = ProgressBar("Metadata Fetch Progress", total=parallelism)
    for fs in np.array_split(files, parallelism):
        if len(fs) == 0:
            continue
        metas.append(remote_fetch_metadata.remote(fs, filesystem, **dataset_kwargs))
    metas = meta_fetch_bar.fetch_until_complete(metas)
    return list(itertools.chain.from_iterable(metas))


def _fetch_metadata(
    files: List[PartitionedFile],
    filesystem,
    **dataset_kwargs,
) -> List["pyarrow.parquet.FileMetaData"]:
    import pyarrow.parquet as pq

    if isinstance(filesystem, _S3FileSystemWrapper):
        filesystem = filesystem.unwrap()

    pq_ds = pq.ParquetDataset(
        [f.path for f in files],
        **dataset_kwargs,
        filesystem=filesystem,
        use_legacy_dataset=False,
        split_row_groups=False,
    )
    assert len(pq_ds.pieces) == len(files), pq_ds.pieces
    metadata = {f.path: f.metadata for f in pq_ds.pieces}
    return [metadata[f.path] for f in files]


def _build_block_metadata(
    files: List[PartitionedFile],
    metadata: List["pyarrow.parquet.FileMetaData"],
    schema: Optional[Union[type, "pyarrow.lib.Schema"]],
) -> BlockMetadata:
    input_files = list(set([p.path for p in files]))
    if len(metadata) == len(files):
        # Piece metadata was available, construct a normal
        # BlockMetadata.
        block_metadata = BlockMetadata(
            num_rows=sum(m.num_rows for m in metadata),
            size_bytes=sum(
                sum(m.row_group(i).total_byte_size for i in range(m.num_row_groups))
                for m in metadata
            ),
            schema=schema,
            input_files=input_files,
            exec_stats=None,
        )  # Exec stats filled in later.
    else:
        # Piece metadata was not available, construct an empty
        # BlockMetadata.
        block_metadata = BlockMetadata(
            num_rows=None, size_bytes=None, schema=schema, input_files=input_files
        )
    return block_metadata
