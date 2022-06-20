import logging
import itertools
from typing import Callable, Optional, List, Union, Iterator, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pyarrow

import ray
from ray.types import ObjectRef
from ray.data.block import Block
from ray.data.context import DatasetContext
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem
from ray.data.datasource.parquet_base_datasource import ParquetBaseDatasource
from ray.data.datasource.file_meta_provider import (
    ParquetMetadataProvider,
    DefaultParquetMetadataProvider,
)
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import _check_pyarrow_version
from ray.util.annotations import PublicAPI


logger = logging.getLogger(__name__)

PIECES_PER_META_FETCH = 6
PARALLELIZE_META_FETCH_THRESHOLD = 24

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
PARQUET_READER_ROW_BATCH_SIZE = 100000
FILE_READING_RETRY = 8


def _register_parquet_file_fragment_serialization():
    from pyarrow.dataset import ParquetFileFragment

    def serialize(frag):
        return (frag.format, frag.path, frag.filesystem, frag.partition_expression)

    def deserialize(obj):
        file_format, path, filesystem, partition_expression = obj
        return file_format.make_fragment(path, filesystem, partition_expression)

    ray.util.register_serializer(
        ParquetFileFragment, serializer=serialize, deserializer=deserialize
    )


def _deregister_parquet_file_fragment_serialization():
    from pyarrow.dataset import ParquetFileFragment

    ray.util.deregister_serializer(ParquetFileFragment)


# This is the bare bone deserializing function with no retry
# easier to mock its behavior for testing when isolated from retry logic
def _deserialize_pieces(
    serialized_pieces: str,
) -> List["pyarrow._dataset.ParquetFileFragment"]:
    from ray import cloudpickle

    pieces: List["pyarrow._dataset.ParquetFileFragment"] = cloudpickle.loads(
        serialized_pieces
    )
    return pieces


# This retry helps when the upstream datasource is not able to handle
# overloaded read request or failed with some retriable failures.
# For example when reading data from HA hdfs service, hdfs might
# lose connection for some unknown reason expecially when
# simutaneously running many hyper parameter tuning jobs
# with ray.data parallelism setting at high value like the default 200
# Such connection failure can be restored with some waiting and retry.
def _deserialize_pieces_with_retry(
    serialized_pieces: str,
) -> List["pyarrow._dataset.ParquetFileFragment"]:
    min_interval = 0
    final_exception = None
    for i in range(FILE_READING_RETRY):
        try:
            return _deserialize_pieces(serialized_pieces)
        except Exception as e:
            import time
            import random

            retry_timing = (
                ""
                if i == FILE_READING_RETRY - 1
                else (f"Retry after {min_interval} sec. ")
            )
            log_only_show_in_1st_retry = (
                ""
                if i
                else (
                    f"If earlier read attempt threw certain Exception"
                    f", it may or may not be an issue depends on these retries "
                    f"succeed or not. serialized_pieces:{serialized_pieces}"
                )
            )
            logger.exception(
                f"{i + 1}th attempt to deserialize ParquetFileFragment failed. "
                f"{retry_timing}"
                f"{log_only_show_in_1st_retry}"
            )
            if not min_interval:
                # to make retries of different process hit hdfs server
                # at slightly different time
                min_interval = 1 + random.random()
            # exponential backoff at
            # 1, 2, 4, 8, 16, 32, 64
            time.sleep(min_interval)
            min_interval = min_interval * 2
            final_exception = e
    raise final_exception


@PublicAPI
class ParquetDatasource(ParquetBaseDatasource):
    """Parquet datasource, for reading and writing Parquet files.

    The primary difference from ParquetBaseDatasource is that this uses
    PyArrow's `ParquetDataset` abstraction for dataset reads, and thus offers
    automatic Arrow dataset schema inference and row count collection at the
    cost of some potential performance and/or compatibility penalties.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import ParquetDatasource
        >>> source = ParquetDatasource() # doctest: +SKIP
        >>> ray.data.read_datasource( # doctest: +SKIP
        ...     source, paths="/path/to/dir").take()
        [{"a": 1, "b": "foo"}, ...]
    """

    def prepare_read(
        self,
        parallelism: int,
        paths: Union[str, List[str]],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        columns: Optional[List[str]] = None,
        schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None,
        meta_provider: ParquetMetadataProvider = DefaultParquetMetadataProvider(),
        _block_udf: Optional[Callable[[Block], Block]] = None,
        **reader_args,
    ) -> List[ReadTask]:
        """Creates and returns read tasks for a Parquet file-based datasource."""
        # NOTE: We override the base class FileBasedDatasource.prepare_read
        # method in order to leverage pyarrow's ParquetDataset abstraction,
        # which simplifies partitioning logic. We still use
        # FileBasedDatasource's write side (do_write), however.
        _check_pyarrow_version()
        from ray import cloudpickle
        import pyarrow as pa
        import pyarrow.parquet as pq
        import numpy as np

        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        if len(paths) == 1:
            paths = paths[0]

        dataset_kwargs = reader_args.pop("dataset_kwargs", {})
        pq_ds = pq.ParquetDataset(
            paths, **dataset_kwargs, filesystem=filesystem, use_legacy_dataset=False
        )
        if schema is None:
            schema = pq_ds.schema
        if columns:
            schema = pa.schema(
                [schema.field(column) for column in columns], schema.metadata
            )

        def read_pieces(serialized_pieces: str) -> Iterator[pa.Table]:
            # Implicitly trigger S3 subsystem initialization by importing
            # pyarrow.fs.
            import pyarrow.fs  # noqa: F401

            # Deserialize after loading the filesystem class.
            try:
                _register_parquet_file_fragment_serialization()
                pieces: List[
                    "pyarrow._dataset.ParquetFileFragment"
                ] = _deserialize_pieces_with_retry(serialized_pieces)
            finally:
                _deregister_parquet_file_fragment_serialization()

            # Ensure that we're reading at least one dataset fragment.
            assert len(pieces) > 0

            from pyarrow.dataset import _get_partition_keys

            ctx = DatasetContext.get_current()
            output_buffer = BlockOutputBuffer(
                block_udf=_block_udf, target_max_block_size=ctx.target_max_block_size
            )

            logger.debug(f"Reading {len(pieces)} parquet pieces")
            use_threads = reader_args.pop("use_threads", False)
            for piece in pieces:
                part = _get_partition_keys(piece.partition_expression)
                batches = piece.to_batches(
                    use_threads=use_threads,
                    columns=columns,
                    schema=schema,
                    batch_size=PARQUET_READER_ROW_BATCH_SIZE,
                    **reader_args,
                )
                for batch in batches:
                    table = pyarrow.Table.from_batches([batch], schema=schema)
                    if part:
                        for col, value in part.items():
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
        metadata = meta_provider.prefetch_file_metadata(pq_ds.pieces) or []
        try:
            _register_parquet_file_fragment_serialization()
            for pieces, metadata in zip(
                np.array_split(pq_ds.pieces, parallelism),
                np.array_split(metadata, parallelism),
            ):
                if len(pieces) <= 0:
                    continue
                serialized_pieces = cloudpickle.dumps(pieces)
                input_files = [p.path for p in pieces]
                meta = meta_provider(
                    input_files,
                    inferred_schema,
                    pieces=pieces,
                    prefetched_metadata=metadata,
                )
                read_tasks.append(
                    ReadTask(lambda p=serialized_pieces: read_pieces(p), meta)
                )
        finally:
            _deregister_parquet_file_fragment_serialization()

        return read_tasks


def _fetch_metadata_remotely(
    pieces: List["pyarrow._dataset.ParquetFileFragment"],
) -> List[ObjectRef["pyarrow.parquet.FileMetaData"]]:
    from ray import cloudpickle

    remote_fetch_metadata = cached_remote_fn(_fetch_metadata_serialization_wrapper)
    metas = []
    parallelism = min(len(pieces) // PIECES_PER_META_FETCH, 100)
    meta_fetch_bar = ProgressBar("Metadata Fetch Progress", total=parallelism)
    try:
        _register_parquet_file_fragment_serialization()
        for pcs in np.array_split(pieces, parallelism):
            if len(pcs) == 0:
                continue
            metas.append(remote_fetch_metadata.remote(cloudpickle.dumps(pcs)))
    finally:
        _deregister_parquet_file_fragment_serialization()
    metas = meta_fetch_bar.fetch_until_complete(metas)
    return list(itertools.chain.from_iterable(metas))


def _fetch_metadata_serialization_wrapper(
    pieces: str,
) -> List["pyarrow.parquet.FileMetaData"]:
    # Implicitly trigger S3 subsystem initialization by importing
    # pyarrow.fs.
    import pyarrow.fs  # noqa: F401

    # Deserialize after loading the filesystem class.
    try:
        _register_parquet_file_fragment_serialization()
        pieces: List[
            "pyarrow._dataset.ParquetFileFragment"
        ] = _deserialize_pieces_with_retry(pieces)
    finally:
        _deregister_parquet_file_fragment_serialization()

    return _fetch_metadata(pieces)


def _fetch_metadata(
    pieces: List["pyarrow.dataset.ParquetFileFragment"],
) -> List["pyarrow.parquet.FileMetaData"]:
    piece_metadata = []
    for p in pieces:
        try:
            piece_metadata.append(p.metadata)
        except AttributeError:
            break
    return piece_metadata
