import itertools
import logging
from typing import TYPE_CHECKING, Callable, Iterator, List, Optional, Union

import numpy as np

from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block
from ray.data.context import DatasetContext
from ray.data.datasource.datasource import Reader, ReadTask
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem
from ray.data.datasource.file_meta_provider import (
    DefaultParquetMetadataProvider,
    ParquetMetadataProvider,
    _handle_read_os_error,
)
from ray.data.datasource.parquet_base_datasource import ParquetBaseDatasource
from ray.types import ObjectRef
from ray.util.annotations import PublicAPI
import ray.cloudpickle as cloudpickle

if TYPE_CHECKING:
    import pyarrow
    from pyarrow.dataset import ParquetFileFragment


logger = logging.getLogger(__name__)

PIECES_PER_META_FETCH = 6
PARALLELIZE_META_FETCH_THRESHOLD = 24

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
PARQUET_READER_ROW_BATCH_SIZE = 100000
FILE_READING_RETRY = 8

# The default size multiplier for reading Parquet data source in Arrow.
# Parquet data format is encoded with various encoding techniques (such as
# dictionary, RLE, delta), so Arrow in-memory representation uses much more memory
# compared to Parquet encoded representation. Parquet file statistics only record
# encoded (i.e. uncompressed) data size information.
#
# To estimate real-time in-memory data size, Datasets will try to estimate the correct
# inflation ratio from Parquet to Arrow, using this constant as the default value for
# safety. See https://github.com/ray-project/ray/pull/26516 for more context.
PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT = 5

# The lower bound size to estimate Parquet encoding ratio.
PARQUET_ENCODING_RATIO_ESTIMATE_LOWER_BOUND = 2

# The percentage of files (1% by default) to be sampled from the dataset to estimate
# Parquet encoding ratio.
PARQUET_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO = 0.01

# The minimal and maximal number of file samples to take from the dataset to estimate
# Parquet encoding ratio.
# This is to restrict `PARQUET_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO` within the
# proper boundary.
PARQUET_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES = 2
PARQUET_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES = 10

# The number of rows to read from each file for sampling. Try to keep it low to avoid
# reading too much data into memory.
PARQUET_ENCODING_RATIO_ESTIMATE_NUM_ROWS = 1024


# TODO(ekl) this is a workaround for a pyarrow serialization bug, where serializing a
# raw pyarrow file fragment causes S3 network calls.
class _SerializedPiece:
    def __init__(self, frag: "ParquetFileFragment"):
        self._data = cloudpickle.dumps(
            (frag.format, frag.path, frag.filesystem, frag.partition_expression)
        )

    def deserialize(self) -> "ParquetFileFragment":
        # Implicitly trigger S3 subsystem initialization by importing
        # pyarrow.fs.
        import pyarrow.fs  # noqa: F401

        (file_format, path, filesystem, partition_expression) = cloudpickle.loads(
            self._data
        )
        return file_format.make_fragment(path, filesystem, partition_expression)


# Visible for test mocking.
def _deserialize_pieces(
    serialized_pieces: List[_SerializedPiece],
) -> List["pyarrow._dataset.ParquetFileFragment"]:
    return [p.deserialize() for p in serialized_pieces]


# This retry helps when the upstream datasource is not able to handle
# overloaded read request or failed with some retriable failures.
# For example when reading data from HA hdfs service, hdfs might
# lose connection for some unknown reason expecially when
# simutaneously running many hyper parameter tuning jobs
# with ray.data parallelism setting at high value like the default 200
# Such connection failure can be restored with some waiting and retry.
def _deserialize_pieces_with_retry(
    serialized_pieces: List[_SerializedPiece],
) -> List["pyarrow._dataset.ParquetFileFragment"]:
    min_interval = 0
    final_exception = None
    for i in range(FILE_READING_RETRY):
        try:
            return _deserialize_pieces(serialized_pieces)
        except Exception as e:
            import random
            import time

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

    def create_reader(self, **kwargs):
        return _ParquetDatasourceReader(**kwargs)


class _ParquetDatasourceReader(Reader):
    def __init__(
        self,
        paths: Union[str, List[str]],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        columns: Optional[List[str]] = None,
        schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None,
        meta_provider: ParquetMetadataProvider = DefaultParquetMetadataProvider(),
        _block_udf: Optional[Callable[[Block], Block]] = None,
        **reader_args,
    ):
        _check_pyarrow_version()
        import pyarrow as pa
        import pyarrow.parquet as pq

        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        if len(paths) == 1:
            paths = paths[0]

        dataset_kwargs = reader_args.pop("dataset_kwargs", {})
        try:
            pq_ds = pq.ParquetDataset(
                paths, **dataset_kwargs, filesystem=filesystem, use_legacy_dataset=False
            )
        except OSError as e:
            _handle_read_os_error(e, paths)
        if schema is None:
            schema = pq_ds.schema
        if columns:
            schema = pa.schema(
                [schema.field(column) for column in columns], schema.metadata
            )

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

        try:
            self._metadata = meta_provider.prefetch_file_metadata(pq_ds.pieces) or []
        except OSError as e:
            _handle_read_os_error(e, paths)
        self._pq_ds = pq_ds
        self._meta_provider = meta_provider
        self._inferred_schema = inferred_schema
        self._block_udf = _block_udf
        self._reader_args = reader_args
        self._columns = columns
        self._schema = schema
        self._encoding_ratio = self._estimate_files_encoding_ratio()

    def estimate_inmemory_data_size(self) -> Optional[int]:
        total_size = 0
        for file_metadata in self._metadata:
            for row_group_idx in range(file_metadata.num_row_groups):
                row_group_metadata = file_metadata.row_group(row_group_idx)
                total_size += row_group_metadata.total_byte_size
        return total_size * self._encoding_ratio

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        # NOTE: We override the base class FileBasedDatasource.get_read_tasks()
        # method in order to leverage pyarrow's ParquetDataset abstraction,
        # which simplifies partitioning logic. We still use
        # FileBasedDatasource's write side (do_write), however.
        read_tasks = []
        for pieces, metadata in zip(
            np.array_split(self._pq_ds.pieces, parallelism),
            np.array_split(self._metadata, parallelism),
        ):
            if len(pieces) <= 0:
                continue
            serialized_pieces = [_SerializedPiece(p) for p in pieces]
            input_files = [p.path for p in pieces]
            meta = self._meta_provider(
                input_files,
                self._inferred_schema,
                pieces=pieces,
                prefetched_metadata=metadata,
            )
            if meta.size_bytes is not None:
                meta.size_bytes = int(meta.size_bytes * self._encoding_ratio)
            block_udf, reader_args, columns, schema = (
                self._block_udf,
                self._reader_args,
                self._columns,
                self._schema,
            )
            read_tasks.append(
                ReadTask(
                    lambda p=serialized_pieces: _read_pieces(
                        block_udf,
                        reader_args,
                        columns,
                        schema,
                        p,
                    ),
                    meta,
                )
            )

        return read_tasks

    def _estimate_files_encoding_ratio(self) -> float:
        """Return an estimate of the Parquet files encoding ratio.

        To avoid OOMs, it is safer to return an over-estimate than an underestimate.
        """
        if not DatasetContext.get_current().decoding_size_estimation:
            return PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT

        # Sample a few rows from Parquet files to estimate the encoding ratio.
        # Launch tasks to sample multiple files remotely in parallel.
        # Evenly distributed to sample N rows in i-th row group in i-th file.
        # TODO(ekl/cheng) take into account column pruning.
        num_files = len(self._pq_ds.pieces)
        num_samples = int(num_files * PARQUET_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO)
        min_num_samples = min(
            PARQUET_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES, num_files
        )
        max_num_samples = min(
            PARQUET_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES, num_files
        )
        num_samples = max(min(num_samples, max_num_samples), min_num_samples)

        # Evenly distributed to choose which file to sample, to avoid biased prediction
        # if data is skewed.
        file_samples = [
            self._pq_ds.pieces[idx]
            for idx in np.linspace(0, num_files - 1, num_samples).astype(int).tolist()
        ]

        sample_piece = cached_remote_fn(_sample_piece)
        futures = []
        for sample in file_samples:
            # Sample the first rows batch in i-th file.
            # Use SPREAD scheduling strategy to avoid packing many sampling tasks on
            # same machine to cause OOM issue, as sampling can be memory-intensive.
            serialized_sample = _SerializedPiece(sample)
            futures.append(
                sample_piece.options(scheduling_strategy="SPREAD").remote(
                    self._reader_args,
                    self._columns,
                    self._schema,
                    serialized_sample,
                )
            )
        sample_bar = ProgressBar("Parquet Files Sample", len(futures))
        sample_ratios = sample_bar.fetch_until_complete(futures)
        sample_bar.close()
        ratio = np.mean(sample_ratios)
        logger.debug(f"Estimated Parquet encoding ratio from sampling is {ratio}.")
        return max(ratio, PARQUET_ENCODING_RATIO_ESTIMATE_LOWER_BOUND)


def _read_pieces(
    block_udf, reader_args, columns, schema, serialized_pieces: List[_SerializedPiece]
) -> Iterator["pyarrow.Table"]:
    # This import is necessary to load the tensor extension type.
    from ray.data.extensions.tensor_extension import ArrowTensorType  # noqa

    # Deserialize after loading the filesystem class.
    pieces: List[
        "pyarrow._dataset.ParquetFileFragment"
    ] = _deserialize_pieces_with_retry(serialized_pieces)

    # Ensure that we're reading at least one dataset fragment.
    assert len(pieces) > 0

    import pyarrow as pa
    from pyarrow.dataset import _get_partition_keys

    ctx = DatasetContext.get_current()
    output_buffer = BlockOutputBuffer(
        block_udf=block_udf,
        target_max_block_size=ctx.target_max_block_size,
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
            table = pa.Table.from_batches([batch], schema=schema)
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


def _fetch_metadata_remotely(
    pieces: List["pyarrow._dataset.ParquetFileFragment"],
) -> List[ObjectRef["pyarrow.parquet.FileMetaData"]]:

    remote_fetch_metadata = cached_remote_fn(_fetch_metadata_serialization_wrapper)
    metas = []
    parallelism = min(len(pieces) // PIECES_PER_META_FETCH, 100)
    meta_fetch_bar = ProgressBar("Metadata Fetch Progress", total=parallelism)
    for pcs in np.array_split(pieces, parallelism):
        if len(pcs) == 0:
            continue
        metas.append(remote_fetch_metadata.remote([_SerializedPiece(p) for p in pcs]))
    metas = meta_fetch_bar.fetch_until_complete(metas)
    return list(itertools.chain.from_iterable(metas))


def _fetch_metadata_serialization_wrapper(
    pieces: str,
) -> List["pyarrow.parquet.FileMetaData"]:

    pieces: List[
        "pyarrow._dataset.ParquetFileFragment"
    ] = _deserialize_pieces_with_retry(pieces)

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


def _sample_piece(
    reader_args,
    columns,
    schema,
    file_piece: _SerializedPiece,
) -> float:
    # Sample the first rows batch from file piece `serialized_piece`.
    # Return the encoding ratio calculated from the sampled rows.
    piece = _deserialize_pieces_with_retry([file_piece])[0]

    # Only sample the first row group.
    piece = piece.subset(row_group_ids=[0])
    batch_size = min(piece.metadata.num_rows, PARQUET_ENCODING_RATIO_ESTIMATE_NUM_ROWS)
    batches = piece.to_batches(
        columns=columns,
        schema=schema,
        batch_size=batch_size,
        **reader_args,
    )
    # Use first batch in-memory size as ratio estimation.
    try:
        batch = next(batches)
        in_memory_size = batch.nbytes / batch.num_rows
        metadata = piece.metadata
        total_size = 0
        for idx in range(metadata.num_row_groups):
            total_size += metadata.row_group(idx).total_byte_size
        file_size = total_size / metadata.num_rows
        ratio = in_memory_size / file_size
    except StopIteration:
        ratio = PARQUET_ENCODING_RATIO_ESTIMATE_LOWER_BOUND
    logger.debug(
        f"Estimated Parquet encoding ratio is {ratio} for piece {piece} "
        f"with batch size {batch_size}."
    )
    return ratio
