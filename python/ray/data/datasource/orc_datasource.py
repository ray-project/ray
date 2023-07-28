import logging
from typing import TYPE_CHECKING, Callable, Iterator, List, Optional, Union
from functools import cached_property
from dataclasses import dataclass

import numpy as np
from pyarrow.orc import ORCFile

from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block
from ray.data.context import DataContext
from ray.data.datasource.datasource import Reader, ReadTask
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem
from ray.data.datasource.file_meta_provider import (
    DefaultORCMetadataProvider,
    ORCMetadataProvider,
    _handle_read_os_error,
)
from ray.data.datasource.orc_base_datasource import ORCBaseDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


logger = logging.getLogger(__name__)

PIECES_PER_META_FETCH = 6
PARALLELIZE_META_FETCH_THRESHOLD = 24

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
ORC_READER_ROW_BATCH_SIZE = 100000
FILE_READING_RETRY = 8

# The default size multiplier for reading ORC data source in Arrow.
# ORC data format is encoded with various encoding techniques (such as
# dictionary, RLE, delta), so Arrow in-memory representation uses much more memory
# compared to ORC encoded representation. ORC file statistics only record
# encoded (i.e. uncompressed) data size information.
#
# To estimate real-time in-memory data size, Datasets will try to estimate the
# correct inflation ratio from ORC to Arrow, using this constant as the default
# value for safety. See https://github.com/ray-project/ray/pull/26516 for more context.
ORC_ENCODING_RATIO_ESTIMATE_DEFAULT = 5

# The lower bound size to estimate ORC encoding ratio.
ORC_ENCODING_RATIO_ESTIMATE_LOWER_BOUND = 2

# The percentage of files (1% by default) to be sampled from the dataset to estimate
# ORC encoding ratio.
ORC_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO = 0.01

# The minimal and maximal number of file samples to take from the dataset to estimate
# ORC encoding ratio.
# This is to restrict `ORC_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO` within the
# proper boundary.
ORC_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES = 2
ORC_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES = 10

# The number of rows to read from each file for sampling. Try to keep it low to avoid
# reading too much data into memory.
ORC_ENCODING_RATIO_ESTIMATE_NUM_ROWS = 1024


class _ORCDataset:
    def __init__(self, paths, filesystem):
        self.paths = paths
        self.filesystem = filesystem

        schema = None
        pieces = []
        for path in paths:
            if schema is None:
                with filesystem.open_input_file(path) as f:
                    o = ORCFile(f)
                    schema = o.schema
            pieces.append(_ORCFragment(path, filesystem))
        self.schema = schema
        self.pieces = pieces


class _ORCFragment:
    def __init__(self, path, filesystem):
        self.path = path
        self.filesystem = filesystem

    @cached_property
    def metadata(self):
        with self.filesystem.open_input_file(self.path) as f:
            o = ORCFile(f)
            return _ORCFragmentMetadata(
                num_rows=o.nrows,
                total_byte_size=o.file_length,
            )

    def to_batches(
        self,
        schema,
        stripes=None,
        columns=None,
        **reader_args,
    ):
        """Construct a list of RecordBatch objects.
        Each ORC stripe will corresonpond to a single RecordBatch.
        """
        with self.filesystem.open_input_file(self.path) as f:
            o = ORCFile(f)
            _stripes = range(o.nstripes) if stripes is None else stripes
            for stripe in _stripes:
                yield o.read_stripe(stripe, columns, **reader_args)


@dataclass
class _ORCFragmentMetadata:
    num_rows: int
    total_byte_size: int


@PublicAPI
class ORCDatasource(ORCBaseDatasource):
    """ORC datasource, for reading and writing ORC files.

    The primary difference from ORCBaseDatasource is that this uses
    PyArrow's `ORCFile` abstraction for dataset reads, and thus offers
    automatic Arrow dataset schema inference and row count collection at the
    cost of some potential performance and/or compatibility penalties.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import ORCDatasource
        >>> source = ORCDatasource() # doctest: +SKIP
        >>> ray.data.read_datasource( # doctest: +SKIP
        ...     source, paths="/path/to/dir").take()
        [{"a": 1, "b": "foo"}, ...]
    """

    def get_name(self):
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        Note: overrides the base `ORCBaseDatasource` method.
        """
        return "ORC"

    def create_reader(self, **kwargs):
        return _ORCDatasourceReader(**kwargs)


class _ORCDatasourceReader(Reader):
    def __init__(
        self,
        paths: Union[str, List[str]],
        local_uri: bool = False,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        columns: Optional[List[str]] = None,
        schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None,
        meta_provider: ORCMetadataProvider = DefaultORCMetadataProvider(),
        _block_udf: Optional[Callable[[Block], Block]] = None,
        **reader_args,
    ):
        _check_pyarrow_version()
        import pyarrow as pa

        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        paths = meta_provider.expand_paths(
            paths, filesystem
        )

        self._local_scheduling = None
        if local_uri:
            import ray
            from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

            self._local_scheduling = NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(), soft=False
            )

        dataset_kwargs = reader_args.pop("dataset_kwargs", {})
        try:
            orc_ds = _ORCDataset(
                paths, **dataset_kwargs, filesystem=filesystem
            )
        except OSError as e:
            _handle_read_os_error(e, paths)
        if schema is None:
            schema = orc_ds.schema
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
            prefetch_remote_args = {}
            if self._local_scheduling:
                prefetch_remote_args["scheduling_strategy"] = self._local_scheduling
            self._metadata = (
                meta_provider.prefetch_file_metadata(
                    orc_ds.pieces, **prefetch_remote_args
                )
                or []
            )
        except OSError as e:
            _handle_read_os_error(e, paths)
        self._orc_ds = orc_ds
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
            total_size += file_metadata.total_byte_size
        return total_size * self._encoding_ratio

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        # NOTE: We override the base class FileBasedDatasource.get_read_tasks()
        # method in order to leverage pyarrow's ORCFile abstraction,
        # We still use FileBasedDatasource's write side (do_write), however.
        read_tasks = []
        for pieces, metadata in zip(
            np.array_split(self._orc_ds.pieces, parallelism),
            np.array_split(self._metadata, parallelism),
        ):
            if len(pieces) <= 0:
                continue

            input_files = [p.path for p in pieces]
            meta = self._meta_provider(
                input_files,
                self._inferred_schema,
                pieces=pieces,
                prefetched_metadata=metadata,
            )
            # If there is a filter operation, reset the calculated row count,
            # since the resulting row count is unknown.
            if self._reader_args.get("filter") is not None:
                meta.num_rows = None

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
                    lambda p=pieces: _read_pieces(
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
        """Return an estimate of the ORC files encoding ratio.

        To avoid OOMs, it is safer to return an over-estimate than an underestimate.
        """
        if not DataContext.get_current().decoding_size_estimation:
            return ORC_ENCODING_RATIO_ESTIMATE_DEFAULT

        # Sample a few rows from ORC files to estimate the encoding ratio.
        # Launch tasks to sample multiple files remotely in parallel.
        # Evenly distributed to sample N rows in i-th row group in i-th file.
        # TODO(ekl/cheng) take into account column pruning.
        num_files = len(self._orc_ds.pieces)
        num_samples = int(num_files * ORC_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO)
        min_num_samples = min(
            ORC_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES, num_files
        )
        max_num_samples = min(
            ORC_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES, num_files
        )
        num_samples = max(min(num_samples, max_num_samples), min_num_samples)

        # Evenly distributed to choose which file to sample, to avoid biased prediction
        # if data is skewed.
        file_samples = [
            self._orc_ds.pieces[idx]
            for idx in np.linspace(0, num_files - 1, num_samples).astype(int).tolist()
        ]

        sample_piece = cached_remote_fn(_sample_piece)
        futures = []
        scheduling = self._local_scheduling or "SPREAD"
        for sample in file_samples:
            # Sample the first rows batch in i-th file.
            # Use SPREAD scheduling strategy to avoid packing many sampling tasks on
            # same machine to cause OOM issue, as sampling can be memory-intensive.
            futures.append(
                sample_piece.options(scheduling_strategy=scheduling).remote(
                    self._reader_args,
                    self._columns,
                    self._schema,
                    sample,
                )
            )
        sample_bar = ProgressBar("ORC Files Sample", len(futures))
        sample_ratios = sample_bar.fetch_until_complete(futures)
        sample_bar.close()
        ratio = np.mean(sample_ratios)
        logger.debug(f"Estimated ORC encoding ratio from sampling is {ratio}.")
        return max(ratio, ORC_ENCODING_RATIO_ESTIMATE_LOWER_BOUND)


def _read_pieces(
    block_udf,
    reader_args,
    columns,
    schema,
    pieces: List[_ORCFragment],
) -> Iterator["pyarrow.Table"]:
    # This import is necessary to load the tensor extension type.
    from ray.data.extensions.tensor_extension import ArrowTensorType  # noqa


    # Ensure that we're reading at least one dataset fragment.
    assert len(pieces) > 0

    import pyarrow as pa

    ctx = DataContext.get_current()
    output_buffer = BlockOutputBuffer(
        block_udf=block_udf,
        target_max_block_size=ctx.target_max_block_size,
    )

    logger.debug(f"Reading {len(pieces)} ORC pieces")
    for piece in pieces:
        batches = piece.to_batches(
            schema=schema,
            columns=columns,
            **reader_args,
        )
        for batch in batches:
            table = pa.Table.from_batches([batch], schema=schema)
            # If the table is empty, drop it.
            if table.num_rows > 0:
                output_buffer.add_block(table)
                if output_buffer.has_next():
                    yield output_buffer.next()
    output_buffer.finalize()
    if output_buffer.has_next():
        yield output_buffer.next()


def _fetch_metadata(
    pieces: List[_ORCFragment],
) -> List[_ORCFragmentMetadata]:
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
    piece: _ORCFragment,
) -> float:
    # Sample the first rows batch from file piece `serialized_piece`.
    # Return the encoding ratio calculated from the sampled rows.

    # Only sample the first row group.
    batches = piece.to_batches(
        schema=schema,
        stripes=[0],
        columns=columns,
        **reader_args,
    )
    # Use first batch in-memory size as ratio estimation.
    try:
        batch = next(batches)
    except StopIteration:
        ratio = ORC_ENCODING_RATIO_ESTIMATE_LOWER_BOUND
    else:
        if batch.num_rows > 0:
            in_memory_size = batch.nbytes / batch.num_rows
            metadata = piece.metadata
            total_size = metadata.total_byte_size
            file_size = total_size / metadata.num_rows
            ratio = in_memory_size / file_size
        else:
            ratio = ORC_ENCODING_RATIO_ESTIMATE_LOWER_BOUND
    logger.debug(
        f"Estimated ORC encoding ratio is {ratio} for piece {piece}"
    )
    return ratio
