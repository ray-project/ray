import logging
import math
import os
import warnings
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import numpy as np
from packaging.version import parse as parse_version

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import (
    RetryingPyFileSystem,
    _check_pyarrow_version,
    _is_local_scheme,
    iterate_with_retry,
)
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.file_meta_provider import (
    FileMetadataProvider,
    _handle_read_os_error,
    _list_files,
)
from ray.data.datasource.partitioning import (
    PartitionDataType,
    Partitioning,
    PathPartitionFilter,
    PathPartitionParser,
)
from ray.data.datasource.path_util import (
    _has_file_extension,
    _resolve_paths_and_filesystem,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    import pyarrow
    from pyarrow import parquet as pq
    from pyarrow.dataset import ParquetFileFragment


logger = logging.getLogger(__name__)


MIN_PYARROW_TO_BATCHES_READAHEAD = parse_version("10.0.0")


# The `num_cpus` for each metadata prefetching task.
# Default to 0.5 instead of 1 because it is cheaper than normal read task.
NUM_CPUS_FOR_META_FETCH_TASK = 0.5

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
DEFAULT_PARQUET_READER_ROW_BATCH_SIZE = 10_000
FILE_READING_RETRY = 8

# The default size multiplier for reading Parquet data source in Arrow.
# Parquet data format is encoded with various encoding techniques (such as
# dictionary, RLE, delta), so Arrow in-memory representation uses much more memory
# compared to Parquet encoded representation. Parquet file statistics only record
# encoded (i.e. uncompressed) data size information.
#
# To estimate real-time in-memory data size, Datasets will try to estimate the
# correct inflation ratio from Parquet to Arrow, using this constant as the default
# value for safety. See https://github.com/ray-project/ray/pull/26516 for more context.
PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT = 5

# The lower bound size to estimate Parquet encoding ratio.
PARQUET_ENCODING_RATIO_ESTIMATE_LOWER_BOUND = 1

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


_BATCH_SIZE_PRESERVING_STUB_COL_NAME = "__bsp_stub"


class _ParquetFragment:
    """This wrapper class is created to avoid utilizing `ParquetFileFragment` original
    serialization protocol that actually does network RPCs during serialization
    (to fetch actual parquet metadata)"""

    def __init__(self, f: "ParquetFileFragment", file_size: int):
        self._fragment = f
        self._file_size = file_size

    @property
    def file_size(self) -> int:
        return self._file_size

    @property
    def original(self) -> "ParquetFileFragment":
        return self._fragment

    def __reduce__(self):
        return _ParquetFragment.make_fragment, (
            self._fragment.format,
            self._fragment.path,
            self._fragment.filesystem,
            self._fragment.partition_expression,
            self._file_size,
        )

    @staticmethod
    def make_fragment(format, path, filesystem, partition_expression, file_size):
        fragment = format.make_fragment(path, filesystem, partition_expression)
        return _ParquetFragment(fragment, file_size)


def check_for_legacy_tensor_type(schema):
    """Check for the legacy tensor extension type and raise an error if found.

    Ray Data uses an extension type to represent tensors in Arrow tables. Previously,
    the extension type extended `PyExtensionType`. However, this base type can expose
    users to arbitrary code execution. To prevent this, we don't load the type by
    default.
    """
    import pyarrow as pa

    for name, type in zip(schema.names, schema.types):
        if isinstance(type, pa.UnknownExtensionType) and isinstance(
            type, pa.PyExtensionType
        ):
            raise RuntimeError(
                f"Ray Data couldn't infer the type of column '{name}'. This might mean "
                "you're trying to read data written with an older version of Ray. "
                "Reading data written with older versions of Ray might expose you to "
                "arbitrary code execution. To try reading the data anyway, set "
                "`RAY_DATA_AUTOLOAD_PYEXTENSIONTYPE=1` on *all* nodes."
                "To learn more, see https://github.com/ray-project/ray/issues/41314."
            )


class ParquetDatasource(Datasource):
    """Parquet datasource, for reading and writing Parquet files.

    The primary difference from ParquetBulkDatasource is that this uses
    PyArrow's `ParquetDataset` abstraction for dataset reads, and thus offers
    automatic Arrow dataset schema inference and row count collection at the
    cost of some potential performance and/or compatibility penalties.
    """

    _FUTURE_FILE_EXTENSIONS = ["parquet"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        *,
        columns: Optional[List[str]] = None,
        dataset_kwargs: Optional[Dict[str, Any]] = None,
        to_batch_kwargs: Optional[Dict[str, Any]] = None,
        _block_udf: Optional[Callable[[Block], Block]] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None,
        meta_provider: Optional[FileMetadataProvider] = None,
        partition_filter: PathPartitionFilter = None,
        partitioning: Optional[Partitioning] = Partitioning("hive"),
        shuffle: Union[Literal["files"], None] = None,
        include_paths: bool = False,
        file_extensions: Optional[List[str]] = None,
    ):
        _check_pyarrow_version()

        self._supports_distributed_reads = not _is_local_scheme(paths)
        if not self._supports_distributed_reads and ray.util.client.ray.is_connected():
            raise ValueError(
                "Because you're using Ray Client, read tasks scheduled on the Ray "
                "cluster can't access your local files. To fix this issue, store "
                "files in cloud storage or a distributed filesystem like NFS."
            )

        self._local_scheduling = None
        if not self._supports_distributed_reads:
            from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

            self._local_scheduling = NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(), soft=False
            )

        paths, self._filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        filesystem = RetryingPyFileSystem.wrap(
            self._filesystem,
            retryable_errors=DataContext.get_current().retried_io_errors,
        )

        listed_files = _list_files(
            paths,
            filesystem,
            partition_filter=partition_filter,
            file_extensions=file_extensions,
        )

        if listed_files:
            paths, file_sizes = zip(*listed_files)
        else:
            paths, file_sizes = [], []

        if dataset_kwargs is not None:
            logger.warning(
                "Please note that `ParquetDatasource.__init__`s `dataset_kwargs` "
                "is a deprecated parameter and will be removed in the future."
            )
        else:
            dataset_kwargs = {}

        if "partitioning" in dataset_kwargs:
            raise ValueError(
                "The 'partitioning' parameter isn't supported in 'dataset_kwargs'. "
                "Use the top-level 'partitioning' parameter instead."
            )

        # This datasource manually adds partition data at the Ray Data-level. To avoid
        # duplicating the partition data, we disable PyArrow's partitioning.
        dataset_kwargs["partitioning"] = None

        # NOTE: ParquetDataset only accepts list of paths, hence we need to convert
        #       it to a list
        pq_ds = get_parquet_dataset(list(paths), filesystem, dataset_kwargs)

        # `read_schema` is the schema object that will be used to perform
        # read operations.
        # It should be None, unless user has specified the schema or columns.
        # We don't use the inferred schema for read, because we infer the schema based
        # on the first file. Thus, files with different schemas will end up producing
        # blocks with wrong schema.
        # See https://github.com/ray-project/ray/issues/47960 for more context.
        read_schema = schema
        inferred_schema = _infer_schema(
            pq_ds, schema, columns, partitioning, _block_udf
        )

        # Users can pass both data columns and partition columns in the 'columns'
        # argument. To prevent PyArrow from complaining about missing columns, we
        # separate the partition columns from the data columns. When we read the
        # fragments, we pass the data columns to PyArrow and add the partition
        # columns manually.
        data_columns, partition_columns = None, None
        if columns is not None:
            data_columns, partition_columns = _infer_data_and_partition_columns(
                columns, pq_ds.fragments[0], partitioning
            )

        if to_batch_kwargs is None:
            to_batch_kwargs = {}

        # NOTE: Store the custom serialized `ParquetFileFragment` to avoid unexpected
        # network calls when `_ParquetDatasourceReader` is serialized. See
        # `_SerializedFragment()` implementation for more details.
        self._pq_fragments = [
            _ParquetFragment(fragment, file_size)
            for fragment, file_size in zip(pq_ds.fragments, file_sizes)
        ]
        self._pq_paths = [p.path for p in pq_ds.fragments]
        self._block_udf = _block_udf
        self._to_batches_kwargs = to_batch_kwargs
        self._data_columns = data_columns
        self._partition_columns = partition_columns
        self._read_schema = read_schema
        self._inferred_schema = inferred_schema
        self._file_metadata_shuffler = None
        self._include_paths = include_paths
        self._partitioning = partitioning

        if shuffle == "files":
            self._file_metadata_shuffler = np.random.default_rng()
        elif isinstance(shuffle, FileShuffleConfig):
            self._file_metadata_shuffler = np.random.default_rng(shuffle.seed)

        # Sample small number of parquet files to estimate
        #   - Encoding ratio: ratio of file size on disk to approximate expected
        #     size of the corresponding block in memory
        #   - Default batch-size: number of rows to be read from a file at a time,
        #     used to limit amount of memory pressure
        sampled_fragments = _sample_fragments(
            self._pq_fragments,
        )

        sampled_file_infos = _fetch_file_infos(
            sampled_fragments,
            columns=self._data_columns,
            schema=schema,
            local_scheduling=self._local_scheduling,
        )

        self._encoding_ratio = _estimate_files_encoding_ratio(
            sampled_fragments,
            sampled_file_infos,
        )

        self._default_batch_size = _estimate_reader_batch_size(
            sampled_file_infos, DataContext.get_current().target_max_block_size
        )

        if file_extensions is None:
            for path in self._pq_paths:
                if not _has_file_extension(
                    path, self._FUTURE_FILE_EXTENSIONS
                ) and log_once("read_parquet_file_extensions_future_warning"):
                    emit_file_extensions_future_warning(self._FUTURE_FILE_EXTENSIONS)
                    break

    def estimate_inmemory_data_size(self) -> int:
        return self._estimate_in_mem_size(self._pq_fragments)

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        # NOTE: We override the base class FileBasedDatasource.get_read_tasks()
        # method in order to leverage pyarrow's ParquetDataset abstraction,
        # which simplifies partitioning logic. We still use
        # FileBasedDatasource's write side, however.
        if self._file_metadata_shuffler is not None:
            files_metadata = list(zip(self._pq_fragments, self._pq_paths))
            shuffled_files_metadata = [
                files_metadata[i]
                for i in self._file_metadata_shuffler.permutation(len(files_metadata))
            ]
            pq_fragments, pq_paths = list(map(list, zip(*shuffled_files_metadata)))
        else:
            pq_fragments, pq_paths = (
                self._pq_fragments,
                self._pq_paths,
            )

        read_tasks = []
        for fragments, paths in zip(
            np.array_split(pq_fragments, parallelism),
            np.array_split(pq_paths, parallelism),
        ):
            if len(fragments) <= 0:
                continue

            meta = BlockMetadata(
                num_rows=None,
                size_bytes=self._estimate_in_mem_size(fragments),
                input_files=paths,
                exec_stats=None,
            )

            (
                block_udf,
                to_batches_kwargs,
                default_read_batch_size_rows,
                data_columns,
                partition_columns,
                read_schema,
                include_paths,
                partitioning,
            ) = (
                self._block_udf,
                self._to_batches_kwargs,
                self._default_batch_size,
                self._data_columns,
                self._partition_columns,
                self._read_schema,
                self._include_paths,
                self._partitioning,
            )

            read_tasks.append(
                ReadTask(
                    lambda f=fragments: read_fragments(
                        block_udf,
                        to_batches_kwargs,
                        default_read_batch_size_rows,
                        data_columns,
                        partition_columns,
                        read_schema,
                        f,
                        include_paths,
                        partitioning,
                    ),
                    meta,
                    schema=self._inferred_schema,
                )
            )

        return read_tasks

    def get_name(self):
        """Return a human-readable name for this datasource.

        This will be used as the names of the read tasks.
        """
        return "Parquet"

    @property
    def supports_distributed_reads(self) -> bool:
        return self._supports_distributed_reads

    def _estimate_in_mem_size(self, fragments: List[_ParquetFragment]) -> int:
        in_mem_size = sum([f.file_size for f in fragments]) * self._encoding_ratio

        return round(in_mem_size)


def read_fragments(
    block_udf,
    to_batches_kwargs,
    default_read_batch_size_rows,
    data_columns,
    partition_columns,
    schema,
    fragments: List[_ParquetFragment],
    include_paths: bool,
    partitioning: Partitioning,
) -> Iterator["pyarrow.Table"]:
    # This import is necessary to load the tensor extension type.
    from ray.data.extensions.tensor_extension import ArrowTensorType  # noqa

    # Ensure that we're reading at least one dataset fragment.
    assert len(fragments) > 0

    logger.debug(f"Reading {len(fragments)} parquet fragments")
    for fragment in fragments:
        # S3 can raise transient errors during iteration, and PyArrow doesn't expose a
        # way to retry specific batches.
        ctx = ray.data.DataContext.get_current()
        for table in iterate_with_retry(
            lambda: _read_batches_from(
                fragment.original,
                schema=schema,
                data_columns=data_columns,
                partition_columns=partition_columns,
                partitioning=partitioning,
                include_path=include_paths,
                batch_size=default_read_batch_size_rows,
                to_batches_kwargs=to_batches_kwargs,
            ),
            "reading batches",
            match=ctx.retried_io_errors,
        ):
            # If the table is empty, drop it.
            if table.num_rows > 0:
                if block_udf is not None:
                    yield block_udf(table)
                else:
                    yield table


def _read_batches_from(
    fragment: "ParquetFileFragment",
    *,
    schema: "pyarrow.Schema",
    data_columns: Optional[List[str]],
    partition_columns: Optional[List[str]],
    partitioning: Partitioning,
    filter_expr: Optional["pyarrow.dataset.Expression"] = None,
    batch_size: Optional[int] = None,
    include_path: bool = False,
    use_threads: bool = False,
    to_batches_kwargs: Optional[Dict[str, Any]] = None,
) -> Iterable["pyarrow.Table"]:
    """Get an iterable of batches from a parquet fragment."""

    import pyarrow as pa

    # Copy to avoid modifying passed in arg
    to_batches_kwargs = dict(to_batches_kwargs or {})

    # NOTE: Passed in kwargs overrides always take precedence
    # TODO deprecate to_batches_kwargs
    use_threads = to_batches_kwargs.pop("use_threads", use_threads)
    filter_expr = to_batches_kwargs.pop("filter", filter_expr)
    # NOTE: Arrow's ``to_batches`` expects ``batch_size`` as an int
    if batch_size is not None:
        to_batches_kwargs.setdefault("batch_size", batch_size)

    partition_col_values = _parse_partition_column_values(
        fragment, partition_columns, partitioning
    )

    try:
        for batch in fragment.to_batches(
            columns=data_columns,
            filter=filter_expr,
            schema=schema,
            use_threads=use_threads,
            **to_batches_kwargs,
        ):
            table = pa.Table.from_batches([batch])

            if include_path:
                table = ArrowBlockAccessor.for_block(table).fill_column(
                    "path", fragment.path
                )

            if partition_col_values:
                table = _add_partitions_to_table(partition_col_values, table)

            # ``ParquetFileFragment.to_batches`` returns ``RecordBatch``,
            # which could have empty projection (ie ``num_columns`` == 0)
            # while having non-empty rows (ie ``num_rows`` > 0), which
            # could occur when list of requested columns is empty.
            #
            # However, when ``RecordBatches`` are concatenated using
            # ``pyarrow.concat_tables`` it will return a single ``Table``
            # with 0 columns and therefore 0 rows (since ``Table``s number of
            # rows is determined as the length of its columns).
            #
            # To avoid running into this pitfall, we introduce a stub column
            # holding just nulls to maintain invariance of the number of rows.
            #
            # NOTE: There's no impact from this as the binary size of the
            #       extra column is basically 0
            if table.num_columns == 0 and table.num_rows > 0:
                table = table.append_column(
                    _BATCH_SIZE_PRESERVING_STUB_COL_NAME, pa.nulls(table.num_rows)
                )

            yield table

    except pa.lib.ArrowInvalid as e:
        error_message = str(e)
        if "No match for FieldRef.Name" in error_message and filter_expr is not None:
            filename = os.path.basename(fragment.path)
            file_columns = set(fragment.physical_schema.names)
            raise RuntimeError(
                f"Filter expression: '{filter_expr}' failed on parquet "
                f"file: '{filename}' with columns: {file_columns}"
            )
        raise


def _parse_partition_column_values(
    fragment: "ParquetFileFragment",
    partition_columns: Optional[List[str]],
    partitioning: Partitioning,
):
    partitions = {}

    if partitioning is not None:
        parse = PathPartitionParser(partitioning)
        partitions = parse(fragment.path)

    # Filter out partitions that aren't in the user-specified columns list.
    if partition_columns is not None:
        partitions = {
            field_name: value
            for field_name, value in partitions.items()
            if field_name in partition_columns
        }

    return partitions


def _fetch_parquet_file_info(
    fragment: _ParquetFragment,
    *,
    columns: Optional[List[str]],
    schema: Optional["pyarrow.Schema"],
) -> Optional["_ParquetFileInfo"]:
    # If the fragment has no row groups, it's an empty or metadata-only file.
    # Skip it by returning empty sample info.
    #
    # NOTE: Accessing `ParquetFileFragment.metadata` does fetch a parquet footer
    #       from storage
    metadata = fragment.original.metadata

    if metadata.num_row_groups == 0:
        return None

    # Only sample the first row group.
    row_group_fragment = fragment.original.subset(row_group_ids=[0])
    batch_size = max(
        min(
            row_group_fragment.metadata.num_rows,
            PARQUET_ENCODING_RATIO_ESTIMATE_NUM_ROWS,
        ),
        1,
    )

    to_batches_kwargs = {}

    if get_pyarrow_version() >= MIN_PYARROW_TO_BATCHES_READAHEAD:
        # Limit prefetching to just 1 batch
        to_batches_kwargs["batch_readahead"] = 1

    batches_iter = row_group_fragment.to_batches(
        columns=columns,
        schema=schema,
        batch_size=batch_size,
        **to_batches_kwargs,
    )

    avg_row_size: Optional[int] = None
    # Use first batch non-empty batch to estimate the avg size of the
    # row in-memory
    for batch in batches_iter:
        if batch.num_rows > 0:
            avg_row_size = math.ceil(batch.nbytes / batch.num_rows)
            break

    return _ParquetFileInfo(
        avg_row_in_mem_bytes=avg_row_size,
        metadata=metadata,
    )


@dataclass
class _ParquetFileInfo:
    # Estimated avg byte size of a row (in-memory)
    avg_row_in_mem_bytes: Optional[int]
    # Corresponding file metadata
    metadata: "pyarrow._parquet.FileMetaData"

    def estimate_in_memory_bytes(self) -> Optional[int]:
        if self.avg_row_in_mem_bytes is None:
            return None

        return self.avg_row_in_mem_bytes * self.metadata.num_rows


def _estimate_files_encoding_ratio(
    fragments: List[_ParquetFragment],
    file_infos: List[_ParquetFileInfo],
) -> float:
    """Return an estimate of the Parquet files encoding ratio.

    To avoid OOMs, it is safer to return an over-estimate than an underestimate.
    """
    if not DataContext.get_current().decoding_size_estimation:
        return PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT

    assert len(file_infos) == len(fragments)

    # Estimate size of the rows in a file in memory
    estimated_in_mem_size_arr = [
        fi.estimate_in_memory_bytes() if fi is not None else None for fi in file_infos
    ]

    file_size_arr = [f.file_size for f in fragments]

    estimated_encoding_ratios = [
        float(in_mem_size) / file_size
        for in_mem_size, file_size in zip(estimated_in_mem_size_arr, file_size_arr)
        if file_size > 0 and in_mem_size is not None
    ]

    # Return default estimate of 5 if all sampled files turned out to be empty
    if not estimated_encoding_ratios:
        return PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT

    estimated_ratio = np.mean(estimated_encoding_ratios)

    logger.info(f"Estimated parquet encoding ratio is {estimated_ratio:.3f}.")

    return max(estimated_ratio, PARQUET_ENCODING_RATIO_ESTIMATE_LOWER_BOUND)


def _fetch_file_infos(
    sampled_fragments: List[_ParquetFragment],
    *,
    columns: Optional[List[str]],
    schema: Optional["pyarrow.Schema"],
    local_scheduling: Optional[bool],
) -> List[Optional[_ParquetFileInfo]]:
    fetc_file_info = cached_remote_fn(_fetch_parquet_file_info)
    futures = []

    for fragment in sampled_fragments:
        # Sample the first rows batch in i-th file.
        # Use SPREAD scheduling strategy to avoid packing many sampling tasks on
        # same machine to cause OOM issue, as sampling can be memory-intensive.
        futures.append(
            fetc_file_info.options(
                scheduling_strategy=local_scheduling
                or DataContext.get_current().scheduling_strategy,
                # Retry in case of transient errors during sampling.
                retry_exceptions=[OSError],
            ).remote(
                fragment,
                columns=columns,
                schema=schema,
            )
        )

    sample_bar = ProgressBar("Parquet dataset sampling", len(futures), unit="file")
    file_infos = sample_bar.fetch_until_complete(futures)
    sample_bar.close()

    return file_infos


def _estimate_reader_batch_size(
    file_infos: List[Optional[_ParquetFileInfo]], target_block_size: Optional[int]
) -> Optional[int]:
    if target_block_size is None:
        return None

    avg_num_rows_per_block = [
        target_block_size / fi.avg_row_in_mem_bytes
        for fi in file_infos
        if (
            fi is not None
            and fi.avg_row_in_mem_bytes is not None
            and fi.avg_row_in_mem_bytes > 0
        )
    ]

    if not avg_num_rows_per_block:
        return DEFAULT_PARQUET_READER_ROW_BATCH_SIZE

    estimated_batch_size: int = max(math.ceil(np.mean(avg_num_rows_per_block)), 1)

    logger.info(f"Estimated parquet reader batch size at {estimated_batch_size} rows")

    return estimated_batch_size


def get_parquet_dataset(paths, filesystem, dataset_kwargs):
    import pyarrow.parquet as pq

    # If you pass a list containing a single directory path to `ParquetDataset`, PyArrow
    # errors with 'IsADirectoryError: Path ... points to a directory, but only file
    # paths are supported'. To avoid this, we pass the directory path directly.
    if len(paths) == 1:
        paths = paths[0]

    try:
        dataset = pq.ParquetDataset(
            paths,
            **dataset_kwargs,
            filesystem=filesystem,
        )
    except OSError as e:
        _handle_read_os_error(e, paths)

    return dataset


def _sample_fragments(
    fragments: List[_ParquetFragment],
) -> List[_ParquetFragment]:
    num_files = len(fragments)
    num_samples = int(num_files * PARQUET_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO)
    min_num_samples = min(PARQUET_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES, num_files)
    max_num_samples = min(PARQUET_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES, num_files)
    num_samples = max(min(num_samples, max_num_samples), min_num_samples)

    # Evenly distributed to choose which file to sample, to avoid biased prediction
    # if data is skewed.
    return [
        fragments[idx]
        for idx in np.linspace(0, num_files - 1, num_samples).astype(int).tolist()
    ]


def _add_partitions_to_table(
    partition_col_values: Dict[str, PartitionDataType], table: "pyarrow.Table"
) -> "pyarrow.Table":

    for partition_col, value in partition_col_values.items():
        field_index = table.schema.get_field_index(partition_col)
        if field_index == -1:
            table = BlockAccessor.for_block(table).fill_column(partition_col, value)
        elif log_once(f"duplicate_partition_field_{partition_col}"):
            logger.warning(
                f"The partition field '{partition_col}' also exists in the Parquet "
                f"file. Ray Data will default to using the value in the Parquet file."
            )

    return table


def _add_partition_fields_to_schema(
    partitioning: Partitioning,
    schema: "pyarrow.Schema",
    parquet_dataset: "pyarrow.dataset.Dataset",
) -> "pyarrow.Schema":
    """Return a new schema with partition fields added.

    This function infers the partition fields from the first file path in the dataset.
    """
    import pyarrow as pa

    # If the dataset is empty, we can't infer the partitioning.
    if len(parquet_dataset.fragments) == 0:
        return schema

    # If the dataset isn't partitioned, we don't need to add any fields.
    if partitioning is None:
        return schema

    first_path = parquet_dataset.fragments[0].path
    parse = PathPartitionParser(partitioning)
    partitions = parse(first_path)
    for field_name in partitions:
        if field_name in partitioning.field_types:
            field_type = pa.from_numpy_dtype(partitioning.field_types[field_name])
        else:
            field_type = pa.string()
        if field_name not in schema.names:
            # Without this check, we would add the same partition field multiple times,
            # which silently fails when asking for `pa.field()`.
            schema = schema.append(pa.field(field_name, field_type))

    return schema


def emit_file_extensions_future_warning(future_file_extensions: List[str]):
    warnings.warn(
        "The default `file_extensions` for `read_parquet` will change "
        f"from `None` to {future_file_extensions} after Ray 2.43, and your dataset "
        "contains files that don't match the new `file_extensions`. To maintain "
        "backwards compatibility, set `file_extensions=None` explicitly.",
        FutureWarning,
    )


def _infer_schema(
    parquet_dataset: "pq.ParquetDataset",
    schema: "pyarrow.Schema",
    columns: Optional[List[str]],
    partitioning,
    _block_udf,
) -> "pyarrow.Schema":
    """Infer the schema of read data using the user-specified parameters."""
    import pyarrow as pa

    inferred_schema = schema

    if schema is None:
        inferred_schema = parquet_dataset.schema
        inferred_schema = _add_partition_fields_to_schema(
            partitioning, inferred_schema, parquet_dataset
        )

    if columns is not None:
        inferred_schema = pa.schema(
            [inferred_schema.field(column) for column in columns],
            inferred_schema.metadata,
        )

    if _block_udf is not None:
        # Try to infer dataset schema by passing dummy table through UDF.
        dummy_table = inferred_schema.empty_table()
        try:
            inferred_schema = _block_udf(dummy_table).schema.with_metadata(
                inferred_schema.metadata
            )
        except Exception:
            logger.debug(
                "Failed to infer schema of dataset by passing dummy table "
                "through UDF due to the following exception:",
                exc_info=True,
            )

    check_for_legacy_tensor_type(inferred_schema)
    return inferred_schema


def _infer_data_and_partition_columns(
    user_specified_columns: List[str],
    fragment: "ParquetFileFragment",
    partitioning: Optional[Partitioning],
) -> Tuple[List[str], List[str]]:
    """Infer which columns are in the files and which columns are partition columns.

    This function uses the schema and path of the first file to infer what columns
    represent.

    Args:
        user_specified_columns: A list of column names that the user specified.
        fragment: The first fragment in the dataset.
        partitioning: The partitioning scheme used to partition the data.

    Returns:
        A tuple of lists of column names. The first list contains the columns that are
        in the file, and the second list contains the columns that are partition
        columns.
    """
    data_columns = [
        column
        for column in user_specified_columns
        if column in fragment.physical_schema.names
    ]
    if partitioning is not None:
        parse = PathPartitionParser(partitioning)
        partitions = parse(fragment.path)
        partition_columns = [
            column for column in user_specified_columns if column in partitions
        ]
    else:
        partition_columns = []

    return data_columns, partition_columns
