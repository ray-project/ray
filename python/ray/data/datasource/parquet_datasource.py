import logging
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Union,
)

import numpy as np
from packaging.version import parse as parse_version

import ray
import ray.cloudpickle as cloudpickle
from ray._private.utils import _get_pyarrow_version
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import (
    _check_pyarrow_version,
    _is_local_scheme,
    call_with_retry,
)
from ray.data.block import Block
from ray.data.context import DataContext
from ray.data.datasource import Datasource
from ray.data.datasource._default_metadata_providers import (
    get_generic_metadata_provider,
)
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.file_meta_provider import _handle_read_os_error
from ray.data.datasource.parquet_meta_provider import ParquetMetadataProvider
from ray.data.datasource.partitioning import PathPartitionFilter
from ray.data.datasource.path_util import (
    _has_file_extension,
    _resolve_paths_and_filesystem,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow
    from pyarrow.dataset import ParquetFileFragment


logger = logging.getLogger(__name__)

# The `num_cpus` for each metadata prefetching task.
# Default to 0.5 instead of 1 because it is cheaper than normal read task.
NUM_CPUS_FOR_META_FETCH_TASK = 0.5

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
PARQUET_READER_ROW_BATCH_SIZE = 10_000
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


@dataclass(frozen=True)
class _SampleInfo:
    actual_bytes_per_row: Optional[int]
    estimated_bytes_per_row: Optional[int]


# TODO(ekl) this is a workaround for a pyarrow serialization bug, where serializing a
# raw pyarrow file fragment causes S3 network calls.
class _SerializedFragment:
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
def _deserialize_fragments(
    serialized_fragments: List[_SerializedFragment],
) -> List["pyarrow._dataset.ParquetFileFragment"]:
    return [p.deserialize() for p in serialized_fragments]


def _check_for_legacy_tensor_type(schema):
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
                "`RAY_DATA_AUTOLOAD_PYEXTENSIONTYPE=1` on all nodes."
                "To learn more, see https://github.com/ray-project/ray/issues/41314."
            )


@PublicAPI
class ParquetDatasource(Datasource):
    """Parquet datasource, for reading and writing Parquet files.

    The primary difference from ParquetBulkDatasource is that this uses
    PyArrow's `ParquetDataset` abstraction for dataset reads, and thus offers
    automatic Arrow dataset schema inference and row count collection at the
    cost of some potential performance and/or compatibility penalties.
    """

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
        meta_provider: ParquetMetadataProvider = ParquetMetadataProvider(),
        partition_filter: PathPartitionFilter = None,
        shuffle: Union[Literal["files"], None] = None,
        include_paths: bool = False,
        file_extensions: Optional[List[str]] = None,
    ):
        _check_pyarrow_version()

        import pyarrow as pa
        import pyarrow.parquet as pq

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

        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)

        # HACK: PyArrow's `ParquetDataset` errors if input paths contain non-parquet
        # files. To avoid this, we expand the input paths with the default metadata
        # provider and then apply the partition filter or file extensions.
        if partition_filter is not None or file_extensions is not None:
            default_meta_provider = get_generic_metadata_provider(file_extensions=None)
            expanded_paths, _ = map(
                list, zip(*default_meta_provider.expand_paths(paths, filesystem))
            )

            paths = list(expanded_paths)
            if partition_filter is not None:
                paths = partition_filter(paths)
            if file_extensions is not None:
                paths = [
                    path for path in paths if _has_file_extension(path, file_extensions)
                ]

            filtered_paths = set(expanded_paths) - set(paths)
            if filtered_paths:
                logger.info(f"Filtered out {len(filtered_paths)} paths")
        else:
            if len(paths) == 1:
                paths = paths[0]

        if dataset_kwargs is None:
            dataset_kwargs = {}

        try:
            # The `use_legacy_dataset` parameter is deprecated in Arrow 15.
            if parse_version(_get_pyarrow_version()) >= parse_version("15.0.0"):
                pq_ds = pq.ParquetDataset(
                    paths,
                    **dataset_kwargs,
                    filesystem=filesystem,
                )
            else:
                pq_ds = pq.ParquetDataset(
                    paths,
                    **dataset_kwargs,
                    filesystem=filesystem,
                    use_legacy_dataset=False,
                )
        except OSError as e:
            _handle_read_os_error(e, paths)
        if schema is None:
            schema = pq_ds.schema
        if columns:
            schema = pa.schema(
                [schema.field(column) for column in columns], schema.metadata
            )

        _check_for_legacy_tensor_type(schema)

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
            prefetch_remote_args["num_cpus"] = NUM_CPUS_FOR_META_FETCH_TASK
            if self._local_scheduling:
                prefetch_remote_args["scheduling_strategy"] = self._local_scheduling
            else:
                # Use the scheduling strategy ("SPREAD" by default) provided in
                # `DataContext``, to spread out prefetch tasks in cluster, avoid
                # AWS S3 throttling error.
                # Note: this is the same scheduling strategy used by read tasks.
                prefetch_remote_args[
                    "scheduling_strategy"
                ] = DataContext.get_current().scheduling_strategy

            self._metadata = (
                meta_provider.prefetch_file_metadata(
                    pq_ds.fragments, **prefetch_remote_args
                )
                or []
            )
        except OSError as e:
            _handle_read_os_error(e, paths)

        if to_batch_kwargs is None:
            to_batch_kwargs = {}

        # NOTE: Store the custom serialized `ParquetFileFragment` to avoid unexpected
        # network calls when `_ParquetDatasourceReader` is serialized. See
        # `_SerializedFragment()` implementation for more details.
        self._pq_fragments = [_SerializedFragment(p) for p in pq_ds.fragments]
        self._pq_paths = [p.path for p in pq_ds.fragments]
        self._meta_provider = meta_provider
        self._inferred_schema = inferred_schema
        self._block_udf = _block_udf
        self._to_batches_kwargs = to_batch_kwargs
        self._columns = columns
        self._schema = schema
        self._file_metadata_shuffler = None
        self._include_paths = include_paths
        if shuffle == "files":
            self._file_metadata_shuffler = np.random.default_rng()

        sample_infos = self._sample_fragments()
        self._encoding_ratio = _estimate_files_encoding_ratio(sample_infos)
        self._default_read_batch_size_rows = _estimate_default_read_batch_size_rows(
            sample_infos
        )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        total_size = 0
        for file_metadata in self._metadata:
            total_size += file_metadata.total_byte_size
        return total_size * self._encoding_ratio

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        # NOTE: We override the base class FileBasedDatasource.get_read_tasks()
        # method in order to leverage pyarrow's ParquetDataset abstraction,
        # which simplifies partitioning logic. We still use
        # FileBasedDatasource's write side, however.
        pq_metadata = self._metadata
        if len(pq_metadata) < len(self._pq_fragments):
            # Pad `pq_metadata` to be same length of `self._pq_fragments`.
            # This can happen when no file metadata being prefetched.
            pq_metadata += [None] * (len(self._pq_fragments) - len(pq_metadata))

        if self._file_metadata_shuffler is not None:
            files_metadata = list(zip(self._pq_fragments, self._pq_paths, pq_metadata))
            shuffled_files_metadata = [
                files_metadata[i]
                for i in self._file_metadata_shuffler.permutation(len(files_metadata))
            ]
            pq_fragments, pq_paths, pq_metadata = list(
                map(list, zip(*shuffled_files_metadata))
            )
        else:
            pq_fragments, pq_paths, pq_metadata = (
                self._pq_fragments,
                self._pq_paths,
                pq_metadata,
            )

        read_tasks = []
        for fragments, paths, metadata in zip(
            np.array_split(pq_fragments, parallelism),
            np.array_split(pq_paths, parallelism),
            np.array_split(pq_metadata, parallelism),
        ):
            if len(fragments) <= 0:
                continue

            meta = self._meta_provider(
                paths,
                self._inferred_schema,
                num_fragments=len(fragments),
                prefetched_metadata=metadata,
            )
            # If there is a filter operation, reset the calculated row count,
            # since the resulting row count is unknown.
            if self._to_batches_kwargs.get("filter") is not None:
                meta.num_rows = None

            if meta.size_bytes is not None:
                meta.size_bytes = int(meta.size_bytes * self._encoding_ratio)

            (
                block_udf,
                to_batches_kwargs,
                default_read_batch_size_rows,
                columns,
                schema,
                include_paths,
            ) = (
                self._block_udf,
                self._to_batches_kwargs,
                self._default_read_batch_size_rows,
                self._columns,
                self._schema,
                self._include_paths,
            )
            read_tasks.append(
                ReadTask(
                    lambda f=fragments: _read_fragments(
                        block_udf,
                        to_batches_kwargs,
                        default_read_batch_size_rows,
                        columns,
                        schema,
                        f,
                        include_paths,
                    ),
                    meta,
                )
            )

        return read_tasks

    def _sample_fragments(self) -> List[_SampleInfo]:
        # Sample a few rows from Parquet files to estimate the encoding ratio.
        # Launch tasks to sample multiple files remotely in parallel.
        # Evenly distributed to sample N rows in i-th row group in i-th file.
        # TODO(ekl/cheng) take into account column pruning.
        num_files = len(self._pq_fragments)
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
            self._pq_fragments[idx]
            for idx in np.linspace(0, num_files - 1, num_samples).astype(int).tolist()
        ]

        sample_fragment = cached_remote_fn(_sample_fragment)
        futures = []
        scheduling = self._local_scheduling or "SPREAD"
        for sample in file_samples:
            # Sample the first rows batch in i-th file.
            # Use SPREAD scheduling strategy to avoid packing many sampling tasks on
            # same machine to cause OOM issue, as sampling can be memory-intensive.
            futures.append(
                sample_fragment.options(
                    scheduling_strategy=scheduling,
                    # Retry in case of transient errors during sampling.
                    retry_exceptions=[OSError],
                ).remote(
                    self._to_batches_kwargs,
                    self._columns,
                    self._schema,
                    sample,
                )
            )
        sample_bar = ProgressBar("Parquet Files Sample", len(futures))
        sample_infos = sample_bar.fetch_until_complete(futures)
        sample_bar.close()

        return sample_infos

    def get_name(self):
        """Return a human-readable name for this datasource.

        This will be used as the names of the read tasks.
        """
        return "Parquet"

    @property
    def supports_distributed_reads(self) -> bool:
        return self._supports_distributed_reads


def _read_fragments(
    block_udf,
    to_batches_kwargs,
    default_read_batch_size_rows,
    columns,
    schema,
    serialized_fragments: List[_SerializedFragment],
    include_paths: bool,
) -> Iterator["pyarrow.Table"]:
    # This import is necessary to load the tensor extension type.
    from ray.data.extensions.tensor_extension import ArrowTensorType  # noqa

    # Deserialize after loading the filesystem class.
    fragments: List[
        "pyarrow._dataset.ParquetFileFragment"
    ] = _deserialize_fragments_with_retry(serialized_fragments)

    # Ensure that we're reading at least one dataset fragment.
    assert len(fragments) > 0

    import pyarrow as pa

    logger.debug(f"Reading {len(fragments)} parquet fragments")
    use_threads = to_batches_kwargs.pop("use_threads", False)
    batch_size = to_batches_kwargs.pop("batch_size", default_read_batch_size_rows)
    for fragment in fragments:
        batches = fragment.to_batches(
            use_threads=use_threads,
            columns=columns,
            schema=schema,
            batch_size=batch_size,
            **to_batches_kwargs,
        )
        for batch in batches:
            table = pa.Table.from_batches([batch], schema=schema)
            if include_paths:
                table = table.append_column("path", [[fragment.path]] * len(table))
            # If the table is empty, drop it.
            if table.num_rows > 0:
                if block_udf is not None:
                    yield block_udf(table)
                else:
                    yield table


def _deserialize_fragments_with_retry(fragments):
    # The deserialization retry helps when the upstream datasource is not able to
    # handle overloaded read request or failed with some retriable failures.
    # For example when reading data from HA hdfs service, hdfs might
    # lose connection for some unknown reason expecially when
    # simutaneously running many hyper parameter tuning jobs
    # with ray.data parallelism setting at high value like the default 200
    # Such connection failure can be restored with some waiting and retry.
    return call_with_retry(
        lambda: _deserialize_fragments(fragments),
        description="deserialize fragments",
        max_attempts=FILE_READING_RETRY,
    )


def _sample_fragment(
    to_batches_kwargs,
    columns,
    schema,
    file_fragment: _SerializedFragment,
) -> _SampleInfo:
    # Sample the first rows batch from file fragment `serialized_fragment`.
    fragment = _deserialize_fragments_with_retry([file_fragment])[0]

    # Only sample the first row group.
    fragment = fragment.subset(row_group_ids=[0])
    batch_size = max(
        min(fragment.metadata.num_rows, PARQUET_ENCODING_RATIO_ESTIMATE_NUM_ROWS), 1
    )
    # Use the batch_size calculated above, and ignore the one specified by user if set.
    # This is to avoid sampling too few or too many rows.
    to_batches_kwargs.pop("batch_size", None)
    batches = fragment.to_batches(
        columns=columns,
        schema=schema,
        batch_size=batch_size,
        **to_batches_kwargs,
    )
    # Use first batch in-memory size for estimation.
    try:
        batch = next(batches)
    except StopIteration:
        sample_data = _SampleInfo(
            actual_bytes_per_row=None, estimated_bytes_per_row=None
        )
    else:
        if batch.num_rows > 0:
            metadata = fragment.metadata
            total_size = 0
            for idx in range(metadata.num_row_groups):
                total_size += metadata.row_group(idx).total_byte_size
            sample_data = _SampleInfo(
                actual_bytes_per_row=batch.nbytes / batch.num_rows,
                estimated_bytes_per_row=total_size / metadata.num_rows,
            )
        else:
            sample_data = _SampleInfo(
                actual_bytes_per_row=None, estimated_bytes_per_row=None
            )
    return sample_data


def _estimate_files_encoding_ratio(sample_infos: List[_SampleInfo]) -> float:
    """Return an estimate of the Parquet files encoding ratio.

    To avoid OOMs, it is safer to return an over-estimate than an underestimate.
    """
    if not DataContext.get_current().decoding_size_estimation:
        return PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT

    def compute_encoding_ratio(sample_info: _SampleInfo) -> float:
        if (
            sample_info.actual_bytes_per_row is None
            or sample_info.estimated_bytes_per_row is None
        ):
            return PARQUET_ENCODING_RATIO_ESTIMATE_LOWER_BOUND
        else:
            return (
                sample_info.actual_bytes_per_row / sample_info.estimated_bytes_per_row
            )

    ratio = np.mean(list(map(compute_encoding_ratio, sample_infos)))
    logger.debug(f"Estimated Parquet encoding ratio from sampling is {ratio}.")
    return max(ratio, PARQUET_ENCODING_RATIO_ESTIMATE_LOWER_BOUND)


def _estimate_default_read_batch_size_rows(sample_infos: List[_SampleInfo]) -> int:
    def compute_batch_size_rows(sample_info: _SampleInfo) -> int:
        if sample_info.actual_bytes_per_row is None:
            return PARQUET_READER_ROW_BATCH_SIZE
        else:
            max_parquet_reader_row_batch_size_bytes = (
                DataContext.get_current().target_max_block_size // 10
            )
            return max(
                1,
                min(
                    PARQUET_READER_ROW_BATCH_SIZE,
                    max_parquet_reader_row_batch_size_bytes
                    // sample_info.actual_bytes_per_row,
                ),
            )

    return np.mean(list(map(compute_batch_size_rows, sample_infos)))
