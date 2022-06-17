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
from ray.data.datasource.datasource import ReadTask, Reader
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
            **reader_args):
        # NOTE: We override the base class FileBasedDatasource.prepare_read
        # method in order to leverage pyarrow's ParquetDataset abstraction,
        # which simplifies partitioning logic. We still use
        # FileBasedDatasource's write side (do_write), however.
        _check_pyarrow_version()
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
        self._metadata = meta_provider.prefetch_file_metadata(pq_ds.pieces) or []
        self._pq_ds = pq_ds
        self._meta_provider = meta_provider
        self._inferred_schema = inferred_schema
        self._block_udf = _block_udf
        self._reader_args = reader_args
        self._columns = columns
        self._schema = schema

    def estimate_inmemory_data_size(self) -> Optional[int]:
        total_size = 0
        for meta in self._metadata:
            total_size += meta.serialized_size
        return total_size

    def read(self, parallelism: int) -> List[ReadTask]:
        import pyarrow as pa
        from ray import cloudpickle

        def read_pieces(serialized_pieces: str) -> Iterator[pa.Table]:
            # Implicitly trigger S3 subsystem initialization by importing
            # pyarrow.fs.
            import pyarrow.fs  # noqa: F401

            # Deserialize after loading the filesystem class.
            try:
                _register_parquet_file_fragment_serialization()
                pieces: List[
                    "pyarrow._dataset.ParquetFileFragment"
                ] = cloudpickle.loads(serialized_pieces)
            finally:
                _deregister_parquet_file_fragment_serialization()

            # Ensure that we're reading at least one dataset fragment.
            assert len(pieces) > 0

            from pyarrow.dataset import _get_partition_keys

            ctx = DatasetContext.get_current()
            output_buffer = BlockOutputBuffer(
                block_udf=self._block_udf, target_max_block_size=ctx.target_max_block_size
            )

            logger.debug(f"Reading {len(pieces)} parquet pieces")
            use_threads = self._reader_args.pop("use_threads", False)
            for piece in pieces:
                part = _get_partition_keys(piece.partition_expression)
                batches = piece.to_batches(
                    use_threads=use_threads,
                    columns=self._columns,
                    schema=self._schema,
                    batch_size=PARQUET_READER_ROW_BATCH_SIZE,
                    **self._reader_args,
                )
                for batch in batches:
                    table = pyarrow.Table.from_batches([batch], schema=self._schema)
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

        read_tasks = []
        try:
            _register_parquet_file_fragment_serialization()
            for pieces, metadata in zip(
                np.array_split(self._pq_ds.pieces, parallelism),
                np.array_split(self._metadata, parallelism),
            ):
                if len(pieces) <= 0:
                    continue
                serialized_pieces = cloudpickle.dumps(pieces)
                input_files = [p.path for p in pieces]
                meta = self._meta_provider(
                    input_files,
                    self._inferred_schema,
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
    from ray import cloudpickle

    # Deserialize after loading the filesystem class.
    try:
        _register_parquet_file_fragment_serialization()
        pieces: List["pyarrow._dataset.ParquetFileFragment"] = cloudpickle.loads(pieces)
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
