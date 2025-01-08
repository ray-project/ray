import functools
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pyarrow
import pyarrow as pa
import pyarrow.dataset
from pyarrow.parquet import ParquetFile

from .file_reader import FileReader
from ray.data._internal.datasource.parquet_datasource import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
    check_for_legacy_tensor_type,
    get_parquet_dataset,
)
from ray.data._internal.util import call_with_retry, iterate_with_retry, make_async_gen
from ray.data.block import Block, DataBatch
from ray.data.context import DataContext
from ray.data.datasource import Partitioning, PathPartitionParser

# The number of rows to read per batch. This is the default we use in OSS.
DEFAULT_BATCH_SIZE = 10_000


class ParquetReader(FileReader):
    """Reads Parquet files.

    This file reader implementation leverages PyArrow's `ParquetDataset` and
    `ParquetFileFragment.to_batches` APIs to efficiently read Parquet files. It first
    creates fragments from the given paths and then reads batches from each fragment
    using multiple threads.
    """

    _NUM_THREADS_PER_TASK = 16

    def __init__(
        self,
        *,
        schema: "pyarrow.Schema",
        dataset_kwargs: Dict[str, Any],
        batch_size: Optional[int],
        use_threads: bool,
        to_batches_kwargs: Dict[str, Any],
        block_udf: Optional[Callable[[Block], Block]],
        include_paths: bool,
        partitioning: Optional[Partitioning],
    ):
        """

        Args:
            schema: An explicit user-provided schema. If not provided, the schema is
                inferred from the data.
            dataset_kwargs: Additional keyword arguments to pass to `ParquetDataset`
                when this class creates fragments.
            batch_size: The number of rows to read per batch. If not provided, a default
                value is used.
            use_threads: Whether PyArrow should use multiple threads to read batches.
                Separately from PyArrow, this class always uses multiple threads to read
                fragments.
            to_batches_kwargs: Additional keyword arguments to pass to
                `ParquetFileFragment.to_batches`.
            block_udf: A function that takes a `Block` and returns a `Block`. This
                argument is required for legacy reasons.
            include_paths: Whether to include the file path in the output.
            partitioning: The partitioning scheme to use when reading the data.
        """
        if batch_size is None:
            batch_size = DEFAULT_BATCH_SIZE

        self._schema = schema
        self._dataset_kwargs = dataset_kwargs
        self._batch_size = batch_size
        self._use_threads = use_threads
        self._to_batches_kwargs = to_batches_kwargs
        self._block_udf = block_udf
        self._include_paths = include_paths
        self._partitioning = partitioning

        # Users should use the top-level 'partitioning' argument instead of passing it
        # through 'dataset_kwargs'.
        assert "partitioning" not in dataset_kwargs
        # This reader adds partitions at the Ray Data-level. To prevent PyArrow from
        # adding partitions, we set the 'partitioning' to 'None'.
        self._dataset_kwargs["partitioning"] = None

        self._data_context = DataContext.get_current()

    def read_paths(
        self,
        paths: List[str],
        *,
        filter_expr: Optional[pyarrow.dataset.Expression] = None,
        columns: Optional[List[str]] = None,
        columns_rename: Optional[Dict[str, str]] = None,
        filesystem,
    ) -> Iterable[DataBatch]:
        if columns and columns_rename:
            assert set(columns_rename.keys()).issubset(columns), (
                f"All column rename keys must be a subset of the columns list. "
                f"Invalid keys: {set(columns_rename.keys()) - set(columns)}"
            )

        fragments = self._create_fragments(paths, filesystem=filesystem)

        # Users can pass both data columns and partition columns in the 'columns'
        # argument. To prevent PyArrow from complaining about missing columns, we
        # separate the partition columns from the data columns. When we read the
        # fragments, we pass the data columns to PyArrow and add the partition
        # columns manually.
        data_columns = None
        partition_columns = None
        if columns is not None:
            data_columns = [
                column
                for column in columns
                if column in fragments[0].physical_schema.names
            ]
            if self._partitioning is not None:
                parse = PathPartitionParser(self._partitioning)
                partitions = parse(fragments[0].path)
                partition_columns = [
                    column for column in columns if column in partitions
                ]

        num_threads = self._get_num_threads(paths)
        if num_threads > 0:
            yield from make_async_gen(
                iter(fragments),
                functools.partial(
                    self._read_fragments,
                    filter_expr=filter_expr,
                    schema=self._schema,
                    data_columns=data_columns,
                    partition_columns=partition_columns,
                    columns_rename=columns_rename,
                ),
                num_workers=num_threads,
            )
        else:
            yield from self._read_fragments(
                fragments,
                filter_expr=filter_expr,
                schema=self._schema,
                data_columns=data_columns,
                partition_columns=partition_columns,
                columns_rename=columns_rename,
            )

    def _create_fragments(
        self,
        paths: List[str],
        *,
        filesystem: pa.fs.FileSystem,
    ) -> Tuple[List[pyarrow.dataset.ParquetFileFragment], pyarrow.Schema]:
        parquet_dataset = call_with_retry(
            lambda: get_parquet_dataset(paths, filesystem, self._dataset_kwargs),
            "create ParquetDataset",
            match=self._data_context.retried_io_errors,
        )
        check_for_legacy_tensor_type(parquet_dataset.schema)
        return parquet_dataset.fragments

    def _get_num_threads(self, paths):
        num_threads = self._NUM_THREADS_PER_TASK
        if len(paths) < num_threads:
            num_threads = len(paths)

        # TODO: We should refactor the code so that we can get the results in order even
        # when using multiple threads.
        if self._data_context.execution_options.preserve_order:
            num_threads = 0

        return num_threads

    def _read_fragments(
        self,
        fragments: List[pyarrow.dataset.ParquetFileFragment],
        filter_expr: pyarrow.dataset.Expression,
        schema: pyarrow.Schema,
        data_columns: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
        columns_rename: Optional[Dict[str, str]] = None,
    ) -> Iterable["pyarrow.Table"]:
        for fragment in fragments:
            partitions = {}
            if self._partitioning is not None:
                parse = PathPartitionParser(self._partitioning)
                partitions = parse(fragment.path)

            for batch in self._read_batches(
                fragment, filter_expr, schema, data_columns
            ):
                if self._include_paths:
                    batch = batch.append_column(
                        "path", pa.array([fragment.path] * len(batch))
                    )
                for partition, value in partitions.items():
                    if partition_columns is None or partition in partition_columns:
                        batch = batch.append_column(
                            partition, pa.array([value] * len(batch))
                        )
                if columns_rename is not None:
                    batch = batch.rename_columns(
                        [columns_rename.get(col, col) for col in batch.schema.names]
                    )
                yield batch

    def _read_batches(
        self,
        fragment: pyarrow.dataset.ParquetFileFragment,
        filter_expr: pyarrow.dataset.Expression,
        schema: pyarrow.Schema,
        columns: Optional[List[str]],
    ) -> Iterable[pyarrow.Table]:
        def get_batch_iterable():
            return fragment.to_batches(
                use_threads=self._use_threads,
                # Workaround for https://github.com/anyscale/rayturbo/issues/1328.
                columns=None if not columns else columns,
                filter=filter_expr,
                schema=schema,
                batch_size=self._batch_size,
                **self._to_batches_kwargs,
            )

        # S3 can raise transient errors during iteration, and PyArrow doesn't expose a
        # way to retry specific batches.
        for batch in iterate_with_retry(
            get_batch_iterable,
            "ParquetReader load batch",
            match=self._data_context.retried_io_errors,
        ):
            # TODO: If the table is much larger than the target block size, emit a
            # warning instructing the user to decrease the batch size.
            table = pa.Table.from_batches([batch])

            # If the table is empty, drop it.
            if table.num_rows > 0:
                if self._block_udf is not None:
                    yield self._block_udf(table)
                else:
                    yield table

    def estimate_in_memory_size(self, path: str, file_size: int, *, filesystem) -> int:
        # Reading a batch of Parquet data can be slow, even if you try to read a single
        # row. To avoid slow startup times, just return a constant value. For more
        # information, see https://github.com/anyscale/rayturbo/issues/924.
        return PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT * file_size

    def count_rows(self, paths: List[str], *, filesystem) -> int:
        def open_file(path: str) -> ParquetFile:
            stream = filesystem.open_input_file(path)
            return ParquetFile(stream)

        num_rows = 0
        for path in paths:
            file = call_with_retry(
                lambda: open_file(path),
                description="open Parquet file",
                match=self._data_context.retried_io_errors,
            )
            # Getting the metadata requires network calls, so it might fail with
            # transient errors.
            num_rows += call_with_retry(
                lambda: file.metadata.num_rows,
                description="get count from Parquet metadata",
                match=self._data_context.retried_io_errors,
            )
        return num_rows

    def supports_count_rows(self) -> bool:
        return "filter" not in self._to_batches_kwargs

    def supports_predicate_pushdown(self) -> bool:
        return True
