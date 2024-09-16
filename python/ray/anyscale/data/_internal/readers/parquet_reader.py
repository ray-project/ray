import functools
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pyarrow
import pyarrow as pa
import pyarrow.dataset

import ray
from .file_reader import FileReader
from ray.data._internal.datasource.parquet_datasource import (
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
        columns: Optional[List[str]],
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
            columns: A list of column names to read. If not provided, all columns are
                read. This list can include partition columns.
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

        self._user_provided_schema = schema
        self._columns = columns
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

    def read_paths(
        self,
        paths: List[str],
        *,
        filesystem,
    ) -> Iterable[DataBatch]:
        fragments, schema = self._create_fragments(paths, filesystem=filesystem)
        if self._user_provided_schema is not None:
            schema = self._user_provided_schema

        num_threads = self._get_num_threads(paths)
        if num_threads > 0:
            yield from make_async_gen(
                iter(fragments),
                functools.partial(self._read_fragments, schema=schema),
                num_workers=num_threads,
            )
        else:
            yield from self._read_fragments(fragments, schema)

    def _create_fragments(
        self, paths: List[str], *, filesystem: pa.fs.FileSystem
    ) -> Tuple[List[pyarrow.dataset.ParquetFileFragment], pyarrow.Schema]:
        ctx = DataContext.get_current()
        parquet_dataset = call_with_retry(
            lambda: get_parquet_dataset(paths, filesystem, self._dataset_kwargs),
            "create ParquetDataset",
            match=ctx.retried_io_errors,
        )
        check_for_legacy_tensor_type(parquet_dataset.schema)
        return parquet_dataset.fragments, parquet_dataset.schema

    def _get_num_threads(self, paths):
        num_threads = self._NUM_THREADS_PER_TASK
        if len(paths) < num_threads:
            num_threads = len(paths)

        # TODO: We should refactor the code so that we can get the results in order even
        # when using multiple threads.
        ctx = DataContext.get_current()
        if ctx.execution_options.preserve_order:
            num_threads = 0

        return num_threads

    def _read_fragments(
        self,
        fragments: List[pyarrow.dataset.ParquetFileFragment],
        schema: pyarrow.Schema,
    ) -> Iterable["pyarrow.Table"]:
        for fragment in fragments:
            partitions = {}
            if self._partitioning is not None:
                parse = PathPartitionParser(self._partitioning)
                partitions = parse(fragment.path)

            for batch in self._read_batches(fragment, schema):
                if self._include_paths:
                    batch = batch.append_column(
                        "path", pa.array([fragment.path] * len(batch))
                    )
                for partition, value in partitions.items():
                    if self._columns is None or partition in self._columns:
                        batch = batch.append_column(
                            partition, pa.array([value] * len(batch))
                        )
                yield batch

    def _read_batches(
        self, fragment: pyarrow.dataset.ParquetFileFragment, schema: pyarrow.Schema
    ) -> Iterable[pyarrow.Table]:
        ctx = ray.data.DataContext.get_current()

        columns = self._columns
        if columns is not None:
            # The actual data might not contain all of the user-specified columns (e.g.,
            # if the user includes partition columns). So, filter out columns that
            # aren't in the actual data.
            columns = [column for column in self._columns if column in schema.names]

        def get_batch_iterable():
            return fragment.to_batches(
                use_threads=self._use_threads,
                columns=columns,
                schema=schema,
                batch_size=self._batch_size,
                **self._to_batches_kwargs,
            )

        # S3 can raise transient errors during iteration, and PyArrow doesn't expose a
        # way to retry specific batches.
        for batch in iterate_with_retry(
            get_batch_iterable, "ParquetReader load batch", match=ctx.retried_io_errors
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
