import builtins
from copy import copy
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI

WriteResult = Any


@PublicAPI
class Datasource:
    """Interface for defining a custom :class:`~ray.data.Dataset` datasource.

    To read a datasource into a dataset, use :meth:`~ray.data.read_datasource`.
    To write to a writable datasource, use :meth:`~ray.data.Dataset.write_datasource`.

    See ``RangeDatasource`` and ``DummyOutputDatasource`` for examples
    of how to implement readable and writable datasources.

    For an example of subclassing :class:`~ray.data.Datasource`, read
    :ref:`Implementing a Custom Datasource <custom_datasources>`.

    .. note::
        Datasource instances must be serializable, since
        :meth:`~ray.data.Datasource.create_reader` and
        :meth:`~ray.data.Datasource.write` are called in remote tasks.

    """  # noqa: E501

    def create_reader(self, **read_args) -> "Reader":
        """Return a Reader for the given read arguments.

        The reader object will be responsible for querying the read metadata, and
        generating the actual read tasks to retrieve the data blocks upon request.

        Args:
            read_args: Additional kwargs to pass to the datasource impl.
        """
        return _LegacyDatasourceReader(self, **read_args)

    @Deprecated
    def prepare_read(self, parallelism: int, **read_args) -> List["ReadTask"]:
        """Deprecated: Please implement create_reader() instead."""
        raise NotImplementedError

    def on_write_start(self, **write_args) -> None:
        """Callback for when a write job starts.

        Use this method to perform setup for write tasks. For example, creating a
        staging bucket in S3.

        Args:
            write_args: Additional kwargs to pass to the datasource impl.
        """
        pass

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        **write_args,
    ) -> WriteResult:
        """Write blocks out to the datasource. This is used by a single write task.

        Args:
            blocks: List of data blocks.
            ctx: ``TaskContext`` for the write task.
            write_args: Additional kwargs to pass to the datasource impl.

        Returns:
            The output of the write task.
        """
        raise NotImplementedError

    @Deprecated(
        message="do_write() is deprecated in Ray 2.4. Use write() instead", warning=True
    )
    def do_write(
        self,
        blocks: List[ObjectRef[Block]],
        metadata: List[BlockMetadata],
        ray_remote_args: Dict[str, Any],
        **write_args,
    ) -> List[ObjectRef[WriteResult]]:
        """Launch Ray tasks for writing blocks out to the datasource.

        Args:
            blocks: List of data block references. It is recommended that one
                write task be generated per block.
            metadata: List of block metadata.
            ray_remote_args: Kwargs passed to ray.remote in the write tasks.
            write_args: Additional kwargs to pass to the datasource impl.

        Returns:
            A list of the output of the write tasks.
        """
        raise NotImplementedError

    def on_write_complete(self, write_results: List[WriteResult], **kwargs) -> None:
        """Callback for when a write job completes.

        This can be used to "commit" a write output. This method must
        succeed prior to ``write_datasource()`` returning to the user. If this
        method fails, then ``on_write_failed()`` will be called.

        Args:
            write_results: The list of the write task results.
            kwargs: Forward-compatibility placeholder.
        """
        pass

    def on_write_failed(
        self, write_results: List[ObjectRef[WriteResult]], error: Exception, **kwargs
    ) -> None:
        """Callback for when a write job fails.

        This is called on a best-effort basis on write failures.

        Args:
            write_results: The list of the write task result futures.
            error: The first error encountered.
            kwargs: Forward-compatibility placeholder.
        """
        pass

    def get_name(self) -> str:
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        """
        name = type(self).__name__
        datasource_suffix = "Datasource"
        if name.endswith(datasource_suffix):
            name = name[: -len(datasource_suffix)]
        return name


@PublicAPI
class Reader:
    """A bound read operation for a :class:`~ray.data.Datasource`.

    This is a stateful class so that reads can be prepared in multiple stages.
    For example, it is useful for :class:`Datasets <ray.data.Dataset>` to know the
    in-memory size of the read prior to executing it.
    """

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size, or None if unknown.

        Note that the in-memory data size may be larger than the on-disk data size.
        """
        raise NotImplementedError

    def get_read_tasks(self, parallelism: int) -> List["ReadTask"]:
        """Execute the read and return read tasks.

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should equal to this value if possible.
            read_args: Additional kwargs to pass to the datasource impl.

        Returns:
            A list of read tasks that can be executed to read blocks from the
            datasource in parallel.
        """
        raise NotImplementedError


class _LegacyDatasourceReader(Reader):
    def __init__(self, datasource: Datasource, **read_args):
        self._datasource = datasource
        self._read_args = read_args

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(self, parallelism: int) -> List["ReadTask"]:
        return self._datasource.prepare_read(parallelism, **self._read_args)


@DeveloperAPI
class ReadTask(Callable[[], Iterable[Block]]):
    """A function used to read blocks from the :class:`~ray.data.Dataset`.

    Read tasks are generated by :meth:`~ray.data.datasource.Reader.get_read_tasks`,
    and return a list of ``ray.data.Block`` when called. Initial metadata about the read
    operation can be retrieved via ``get_metadata()`` prior to executing the
    read. Final metadata is returned after the read along with the blocks.

    Ray will execute read tasks in remote functions to parallelize execution.
    Note that the number of blocks returned can vary at runtime. For example,
    if a task is reading a single large file it can return multiple blocks to
    avoid running out of memory during the read.

    The initial metadata should reflect all the blocks returned by the read,
    e.g., if the metadata says ``num_rows=1000``, the read can return a single
    block of 1000 rows, or multiple blocks with 1000 rows altogether.

    The final metadata (returned with the actual block) reflects the exact
    contents of the block itself.
    """

    def __init__(self, read_fn: Callable[[], Iterable[Block]], metadata: BlockMetadata):
        self._metadata = metadata
        self._read_fn = read_fn

    def get_metadata(self) -> BlockMetadata:
        return self._metadata

    def __call__(self) -> Iterable[Block]:
        result = self._read_fn()
        if not hasattr(result, "__iter__"):
            DeprecationWarning(
                "Read function must return Iterable[Block], got {}. "
                "Probably you need to return `[block]` instead of "
                "`block`.".format(result)
            )
        yield from result


@PublicAPI
class RangeDatasource(Datasource):
    """An example datasource that generates ranges of numbers from [0..n).

    Examples:
        >>> import ray
        >>> from ray.data.datasource import RangeDatasource
        >>> source = RangeDatasource() # doctest: +SKIP
        >>> ray.data.read_datasource(source, n=10).take() # doctest: +SKIP
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    """

    def create_reader(
        self,
        n: int,
        block_format: str = "arrow",
        tensor_shape: Tuple = (1,),
        column_name: Optional[str] = None,
    ) -> List[ReadTask]:
        return _RangeDatasourceReader(n, block_format, tensor_shape, column_name)


class _RangeDatasourceReader(Reader):
    def __init__(
        self,
        n: int,
        block_format: str = "list",
        tensor_shape: Tuple = (1,),
        column_name: Optional[str] = None,
    ):
        self._n = n
        self._block_format = block_format
        self._tensor_shape = tensor_shape
        self._column_name = column_name

    def estimate_inmemory_data_size(self) -> Optional[int]:
        if self._block_format == "tensor":
            element_size = np.product(self._tensor_shape)
        else:
            element_size = 1
        return 8 * self._n * element_size

    def get_read_tasks(
        self,
        parallelism: int,
    ) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        n = self._n
        block_format = self._block_format
        tensor_shape = self._tensor_shape
        block_size = max(1, n // parallelism)

        # Example of a read task. In a real datasource, this would pull data
        # from an external system instead of generating dummy data.
        def make_block(start: int, count: int) -> Block:
            if block_format == "arrow":
                import pyarrow as pa

                return pa.Table.from_arrays(
                    [np.arange(start, start + count)],
                    names=[self._column_name or "value"],
                )
            elif block_format == "tensor":
                import pyarrow as pa

                tensor = np.ones(tensor_shape, dtype=np.int64) * np.expand_dims(
                    np.arange(start, start + count),
                    tuple(range(1, 1 + len(tensor_shape))),
                )
                return BlockAccessor.batch_to_block(
                    {self._column_name: tensor} if self._column_name else tensor
                )
            else:
                return list(builtins.range(start, start + count))

        if block_format == "arrow":
            _check_pyarrow_version()
            import pyarrow as pa

            schema = pa.Table.from_pydict({self._column_name or "value": [0]}).schema
        elif block_format == "tensor":
            _check_pyarrow_version()
            import pyarrow as pa

            tensor = np.ones(tensor_shape, dtype=np.int64) * np.expand_dims(
                np.arange(0, 10), tuple(range(1, 1 + len(tensor_shape)))
            )
            schema = BlockAccessor.batch_to_block(
                {self._column_name: tensor} if self._column_name else tensor
            ).schema
        elif block_format == "list":
            schema = int
        else:
            raise ValueError("Unsupported block type", block_format)
        if block_format == "tensor":
            element_size = np.product(tensor_shape)
        else:
            element_size = 1

        i = 0
        while i < n:
            count = min(block_size, n - i)
            meta = BlockMetadata(
                num_rows=count,
                size_bytes=8 * count * element_size,
                schema=copy(schema),
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(lambda i=i, count=count: [make_block(i, count)], meta)
            )
            i += block_size

        return read_tasks


@DeveloperAPI
class DummyOutputDatasource(Datasource):
    """An example implementation of a writable datasource for testing.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import DummyOutputDatasource
        >>> output = DummyOutputDatasource() # doctest: +SKIP
        >>> ray.data.range(10).write_datasource(output) # doctest: +SKIP
        >>> assert output.num_ok == 1 # doctest: +SKIP
    """

    def __init__(self):
        ctx = DataContext.get_current()

        # Setup a dummy actor to send the data. In a real datasource, write
        # tasks would send data to an external system instead of a Ray actor.
        @ray.remote(scheduling_strategy=ctx.scheduling_strategy)
        class DataSink:
            def __init__(self):
                self.rows_written = 0
                self.enabled = True

            def write(self, block: Block) -> str:
                block = BlockAccessor.for_block(block)
                self.rows_written += block.num_rows()
                return "ok"

            def get_rows_written(self):
                return self.rows_written

        self.data_sink = DataSink.remote()
        self.num_ok = 0
        self.num_failed = 0
        self.enabled = True

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        **write_args,
    ) -> WriteResult:
        tasks = []
        if not self.enabled:
            raise ValueError("disabled")
        for b in blocks:
            tasks.append(self.data_sink.write.remote(b))
        ray.get(tasks)
        return "ok"

    def on_write_complete(self, write_results: List[WriteResult]) -> None:
        assert all(w == "ok" for w in write_results), write_results
        self.num_ok += 1

    def on_write_failed(
        self, write_results: List[ObjectRef[WriteResult]], error: Exception
    ) -> None:
        self.num_failed += 1


@DeveloperAPI
class RandomIntRowDatasource(Datasource):
    """An example datasource that generates rows with random int64 columns.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import RandomIntRowDatasource
        >>> source = RandomIntRowDatasource() # doctest: +SKIP
        >>> ray.data.read_datasource( # doctest: +SKIP
        ...     source, n=10, num_columns=2).take()
        {'c_0': 1717767200176864416, 'c_1': 999657309586757214}
        {'c_0': 4983608804013926748, 'c_1': 1160140066899844087}
    """

    def get_name(self) -> str:
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        Note: overrides the base `Datasource` method.
        """
        return "RandomInt"

    def create_reader(
        self,
        n: int,
        num_columns: int,
    ) -> List[ReadTask]:
        return _RandomIntRowDatasourceReader(n, num_columns)


class _RandomIntRowDatasourceReader(Reader):
    def __init__(self, n: int, num_columns: int):
        self._n = n
        self._num_columns = num_columns

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._n * self._num_columns * 8

    def get_read_tasks(
        self,
        parallelism: int,
    ) -> List[ReadTask]:
        _check_pyarrow_version()
        import pyarrow

        read_tasks: List[ReadTask] = []
        n = self._n
        num_columns = self._num_columns
        block_size = max(1, n // parallelism)

        def make_block(count: int, num_columns: int) -> Block:
            return pyarrow.Table.from_arrays(
                np.random.randint(
                    np.iinfo(np.int64).max, size=(num_columns, count), dtype=np.int64
                ),
                names=[f"c_{i}" for i in range(num_columns)],
            )

        schema = pyarrow.Table.from_pydict(
            {f"c_{i}": [0] for i in range(num_columns)}
        ).schema

        i = 0
        while i < n:
            count = min(block_size, n - i)
            meta = BlockMetadata(
                num_rows=count,
                size_bytes=8 * count * num_columns,
                schema=schema,
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    lambda count=count, num_columns=num_columns: [
                        make_block(count, num_columns)
                    ],
                    meta,
                )
            )
            i += block_size

        return read_tasks
