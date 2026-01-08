import copy
from typing import TYPE_CHECKING, Callable, Dict, Generator, Iterable, List, Optional

import numpy as np
import pyarrow as pa

from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockMetadata, Schema
from ray.data.datasource.util import _iter_sliced_blocks
from ray.data.expressions import Expr
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data.context import DataContext


class _DatasourceProjectionPushdownMixin:
    """Mixin for reading operators supporting projection pushdown"""

    def supports_projection_pushdown(self) -> bool:
        """Returns ``True`` in case ``Datasource`` supports projection operation
        being pushed down into the reading layer"""
        return False

    def get_projection_map(self) -> Optional[Dict[str, str]]:
        """Return the projection map (original column names -> final column names).

        Returns:
            Dict mapping original column names (in storage) to final column names
            (after optional renames). Keys indicate which columns are selected.
            None means all columns are selected with no renames.
            Empty dict {} means no columns are selected.
        """
        return self._projection_map

    def get_column_renames(self) -> Optional[Dict[str, str]]:
        """Return the column renames from the projection map.

        This is used by predicate pushdown to rewrite filter expressions
        from renamed column names back to original column names.

        Returns:
            Dict mapping original column names to renamed names,
            or None if no renaming has been applied.
        """
        if self._projection_map is None:
            return None
        # Only include actual renames (where key != value)
        renames = {k: v for k, v in self._projection_map.items() if k != v}
        return renames if renames else None

    def _get_data_columns(self) -> Optional[List[str]]:
        """Extract data columns from projection map.

        Helper method for datasources that need to pass columns to legacy
        read functions expecting separate columns and rename_map parameters.

        Returns:
            List of column names, or None if all columns should be read.
            Empty list [] means no columns.
        """
        return (
            list(self._projection_map.keys())
            if self._projection_map is not None
            else None
        )

    @staticmethod
    def _combine_projection_map(
        prev_projection_map: Optional[Dict[str, str]],
        new_projection_map: Optional[Dict[str, str]],
    ) -> Optional[Dict[str, str]]:
        """Combine two projection maps via transitive composition.

        Args:
            prev_projection_map: Previous projection (original -> intermediate names)
            new_projection_map: New projection to apply (intermediate -> final names)

        Returns:
            Combined projection map (original -> final names)

        Examples:
            >>> # Select columns a, b with no renames
            >>> prev = {"a": "a", "b": "b"}
            >>> # Select only 'a', rename to 'x'
            >>> new = {"a": "x"}
            >>> _DatasourceProjectionPushdownMixin._combine_projection_map(prev, new)
            {'a': 'x'}

            >>> # First rename a->temp
            >>> prev = {"a": "temp"}
            >>> # Then rename temp->final
            >>> new = {"temp": "final"}
            >>> _DatasourceProjectionPushdownMixin._combine_projection_map(prev, new)
            {'a': 'final'}
        """
        # Handle None cases (None means "all columns, no renames")
        if prev_projection_map is None:
            return new_projection_map
        elif new_projection_map is None:
            return prev_projection_map

        # Compose projections: for each original->intermediate mapping in prev,
        # check if intermediate is selected by new projection
        composed = {}
        for orig_col, intermediate_name in prev_projection_map.items():
            # If intermediate name is in new projection, follow the chain
            if intermediate_name in new_projection_map:
                final_name = new_projection_map[intermediate_name]
                composed[orig_col] = final_name

        # The composition already handles transitive chains correctly:
        # prev {a: temp}, new {temp: final} -> composed {a: final}
        # No need for collapse_transitive_map which would incorrectly remove
        # identity mappings like {b: b}
        return composed

    def apply_projection(
        self,
        projection_map: Optional[Dict[str, str]],
    ) -> "Datasource":
        """Apply a projection to this datasource.

        Args:
            projection_map: Dict mapping original column names (in storage)
                to final column names (after optional renames). Keys indicate
                which columns to select. None means select all columns with no renames.

        Returns:
            A new datasource instance with the projection applied.
        """
        clone = copy.copy(self)

        # Combine projections via transitive map composition
        clone._projection_map = self._combine_projection_map(
            self._projection_map, projection_map
        )

        return clone

    @staticmethod
    def _apply_rename(
        table: "pa.Table",
        column_rename_map: Optional[Dict[str, str]],
    ) -> "pa.Table":
        """Apply column renaming to a PyArrow table.

        Args:
            table: PyArrow table to rename
            column_rename_map: Mapping from old column names to new names

        Returns:
            Table with renamed columns
        """
        if not column_rename_map:
            return table

        new_names = [column_rename_map.get(col, col) for col in table.schema.names]
        return table.rename_columns(new_names)

    @staticmethod
    def _apply_rename_to_tables(
        tables: Iterable["pa.Table"],
        column_rename_map: Optional[Dict[str, str]],
    ) -> Generator["pa.Table", None, None]:
        """Wrap a table generator to apply column renaming to each table.

        This helper eliminates duplication across datasources that need to apply
        column renames to tables yielded from generators.

        Args:
            tables: Iterator/generator yielding PyArrow tables
            column_rename_map: Mapping from old column names to new names

        Yields:
            pa.Table: Tables with renamed columns
        """
        for table in tables:
            yield _DatasourceProjectionPushdownMixin._apply_rename(
                table, column_rename_map
            )


class _DatasourcePredicatePushdownMixin:
    """Mixin for reading operators supporting predicate pushdown"""

    def __init__(self):
        self._predicate_expr: Optional[Expr] = None

    def supports_predicate_pushdown(self) -> bool:
        return False

    def get_current_predicate(self) -> Optional[Expr]:
        return self._predicate_expr

    def apply_predicate(
        self,
        predicate_expr: Expr,
    ) -> "Datasource":
        """Apply a predicate to this datasource.

        Default implementation that combines predicates using AND.
        Subclasses that support predicate pushdown should have a _predicate_expr
        attribute to store the predicate.

        Note: Column rebinding is handled by the PredicatePushdown rule
        before this method is called, so the predicate_expr should already
        reference the correct column names.
        """
        import copy

        clone = copy.copy(self)

        # Combine with existing predicate using AND
        clone._predicate_expr = (
            predicate_expr
            if clone._predicate_expr is None
            else clone._predicate_expr & predicate_expr
        )

        return clone


@PublicAPI
class Datasource(_DatasourceProjectionPushdownMixin, _DatasourcePredicatePushdownMixin):
    """Interface for defining a custom :class:`~ray.data.Dataset` datasource.

    To read a datasource into a dataset, use :meth:`~ray.data.read_datasource`.
    """  # noqa: E501

    def __init__(self):
        """Initialize the datasource and its mixins."""
        _DatasourcePredicatePushdownMixin.__init__(self)

    @Deprecated
    def create_reader(self, **read_args) -> "Reader":
        """
        Deprecated: Implement :meth:`~ray.data.Datasource.get_read_tasks` and
        :meth:`~ray.data.Datasource.estimate_inmemory_data_size` instead.
        """
        return _LegacyDatasourceReader(self, **read_args)

    @Deprecated
    def prepare_read(self, parallelism: int, **read_args) -> List["ReadTask"]:
        """
        Deprecated: Implement :meth:`~ray.data.Datasource.get_read_tasks` and
        :meth:`~ray.data.Datasource.estimate_inmemory_data_size` instead.
        """
        raise NotImplementedError

    def get_name(self) -> str:
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        """
        name = type(self).__name__
        datasource_suffix = "Datasource"
        if name.endswith(datasource_suffix):
            name = name[: -len(datasource_suffix)]
        return name

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size, or None if unknown.

        Note that the in-memory data size may be larger than the on-disk data size.
        """
        raise NotImplementedError

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List["ReadTask"]:
        """Execute the read and return read tasks.

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should equal to this value if possible.
            per_task_row_limit: The per-task row limit for the read tasks.
            data_context: The data context to use to get read tasks.
        Returns:
            A list of read tasks that can be executed to read blocks from the
            datasource in parallel.
        """
        raise NotImplementedError

    @property
    def should_create_reader(self) -> bool:
        has_implemented_get_read_tasks = (
            type(self).get_read_tasks is not Datasource.get_read_tasks
        )
        has_implemented_estimate_inmemory_data_size = (
            type(self).estimate_inmemory_data_size
            is not Datasource.estimate_inmemory_data_size
        )
        return (
            not has_implemented_get_read_tasks
            or not has_implemented_estimate_inmemory_data_size
        )

    @property
    def supports_distributed_reads(self) -> bool:
        """If ``False``, only launch read tasks on the driver's node."""
        return True


@Deprecated
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

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List["ReadTask"]:
        """Execute the read and return read tasks.

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should equal to this value if possible.
            per_task_row_limit: The per-task row limit for the read tasks.
            data_context: The data context to use to get read tasks. Not used by this
                legacy reader.

        Returns:
            A list of read tasks that can be executed to read blocks from the
            datasource in parallel.
        """
        return self._datasource.prepare_read(parallelism, **self._read_args)


@DeveloperAPI
class ReadTask(Callable[[], Iterable[Block]]):
    """A function used to read blocks from the :class:`~ray.data.Dataset`.

    Read tasks are generated by :meth:`~ray.data.Datasource.get_read_tasks`,
    and return a list of ``ray.data.Block`` when called. Initial metadata about the read
    operation can be retrieved via the ``metadata`` attribute prior to executing the
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

    def __init__(
        self,
        read_fn: Callable[[], Iterable[Block]],
        metadata: BlockMetadata,
        schema: Optional["Schema"] = None,
        per_task_row_limit: Optional[int] = None,
    ):
        self._metadata = metadata
        self._read_fn = read_fn
        self._schema = schema
        self._per_task_row_limit = per_task_row_limit

    @property
    def metadata(self) -> BlockMetadata:
        return self._metadata

    # TODO(justin): We want to remove schema from `ReadTask` later on
    @property
    def schema(self) -> Optional["Schema"]:
        return self._schema

    @property
    def read_fn(self) -> Callable[[], Iterable[Block]]:
        return self._read_fn

    @property
    def per_task_row_limit(self) -> Optional[int]:
        """Get the per-task row limit for this read task."""
        return self._per_task_row_limit

    def __call__(self) -> Iterable[Block]:
        result = self._read_fn()
        if not hasattr(result, "__iter__"):
            DeprecationWarning(
                "Read function must return Iterable[Block], got {}. "
                "Probably you need to return `[block]` instead of "
                "`block`.".format(result)
            )
        if self._per_task_row_limit is None:
            yield from result
            return

        yield from _iter_sliced_blocks(result, self._per_task_row_limit)


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

    def __init__(self, n: int, num_columns: int):
        """Initialize the datasource that generates random-integer rows.

        Args:
            n: The number of rows to generate.
            num_columns: The number of columns to generate.
        """
        self._n = n
        self._num_columns = num_columns

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._n * self._num_columns * 8

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
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
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    lambda count=count, num_columns=num_columns: [
                        make_block(count, num_columns)
                    ],
                    meta,
                    schema=schema,
                    per_task_row_limit=per_task_row_limit,
                )
            )
            i += block_size

        return read_tasks

    def get_name(self) -> str:
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        Note: overrides the base `Datasource` method.
        """
        return "RandomInt"
