import abc
import copy
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, final

import numpy as np

from ray.data._internal.untrusted_unpickling import forbid_untrusted_unpickling
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockMetadata, Schema
from ray.data.datasource.util import _iter_sliced_blocks
from ray.data.expressions import Expr
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data.context import DataContext


class _DatasourceProjectionPushdownMixin:
    """Mixin for reading operators supporting projection pushdown.

    The read stage only prunes columns; it never renames. Column renaming
    is always carried by an ``AliasExpr`` in a ``Project`` operator above
    the read. As a consequence, projection maps stored here are always
    identity (``{name: name}``).
    """

    def supports_projection_pushdown(self) -> bool:
        """Returns ``True`` in case ``Datasource`` supports projection operation
        being pushed down into the reading layer"""
        return False

    def get_projection_map(self) -> Optional[Dict[str, str]]:
        """Return the projection map (always an identity mapping).

        Returns:
            Dict mapping selected column names to themselves. ``None``
            means all columns are selected. Empty dict ``{}`` means no
            columns are selected.
        """
        return self._projection_map

    def _get_data_columns(self) -> Optional[List[str]]:
        """Extract data columns from projection map.

        Helper method for datasources that need to pass columns to legacy
        read functions expecting a list of columns.

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
        """Combine two projection maps. Identity-only; renames are not stored.

        Args:
            prev_projection_map: Previously-applied identity map.
            new_projection_map: New identity map to compose.

        Returns:
            Combined identity map containing the columns present in both.
            ``None`` means "all columns" and acts as a passthrough.
        """
        # Handle None cases (None means "all columns")
        if prev_projection_map is None:
            return new_projection_map
        elif new_projection_map is None:
            return prev_projection_map

        # Both are identity maps; keep only columns present in both.
        return {
            name: name for name in prev_projection_map if name in new_projection_map
        }

    def apply_projection(
        self,
        projection_map: Optional[Dict[str, str]],
    ) -> "Datasource":
        """Apply a projection (column selection) to this datasource.

        Args:
            projection_map: Dict whose keys are the column names to select.
                ``None`` means select all columns. Any non-identity values
                are ignored — the read stage does not rename.

        Returns:
            A new datasource instance with the projection applied.
        """
        clone = copy.copy(self)

        # Normalize any rename entries to identity — the read stage
        # never renames.
        normalized = None if projection_map is None else {k: k for k in projection_map}

        clone._projection_map = self._combine_projection_map(
            self._projection_map, normalized
        )

        return clone


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


class _GuardedConstructionMeta(abc.ABCMeta):
    """Constructs every datasource with unpickling of untrusted bytes refused.

    Datasource constructors often list, sample or open the files they read, so
    ``SomeDatasource(...)`` runs under the same guard as ``get_read_tasks``.
    Based on ``ABCMeta`` so subclasses that also inherit ``ABC`` don't conflict.
    Unpickling a datasource doesn't call the class, so it isn't affected.
    """

    def __call__(cls, *args, **kwargs):
        with forbid_untrusted_unpickling():
            return super().__call__(*args, **kwargs)


@PublicAPI
class Datasource(
    _DatasourceProjectionPushdownMixin,
    _DatasourcePredicatePushdownMixin,
    metaclass=_GuardedConstructionMeta,
):
    """Interface for defining a custom :class:`~ray.data.Dataset` datasource.

    User may subclass this class to implement a custom datasource. The subclass should
    implement :meth:`.get_read_tasks` and
    :meth:`.estimate_inmemory_data_size` to read the data and estimate the in-memory data size, respectively.

    To read a datasource into a dataset, use :meth:`~ray.data.read_datasource`.

    ``Datasource`` has a metaclass that constructs instances with unpickling
    forbidden. A subclass that also inherits from a class with another
    metaclass needs a metaclass derived from both, for example
    ``class Meta(type(Datasource), type(Other))``.

    Example:
        >>> from ray.data.context import DataContext
        >>> class MyDatasource(Datasource):
        ...     def __init__(self, num_rows: int = 100):
        ...         super().__init__()
        ...         self.num_rows = num_rows
        ...     def get_read_tasks(
        ...         self,
        ...         parallelism: int,
        ...         per_task_row_limit: int | None = None,
        ...         data_context: DataContext | None = None,
        ...     ) -> List["ReadTask"]:
        ...         # Split num_rows across parallelism tasks
        ...         rows_per_task = self.num_rows // parallelism
        ...         return [
        ...             ReadTask(
        ...                 lambda: [pa.Table.from_pydict({"data": range(rows_per_task)})],
        ...                 BlockMetadata(rows_per_task, rows_per_task * 8, None, None),
        ...             ) for _ in range(parallelism)
        ...         ]
        ...     def estimate_inmemory_data_size(self) -> Optional[int]:
        ...         # Return total size for all data (independent of parallelism)
        ...         return self.num_rows * 8
        >>> ds = MyDatasource(num_rows=100)
        >>> tasks = ds.get_read_tasks(parallelism=5)
        >>> len(tasks) == 5
        True
        >>> tasks[0].metadata.num_rows == 20
        True
        >>> ds.estimate_inmemory_data_size() == sum(t.metadata.size_bytes for t in tasks)
        True
    """  # noqa: E501

    def __init__(self):
        """Initialize the datasource and its mixins."""
        _DatasourcePredicatePushdownMixin.__init__(self)

    @final
    def guarded_get_read_tasks(
        self, parallelism: int, **kwargs: Any
    ) -> List["ReadTask"]:
        """Call :meth:`~ray.data.Datasource.get_read_tasks` with unpickling refused.

        Ray Data plans reads through this method, never ``get_read_tasks``
        directly, so datasource code that parses external bytes can't unpickle
        them. Implement :meth:`~ray.data.Datasource.get_read_tasks`; don't override
        this method.

        Args:
            parallelism: The requested read parallelism.
            **kwargs: Passed through to :meth:`~ray.data.Datasource.get_read_tasks`.

        Returns:
            The read tasks returned by :meth:`~ray.data.Datasource.get_read_tasks`.
        """
        with forbid_untrusted_unpickling():
            return self.get_read_tasks(parallelism, **kwargs)

    @final
    def guarded_estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate the in-memory data size with unpickling refused.

        Ray Data calls this method, never ``estimate_inmemory_data_size``
        directly. Implement
        :meth:`~ray.data.Datasource.estimate_inmemory_data_size`; don't override
        this method.

        Returns:
            The estimate returned by
            :meth:`~ray.data.Datasource.estimate_inmemory_data_size`.
        """
        with forbid_untrusted_unpickling():
            return self.estimate_inmemory_data_size()

    @Deprecated
    def create_reader(self, **read_args) -> "Reader":
        """Deprecated: Implement :meth:`~ray.data.Datasource.get_read_tasks` and
        :meth:`~ray.data.Datasource.estimate_inmemory_data_size` instead.
        """
        return _LegacyDatasourceReader(self, **read_args)

    @Deprecated
    def prepare_read(self, parallelism: int, **read_args) -> List["ReadTask"]:
        """Deprecated: Implement :meth:`~ray.data.Datasource.get_read_tasks` and
        :meth:`~ray.data.Datasource.estimate_inmemory_data_size` instead.
        """
        raise NotImplementedError

    def get_name(self) -> str:
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.

        Returns:
            The datasource name. The default implementation returns the class name
            without its ``Datasource`` suffix.
        """
        name = type(self).__name__
        datasource_suffix = "Datasource"
        if name.endswith(datasource_suffix):
            name = name[: -len(datasource_suffix)]
        return name

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size, or None if unknown.

        Note that the in-memory data size may be larger than the on-disk data size.

        Returns:
            The estimated in-memory size in bytes, or ``None`` if it's unknown.
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
        """Return True if the datasource should create a legacy reader"""

        has_implemented_get_read_tasks = (
            type(self).get_read_tasks is not Datasource.get_read_tasks
        )
        has_implemented_estimate_inmemory_data_size = (
            type(self).estimate_inmemory_data_size
            is not Datasource.estimate_inmemory_data_size
        )
        # False when both get_read_tasks and estimate_inmemory_data_size are implemented
        return not (
            has_implemented_get_read_tasks
            and has_implemented_estimate_inmemory_data_size
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

    # See ``Datasource.guarded_get_read_tasks``.
    @final
    def guarded_get_read_tasks(
        self, parallelism: int, **kwargs: Any
    ) -> List["ReadTask"]:
        with forbid_untrusted_unpickling():
            return self.get_read_tasks(parallelism, **kwargs)

    @final
    def guarded_estimate_inmemory_data_size(self) -> Optional[int]:
        with forbid_untrusted_unpickling():
            return self.estimate_inmemory_data_size()

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


class _DuckTypedReader(Reader):
    """Gives a legacy reader that doesn't subclass ``Reader`` the guarded entry points.

    Deprecated ``Datasource.create_reader`` implementations may return any object
    with ``get_read_tasks`` and ``estimate_inmemory_data_size``.
    """

    def __init__(self, reader: Any):
        self._reader = reader

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._reader.estimate_inmemory_data_size()

    def get_read_tasks(self, parallelism: int, **kwargs: Any) -> List["ReadTask"]:
        return self._reader.get_read_tasks(parallelism, **kwargs)

    def __getattr__(self, name: str) -> Any:
        if name == "_reader":
            # Not set yet, e.g. while unpickling.
            raise AttributeError(name)
        return getattr(self._reader, name)


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
