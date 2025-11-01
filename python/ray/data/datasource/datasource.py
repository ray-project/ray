from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional

import numpy as np

from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockMetadata, Schema
from ray.data.datasource.util import _iter_sliced_blocks
from ray.data.expressions import Expr
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI


@dataclass
class ProjectionProcessingResult:
    """Result of processing a projection with existing column renames.

    Attributes:
        rebound_columns: Column names translated to original/storage space
        filtered_rename_map: Rename map filtered to only selected columns
    """

    rebound_columns: Optional[List[str]]
    filtered_rename_map: Optional[Dict[str, str]]


class _DatasourceProjectionPushdownMixin:
    """Mixin for reading operators supporting projection pushdown"""

    def supports_projection_pushdown(self) -> bool:
        """Returns ``True`` in case ``Datasource`` supports projection operation
        being pushed down into the reading layer"""
        return False

    def get_current_projection(self) -> Optional[List[str]]:
        """Retrurns current projection"""
        return None

    def get_column_renames(self) -> Optional[Dict[str, str]]:
        """Return the column renames applied to this datasource.

        Returns:
            A dictionary mapping old column names to new column names,
            or None if no renaming has been applied.
        """
        return None

    @staticmethod
    def _apply_column_mapping(
        columns: List[str],
        mapping: Dict[str, str],
    ) -> List[str]:
        """Apply a column name mapping, keeping unmapped columns as-is.

        Args:
            columns: List of column names to map
            mapping: Dictionary mapping from one name to another

        Returns:
            List of column names with mapping applied
        """
        return [mapping.get(col, col) for col in columns]

    @staticmethod
    def _process_projection_with_renames(
        columns: Optional[List[str]],
        existing_rename_map: Optional[Dict[str, str]],
    ) -> ProjectionProcessingResult:
        """Process column projection accounting for existing renames.

        When columns have been renamed, this handles:
        1. Translating column names from renamed space back to original names
        2. Filtering the rename map to only include selected columns

        Args:
            columns: Columns to select (in renamed space if rename_map exists)
            existing_rename_map: Current rename mapping (original -> renamed)

        Returns:
            ProjectionProcessingResult containing:
            - rebound_columns: Column names in original/storage space
            - filtered_rename_map: Rename map containing only selected columns
        """
        if not existing_rename_map or columns is None:
            # No renames to process
            return ProjectionProcessingResult(
                rebound_columns=columns,
                filtered_rename_map=existing_rename_map,
            )

        # Create reverse mapping: renamed -> original
        reverse_map = {
            renamed: original for original, renamed in existing_rename_map.items()
        }

        # Translate columns to original space
        rebound_columns = _DatasourceProjectionPushdownMixin._apply_column_mapping(
            columns, reverse_map
        )

        # Filter rename map to only selected columns (where renamed name is in columns)
        filtered_rename_map = {
            original: renamed
            for original, renamed in existing_rename_map.items()
            if renamed in columns
        }

        return ProjectionProcessingResult(
            rebound_columns=rebound_columns,
            filtered_rename_map=filtered_rename_map or None,
        )

    @staticmethod
    def _combine_rename_map(
        prev_column_rename_map: Optional[Dict[str, str]],
        new_column_rename_map: Optional[Dict[str, str]],
    ) -> Optional[Dict[str, str]]:
        """Combine two column rename maps, handling transitive renames.

        Args:
            prev_column_rename_map: Previous rename map (original -> renamed)
            new_column_rename_map: New rename map to apply (original -> renamed)

        Returns:
            Combined rename map with transitive mappings collapsed
        """
        from ray.data._internal.collections import collapse_transitive_map

        if not prev_column_rename_map:
            combined = new_column_rename_map
        elif not new_column_rename_map:
            combined = prev_column_rename_map
        else:
            combined = prev_column_rename_map | new_column_rename_map

        return collapse_transitive_map(combined) if combined else None

    def apply_projection(
        self,
        columns: Optional[List[str]],
        column_rename_map: Optional[Dict[str, str]],
    ) -> "Datasource":
        """Apply a projection to this datasource.

        Args:
            columns: List of columns to select, or None to select all columns.
            column_rename_map: Dictionary mapping old column names to new names.

        Returns:
            A new datasource instance with the projection applied.
        """
        import copy

        clone = copy.copy(self)

        # Process projection with existing renames
        result = self._process_projection_with_renames(
            columns, self._data_columns_rename_map
        )

        # Combine projections (now in original column space)
        clone._data_columns = self._combine_projection(
            self._data_columns, result.rebound_columns
        )

        # Combine rename maps
        clone._data_columns_rename_map = self._combine_rename_map(
            result.filtered_rename_map, column_rename_map
        )

        # Hook for datasource-specific cleanup
        self._post_apply_projection(clone)

        return clone

    @staticmethod
    def _combine_projection(
        prev_projected_cols: Optional[List[str]],
        new_projected_cols: Optional[List[str]],
    ) -> Optional[List[str]]:
        """Combine two projections, validating that new projection is valid.

        Args:
            prev_projected_cols: Previous projection, or None for all columns
            new_projected_cols: New projection, or None for all columns

        Returns:
            Combined projection, or None for all columns
        """
        # NOTE: None projection carries special meaning of all columns being selected
        if prev_projected_cols is None:
            return new_projected_cols
        elif new_projected_cols is None:
            # Retain original projection
            return prev_projected_cols
        else:
            illegal_refs = [
                col for col in new_projected_cols if col not in prev_projected_cols
            ]

            if illegal_refs:
                raise ValueError(
                    f"New projection {new_projected_cols} references non-existent columns "
                    f"(existing projection {prev_projected_cols})"
                )

            return new_projected_cols

    def _post_apply_projection(self, clone: "Datasource") -> None:
        """Hook for datasource-specific cleanup after applying projection.

        Override in subclasses for cleanup like invalidating caches.
        Default implementation does nothing.

        Args:
            clone: The cloned datasource instance with projection already applied
        """
        pass

    @staticmethod
    def _apply_rename(
        table: "Schema",
        column_rename_map: Optional[Dict[str, str]],
    ) -> "Schema":
        """Apply column renaming to a PyArrow table.

        Args:
            table: PyArrow table to rename
            column_rename_map: Mapping from old column names to new names

        Returns:
            Table with renamed columns
        """
        if not column_rename_map:
            return table

        new_names = _DatasourceProjectionPushdownMixin._apply_column_mapping(
            table.schema.names, column_rename_map
        )
        return table.rename_columns(new_names)


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
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List["ReadTask"]:
        """Execute the read and return read tasks.

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should equal to this value if possible.
            per_task_row_limit: The per-task row limit for the read tasks.
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
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List["ReadTask"]:
        """Execute the read and return read tasks.

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should equal to this value if possible.
            per_task_row_limit: The per-task row limit for the read tasks.

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
