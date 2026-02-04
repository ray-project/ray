import functools
import math
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, List, Optional, Union

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePushdown,
    LogicalOperatorSupportsProjectionPushdown,
    SourceOperator,
)
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.block import (
    BlockMetadata,
    BlockMetadataWithSchema,
)
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, Reader
from ray.data.expressions import Expr

__all__ = [
    "Read",
]


@dataclass(frozen=True, init=False, repr=False)
class Read(
    AbstractMap,
    SourceOperator,
    LogicalOperatorSupportsProjectionPushdown,
    LogicalOperatorSupportsPredicatePushdown,
):
    """Logical operator for read."""

    datasource: Datasource
    datasource_or_legacy_reader: Union[Datasource, Reader]
    parallelism: int
    detected_parallelism: Optional[int]

    def __init__(
        self,
        input_op: Optional[LogicalOperator] = None,
        datasource: Optional[Datasource] = None,
        datasource_or_legacy_reader: Optional[Union[Datasource, Reader]] = None,
        parallelism: Optional[int] = None,
        num_outputs: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        compute: Optional[ComputeStrategy] = None,
        name: Optional[str] = None,
        min_rows_per_bundled_input: Optional[int] = None,
        per_block_limit: Optional[int] = None,
        detected_parallelism: Optional[int] = None,
        can_modify_num_rows: bool = True,
        input_dependencies: Optional[List[LogicalOperator]] = None,
    ):
        assert datasource is not None
        assert datasource_or_legacy_reader is not None
        assert parallelism is not None
        assert input_op is None
        if name is None:
            name = f"Read{datasource.get_name()}"
        if input_dependencies is None:
            input_dependencies = []
        assert not input_dependencies, "Read should not have input dependencies."
        super().__init__(
            name=name,
            input_op=None,
            can_modify_num_rows=can_modify_num_rows,
            num_outputs=num_outputs,
            min_rows_per_bundled_input=min_rows_per_bundled_input,
            ray_remote_args=ray_remote_args,
            ray_remote_args_fn=ray_remote_args_fn,
            compute=compute,
        )
        if per_block_limit is not None:
            object.__setattr__(self, "per_block_limit", per_block_limit)
        object.__setattr__(self, "datasource", datasource)
        object.__setattr__(
            self, "datasource_or_legacy_reader", datasource_or_legacy_reader
        )
        object.__setattr__(self, "parallelism", parallelism)
        object.__setattr__(self, "detected_parallelism", detected_parallelism)

    def output_data(self):
        return None

    def with_detected_parallelism(self, parallelism: int) -> "Read":
        """
        Set the true parallelism that should be used during execution. This
        should be specified by the user or detected by the optimizer.
        """
        return replace(self, detected_parallelism=parallelism)

    def get_detected_parallelism(self) -> Optional[int]:
        """
        Get the true parallelism that should be used during execution.
        """
        return self.detected_parallelism

    def estimated_num_outputs(self) -> Optional[int]:
        return self.num_outputs or self._estimate_num_outputs()

    def infer_metadata(self) -> BlockMetadata:
        """A ``BlockMetadata`` that represents the aggregate metadata of the outputs.

        This method gets metadata from the read tasks. It doesn't trigger any actual
        execution.
        """
        return self._cached_output_metadata.metadata

    def infer_schema(self):
        return self._cached_output_metadata.schema

    def _estimate_num_outputs(self) -> Optional[int]:
        metadata = self._cached_output_metadata.metadata

        target_max_block_size = DataContext.get_current().target_max_block_size

        # In either case of
        #   - Total byte-size estimate not available
        #   - Target max-block-size not being configured
        #
        # We fallback to estimating number of outputs to be equivalent to the
        # number of input files being read (if any)
        if metadata.size_bytes is None or target_max_block_size is None:
            # NOTE: If there's no input files specified, return the count (could be 0)
            return (
                len(metadata.input_files) if metadata.input_files is not None else None
            )

        # Otherwise, estimate total number of blocks from estimated total
        # byte size
        return math.ceil(metadata.size_bytes / target_max_block_size)

    @functools.cached_property
    def _cached_output_metadata(self) -> "BlockMetadataWithSchema":
        # Legacy datasources might not implement `get_read_tasks`.
        if self.datasource.should_create_reader:
            empty_meta = BlockMetadata(None, None, None, None)
            return BlockMetadataWithSchema(metadata=empty_meta, schema=None)

        # HACK: Try to get a single read task to get the metadata.
        read_tasks = self.datasource.get_read_tasks(1)
        if len(read_tasks) == 0:
            # If there are no read tasks, the dataset is probably empty.
            empty_meta = BlockMetadata(None, None, None, None)
            return BlockMetadataWithSchema(metadata=empty_meta, schema=None)

        # `get_read_tasks` isn't guaranteed to return exactly one read task.
        metadata = [read_task.metadata for read_task in read_tasks]

        if all(meta.num_rows is not None for meta in metadata):
            num_rows = sum(meta.num_rows for meta in metadata)
            original_num_rows = num_rows
            # Apply per-block limit if set
            if self.per_block_limit is not None:
                num_rows = min(num_rows, self.per_block_limit)
        else:
            num_rows = None
            original_num_rows = None

        if all(meta.size_bytes is not None for meta in metadata):
            size_bytes = sum(meta.size_bytes for meta in metadata)
            # Pro-rate the byte size if we applied a row limit
            if (
                self.per_block_limit is not None
                and original_num_rows is not None
                and original_num_rows > 0
            ):
                size_bytes = int(size_bytes * (num_rows / original_num_rows))
        else:
            size_bytes = None

        input_files = []
        for meta in metadata:
            if meta.input_files is not None:
                input_files.extend(meta.input_files)

        meta = BlockMetadata(
            num_rows=num_rows,
            size_bytes=size_bytes,
            input_files=input_files,
            exec_stats=None,
        )
        schemas = [
            read_task.schema for read_task in read_tasks if read_task.schema is not None
        ]
        from ray.data._internal.util import unify_schemas_with_validation

        schema = None
        if schemas:
            schema = unify_schemas_with_validation(schemas)
        return BlockMetadataWithSchema(metadata=meta, schema=schema)

    def supports_projection_pushdown(self) -> bool:
        return self.datasource.supports_projection_pushdown()

    def get_projection_map(self) -> Optional[Dict[str, str]]:
        return self.datasource.get_projection_map()

    def apply_projection(
        self,
        projection_map: Optional[Dict[str, str]],
    ) -> "Read":
        projected_datasource = self.datasource.apply_projection(projection_map)
        return replace(
            self,
            datasource=projected_datasource,
            datasource_or_legacy_reader=projected_datasource,
        )

    def get_column_renames(self) -> Optional[Dict[str, str]]:
        return self.datasource.get_column_renames()

    def supports_predicate_pushdown(self) -> bool:
        return self.datasource.supports_predicate_pushdown()

    def get_current_predicate(self) -> Optional[Expr]:
        return self.datasource.get_current_predicate()

    def apply_predicate(self, predicate_expr: Expr) -> "Read":
        predicated_datasource = self.datasource.apply_predicate(predicate_expr)
        return replace(
            self,
            datasource=predicated_datasource,
            datasource_or_legacy_reader=predicated_datasource,
        )
