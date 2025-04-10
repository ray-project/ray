import logging
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional

from ray.anyscale.data._internal.logical.operators.list_files_operator import (
    PATH_COLUMN_NAME,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    BlocksToBatchesMapTransformFn,
    BuildOutputBlocksMapTransformFn,
    MapTransformer,
    MapTransformFn,
    MapTransformFnCategory,
    MapTransformFnDataType,
)
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import Block, BlockType, DataBatch
from ray.data.context import DataContext

if TYPE_CHECKING:
    import pyarrow.dataset

logger = logging.getLogger(__name__)


class FilterMapTransformFn(MapTransformFn):
    """A MapTransformFn that filters input blocks."""

    def __init__(self, filter_expr: "pyarrow.dataset.Expression"):
        self._filter_expr = filter_expr
        super().__init__(
            MapTransformFnDataType.Block,
            MapTransformFnDataType.Block,
            MapTransformFnCategory.DataProcess,
        )

    def __call__(self, blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        for block in blocks:
            block = TableBlockAccessor.normalize_block_types([block], BlockType.ARROW)[
                0
            ]
            yield block.filter(self._filter_expr)

    def __repr__(self) -> str:
        return f"FilterMapTransformFn(filter_expr={self._filter_expr})"


def plan_read_files_op(
    op: ReadFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    assert len(physical_children) == 1
    input_op = physical_children[0]

    #
    # NOTE: Avoid capturing operators in closures!
    #
    columns: Optional[List[str]] = op.columns
    columns_rename_map: Optional[Dict[str, str]] = op.columns_rename

    filter_expr = op.filter_expr

    fs = op.filesystem
    reader = op.reader

    def read_paths(blocks: Iterable[Block], _: TaskContext) -> Iterable[DataBatch]:
        import pyarrow as pa

        for block in blocks:
            assert isinstance(block, pa.Table), type(block)
            paths = block[PATH_COLUMN_NAME].to_pylist()
            # For some readers, we need to filter the rows in-memory.
            yield from reader.read_paths(
                paths,
                columns=columns,
                columns_rename=columns_rename_map,
                filter_expr=filter_expr,
                filesystem=fs,
            )

    transform_fns: List[MapTransformFn] = [
        BlocksToBatchesMapTransformFn(batch_format=None),
        BatchMapTransformFn(read_paths),
        BuildOutputBlocksMapTransformFn.for_batches(),
    ]

    # Operator fusion *should* take care of the in-memory filtering
    # instead - but needs https://github.com/anyscale/rayturbo/pull/881
    if op.filter_expr is not None and not op.reader.supports_predicate_pushdown():
        transform_fns.append(FilterMapTransformFn(op.filter_expr))

    map_transformer = MapTransformer(transform_fns)

    return MapOperator.create(
        map_transformer,
        input_op,
        data_context,
        name="ReadFiles",
        target_max_block_size=None,
        compute_strategy=TaskPoolStrategy(op.concurrency),
        supports_fusion=False,
        ray_remote_args=op.ray_remote_args,
    )
