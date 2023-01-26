from typing import Any, Callable, Dict, List, Optional, Tuple

from ray.data._internal.block_list import BlockList
from ray.data._internal.compute import BlockTransform
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.logical.interfaces import LogicalOperator

from ray.data._internal.stats import StatsDict


class AbstractAllToAll(LogicalOperator):
    """Abstract class for logical operators should be converted to physical
    AllToAllOperator.
    """

    def __init__(
        self,
        name: str,
        input_op: LogicalOperator,
        fn: Callable[[BlockList, bool, Callable, dict], Tuple[BlockList, dict]],
        num_blocks: Optional[int] = None,
        supports_block_udf: bool = False,
        block_udf: Optional[BlockTransform] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            name: Name for this operator. This is the name that will appear when
                inspecting the logical plan of a Dataset.
            input_op: The operator preceding this operator in the plan DAG. The outputs
                of `input_op` will be the inputs to this operator.
            fn: The transform function to apply to all inputs block to produce output
                blocks.
            num_blocks: The number of blocks outputted by this operator.
            supports_block_udf: Whether this operator supports UDF to be called inside
                `fn`.
            block_udf: The UDF to be called inside `fn`.
            ray_remote_args: Args to provide to ray.remote.
        """
        super().__init__(name, [input_op])
        self._fn = fn
        self._num_blocks = num_blocks
        self._supports_block_udf = supports_block_udf
        self._block_udf = block_udf
        self._ray_remote_args = ray_remote_args or {}


def plan_all_to_all_op(
    op: AbstractAllToAll,
    input_physical_dag: PhysicalOperator
) -> AllToAllOperator:
    """Get the corresponding physical operators DAG for AbstractAllToAll operators."""
    fn = op._fn
    block_udf = op._block_udf
    remote_args = op._ray_remote_args
    op_name = op.name

    def bulk_fn(refs: List[RefBundle]) -> Tuple[List[RefBundle], StatsDict]:
        input_owned = all(b.owns_blocks for b in refs)
        # NOTE: Not use isinstance(op, RandomizeBlocks) here to avoid circular
        # import.
        if op_name == "RandomizeBlocks":
            output_owned = input_owned  # Passthrough ownership hack.
        else:
            output_owned = True

        # Get BlockList from List[RefBundle].
        blocks, metadata = [], []
        for ref_bundle in refs:
            for block, meta in ref_bundle.blocks:
                blocks.append(block)
                metadata.append(meta)
        block_list = BlockList(blocks, metadata, owned_by_consumer=input_owned)

        # Call function to process BlockList.
        block_list, stats_dict = fn(block_list, input_owned, block_udf, remote_args)

        # Get List[RefBundle] from BlockList.
        output = []
        for block, meta in block_list.iter_blocks_with_metadata():
            output.append(RefBundle([(block, meta)], owns_blocks=output_owned))

        if not stats_dict:
            stats_dict = {op_name: block_list.get_metadata()}
        return output, stats_dict

    return AllToAllOperator(
        bulk_fn,
        input_physical_dag,
        num_outputs=op._num_blocks,
        name=op_name,
    )
