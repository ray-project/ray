import itertools
from typing import Iterable, List

from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.anyscale.data.checkpoint.interfaces import (
    CheckpointBackend,
    CheckpointConfig,
    CheckpointFilter,
    CheckpointWriter,
    InvalidCheckpointingConfig,
    InvalidCheckpointingOperators,
)
from ray.data import DataContext
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    BlockMapTransformFn,
    MapTransformFn,
    MapTransformFnDataType,
)
from ray.data._internal.logical.interfaces import Plan, Rule
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.write_operator import Write
from ray.data.block import Block, BlockAccessor, DataBatch
from ray.data.datasource.datasink import Datasink


def filter_checkpointed_rows_for_blocks(
    blocks: Iterable[Block], _: "TaskContext"
) -> Iterable[Block]:
    """For each block, filter rows that have already been checkpointed
    and yield the resulting block."""
    ckpt_filter = CheckpointFilter.create_checkpoint_filter()

    for block in blocks:
        block = ckpt_filter.generate_id_column_for_block(block)
        filtered_block = ckpt_filter.filter_rows_for_block(block)
        ba = BlockAccessor.for_block(filtered_block)
        if ba.num_rows() > 0:
            yield filtered_block


def filter_checkpointed_rows_for_batches(
    batches: Iterable[DataBatch],
    _: TaskContext,
) -> Iterable[DataBatch]:
    """For each batch, filter rows that have already been checkpointed
    and yield the resulting batches."""
    ckpt_filter = CheckpointFilter.create_checkpoint_filter()

    for batch in batches:
        batch = ckpt_filter.generate_id_column_for_batch(batch)
        filtered_batch = ckpt_filter.filter_rows_for_batch(batch)
        yield filtered_batch


class InsertCheckpointingLayerRule(Rule):
    """When row-based checkpointing is enabled, this rule
    modifies the DAG in-place in order to inject the code which
    skips already checkpointed rows during the read step.

    It is required that this is done at the PhysicalPlan optimizer,
    and cannot be done at the LogicalPlan optimizer step,
    because we need to insert the MapTransformer which is only generated
    upon translating the logical Read/ReadFile op -> physical MapOperator."""

    ALLOWED_PHYSICAL_OPS = (MapOperator,)

    def apply(self, plan: Plan) -> Plan:
        assert isinstance(
            plan._context, DataContext
        ), f"Invalid DataContext found: {type(plan._context)}"
        config = plan._context.checkpoint_config

        if not config:
            return plan

        # Set the boolean to False before we check the DAG for any
        # operators which indicate we should temporarily skip checkpointing.
        # For example, if the user is calling `ds.schema()` or `ds.count()`,
        # this should return information for the full dataset, and skip
        # checkpointing mechanisms (both filtering and writing).
        plan._context._skip_checkpoint_temp = False

        if not config.enabled:
            # Checkpointing is not enabled, simply return the original plan.
            return plan

        self._check_valid_checkpoint_config(config)
        plan = self._insert_write_checkpoint(plan, config)

        # If the plan doesn't terminate in a `Write` op,
        # skip inserting the checkpoint write process.
        if not plan._context._skip_checkpoint_temp:
            plan = self._insert_read_filter_checkpoint(plan)
        return plan

    def _check_valid_checkpoint_config(self, config: CheckpointConfig):
        if not isinstance(config.backend, CheckpointBackend):
            raise InvalidCheckpointingConfig(
                f"{config.backend} is not a valid backend for row-based checkpointing. "
                f"Available options: {[backend.value for backend in CheckpointBackend]}"
            )
        if config.id_col is None:
            raise InvalidCheckpointingConfig(
                "Checkpoint ID column is required for row-based checkpointing, but "
                "none was configured in `DataContext.checkpoint_config.id_col`."
            )

    def _insert_write_checkpoint(self, plan: Plan, config: CheckpointConfig):
        """Check that the final operator is a Write op, and insert
        the MapTransformFn which writes the checkpoint file."""
        sink_physical_op = plan.dag
        assert isinstance(sink_physical_op, PhysicalOperator)
        sink_logical_op = sink_physical_op._logical_operators[0]

        if not isinstance(sink_logical_op, Write):
            # The final op is not a Write op, so we skip checkpointing
            # and return the original plan.
            plan._context._skip_checkpoint_temp = True
            return plan

        datasink = sink_logical_op._datasink_or_legacy_datasource
        if not isinstance(datasink, Datasink):
            raise InvalidCheckpointingOperators(
                f"To enable row-based checkpointing, Write operation must use a "
                f"Datasink and not a legacy Datasource, but got: "
                f"{type(datasink)}"
            )

        checkpoint_writer = CheckpointWriter.create_checkpoint_writer()

        # MapTransformFn for writing checkpoint files after write completes.
        def write_checkpoint_for_block(
            blocks: Iterable[Block], ctx: TaskContext
        ) -> Iterable[Block]:
            it1, it2 = itertools.tee(blocks, 2)
            for block in it1:
                ba = BlockAccessor.for_block(block)
                checkpoint_writer.write_block_checkpoint(ba)

            return list(it2)

        # Insert the MapTransformFn into the physical MapOperator
        # created from logical Write op.
        assert isinstance(sink_physical_op, MapOperator), type(sink_physical_op)
        transform_fns: List[
            MapTransformFn
        ] = sink_physical_op._map_transformer.get_transform_fns().copy()

        # Check that `transform_fns` are compatible with `write_checkpoint_for_block`.
        assert len(transform_fns) >= 2, transform_fns
        assert transform_fns[0].output_type == MapTransformFnDataType.Block
        assert transform_fns[1].input_type == MapTransformFnDataType.Block

        # Insert the MapTransform directly after read transform:
        # BlockMapTransformFn(write_fn)
        # -> BlockMaptransformFn(write_checkpoint_for_block)
        # -> BlockMaptransformFn(write_stats_fn)
        transform_fns.insert(1, BlockMapTransformFn(write_checkpoint_for_block))
        sink_physical_op._map_transformer.set_transform_fns(transform_fns)

        return plan

    def _insert_read_filter_checkpoint(self, plan: Plan) -> Plan:
        # Traverse the DAG and find the Read or ReadFiles op.
        physical_op = plan.dag
        assert isinstance(physical_op, PhysicalOperator)
        logical_op = physical_op._logical_operators[0]

        while physical_op.input_dependencies:
            # Check that the physical operator is valid for checkpointing.
            op_is_eligible = False
            for allowed_op in self.ALLOWED_PHYSICAL_OPS:
                if isinstance(physical_op, allowed_op):
                    op_is_eligible = True
                    break
            if not op_is_eligible:
                raise InvalidCheckpointingOperators(
                    f"Only Map-type operators (Read, Map, Write) are supported "
                    f"for checkpointing, but found an invalid operator: "
                    f"`{type(physical_op)}` (logical operator: `{type(logical_op)}`)"
                )

            if len(physical_op.input_dependencies) != 1:
                raise InvalidCheckpointingOperators(
                    f"To enable row-based checkpointing, all operators must have "
                    f"exactly one input and one output dependency. Found an "
                    f"operator {type(physical_op)} with multiple input "
                    f"dependencies: {physical_op.input_dependencies}"
                )

            if isinstance(logical_op, (Read, ReadFiles)):
                break

            physical_op = physical_op.input_dependencies[0]
            logical_op = physical_op._logical_operators[0]

        assert isinstance(physical_op, MapOperator), type(physical_op)
        transform_fns: List[
            MapTransformFn
        ] = physical_op._map_transformer.get_transform_fns().copy()

        # Check that `transform_fns` are compatible with `write_checkpoint_for_block`.
        assert len(transform_fns) >= 2, transform_fns

        # Insert the FilterCheckpointedRows transform
        if isinstance(logical_op, ReadFiles):
            assert transform_fns[1].output_type == MapTransformFnDataType.Batch
            assert transform_fns[2].input_type == MapTransformFnDataType.Batch

            # Insert the MapTransform directly after read_paths transform:
            # BlocksToBatchesMapTransformFn()
            # -> BatchMapTransformFn(read_paths)
            # -> BatchMapTransformFn(filter_checkpointed_rows_for_batches) -> ...
            transform_fns.insert(
                2, BatchMapTransformFn(filter_checkpointed_rows_for_batches)
            )
        elif isinstance(logical_op, Read):
            assert transform_fns[0].output_type == MapTransformFnDataType.Block
            assert transform_fns[1].input_type == MapTransformFnDataType.Block

            # Insert the MapTransform directly after read transform:
            # BlockMapTransformFn(do_read)
            # -> BlockMapTransformFn(filter_checkpointed_rows_for_blocks)
            # -> BlocksToBatchesMapTransformFn() -> ...
            transform_fns.insert(
                1, BlockMapTransformFn(filter_checkpointed_rows_for_blocks)
            )
        else:
            raise Exception(
                f"Expected source op to be (Read, ReadFiles) but "
                f"instead found: `{logical_op}`."
            )
        physical_op._map_transformer.set_transform_fns(transform_fns)

        return plan
