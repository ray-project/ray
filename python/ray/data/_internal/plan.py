import copy
import itertools
from typing import TYPE_CHECKING, Iterator, Optional, Tuple, Union

import ray
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.data._internal.block_list import BlockList
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.lazy_block_list import LazyBlockList
from ray.data._internal.logical.interfaces.logical_operator import LogicalOperator
from ray.data._internal.logical.operators.from_operators import AbstractFrom
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.stats import DatasetStats, DatasetStatsSummary
from ray.data._internal.util import create_dataset_tag, unify_block_metadata_schema
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef
from ray.util.debug import log_once

if TYPE_CHECKING:
    import pyarrow

    from ray.data._internal.execution.interfaces import Executor
    from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan


# Scheduling strategy can be inherited from prev stage if not specified.
INHERITABLE_REMOTE_ARGS = ["scheduling_strategy"]


logger = DatasetLogger(__name__)


class ExecutionPlan:
    """A lazy execution plan for a Dataset."""

    # Implementation Notes:
    #
    # This lazy execution plan takes in an input block list and builds up a chain of
    # BlockList --> BlockList stages. When execution is triggered, it tries to fuse
    # together stages in order to reduce Ray task overhead and data copies.
    #
    # Internally, the execution plan holds two block lists:
    #   * _in_blocks: The (possibly lazy) input block list.
    #   * _snapshot_blocks: A snapshot of a computed block list, where this snapshot
    #     is the cached output of executing some prefix in the stage chain.
    #
    # The stages in this execution plan are partitioned into two subchains: before the
    # snapshot and after the snapshot. When the snapshot exists from a previous
    # execution, any future executions will only have to execute the "after the
    # snapshot" subchain, using the snapshot as the input to that subchain.

    def __init__(
        self,
        in_blocks: BlockList,
        stats: DatasetStats,
        *,
        run_by_consumer: bool,
        data_context: Optional[DataContext] = None,
    ):
        """Create a plan with no transformation stages.

        Args:
            in_blocks: Base list of blocks.
            stats: Stats for the base blocks.
            dataset_uuid: Dataset's UUID.
            run_by_consumer: Whether this plan is invoked to run by the consumption
            APIs (e.g. .iter_batches()).
        """
        self._in_blocks = in_blocks
        self._in_stats = stats
        # A computed snapshot of some prefix of stages.
        self._snapshot_blocks = None
        self._snapshot_operator: Optional[LogicalOperator] = None
        self._snapshot_stats = None
        # Cache of optimized stages.
        self._last_optimized_stages = None
        # Cached schema.
        self._schema = None
        # Set when a Dataset is constructed with this plan
        self._dataset_uuid = None

        self._run_by_consumer = run_by_consumer
        self._dataset_name = None

        if data_context is None:
            # Snapshot the current context, so that the config of Datasets is always
            # determined by the config at the time it was created.
            self._context = copy.deepcopy(DataContext.get_current())
        else:
            self._context = data_context

    def __repr__(self) -> str:
        return (
            f"ExecutionPlan("
            f"dataset_uuid={self._dataset_uuid}, "
            f"run_by_consumer={self._run_by_consumer}, "
            f"in_blocks={self._in_blocks}, "
            f"snapshot_operator={self._snapshot_operator}"
            f"snapshot_blocks={self._snapshot_blocks})"
        )

    def get_plan_as_string(self, classname: str) -> str:
        """Create a cosmetic string representation of this execution plan.

        Returns:
            The string representation of this execution plan.
        """
        # NOTE: this is used for Dataset.__repr__ to give a user-facing string
        # representation. Ideally ExecutionPlan.__repr__ should be replaced with this
        # method as well.

        # Do not force execution for schema, as this method is expected to be very
        # cheap.
        plan_str = ""
        plan_max_depth = 0
        dataset_blocks = None
        if (
            self._snapshot_blocks is None
            or self._snapshot_operator != self._logical_plan.dag
        ):

            def generate_logical_plan_string(
                op: LogicalOperator,
                curr_str: str = "",
                depth: int = 0,
            ):
                """Traverse (DFS) the LogicalPlan DAG and
                return a string representation of the operators."""
                if isinstance(op, (Read, InputData, AbstractFrom)):
                    return curr_str, depth

                curr_max_depth = depth
                op_name = op.name
                if depth == 0:
                    curr_str += f"{op_name}\n"
                else:
                    trailing_space = " " * ((depth - 1) * 3)
                    curr_str += f"{trailing_space}+- {op_name}\n"

                for input in op.input_dependencies:
                    curr_str, input_max_depth = generate_logical_plan_string(
                        input, curr_str, depth + 1
                    )
                    curr_max_depth = max(curr_max_depth, input_max_depth)
                return curr_str, curr_max_depth

            # generate_logical_plan_string(self._logical_plan.dag)
            plan_str, plan_max_depth = generate_logical_plan_string(
                self._logical_plan.dag
            )

            # Get schema of initial blocks.
            if self.needs_eager_execution():
                # In the case where the plan contains only a Read/From operator,
                # it is cheap to execute it.
                # This allows us to get the most accurate estimates related
                # to the dataset, after applying execution plan optimizer rules
                # (e.g. number of blocks may change based on parallelism).
                self.execute()
            if self._snapshot_blocks is not None:
                schema = self._get_unified_blocks_schema(
                    self._snapshot_blocks, fetch_if_missing=False
                )
                dataset_blocks = self._snapshot_blocks
            else:
                assert self._in_blocks is not None
                schema = self._get_unified_blocks_schema(
                    self._in_blocks, fetch_if_missing=False
                )
                dataset_blocks = self._in_blocks
        else:
            # Get schema of output blocks.
            schema = self.schema(fetch_if_missing=False)
            dataset_blocks = self._snapshot_blocks

        if schema is None:
            schema_str = "Unknown schema"
        elif isinstance(schema, type):
            schema_str = str(schema)
        else:
            schema_str = []
            for n, t in zip(schema.names, schema.types):
                if hasattr(t, "__name__"):
                    t = t.__name__
                schema_str.append(f"{n}: {t}")
            schema_str = ", ".join(schema_str)
            schema_str = "{" + schema_str + "}"
        count = self._get_num_rows_from_blocks_metadata(dataset_blocks)
        if count is None:
            count = "?"
        if dataset_blocks is None:
            num_blocks = "?"
        else:
            num_blocks = dataset_blocks.estimated_num_blocks()
        name_str = (
            "name={}, ".format(self._dataset_name)
            if self._dataset_name is not None
            else ""
        )
        dataset_str = "{}({}num_blocks={}, num_rows={}, schema={})".format(
            classname,
            name_str,
            num_blocks,
            count,
            schema_str,
        )

        # If the resulting string representation fits in one line, use it directly.
        SCHEMA_LINE_CHAR_LIMIT = 80
        MIN_FIELD_LENGTH = 10
        INDENT_STR = " " * 3
        trailing_space = INDENT_STR * plan_max_depth

        if len(dataset_str) > SCHEMA_LINE_CHAR_LIMIT:
            # If the resulting string representation exceeds the line char limit,
            # first try breaking up each `Dataset` parameter into its own line
            # and check if each line fits within the line limit. We check the
            # `schema` param's length, since this is likely the longest string.
            schema_str_on_new_line = f"{trailing_space}{INDENT_STR}schema={schema_str}"
            if len(schema_str_on_new_line) > SCHEMA_LINE_CHAR_LIMIT:
                # If the schema cannot fit on a single line, break up each field
                # into its own line.
                schema_str = []
                for n, t in zip(schema.names, schema.types):
                    if hasattr(t, "__name__"):
                        t = t.__name__
                    col_str = f"{trailing_space}{INDENT_STR * 2}{n}: {t}"
                    # If the field line exceeds the char limit, abbreviate
                    # the field name to fit while maintaining the full type
                    if len(col_str) > SCHEMA_LINE_CHAR_LIMIT:
                        shortened_suffix = f"...: {str(t)}"
                        # Show at least 10 characters of the field name, even if
                        # we have already hit the line limit with the type.
                        chars_left_for_col_name = max(
                            SCHEMA_LINE_CHAR_LIMIT - len(shortened_suffix),
                            MIN_FIELD_LENGTH,
                        )
                        col_str = (
                            f"{col_str[:chars_left_for_col_name]}{shortened_suffix}"
                        )
                    schema_str.append(col_str)
                schema_str = ",\n".join(schema_str)
                schema_str = (
                    "{\n" + schema_str + f"\n{trailing_space}{INDENT_STR}" + "}"
                )
            name_str = (
                f"\n{trailing_space}{INDENT_STR}name={self._dataset_name},"
                if self._dataset_name is not None
                else ""
            )
            dataset_str = (
                f"{classname}("
                f"{name_str}"
                f"\n{trailing_space}{INDENT_STR}num_blocks={num_blocks},"
                f"\n{trailing_space}{INDENT_STR}num_rows={count},"
                f"\n{trailing_space}{INDENT_STR}schema={schema_str}"
                f"\n{trailing_space})"
            )

        if plan_max_depth == 0:
            plan_str += dataset_str
        else:
            plan_str += f"{INDENT_STR * (plan_max_depth - 1)}+- {dataset_str}"
        return plan_str

    def link_logical_plan(self, logical_plan: "LogicalPlan"):
        """Link the logical plan into this execution plan.

        This is used for triggering execution for optimizer code path in this legacy
        execution plan.
        """
        self._logical_plan = logical_plan

    def copy(self) -> "ExecutionPlan":
        """Create a shallow copy of this execution plan.

        This copy can be executed without mutating the original, but clearing the copy
        will also clear the original.

        Returns:
            A shallow copy of this execution plan.
        """
        plan_copy = ExecutionPlan(
            self._in_blocks,
            self._in_stats,
            run_by_consumer=self._run_by_consumer,
            data_context=self._context,
        )
        if self._snapshot_blocks is not None:
            # Copy over the existing snapshot.
            plan_copy._snapshot_blocks = self._snapshot_blocks
            plan_copy._snapshot_operator = self._snapshot_operator
            plan_copy._snapshot_stats = self._snapshot_stats
        plan_copy._dataset_name = self._dataset_name
        return plan_copy

    def deep_copy(self) -> "ExecutionPlan":
        """Create a deep copy of this execution plan.

        This copy can be executed AND cleared without mutating the original.

        Returns:
            A deep copy of this execution plan.
        """
        in_blocks = self._in_blocks
        if isinstance(in_blocks, BlockList):
            in_blocks = in_blocks.copy()
        plan_copy = ExecutionPlan(
            in_blocks,
            copy.copy(self._in_stats),
            run_by_consumer=self._run_by_consumer,
        )
        if self._snapshot_blocks:
            # Copy over the existing snapshot.
            plan_copy._snapshot_blocks = self._snapshot_blocks.copy()
            plan_copy._snapshot_operator = copy.copy(self._snapshot_operator)
            plan_copy._snapshot_stats = copy.copy(self._snapshot_stats)
        plan_copy._dataset_name = self._dataset_name
        return plan_copy

    def initial_num_blocks(self) -> int:
        """Get the estimated number of blocks after applying all plan stages."""
        return self._logical_plan.dag.estimated_num_outputs()

    def schema(
        self, fetch_if_missing: bool = False
    ) -> Union[type, "pyarrow.lib.Schema"]:
        """Get the schema after applying all plan stages.

        Args:
            fetch_if_missing: Whether to execute the plan to fetch the schema.

        Returns:
            The schema of the output dataset.
        """
        from ray.data._internal.logical.operators.all_to_all_operator import (
            RandomizeBlocks,
        )

        if self._schema is not None:
            return self._schema

        if self._snapshot_blocks is None or (
            self._snapshot_operator is not None
            and self._snapshot_operator.output_dependencies
        ):
            # There remain some operators yet to be executed.
            # Even if a schema is already previously known, it may change,
            # so we try executing to get the most updated schema.

            # TODO(swang): There are several other stage types that could
            # inherit the schema or we can compute the schema without having to
            # execute any of the dataset: limit, filter, map_batches for
            # add/drop columns, etc.
            if fetch_if_missing:
                if isinstance(self._logical_plan.dag, RandomizeBlocks):
                    # TODO(ekl): this is a hack to optimize the case where we have a
                    # trailing randomize block stages. That stage has no effect and
                    # so we don't need to execute all blocks to get the schema.

                    randomize_blocks_op = self._logical_plan.dag
                    self._logical_plan._dag = randomize_blocks_op.input_dependencies[0]
                    try:
                        self.execute()
                    finally:
                        self._logical_plan = randomize_blocks_op
                else:
                    self.execute()
            elif self.needs_eager_execution() or (
                isinstance(self._logical_plan.dag, RandomizeBlocks)
                and self.needs_eager_execution(
                    self._logical_plan.dag.input_dependencies[0]
                )
            ):
                # If the plan is input/read only, we execute it, so snapshot has output.
                # If RandomizeBlocks is the last operator preceded by a input/read
                # only plan, we can also execute it (regardless of the fetch_if_missing)
                # since RandomizeBlocksStage is just changing the order of references
                # (hence super cheap). Calling execute does not trigger read tasks.
                self.execute()
            else:
                return None
        # Snapshot is now guaranteed to be the output of the final stage or None.
        blocks = self._snapshot_blocks
        if not blocks:
            return None
        self._schema = self._get_unified_blocks_schema(blocks, fetch_if_missing)
        return self._schema

    def cache_schema(self, schema: Union[type, "pyarrow.lib.Schema"]):
        self._schema = schema

    def _get_unified_blocks_schema(
        self, blocks: BlockList, fetch_if_missing: bool = False
    ) -> Union[type, "pyarrow.lib.Schema"]:
        """Get the unified schema of the blocks.

        Args:
            blocks: the blocks to get schema
            fetch_if_missing: Whether to execute the blocks to fetch the schema.
        """

        # Ensure the first block has schema information available in the metadata.
        # Otherwise, this will trigger computation on the first block
        # for a schema read.
        if isinstance(blocks, LazyBlockList):
            blocks.ensure_metadata_for_first_block()

        metadata = blocks.get_metadata(fetch_if_missing=False)

        unified_schema = unify_block_metadata_schema(metadata)
        if unified_schema is not None:
            return unified_schema
        if not fetch_if_missing:
            return None
        # Synchronously fetch the schema.
        # For lazy block lists, this launches read tasks and fetches block metadata
        # until we find the first valid block schema. This is to minimize new
        # computations when fetching the schema.
        for _, m in blocks.iter_blocks_with_metadata():
            if m.schema is not None and (m.num_rows is None or m.num_rows > 0):
                return m.schema
        return None

    def meta_count(self) -> Optional[int]:
        """Get the number of rows after applying all plan optimizations, if possible.

        This method will never trigger any computation.

        Returns:
            The number of records of the result Dataset, or None.
        """
        if self.needs_eager_execution():
            # If the plan is input/read only, we execute it, so snapshot has output.
            # This applies to newly created dataset. For example, initial dataset
            # from read, and output datasets of Dataset.split().
            self.execute()
        # Snapshot is now guaranteed to be the final block or None.
        return self._get_num_rows_from_blocks_metadata(self._snapshot_blocks)

    def _get_num_rows_from_blocks_metadata(self, blocks: BlockList) -> Optional[int]:
        metadata = blocks.get_metadata() if blocks else None
        if metadata and all(m.num_rows is not None for m in metadata):
            return sum(m.num_rows for m in metadata)
        else:
            return None

    def execute_to_iterator(
        self,
        allow_clear_input_blocks: bool = True,
        force_read: bool = False,
    ) -> Tuple[
        Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
        DatasetStats,
        Optional["Executor"],
    ]:
        """Execute this plan, returning an iterator.

        If the streaming execution backend is enabled, this will use streaming
        execution to generate outputs, otherwise it will fall back to bulk exec.

        Args:
            allow_clear_input_blocks: Whether we should try to clear the input blocks
                for each stage.
            force_read: Whether to force the read stage to fully execute.

        Returns:
            Tuple of iterator over output blocks and the executor.
        """

        # Always used the saved context for execution.
        ctx = self._context

        if not ctx.use_streaming_executor or self.has_computed_output():
            return (
                self.execute(
                    allow_clear_input_blocks, force_read
                ).iter_blocks_with_metadata(),
                self._snapshot_stats,
                None,
            )

        from ray.data._internal.execution.legacy_compat import (
            execute_to_legacy_block_iterator,
        )
        from ray.data._internal.execution.streaming_executor import StreamingExecutor

        metrics_tag = create_dataset_tag(self._dataset_name, self._dataset_uuid)
        executor = StreamingExecutor(copy.deepcopy(ctx.execution_options), metrics_tag)
        block_iter = execute_to_legacy_block_iterator(
            executor,
            self,
            allow_clear_input_blocks=allow_clear_input_blocks,
            dataset_uuid=self._dataset_uuid,
        )
        # Since the generator doesn't run any code until we try to fetch the first
        # value, force execution of one bundle before we call get_stats().
        gen = iter(block_iter)
        try:
            block_iter = itertools.chain([next(gen)], gen)
        except StopIteration:
            pass
        self._snapshot_stats = executor.get_stats()
        return block_iter, self._snapshot_stats, executor

    def execute(
        self,
        allow_clear_input_blocks: bool = True,
        force_read: bool = False,
        preserve_order: bool = False,
    ) -> BlockList:
        """Execute this plan.

        Args:
            allow_clear_input_blocks: Whether we should try to clear the input blocks
                for each stage.
            force_read: Whether to force the read stage to fully execute.
            preserve_order: Whether to preserve order in execution.

        Returns:
            The blocks of the output dataset.
        """

        # Always used the saved context for execution.
        context = self._context

        if not ray.available_resources().get("CPU"):
            if log_once("cpu_warning"):
                logger.get_logger().warning(
                    "Warning: The Ray cluster currently does not have "
                    "any available CPUs. The Dataset job will hang unless more CPUs "
                    "are freed up. A common reason is that cluster resources are "
                    "used by Actors or Tune trials; see the following link "
                    "for more details: "
                    "https://docs.ray.io/en/latest/data/data-internals.html#ray-data-and-tune"  # noqa: E501
                )
        if not self.has_computed_output():
            from ray.data._internal.execution.legacy_compat import (
                _get_initial_stats_from_plan,
                execute_to_legacy_block_list,
                get_legacy_lazy_block_list_read_only,
            )

            if self.is_from_in_memory_only():
                # No need to execute MaterializedDatasets with only an InputData
                # operator, since the data is already materialized. This also avoids
                # recording unnecessary metrics for an empty plan execution.
                blocks = self._in_blocks
                stats = _get_initial_stats_from_plan(self)
            elif self.is_read_only():
                # If the Dataset is read-only, get the LazyBlockList without
                # executing the plan by only fetching metadata available from
                # the input Datasource or Reader without executing its ReadTasks.
                blocks = get_legacy_lazy_block_list_read_only(self)
                stats = _get_initial_stats_from_plan(self)
            else:
                from ray.data._internal.execution.streaming_executor import (
                    StreamingExecutor,
                )

                metrics_tag = create_dataset_tag(self._dataset_name, self._dataset_uuid)
                executor = StreamingExecutor(
                    copy.deepcopy(context.execution_options),
                    metrics_tag,
                )
                blocks = execute_to_legacy_block_list(
                    executor,
                    self,
                    allow_clear_input_blocks=allow_clear_input_blocks,
                    dataset_uuid=self._dataset_uuid,
                    preserve_order=preserve_order,
                )
                stats = executor.get_stats()
                stats_summary_string = stats.to_summary().to_string(
                    include_parent=False
                )
                logger.get_logger(log_to_stdout=context.enable_auto_log_stats).info(
                    stats_summary_string,
                )
            # TODO(ekl) we shouldn't need to set this in the future once we move
            # to a fully lazy execution model, unless .materialize() is used. Th
            # reason we need it right now is since the user may iterate over a
            # Dataset multiple times after fully executing it once.
            if not self._run_by_consumer:
                blocks._owned_by_consumer = False

            # Retrieve memory-related stats from ray.
            reply = get_memory_info_reply(
                get_state_from_address(ray.get_runtime_context().gcs_address)
            )
            if reply.store_stats.spill_time_total_s > 0:
                stats.global_bytes_spilled = int(reply.store_stats.spilled_bytes_total)
            if reply.store_stats.restore_time_total_s > 0:
                stats.global_bytes_restored = int(
                    reply.store_stats.restored_bytes_total
                )

            stats.dataset_bytes_spilled = 0

            def collect_stats(cur_stats):
                stats.dataset_bytes_spilled += cur_stats.extra_metrics.get(
                    "obj_store_mem_spilled", 0
                )
                for parent in cur_stats.parents:
                    collect_stats(parent)

            collect_stats(stats)

            # Set the snapshot to the output of the final stage.
            self._snapshot_blocks = blocks
            self._snapshot_operator = self._logical_plan.dag
            self._snapshot_stats = stats
            self._snapshot_stats.dataset_uuid = self._dataset_uuid

            # In the case of a read-only dataset, we replace the
            # input LazyBlockList with a copy that includes the
            # calculated metadata from initializing the InputDataBuffer.
            if self.is_read_only():
                self._in_blocks = blocks
        if _is_lazy(self._snapshot_blocks) and force_read:
            executed_blocks = self._snapshot_blocks.compute_to_blocklist()
            # After executing the snapshot blocks, get its updated stats.
            # The snapshot blocks after execution will contain the execution stats.
            self._snapshot_stats = self._snapshot_blocks.stats()
            self._snapshot_blocks = executed_blocks
            self._snapshot_operator = self._logical_plan.dag
            # When force-read is enabled, we similarly update self._in_blocks.
            if self.is_read_only():
                self._in_blocks = self._snapshot_blocks
        return self._snapshot_blocks

    def clear_block_refs(self) -> None:
        """Clear all cached block references of this plan, including input blocks.

        This will render the plan un-executable unless the root is a LazyBlockList."""
        self._in_blocks.clear()
        self._clear_snapshot()

    def _clear_snapshot(self) -> None:
        """Clear the snapshot kept in the plan to the beginning state."""
        self._snapshot_blocks = None
        self._snapshot_operator = None
        self._snapshot_stats = None

    def stats(self) -> DatasetStats:
        """Return stats for this plan.

        If the plan isn't executed, an empty stats object will be returned.
        """
        if not self._snapshot_stats:
            return DatasetStats(metadata={}, parent=None)
        return self._snapshot_stats

    def stats_summary(self) -> DatasetStatsSummary:
        return self.stats().to_summary()

    def has_lazy_input(self) -> bool:
        """Return whether this plan has lazy input blocks."""
        return _is_lazy(self._in_blocks)

    def needs_eager_execution(self, root_op: Optional[LogicalOperator] = None) -> bool:
        """Return whether the LogicalPlan corresponding to `root_op`
        should be eagerly executed. By default, the last operator of
        the LogicalPlan is used.

        This is often useful for input/read-only plans,
        where eager execution fetches accurate metadata for the dataset
        without executing the underlying read tasks."""
        if root_op is None:
            root_op = self._logical_plan.dag
        # Since read tasks will not be scheduled until data is consumed or materialized,
        # it is cheap to execute the plan (i.e. run the plan optimizer).
        # In the case where the data is already in-memory (InputData,
        # FromXXX operator), it is similarly also cheap to execute it.
        return self.is_from_in_memory_only(root_op) or self.is_read_only(root_op)

    def is_read_only(self, root_op: Optional[LogicalOperator] = None) -> bool:
        """Return whether the LogicalPlan corresponding to `root_op`
        contains only a Read op. By default, the last operator of
        the LogicalPlan is used."""
        if root_op is None:
            root_op = self._logical_plan.dag
        return isinstance(root_op, Read) and len(root_op.input_dependencies) == 0

    def is_from_in_memory_only(self, root_op: Optional[LogicalOperator] = None) -> bool:
        """Return whether the LogicalPlan corresponding to `root_op`
        contains only a read of already in-memory data (e.g. `FromXXX`
        operators for `from_xxx` APIs, `InputData` operator for
        :class:`~ray.data.MaterializedDataset`). By default, the last operator of
        the LogicalPlan is used."""
        if root_op is None:
            root_op = self._logical_plan.dag
        return (
            isinstance(root_op, (InputData, AbstractFrom))
            and len(root_op.input_dependencies) == 0
        )

    def has_computed_output(self) -> bool:
        """Whether this plan has a computed snapshot for the final stage, i.e. for the
        output of this plan.
        """
        return (
            self._snapshot_blocks is not None
            and not self._snapshot_blocks.is_cleared()
            and self._snapshot_operator == self._logical_plan.dag
        )

    def _run_with_new_execution_backend(self) -> bool:
        """Whether this plan should run with new backend.
        By default, the new execution backend is now fully enabled
        unless configured otherwise by the user."""
        return self._context.new_execution_backend

    def require_preserve_order(self) -> bool:
        """Whether this plan requires to preserve order."""
        from ray.data._internal.logical.operators.all_to_all_operator import Sort
        from ray.data._internal.logical.operators.n_ary_operator import Zip

        for op in self._logical_plan.dag.post_order_iter():
            if isinstance(op, (Zip, Sort)):
                return True
        return False


def _is_lazy(blocks: BlockList) -> bool:
    """Whether the provided block list is lazy."""
    return isinstance(blocks, LazyBlockList)
