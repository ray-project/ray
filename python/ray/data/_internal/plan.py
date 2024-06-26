import copy
import itertools
import logging
from typing import TYPE_CHECKING, Iterator, List, Optional, Tuple, Type, Union

import pyarrow

import ray
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces.logical_operator import LogicalOperator
from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
from ray.data._internal.logical.operators.from_operators import AbstractFrom
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.stats import DatasetStats, DatasetStatsSummary
from ray.data._internal.util import create_dataset_tag, unify_block_metadata_schema
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.exceptions import omit_traceback_stdout
from ray.types import ObjectRef
from ray.util.debug import log_once

if TYPE_CHECKING:

    from ray.data._internal.execution.interfaces import Executor
    from ray.data.dataset import Dataset


# Scheduling strategy can be inherited from prev operator if not specified.
INHERITABLE_REMOTE_ARGS = ["scheduling_strategy"]


logger = logging.getLogger(__name__)


class ExecutionPlan:
    """A lazy execution plan for a Dataset."""

    # Implementation Notes:
    #
    # This lazy execution plan takes in an input block list and builds up a chain of
    # List[BlockRef] --> List[BlockRef] operators. Prior to execution,
    # we apply a set of logical plan optimizations, such as operator fusion,
    # in order to reduce Ray task overhead and data copies.
    #
    # Internally, the execution plan holds two block lists:
    #   * _in_blocks: The (possibly lazy) input block list.
    #   * _snapshot_blocks: A snapshot of a computed block list, where this snapshot
    #     is the cached output of executing some prefix in the operator chain.
    #
    # The operators in this execution plan are partitioned into two subchains:
    # before the snapshot and after the snapshot. When the snapshot exists from a
    # previous execution, any future executions will only have to execute the "after the
    # snapshot" subchain, using the snapshot as the input to that subchain.

    def __init__(
        self,
        stats: DatasetStats,
        *,
        data_context: Optional[DataContext] = None,
    ):
        """Create a plan with no transformation operators.

        Args:
            stats: Stats for the base blocks.
            dataset_uuid: Dataset's UUID.
        """
        self._in_stats = stats
        # A computed snapshot of some prefix of operators and their corresponding
        # output blocks and stats.
        self._snapshot_operator: Optional[LogicalOperator] = None
        self._snapshot_stats = None
        self._snapshot_bundle = None

        # Cached schema.
        self._schema = None
        # Set when a Dataset is constructed with this plan
        self._dataset_uuid = None

        self._dataset_name = None

        self._has_started_execution = False

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
            f"snapshot_operator={self._snapshot_operator}"
        )

    def get_plan_as_string(self, dataset_cls: Type["Dataset"]) -> str:
        """Create a cosmetic string representation of this execution plan.

        Returns:
            The string representation of this execution plan.
        """
        # NOTE: this is used for Dataset.__repr__ to give a user-facing string
        # representation. Ideally ExecutionPlan.__repr__ should be replaced with this
        # method as well.

        from ray.data.dataset import MaterializedDataset

        # Do not force execution for schema, as this method is expected to be very
        # cheap.
        plan_str = ""
        plan_max_depth = 0
        if not self.has_computed_output():

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

            if self._snapshot_bundle is not None:
                # This plan has executed some but not all operators.
                schema = unify_block_metadata_schema(self._snapshot_bundle.metadata)
                count = self._snapshot_bundle.num_rows()
            else:
                # This plan hasn't executed any operators.
                sources = self._logical_plan.sources()
                # TODO(@bveeramani): Handle schemas for n-ary operators like `Union`.
                if len(sources) > 1:
                    # Multiple sources, cannot determine schema.
                    schema = None
                    count = None
                else:
                    assert len(sources) == 1
                    plan = ExecutionPlan(DatasetStats(metadata={}, parent=None))
                    plan.link_logical_plan(LogicalPlan(sources[0]))
                    schema = plan.schema()
                    count = plan.meta_count()
        else:
            # Get schema of output blocks.
            schema = self.schema(fetch_if_missing=False)
            count = self._snapshot_bundle.num_rows()

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

        if count is None:
            count = "?"

        num_blocks = None
        if dataset_cls == MaterializedDataset:
            num_blocks = self.initial_num_blocks()
            assert num_blocks is not None

        name_str = (
            "name={}, ".format(self._dataset_name)
            if self._dataset_name is not None
            else ""
        )
        num_blocks_str = f"num_blocks={num_blocks}, " if num_blocks else ""

        dataset_str = "{}({}{}num_rows={}, schema={})".format(
            dataset_cls.__name__,
            name_str,
            num_blocks_str,
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
            num_blocks_str = (
                f"\n{trailing_space}{INDENT_STR}num_blocks={num_blocks},"
                if num_blocks
                else ""
            )
            dataset_str = (
                f"{dataset_cls.__name__}("
                f"{name_str}"
                f"{num_blocks_str}"
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
            self._in_stats,
            data_context=self._context,
        )
        if self._snapshot_bundle is not None:
            # Copy over the existing snapshot.
            plan_copy._snapshot_bundle = self._snapshot_bundle
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
        plan_copy = ExecutionPlan(copy.copy(self._in_stats))
        if self._snapshot_bundle:
            # Copy over the existing snapshot.
            plan_copy._snapshot_bundle = copy.copy(self._snapshot_bundle)
            plan_copy._snapshot_operator = copy.copy(self._snapshot_operator)
            plan_copy._snapshot_stats = copy.copy(self._snapshot_stats)
        plan_copy._dataset_name = self._dataset_name
        return plan_copy

    def initial_num_blocks(self) -> Optional[int]:
        """Get the estimated number of blocks from the logical plan
        after applying execution plan optimizations, but prior to
        fully executing the dataset."""
        return self._logical_plan.dag.estimated_num_outputs()

    def schema(
        self, fetch_if_missing: bool = False
    ) -> Union[type, "pyarrow.lib.Schema"]:
        """Get the schema after applying all execution plan optimizations,
        but prior to fully executing the dataset
        (unless `fetch_if_missing` is set to True).

        Args:
            fetch_if_missing: Whether to execute the plan to fetch the schema.

        Returns:
            The schema of the output dataset.
        """
        if self._schema is not None:
            return self._schema

        schema = None
        if self.has_computed_output():
            schema = unify_block_metadata_schema(self._snapshot_bundle.metadata)
        elif self._logical_plan.dag.schema() is not None:
            schema = self._logical_plan.dag.schema()
        elif fetch_if_missing:
            blocks_with_metadata, _, _ = self.execute_to_iterator()
            for _, metadata in blocks_with_metadata:
                if metadata.schema is not None and (
                    metadata.num_rows is None or metadata.num_rows > 0
                ):
                    schema = metadata.schema
                    break
        elif self.is_read_only():
            # For consistency with the previous implementation, we fetch the schema if
            # the plan is read-only even if `fetch_if_missing` is False.
            blocks_with_metadata, _, _ = self.execute_to_iterator()
            try:
                _, metadata = next(iter(blocks_with_metadata))
                schema = metadata.schema
            except StopIteration:  # Empty dataset.
                schema = None

        self._schema = schema
        return self._schema

    def cache_schema(self, schema: Union[type, "pyarrow.lib.Schema"]):
        self._schema = schema

    def input_files(self) -> Optional[List[str]]:
        """Get the input files of the dataset, if available."""
        return self._logical_plan.dag.input_files()

    def meta_count(self) -> Optional[int]:
        """Get the number of rows after applying all plan optimizations, if possible.

        This method will never trigger any computation.

        Returns:
            The number of records of the result Dataset, or None.
        """
        if self.has_computed_output():
            num_rows = sum(m.num_rows for m in self._snapshot_bundle.metadata)
        elif self._logical_plan.dag.num_rows() is not None:
            num_rows = self._logical_plan.dag.num_rows()
        else:
            num_rows = None
        return num_rows

    @omit_traceback_stdout
    def execute_to_iterator(
        self,
    ) -> Tuple[
        Iterator[Tuple[ObjectRef[Block], BlockMetadata]],
        DatasetStats,
        Optional["Executor"],
    ]:
        """Execute this plan, returning an iterator.

        This will use streaming execution to generate outputs.

        Returns:
            Tuple of iterator over output blocks and the executor.
        """
        self._has_started_execution = True

        # Always used the saved context for execution.
        ctx = self._context

        if self.has_computed_output():
            bundle = self.execute()
            return iter(bundle.blocks), self._snapshot_stats, None

        from ray.data._internal.execution.legacy_compat import (
            execute_to_legacy_block_iterator,
        )
        from ray.data._internal.execution.streaming_executor import StreamingExecutor

        metrics_tag = create_dataset_tag(self._dataset_name, self._dataset_uuid)
        executor = StreamingExecutor(copy.deepcopy(ctx.execution_options), metrics_tag)
        block_iter = execute_to_legacy_block_iterator(
            executor,
            self,
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

    @omit_traceback_stdout
    def execute(
        self,
        preserve_order: bool = False,
    ) -> RefBundle:
        """Execute this plan.

        Args:
            preserve_order: Whether to preserve order in execution.

        Returns:
            The blocks of the output dataset.
        """
        self._has_started_execution = True

        # Always used the saved context for execution.
        context = self._context

        if not ray.available_resources().get("CPU"):
            if log_once("cpu_warning"):
                logger.warning(
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
            )

            if self._logical_plan.dag.output_data() is not None:
                # If the data is already materialized (e.g., `from_pandas`), we can
                # skip execution and directly return the output data. This avoids
                # recording unnecessary metrics for an empty plan execution.
                stats = _get_initial_stats_from_plan(self)

                # TODO(@bveeramani): Make `ExecutionPlan.execute()` return
                # `List[RefBundle]` instead of `RefBundle`. Among other reasons, it'd
                # allow us to remove the unwrapping logic below.
                output_bundles = self._logical_plan.dag.output_data()
                owns_blocks = all(bundle.owns_blocks for bundle in output_bundles)
                bundle = RefBundle(
                    [
                        (block, metadata)
                        for bundle in output_bundles
                        for block, metadata in bundle.blocks
                    ],
                    owns_blocks=owns_blocks,
                )
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
                    dataset_uuid=self._dataset_uuid,
                    preserve_order=preserve_order,
                )
                bundle = RefBundle(
                    tuple(blocks.iter_blocks_with_metadata()),
                    owns_blocks=blocks._owned_by_consumer,
                )
                stats = executor.get_stats()
                stats_summary_string = stats.to_summary().to_string(
                    include_parent=False
                )
                if context.enable_auto_log_stats:
                    logger.info(stats_summary_string)

            # Retrieve memory-related stats from ray.
            try:
                reply = get_memory_info_reply(
                    get_state_from_address(ray.get_runtime_context().gcs_address)
                )
                if reply.store_stats.spill_time_total_s > 0:
                    stats.global_bytes_spilled = int(
                        reply.store_stats.spilled_bytes_total
                    )
                if reply.store_stats.restore_time_total_s > 0:
                    stats.global_bytes_restored = int(
                        reply.store_stats.restored_bytes_total
                    )
            except Exception as e:
                logger.debug(
                    "Skipping recording memory spilled and restored statistics due to "
                    f"exception: {e}"
                )

            stats.dataset_bytes_spilled = 0

            def collect_stats(cur_stats):
                stats.dataset_bytes_spilled += cur_stats.extra_metrics.get(
                    "obj_store_mem_spilled", 0
                )
                for parent in cur_stats.parents:
                    collect_stats(parent)

            collect_stats(stats)

            # Set the snapshot to the output of the final operator.
            self._snapshot_bundle = bundle
            self._snapshot_operator = self._logical_plan.dag
            self._snapshot_stats = stats
            self._snapshot_stats.dataset_uuid = self._dataset_uuid

        return self._snapshot_bundle

    @property
    def has_started_execution(self) -> bool:
        """Return ``True`` if this plan has been partially or fully executed."""
        return self._has_started_execution

    def clear_snapshot(self) -> None:
        """Clear the snapshot kept in the plan to the beginning state."""
        self._snapshot_bundle = None
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
        return all(isinstance(op, Read) for op in self._logical_plan.sources())

    def is_read_only(self, root_op: Optional[LogicalOperator] = None) -> bool:
        """Return whether the LogicalPlan corresponding to `root_op`
        contains only a Read op. By default, the last operator of
        the LogicalPlan is used."""
        if root_op is None:
            root_op = self._logical_plan.dag
        return isinstance(root_op, Read) and len(root_op.input_dependencies) == 0

    def has_computed_output(self) -> bool:
        """Whether this plan has a computed snapshot for the final operator, i.e. for the
        output of this plan.
        """
        return (
            self._snapshot_bundle is not None
            and self._snapshot_operator == self._logical_plan.dag
        )

    def require_preserve_order(self) -> bool:
        """Whether this plan requires to preserve order."""
        from ray.data._internal.logical.operators.all_to_all_operator import Sort
        from ray.data._internal.logical.operators.n_ary_operator import Zip

        for op in self._logical_plan.dag.post_order_iter():
            if isinstance(op, (Zip, Sort)):
                return True
        return False
