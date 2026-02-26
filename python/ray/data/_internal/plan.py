import copy
import itertools
import logging
from typing import TYPE_CHECKING, Iterator, List, Optional, Tuple

import ray
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import SourceOperator
from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
from ray.data._internal.logical.interfaces.operator import Operator
from ray.data._internal.logical.operators import Read
from ray.data._internal.logical.optimizers import get_plan_conversion_fns
from ray.data._internal.stats import DatasetStats
from ray.data.block import _take_first_non_empty_schema
from ray.data.context import DataContext
from ray.data.exceptions import omit_traceback_stdout
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import (
        StreamingExecutor,
    )
    from ray.data.dataset import Dataset


# Scheduling strategy can be inherited from prev operator if not specified.
INHERITABLE_REMOTE_ARGS = ["scheduling_strategy"]


logger = logging.getLogger(__name__)


class ExecutionPlan:
    """A lazy execution plan for a Dataset.

    This lazy execution plan builds up a chain of ``List[RefBundle]`` -->
    ``List[RefBundle]`` operators. Prior to execution, we apply a set of logical
    plan optimizations, such as operator fusion, in order to reduce Ray task
    overhead and data copies.

    Internally, the execution plan holds a cache of a computed list of
    blocks and their associated metadata under ``self._cache``,
    where this snapshot is the cached output of executing the operator chain."""

    def __init__(
        self,
        stats: DatasetStats,
        data_context: DataContext,
    ):
        """Create a plan with no transformation operators.

        Args:
            stats: Stats for the base blocks.
            data_context: :class:`~ray.data.context.DataContext`
                object to use for execution.
        """
        from ray.data.dataset import _ExecutionCache

        self._in_stats = stats
        # Cache for holding data from previous execution or iteration.
        self._cache = _ExecutionCache()

        # Set when a Dataset is constructed with this plan
        self._dataset_uuid = None
        # Index of the current execution.
        self._run_index = -1

        self._dataset_name = None

        self._has_started_execution = False

        self._context = data_context

    def get_dataset_id(self) -> str:
        """Unique ID of the dataset, including the dataset name,
        UUID, and current execution index.
        """
        return (
            f"{self._dataset_name or 'dataset'}_{self._dataset_uuid}_{self._run_index}"
        )

    def create_executor(self) -> "StreamingExecutor":
        """Create an executor for this plan."""
        from ray.data._internal.execution.streaming_executor import StreamingExecutor

        self._run_index += 1
        executor = StreamingExecutor(self._context, self.get_dataset_id())
        return executor

    def __repr__(self) -> str:
        return (
            f"ExecutionPlan("
            f"dataset_uuid={self._dataset_uuid}, "
            f"snapshot_operator={self._cache._operator}"
            f")"
        )

    def explain(self) -> str:
        """Return a string representation of the logical and physical plan."""

        convert_fns = [lambda x: x] + get_plan_conversion_fns()
        titles: List[str] = [
            "Logical Plan",
            "Logical Plan (Optimized)",
            "Physical Plan",
            "Physical Plan (Optimized)",
        ]

        # 1. Set initial plan
        plan = self._logical_plan

        sections = []
        for title, convert_fn in zip(titles, convert_fns):

            # 2. Convert plan to new plan
            plan = convert_fn(plan)

            # 3. Generate plan str from new plan.
            plan_str, _ = self.generate_plan_string(plan.dag, show_op_repr=True)

            banner = f"\n-------- {title} --------\n"
            section = f"{banner}{plan_str}"
            sections.append(section)

        return "".join(sections)

    @staticmethod
    def generate_plan_string(
        op: Operator,
        curr_str: str = "",
        depth: int = 0,
        including_source: bool = True,
        show_op_repr: bool = False,
    ):
        """Traverse (DFS) the Plan DAG and
        return a string representation of the operators."""
        if not including_source and isinstance(op, SourceOperator):
            return curr_str, depth

        curr_max_depth = depth

        # For logical plan, only show the operator name like "Aggregate".
        # But for physical plan, show the operator class name as well like "AllToAllOperator[Aggregate]".
        op_str = repr(op) if show_op_repr else op.name

        if depth == 0:
            curr_str += f"{op_str}\n"
        else:
            trailing_space = " " * ((depth - 1) * 3)
            curr_str += f"{trailing_space}+- {op_str}\n"

        for input in op.input_dependencies:
            curr_str, input_max_depth = ExecutionPlan.generate_plan_string(
                input, curr_str, depth + 1, including_source, show_op_repr
            )
            curr_max_depth = max(curr_max_depth, input_max_depth)
        return curr_str, curr_max_depth

    def get_plan_as_string(self, dataset: "Dataset") -> str:
        """Create a cosmetic string representation of this execution plan.

        Returns:
            The string representation of this execution plan.
        """
        # NOTE: this is used for Dataset.__repr__ to give a user-facing string
        # representation. Ideally ExecutionPlan.__repr__ should be replaced with this
        # method as well.

        from ray.data.dataset import Dataset, MaterializedDataset

        dataset_cls = dataset.__class__

        # Do not force execution for schema, as this method is expected to be very
        # cheap.
        plan_str = ""
        plan_max_depth = 0

        if not self.has_computed_output():
            # using dataset as source here, so don't generate
            # source operator in generate_plan_string
            plan_str, plan_max_depth = self.generate_plan_string(
                self._logical_plan.dag, including_source=False
            )

        schema = dataset.schema(fetch_if_missing=False, base=True)
        count = self._cache.get_num_rows(self._logical_plan.dag)

        if schema is None or count is None:
            has_n_ary_operator = False
            dag = self._logical_plan.dag

            while not isinstance(dag, SourceOperator):
                if len(dag.input_dependencies) > 1:
                    has_n_ary_operator = True
                    break

                dag = dag.input_dependencies[0]

            # TODO(@bveeramani): Handle schemas for n-ary operators like `Union`.
            if not has_n_ary_operator:
                assert isinstance(dag, SourceOperator), dag
                ds = Dataset(
                    ExecutionPlan(
                        DatasetStats(metadata={}, parent=None), self._context
                    ),
                    LogicalPlan(dag, self._context),
                )
                if schema is None:
                    schema = ds.schema(base=True)
                if count is None:
                    count = ds._meta_count()

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
        self._logical_plan._context = self._context

    def copy(self) -> "ExecutionPlan":
        """Create a shallow copy of this execution plan.

        This copy can be executed/cleared without mutating the original,
        but the copied cache data initially shares references with the
        original.

        Returns:
            A shallow copy of this execution plan.
        """
        plan_copy = ExecutionPlan(
            self._in_stats,
            data_context=self._context,
        )
        plan_copy._cache = self._cache.copy()
        plan_copy._dataset_name = self._dataset_name
        return plan_copy

    def deep_copy(self) -> "ExecutionPlan":
        """Create a deep copy of this execution plan.

        Returns:
            A deep copy of this execution plan.
        """
        plan_copy = ExecutionPlan(
            copy.copy(self._in_stats),
            data_context=self._context.copy(),
        )
        plan_copy._cache = self._cache.deep_copy()
        plan_copy._dataset_name = self._dataset_name
        return plan_copy

    def initial_num_blocks(self) -> Optional[int]:
        """Get the estimated number of blocks from the logical plan
        after applying execution plan optimizations, but prior to
        fully executing the dataset."""
        return self._logical_plan.dag.estimated_num_outputs()

    def input_files(self) -> Optional[List[str]]:
        """Get the input files of the dataset, if available."""
        return self._logical_plan.dag.infer_metadata().input_files

    @omit_traceback_stdout
    def execute_to_iterator(
        self,
    ) -> Tuple[Iterator[RefBundle], DatasetStats, Optional["StreamingExecutor"]]:
        """Execute this plan, returning an iterator.

        This will use streaming execution to generate outputs.

        NOTE: Executor will be shutdown upon either of the 2 following conditions:

            - Iterator is fully exhausted (ie until StopIteration is raised)
            - Executor instances is garbage-collected

        Returns:
            Tuple of iterator over output RefBundles, DatasetStats, and the executor.
        """
        self._has_started_execution = True

        cached_bundle = self._cache.get_bundle(self._logical_plan.dag)
        if cached_bundle is not None:
            return iter([cached_bundle]), self._cache.get_stats(), None

        from ray.data._internal.execution.legacy_compat import (
            execute_to_legacy_bundle_iterator,
        )

        executor = self.create_executor()
        bundle_iter = execute_to_legacy_bundle_iterator(executor, self, self._context)
        # Since the generator doesn't run any code until we try to fetch the first
        # value, force execution of one bundle before we call get_stats().
        gen = iter(bundle_iter)
        try:
            bundle_iter = itertools.chain([next(gen)], gen)
        except StopIteration:
            pass
        self._cache.set_stats(executor.get_stats())
        return bundle_iter, self._cache.get_stats(), executor

    @omit_traceback_stdout
    def execute(
        self,
        preserve_order: bool = False,
    ) -> RefBundle:
        """Executes this plan (eagerly).

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
        if self._cache.get_bundle(self._logical_plan.dag) is None:
            from ray.data._internal.execution.legacy_compat import (
                _get_initial_stats_from_plan,
                execute_to_ref_bundle,
            )

            if (
                isinstance(self._logical_plan.dag, SourceOperator)
                and self._logical_plan.dag.output_data() is not None
            ):
                # If the data is already materialized (e.g., `from_pandas`), we can
                # skip execution and directly return the output data. This avoids
                # recording unnecessary metrics for an empty plan execution.
                stats = _get_initial_stats_from_plan(self)

                # TODO(@bveeramani): Make `ExecutionPlan.execute()` return
                # `List[RefBundle]` instead of `RefBundle`. Among other reasons, it'd
                # allow us to remove the unwrapping logic below.
                output_bundles = self._logical_plan.dag.output_data()
                owns_blocks = all(bundle.owns_blocks for bundle in output_bundles)
                schema = _take_first_non_empty_schema(
                    bundle.schema for bundle in output_bundles
                )
                bundle = RefBundle(
                    [
                        (block, metadata)
                        for bundle in output_bundles
                        for block, metadata in bundle.blocks
                    ],
                    owns_blocks=owns_blocks,
                    schema=schema,
                )
            else:
                # Make sure executor is properly shutdown
                with self.create_executor() as executor:
                    bundle = execute_to_ref_bundle(
                        executor,
                        self,
                        dataset_uuid=self._dataset_uuid,
                        preserve_order=preserve_order,
                        data_context=self._context,
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
            stats.dataset_uuid = self._dataset_uuid
            self._cache.set_bundle(self._logical_plan.dag, bundle)
            self._cache.set_stats(stats)

        bundle = self._cache.get_bundle(self._logical_plan.dag)
        assert bundle is not None
        return bundle

    @property
    def has_started_execution(self) -> bool:
        """Return ``True`` if this plan has been partially or fully executed."""
        return self._has_started_execution

    def clear_cache(self) -> None:
        """Clear the cache kept in the plan to the beginning state."""
        self._cache.clear()

    def stats(self) -> DatasetStats:
        """Return stats for this plan.

        If the plan isn't executed, an empty stats object will be returned.
        """
        if not self._cache.get_stats():
            return DatasetStats(metadata={}, parent=None)
        return self._cache.get_stats()

    def has_lazy_input(self) -> bool:
        """Return whether this plan has lazy input blocks."""
        return all(isinstance(op, Read) for op in self._logical_plan.sources())

    def has_computed_output(self) -> bool:
        """Whether this plan has a computed snapshot for the final operator, i.e. for
        the output of this plan.
        """
        return self._cache.get_bundle(self._logical_plan.dag) is not None

    def require_preserve_order(self) -> bool:
        """Whether this plan requires to preserve order."""
        from ray.data._internal.logical.operators import Zip

        for op in self._logical_plan.dag.post_order_iter():
            if isinstance(op, Zip):
                return True
        return False
