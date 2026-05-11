import itertools
import logging
from typing import TYPE_CHECKING, Callable, Iterator, Optional, Tuple

import ray
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.interfaces import SourceOperator
from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
from ray.data._internal.stats import DatasetStats
from ray.data.block import _take_first_non_empty_schema
from ray.data.context import DataContext
from ray.data.exceptions import omit_traceback_stdout
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor import (
        StreamingExecutor,
    )
    from ray.data.dataset import _ExecutionCache

logger = logging.getLogger(__name__)


class ExecutionPlan:
    """A thin execution shell for a Dataset.

    This plan holds shared references to Dataset's cache, stats, context,
    and logical plan. It provides execution methods that bridge the logical
    plan to the streaming executor. All owned state lives on Dataset;
    ExecutionPlan is a temporary wrapper removed in a future PR.
    """

    def __init__(
        self,
        context: DataContext,
        cache: "_ExecutionCache",
        in_stats: DatasetStats,
        logical_plan: LogicalPlan,
    ):
        self._context = context
        self._cache = cache
        self._in_stats = in_stats
        self._logical_plan = logical_plan

    @omit_traceback_stdout
    def execute_to_iterator(
        self,
        create_executor_fn: Callable[[], "StreamingExecutor"],
    ) -> Tuple[Iterator[RefBundle], DatasetStats, Optional["StreamingExecutor"]]:
        """Execute this plan, returning an iterator.

        This will use streaming execution to generate outputs.

        Args:
            create_executor_fn: Factory that creates a StreamingExecutor.

        Returns:
            Tuple of iterator over output RefBundles, DatasetStats, and the executor.
        """
        cached_bundle = self._cache.get_bundle(self._logical_plan.dag)
        if cached_bundle is not None:
            return iter([cached_bundle]), self._cache.get_stats(), None

        from ray.data._internal.execution.legacy_compat import (
            execute_to_legacy_bundle_iterator,
        )

        executor = create_executor_fn()
        bundle_iter = execute_to_legacy_bundle_iterator(executor, self)
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
        dataset_uuid: str,
        create_executor_fn: Callable[[], "StreamingExecutor"],
        preserve_order: bool = False,
    ) -> RefBundle:
        """Executes this plan (eagerly).

        Args:
            dataset_uuid: The dataset UUID for stats tagging.
            create_executor_fn: Factory that creates a StreamingExecutor.
            preserve_order: Whether to preserve order in execution.

        Returns:
            The blocks of the output dataset.
        """
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
                execute_to_ref_bundle,
            )

            if (
                isinstance(self._logical_plan.dag, SourceOperator)
                and self._logical_plan.dag.output_data() is not None
            ):
                # If the data is already materialized (e.g., `from_pandas`), we can
                # skip execution and directly return the output data. This avoids
                # recording unnecessary metrics for an empty plan execution.
                stats = self.initial_stats()

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
                with create_executor_fn() as executor:
                    bundle = execute_to_ref_bundle(
                        executor,
                        self,
                        dataset_uuid=dataset_uuid,
                        preserve_order=preserve_order,
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
            stats.dataset_uuid = dataset_uuid
            self._cache.set_bundle(self._logical_plan.dag, bundle)
            self._cache.set_stats(stats)

        bundle = self._cache.get_bundle(self._logical_plan.dag)
        assert bundle is not None
        return bundle

    def initial_stats(self) -> DatasetStats:
        if self._cache.get_bundle(self._logical_plan.dag) is not None:
            return self._cache.get_stats()
        # For Datasets created from "read_xxx", `plan._in_stats` contains useless data.
        # For Datasets created from "from_xxx", we need to use `plan._in_stats` as
        # the initial stats. Because the `FromXxx` logical operators will be translated to
        # "InputDataBuffer" physical operators, which will be ignored when generating
        # stats, see `StreamingExecutor._generate_stats`.
        # TODO(hchen): Unify the logic by saving the initial stats in `InputDataBuffer
        if self._logical_plan.has_lazy_input():
            return DatasetStats(metadata={}, parent=None)
        else:
            return self._in_stats
