"""The actor-only backend's streaming executor.

``ExperimentalStreamingExecutor`` keeps the standard executor's thread,
lifecycle, progress reporting and error handling, and replaces the scheduling
loop's dispatch model:

- **Pull-based dispatch.** Operators never dispatch on their own. Each step
  processes completed tasks (reading each task up to the pull budget its
  operator quotes), then asks every actor-pool operator ``can_submit_task()`` /
  ``launch_task()`` until it declines. Backpressure policies and the
  ``ResourceManager``-driven operator ranking are not consulted: admission is
  owned by the per-actor input/output budgets.
- **One resource view.** An ``ActorOnlyResourceReporter`` over the shared
  ``AutoscalingCoordinator`` and the ``ResourceBank`` feeds the cluster
  autoscaler's utilization gauge, the ``ResourceBank``'s per-node capacity view,
  the sizer, and the progress status line, so they all agree.
- **Sizing.** Actor-pool sizing belongs to the ``OperatorSizer`` alone; the
  standard actor autoscaler is never triggered.
- **Deadlock escape hatch.** ``relieve_output_limit_deadlock`` runs on an
  adaptive interval and raises one stuck operator's output limit (see
  ``output_limit_escape_hatch``).
"""

from __future__ import annotations

import time
from typing import Optional

from typing_extensions import override

from ray.data._internal.cluster_autoscaler import (
    ClusterAutoscaler,
    DefaultAutoscalingCoordinator,
    create_cluster_autoscaler,
)
from ray.data._internal.cluster_autoscaler.resource_utilization_gauge import (
    RollingLogicalUtilizationGauge,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.execution.streaming_executor_state import OpState, Topology
from ray.data._internal.experimental.execution.actor_only_metrics import (
    ActorOnlyMetrics,
)
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    ActorOnlyResourceReporter,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.output_limit_escape_hatch import (
    relieve_output_limit_deadlock,
)
from ray.data._internal.experimental.execution.sizer.operator_sizer import (
    OperatorSizer,
)
from ray.data._internal.experimental.execution.streaming_executor_state import (
    process_completed_tasks,
)

__all__ = ["ExperimentalStreamingExecutor"]


class ExperimentalStreamingExecutor(StreamingExecutor):
    """Actor-only-backend streaming executor. See the module docstring."""

    # The escape-hatch scan interval halves after a hit (something was stuck,
    # more may be) and doubles after a miss, within these bounds.
    MIN_IDLE_DETECTION_INTERVAL_S: float = 0.1
    MAX_IDLE_DETECTION_INTERVAL_S: float = 2.0
    # ``_launch_tasks`` can run long on a large backlog; refresh progress from
    # inside it at this cadence so the display keeps moving.
    PROGRESS_REFRESH_INTERVAL_S: float = 1.0

    _sizer: Optional[OperatorSizer] = None
    # Built in ``_create_cluster_autoscaler`` (which the base ``execute`` runs
    # before ``bootstrap``) because the autoscaler's utilization gauge reads
    # through it; ``bootstrap`` hands the same instance to the sizer.
    _resource_reporter: Optional[ActorOnlyResourceReporter] = None
    _actor_only_metrics: Optional[ActorOnlyMetrics] = None
    # Set (under ``_shutdown_lock``) by the one shutdown call that runs the
    # final metrics flush and bank finalize.
    _finalize_claimed: bool = False

    @override
    def _create_cluster_autoscaler(self) -> ClusterAutoscaler:
        """Same factory (and RAY_DATA_CLUSTER_AUTOSCALER selection) as the
        standard path, over a coordinator shared with the sizer and a gauge fed
        by the actor-only reporter."""
        assert self._topology is not None
        assert self._resource_bank is not None
        coordinator = DefaultAutoscalingCoordinator(
            requester_id=self._dataset_id,
            subcluster_selector=self._data_context.execution_options.label_selector,
        )
        self._resource_reporter = ActorOnlyResourceReporter(
            self._topology, coordinator, self._resource_bank
        )
        return create_cluster_autoscaler(
            self._topology,
            self._resource_manager,
            self._data_context,
            execution_id=self._dataset_id,
            autoscaling_coordinator=coordinator,
            resource_utilization_calculator=RollingLogicalUtilizationGauge(
                self._resource_reporter, execution_id=self._dataset_id
            ),
        )

    @override
    def bootstrap(self) -> None:
        assert self._topology is not None
        assert self._resource_reporter is not None
        self._last_detection_timestamp: float = 0.0
        self._last_progress_refresh_timestamp: float = 0.0
        self._idle_detection_interval_s: float = 1.0

        # The cluster autoscaler owns the coordinator request's lifecycle; the
        # sizer only reads allocations through the reporter.
        self._sizer = OperatorSizer(
            dataset_id=self._dataset_id,
            topology=self._topology,
            resource_reporter=self._resource_reporter,
        )
        self._sizer.initial_sizing_request()

        self._actor_only_metrics = ActorOnlyMetrics(
            dataset_id=self._dataset_id, update_interval=self.UPDATE_METRICS_INTERVAL_S
        )
        for op, op_state in self._topology.items():
            if isinstance(op, ExperimentalActorPoolMapOperator):
                op.set_actor_only_metrics(self._actor_only_metrics, op_state.op_tag())

    @override
    def _scheduling_loop_step(self, topology: Topology) -> bool:
        """One step of the actor-only scheduling loop. Returns whether the loop
        should keep running."""
        assert self._resource_bank is not None
        assert self._resource_reporter is not None
        assert self._sizer is not None
        assert self._actor_only_metrics is not None

        # One cluster read for this whole step, shared by the bank and the
        # sizer, so they never disagree about capacity within a step.
        cluster_view = self._resource_reporter.get_cluster_view(
            include_labels=True, include_draining_nodes=True
        )
        self._resource_bank.set_node_view(node_view=cluster_view.reserved_by_node)
        self._resource_bank.drain_consumed_blocks()

        num_errored_blocks = process_completed_tasks(
            topology=topology,
            max_errored_blocks=self._max_errored_blocks,
            metadata_fetcher=self._metadata_fetcher,
        )
        if self._max_errored_blocks > 0:
            self._max_errored_blocks -= num_errored_blocks
        self._num_errored_blocks += num_errored_blocks

        self._maybe_run_escape_hatch()

        self._sizer.perform_sizing_decision(
            cluster_view=cluster_view,
            seconds_since_allocation_change=(
                self._resource_reporter.seconds_since_allocation_change()
            ),
        )
        self._process_draining_and_pending_actors(topology)

        self._resource_bank.drain_consumed_blocks()
        self._launch_tasks()
        self._sample_output_backpressure(topology)

        self._actor_only_metrics.maybe_update(
            topology=topology, resource_bank=self._resource_bank
        )
        self._report_current_usage()

        # Node-level autoscaling only; actor-pool sizing belongs to the sizer.
        self._cluster_autoscaler.try_trigger_scaling()

        return self._finalize_scheduling_loop_step(topology)

    @staticmethod
    def _process_draining_and_pending_actors(topology: Topology) -> None:
        """Release drained actors and expire stuck pendings, on every step.

        Runs after sizing, so a same-step upscale can reclaim a draining actor
        before it would otherwise be released. A pending that can never become
        ready (failed restart, stuck env install) would hold its op's sizing
        forever through the pending gate, so it is expired and re-placed
        against current cluster state on the next sizing tick.
        """
        for op in topology:
            if (
                isinstance(op, ExperimentalActorPoolMapOperator)
                and op.get_autoscaling_actor_pools()
            ):
                op.process_draining_actors()
                op.actor_pool.release_expired_pending_actors()

    def _maybe_run_escape_hatch(self) -> None:
        now = time.perf_counter()
        if now - self._last_detection_timestamp < self._idle_detection_interval_s:
            return
        assert self._output_node is not None
        assert self._resource_bank is not None
        assert self._topology is not None
        upgraded_op = relieve_output_limit_deadlock(
            output_operator=self._output_node[0], resource_bank=self._resource_bank
        )
        if upgraded_op is not None:
            assert self._actor_only_metrics is not None
            self._actor_only_metrics.record_output_limit_upgrade(
                self._topology[upgraded_op].op_tag()
            )
            self._idle_detection_interval_s = max(
                self.MIN_IDLE_DETECTION_INTERVAL_S, self._idle_detection_interval_s / 2
            )
        else:
            self._idle_detection_interval_s = min(
                self.MAX_IDLE_DETECTION_INTERVAL_S, self._idle_detection_interval_s * 2
            )
        self._last_detection_timestamp = now

    def _launch_tasks(self) -> None:
        """Launch on every actor-pool operator until each declines."""
        assert self._topology is not None
        for op in self._topology:
            if not isinstance(op, ExperimentalActorPoolMapOperator):
                continue
            while op.can_submit_task():
                op.launch_task()
                now = time.perf_counter()
                if (
                    now - self._last_progress_refresh_timestamp
                    >= self.PROGRESS_REFRESH_INTERVAL_S
                ):
                    self._refresh_progress_manager(self._topology)
                    self._last_progress_refresh_timestamp = now
            # Classic metric, kept for backwards compatibility: a ready bundle
            # no actor will take is this backend's submission backpressure.
            op.notify_in_task_submission_backpressure(
                op.has_queued_bundle(), "ActorAdmission"
            )

    def _sample_output_backpressure(self, topology: Topology) -> None:
        """Classic per-op output-backpressure metric, kept for backwards
        compatibility: an op is backpressured when all its running actors are
        blocked on their output budget."""
        for op in topology:
            if not isinstance(op, ExperimentalActorPoolMapOperator):
                continue
            blocked, running = op.cached_output_backpressured_actors()
            op.notify_in_task_output_backpressure(
                not op.has_completed() and running > 0 and blocked == running,
                "Output",
            )

    @override
    def shutdown(self, force: bool, exception: Optional[Exception] = None):
        # Several threads can call shutdown at once (every split consumer of a
        # streaming_split does), so exactly one of them must claim the final
        # flush + finalize. Otherwise one thread's finalize clears the bank
        # while another is mid-flush, and the flush reads half-cleared stats.
        with self._shutdown_lock:
            should_finalize = (
                self._topology is not None
                and self._execution_started
                and not self._finalize_claimed
            )
            if should_finalize:
                self._finalize_claimed = True
        try:
            return super().shutdown(force, exception)
        finally:
            if should_finalize:
                assert self._resource_bank is not None
                assert self._topology is not None
                try:
                    # Final flush so the dashboards see the end state. Runs
                    # after the base shutdown has stopped the scheduling loop
                    # (the only other writer) and before finalize clears the
                    # bank's stats.
                    if self._actor_only_metrics is not None:
                        self._actor_only_metrics.maybe_update(
                            topology=self._topology,
                            resource_bank=self._resource_bank,
                            force=True,
                        )
                finally:
                    self._resource_bank.finalize_dataset(ops=set(self._topology.keys()))
            # The base shutdown cancels the coordinator allocation through the
            # cluster autoscaler; the sizer holds no coordinator lifecycle.
            self._sizer = None

    @override
    def _update_stats_metrics(self, state: str, force_update: bool = False):
        # Backwards compatibility only: this backend has no ResourceManager
        # usages, so fill the classic per-op metrics it would have set. Remove
        # with them once task pools are gone, in favor of the actor-only metrics.
        if (
            force_update
            or time.time() - self._metrics_last_updated > self.UPDATE_METRICS_INTERVAL_S
        ):
            assert self._topology is not None
            assert self._resource_bank is not None
            for op, op_state in self._topology.items():
                op._metrics.obj_store_mem_used = self._resource_bank.live_object_store(
                    op=op
                ).total_bytes()
                if isinstance(op, ExperimentalActorPoolMapOperator):
                    op_state.refresh_actor_metrics()
        super()._update_stats_metrics(state, force_update=force_update)

    @override
    def _get_operator_progress_extra(self, op_state: OpState) -> str:
        assert self._resource_bank is not None
        return str(self._resource_bank.live_object_store(op=op_state.op))

    @override
    def _report_current_usage(self) -> None:
        assert self._topology is not None
        assert self._resource_bank is not None
        assert self._resource_reporter is not None

        num_inputs = 0
        num_pulled_outputs = 0
        num_prebuffered_outputs = 0
        for op in self._topology:
            live = self._resource_bank.live_object_store(op=op)
            num_inputs += live.num_input_blocks
            num_pulled_outputs += live.num_pulled_output_blocks
            num_prebuffered_outputs += live.num_prebuffered_output_blocks

        # Usage and limits from the shared reporter, so this line and the
        # cluster autoscaler's scale-up signal agree and limits reflect the
        # coordinator allocation.
        running_usage = self._resource_reporter.get_global_usage()
        pending_usage = self._resource_reporter.get_global_pending_usage()
        limits = self._resource_reporter.get_global_limits()

        resources_status = (
            f"Active & requested resources: "
            f"{running_usage.cpu:.4g}/{limits.cpu:.4g} CPU, "
        )
        if running_usage.memory > 0:
            resources_status += (
                f"{running_usage.memory_str()}/{limits.memory_str()} memory, "
            )
        if running_usage.gpu > 0:
            resources_status += f"{running_usage.gpu:.4g}/{limits.gpu:.4g} GPU, "
        resources_status += (
            f"{running_usage.object_store_memory_str()}/"
            f"{limits.object_store_memory_str()} object store "
            f"(input={num_inputs}, "
            f"outputs={num_pulled_outputs + num_prebuffered_outputs}, "
            f"prebuffered={num_prebuffered_outputs})"
        )

        pending_parts = []
        if pending_usage.cpu:
            pending_parts.append(f"{pending_usage.cpu:.4g} CPU")
        if pending_usage.memory:
            pending_parts.append(f"{pending_usage.memory_str()} memory")
        if pending_usage.gpu:
            pending_parts.append(f"{pending_usage.gpu:.4g} GPU")
        if pending_parts:
            resources_status += f" (pending: {', '.join(pending_parts)})"

        if self._progress_manager is not None:
            self._progress_manager.update_total_resource_status(resources_status)
