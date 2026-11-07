import logging
import math
import time
from collections import deque
from itertools import chain
from typing import TYPE_CHECKING, Deque, Dict, Optional, Set, Tuple

from .backpressure_policy import BackpressurePolicy
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import drain_floor
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import Topology

logger = logging.getLogger(__name__)

# Samples per window: the sampling interval is window / SAMPLES_PER_WINDOW.
SAMPLES_PER_WINDOW = 64
# Floor for that interval, so a short window does not sample every iteration.
MIN_SAMPLE_INTERVAL_S = 0.25
# Internal AIMD tuning. The public knobs define when a stall starts and ends;
# these values only govern how cautiously the pool probes sustainable capacity.
CAP_DECAY = 0.8
CAP_GROW = 0.1


class ActorPoolDrainPolicy(BackpressurePolicy):
    """Drain the actor pool of an operator whose output has stalled.

    Draining here means: stop feeding the operator new inputs, so the tasks it
    already has in flight can finish (their outputs are still being read), its
    actors go idle, and the actor autoscaler can take them back.

    An operator whose output is not being read cannot raise throughput by running
    more tasks -- it only keeps actors (and their GPUs) busy producing nothing.
    Those actors cannot be reclaimed either: a task parked inside a generator's
    ``yield`` still counts as in flight, so the pool's utilization stays high, the
    utilization-based downscale branch never fires, and ``_try_remove_idle_actor``
    only ever removes actors with no tasks in flight. New inputs have to stop
    first, so the parked tasks can complete and their actors can go idle.

    "Stalled" is therefore the *state* of being unable to make progress: the
    output is not being read (or only let through by the liveness guard) while
    the operator still has tasks in flight, with its pool above ``min_size``. The
    one guard-driven unblock that does *not* count is a sink unblocked because an
    external consumer is starving for data -- that is a state to feed, not to
    drain, and it is excluded below.

    Decided on the time-weighted share of the last ``window`` seconds in which
    that output was not being read, not on a continuous stretch of it and not on a
    sample count: the raw signal flickers, and this is called once per scheduling
    iteration, so dispatch bursts would otherwise weigh far more than the blocked
    stretches they alternate with. The share deliberately measures the output
    alone -- whether tasks are in flight is an entry requirement, checked
    separately, and folding it in would make an emptied-out pool read as recovered
    while its idle actors are still there to reclaim.

    Entry and exit are both decided on that same share, with two thresholds and a
    band in between (a Schmitt trigger): withholding starts once the share
    reaches ``stall_ratio`` (with tasks still in flight and the pool above
    ``min_size``) and stops once it falls to ``resume_ratio``; in between, the
    policy keeps whatever state it is in. Releasing on the first readable sample
    would refill the pool exactly when the parked tasks become able to complete,
    so relief has to be *sustained* rather than observed once -- which is what the
    share over a rolling window measures. The window keeps rolling while the
    operator is withheld: left frozen, it would keep reporting the state that
    engaged the drain and relief could never be observed.

    Withholding also ends -- unconditionally -- when there is nothing left to
    reclaim (the pool has reached its drain floor: its adaptive ceiling, or
    ``min_size`` when no ceiling is set), or when withholding itself would starve
    the pipeline: an eligible downstream has run dry, or an external consumer is
    blocked waiting for output. Those two safety valves also drop the adaptive
    ceiling, as does the operator leaving the drain candidate set: the ceiling is
    an optimisation, liveness is not.

    Output reads are deliberately left alone (this policy does not limit
    ``max_task_output_bytes_to_read``) -- that is what lets the drain happen. The
    actor autoscaler picks the flagged drain up through
    ``PhysicalOperator.actor_pool_drain_in_progress`` and shrinks the pool
    without waiting for utilization to drop.

    **Adaptive ceiling (AIMD).** Draining down to ``min_size`` and then letting the
    pool be re-grown to its old size is what turns a persistent blockage into an
    oscillation: the reclaimed actors are handed straight back, the backlog is
    re-created, and the pool stalls again. So while an operator keeps being
    stalled, each pool gets an *adaptive ceiling*
    (``AutoscalingActorPool.dynamic_max_size``) that is stepped down by
    ``cap_decay`` once per observation window -- the first step is taken off the
    size the pool currently runs at, so it takes ``1 - cap_decay`` off the pool --
    and stepped back up by ``cap_grow`` per window once its output is being read
    again. Within the hysteresis band the ceiling is left alone. While the operator
    is being drained, the pool is only walked down to that ceiling (never past it),
    and the ceiling *survives* the release, so the pool cannot be re-grown into the
    stall it just came out of. The ceiling is a growth bound, not a hard size: a
    pool that is still above it when the stall clears (the share is in the band) is
    left alone, and only its future growth is refused. ``cap_decay >= 1.0``
    disables the ceiling; the default is ``0.8``, and it is inert unless this
    policy is enabled.

    The actor autoscaler cooperates from the other side: it refuses to grow a pool
    whose output is not being taken (``ResourceManager.downstream_can_absorb``),
    since extra actors would only park on the same budget.

    Opt-in via ``DataContext.actor_pool_drain_window_s``.
    """

    @property
    def name(self) -> str:
        return "ActorPoolDrain"

    def __init__(
        self,
        data_context: DataContext,
        topology: "Topology",
        resource_manager: "ResourceManager",
    ):
        super().__init__(data_context, topology, resource_manager)

        window_s = data_context.actor_pool_drain_window_s
        stall_ratio = data_context.actor_pool_drain_stall_ratio
        resume_ratio = data_context.actor_pool_drain_resume_ratio

        if window_s is not None and window_s <= 0:
            raise ValueError(
                "actor_pool_drain_window_s must be positive, got "
                f"{window_s!r}. Use None to disable the policy."
            )
        if window_s is not None and not (
            isinstance(stall_ratio, (int, float))
            and isinstance(resume_ratio, (int, float))
            and 0 <= resume_ratio < stall_ratio <= 1
        ):
            # Only validated when the policy is enabled: a stray env var must not
            # break every pipeline while the feature is off.
            raise ValueError(
                "actor_pool_drain_stall_ratio and actor_pool_drain_resume_ratio "
                "must be numbers satisfying 0 <= resume_ratio < stall_ratio <= 1, "
                f"got stall_ratio={stall_ratio!r}, resume_ratio={resume_ratio!r}."
            )

        self._window_s = window_s
        self._stall_ratio = stall_ratio
        self._resume_ratio = resume_ratio
        self._sample_interval_s = (
            max(window_s / SAMPLES_PER_WINDOW, MIN_SAMPLE_INTERVAL_S)
            if window_s is not None
            else None
        )

        self._cap_decay = CAP_DECAY
        self._cap_grow = CAP_GROW

        # Operators currently being withheld from.
        self._draining: Set["PhysicalOperator"] = set()
        # Per-op rolling window of ``(timestamp, stalled)`` samples.
        self._samples: Dict["PhysicalOperator", Deque[Tuple[float, bool]]] = {}
        # When we started watching an op, so a single early sample cannot read as
        # a fully stalled window.
        self._watching_since: Dict["PhysicalOperator", float] = {}
        # When the adaptive ceiling of an op's pools was last stepped (at most once
        # per window, so the ceiling moves at ``cap_decay``/``cap_grow`` per window).
        self._cap_updated_at: Dict["PhysicalOperator", float] = {}

    def _in_flight_tasks(self, op: "PhysicalOperator") -> int:
        """Tasks submitted to the op's actor pools that have not completed yet.

        A task parked inside a generator's ``yield`` is still in flight, which is
        why a pool can look busy while it is producing nothing.
        """
        return sum(
            pool.num_tasks_in_flight() for pool in op.get_autoscaling_actor_pools()
        )

    def _output_blocked(self, op: "PhysicalOperator") -> bool:
        """Whether ``op``'s output is not being read.

        A guard-driven unblock does not count as the output being read: the queue
        is still full and the budget is still ~0, only one byte was let through.
        """
        return (
            op.in_task_output_backpressure
            or op.task_output_backpressure_guard_unblocked
        )

    def _at_floor(self, op: "PhysicalOperator") -> bool:
        """Whether every pool has been walked down as far as it should be.

        Without an adaptive ceiling that is the configured ``min_size``. Once the
        ceiling has been set (see ``_update_adaptive_ceiling``) the pool is only
        walked down to the ceiling: the ceiling, unlike ``min_size``, is what keeps
        the pool from being re-grown straight back to where it stalled.
        """
        return all(
            p.current_size() <= drain_floor(p) for p in op.get_autoscaling_actor_pools()
        )

    def _clear_adaptive_ceiling(self, op: "PhysicalOperator", reason: str) -> None:
        """Drop the adaptive ceiling of every pool of ``op``.

        Used when the pipeline needs this operator to produce again (a starving
        downstream or consumer, or the operator leaving the drain candidate set):
        the ceiling is an optimisation, liveness is not. It is re-established after
        a full window of stall evidence.
        """
        for pool in op.get_autoscaling_actor_pools():
            ceiling = pool.dynamic_max_size()
            if ceiling is not None:
                # Read the value *before* clearing it: afterwards
                # `effective_max_size()` reports `max_size`, which would make the
                # log claim a ceiling that was never there.
                pool.set_dynamic_max_size(None)
                logger.info(
                    "Cleared adaptive max size of %s pool (was %d): %s",
                    op,
                    ceiling,
                    reason,
                )
        # The AIMD clock restarts with the ceiling.
        self._cap_updated_at.pop(op, None)

    def _update_adaptive_ceiling(
        self, op: "PhysicalOperator", stalled_fraction: Optional[float]
    ) -> None:
        """Step the pools' adaptive ceiling, at most once per window (AIMD).

        Stalled window  -> ceiling *= ``cap_decay``    (step down)
        Relieved window -> ceiling += ``cap_grow`` * ceiling (step back up)
        In between      -> unchanged (the same hysteresis band the drain uses)

        Stepping down on every stalled window is what keeps a persistently
        over-producing operator from being re-grown to its old size on each cycle;
        stepping back up once the output is read again probes for spare capacity.
        """
        if self._window_s is None or self._cap_decay >= 1.0:
            # No window to step on, or the ceiling is disabled. The former cannot
            # be reached through `_should_drain`, which returns early when the
            # policy is off, but the dependency is made explicit here so the
            # method stays safe to call on its own.
            return
        if stalled_fraction is None:
            return
        pools = op.get_autoscaling_actor_pools()
        if not pools:
            return

        now = time.monotonic()
        last = self._cap_updated_at.get(op)
        if last is not None and now - last < self._window_s:
            return

        adjusted = False
        if stalled_fraction >= self._stall_ratio:
            for pool in pools:
                ceiling = pool.effective_max_size()
                # Step down from whichever is lower: the current ceiling, or the
                # size the pool actually runs at. The first stalled window therefore
                # takes ~`cap_decay` off the *current* pool size (the ceiling still
                # starts out at `max_size`), and later ones keep tightening from the
                # ceiling that is already in effect.
                size = pool.current_size()
                if size <= 0:
                    # An empty pool says nothing about how big this operator needs
                    # to be: `current_size()` is 0 only when no actor exists at all
                    # (none created yet, or every actor handed back). Pending actors
                    # are counted, so they never produce 0. Cutting the ceiling to
                    # `min_size` off that would pin the pool there until a full
                    # window of relief let it grow back.
                    continue
                base = min(ceiling, size)
                new_ceiling = max(pool.min_size(), int(base * self._cap_decay))
                if new_ceiling < ceiling:
                    pool.set_dynamic_max_size(new_ceiling)
                    adjusted = True
                    logger.info(
                        # `%s`, not `%d`: an actor pool with no configured
                        # `max_size` reports an unbounded ceiling (`inf`), and
                        # `%d` on it raises OverflowError.
                        "Stepped down adaptive max size of %s pool: %s -> %d "
                        "(current size=%d, decay base=%d; output backpressured "
                        "for %.0f%% of the last %ss)",
                        op,
                        "unbounded" if math.isinf(ceiling) else ceiling,
                        new_ceiling,
                        size,
                        base,
                        stalled_fraction * 100,
                        self._window_s,
                    )
        elif stalled_fraction <= self._resume_ratio:
            for pool in pools:
                ceiling = pool.effective_max_size()
                if ceiling >= pool.max_size():
                    continue
                # `cap_grow=0` means "keep the ceiling": do not silently grow by
                # one anyway, which would make the knob's documented meaning
                # wrong. Positive values always add at least one actor because of
                # the `ceil`.
                grown = ceiling + int(math.ceil(ceiling * self._cap_grow))
                if grown <= ceiling:
                    continue
                new_ceiling = min(pool.max_size(), grown)
                # Growing back to the configured maximum means the ceiling is no
                # longer doing anything: clear it so the pool is unconstrained.
                pool.set_dynamic_max_size(
                    None if new_ceiling >= pool.max_size() else new_ceiling
                )
                adjusted = True
                logger.info(
                    "Stepped up adaptive max size of %s pool: %s -> %d "
                    "(current size=%d; output not backpressured for %.0f%% "
                    "of the last %ss)",
                    op,
                    "unbounded" if math.isinf(ceiling) else ceiling,
                    new_ceiling,
                    pool.current_size(),
                    (1 - stalled_fraction) * 100,
                    self._window_s,
                )

        if adjusted:
            # Only an actual step consumes the window. A sample in the
            # hysteresis band, or a pool already at its floor, must not delay
            # the next real adjustment by a full window.
            self._cap_updated_at[op] = now

    def _consumer_is_starving(self, op: "PhysicalOperator") -> bool:
        """Whether an external consumer is blocked waiting for output.

        A second safety valve, for the same reason as ``_downstream_stalled``:
        ``OutputBackpressureGuard`` unblocks a sink's output *precisely* so that a
        starving consumer gets data, and a state we must not drain is not a state
        nobody is consuming from. Only the DAG root is read by consumers, so this
        walks the ineligible pass-through operators that may sit between ``op``
        and it (e.g. ``OutputSplitter``).
        """
        candidates = chain(
            (op,), self._resource_manager._get_downstream_ineligible_ops(op)
        )
        return any(
            self._topology[candidate].num_waiting_consumers > 0
            for candidate in candidates
        )

    def _downstream_stalled(self, op: "PhysicalOperator") -> bool:
        """Whether an eligible downstream has run dry: no running tasks, nothing
        queued.

        Safety valve -- withholding must never be what stalls the pipeline. The
        definition lives on `ResourceManager` (``downstream_starved``) because the
        actor autoscaler checks the same condition before refusing to grow a pool;
        both therefore use the same notion of downstream as
        ``OutputBackpressureGuard``, so they cannot disagree about a pipeline whose
        downstream sits behind ineligible pass-through operators (e.g.
        ``LimitOperator``).
        """
        return self._resource_manager.downstream_starved(op)

    def _stalled_fraction(self, op: "PhysicalOperator") -> Optional[float]:
        """Time-weighted share of the window ``op``'s output was not being read.

        Returns None until the op has been watched for a full window.

        Measures ``_output_blocked`` alone, not ``_is_stalled``: whether tasks
        happen to be in flight is an entry condition, and folding it into the
        measurement would make an emptied-out pool read as recovered -- ending the
        drain while its idle actors are still there to reclaim.

        Weighted by elapsed time rather than by sample count because this is
        called once per scheduling iteration: the dispatch burst loop calls it
        many times in a row, while a blocked ``ray.wait`` calls it once per
        timeout, so counting samples would measure iteration density instead of
        time.
        """
        now = time.monotonic()
        samples = self._samples.setdefault(op, deque())
        self._watching_since.setdefault(op, now)

        # Rate limited sampling: the share computed below extends the newest
        # sample's state to `now`, so the window keeps rolling even between
        # samples.
        if not samples or now - samples[-1][0] >= self._sample_interval_s:
            samples.append((now, self._output_blocked(op)))

        cutoff = now - self._window_s
        # Keep the newest sample at or before `cutoff`: its state is the one in
        # effect when the window starts. Older samples add nothing, and dropping
        # the boundary sample would make the share below cover only the samples
        # after the cutoff (a single recent sample after a long idle period could
        # then read as a fully stalled window).
        while len(samples) >= 2 and samples[1][0] <= cutoff:
            samples.popleft()

        if now - self._watching_since[op] < self._window_s:
            return None

        # Integrate from the window start, not from the oldest retained sample:
        # the latter can predate `cutoff` and would otherwise weight time outside
        # the window (or, after pruning, start too late).
        window_start = max(samples[0][0], cutoff)
        stalled_s = 0.0
        prev_t, prev_stalled = window_start, samples[0][1]
        for t, stalled in list(samples)[1:]:
            if prev_stalled:
                stalled_s += t - prev_t
            prev_t, prev_stalled = t, stalled
        if prev_stalled:
            stalled_s += now - prev_t

        elapsed_s = now - window_start
        if elapsed_s <= 0:
            return None
        return stalled_s / elapsed_s

    def _release(
        self, op: "PhysicalOperator", reason: str, *, drop_window: bool = True
    ) -> None:
        """Stop withholding from ``op`` and drop its per-op state.

        Dropping the window is deliberate: the next drain has to earn its own
        evidence, which also gives the operator a grace period after a release.
        ``drop_window=False`` keeps the rolling window alive for operators parked
        at their adaptive ceiling -- those are released every iteration, and the
        ceiling still has to be able to step down while the stall persists.
        """
        if op in self._draining:
            logger.info("Resuming input submission to %s: reason=%s", op, reason)
        self._draining.discard(op)
        if drop_window:
            self._samples.pop(op, None)
            self._watching_since.pop(op, None)
            # The AIMD clock is per-window state as well; keep it in step with the
            # window it paces (it is deliberately *not* dropped on the
            # `drop_window=False` release, where the window keeps rolling).
            self._cap_updated_at.pop(op, None)
        op.notify_actor_pool_drain(False)

    def _should_drain(self, op: "PhysicalOperator") -> bool:
        if self._window_s is None:
            return False

        if not self._resource_manager.is_op_eligible(op):
            self._release(op, "no_longer_eligible")
            self._clear_adaptive_ceiling(
                op, "the operator is no longer eligible for resource reservation"
            )
            return False

        no_downstream = not op.output_dependencies
        # A terminal actor pool is normally not a candidate: there is no
        # downstream operator to feed. When the DAG root is read by an external
        # consumer, though, its task output can be backpressured exactly like an
        # intermediate operator's -- a slow ``iter_batches`` consumer pins its
        # actors. Allow it in that case; `_consumer_is_starving` below is the
        # safety valve that stops withholding when the consumer actually waits.
        terminal_with_consumer = (
            no_downstream
            and op is self._resource_manager._output_operator
            and self._resource_manager.has_external_consumer
        )
        if (
            not op.get_autoscaling_actor_pools()
            or (no_downstream and not terminal_with_consumer)
            or self._resource_manager._is_blocking_materializing_op(op)
        ):
            # Not a candidate: no actor pool to hand back, nothing downstream to
            # feed, or withholding could stall a pipeline that needs full
            # materialization. Clearing matters here: `_is_blocking_materializing_op`
            # flips to True once the downstream operator that made this one a
            # candidate finishes, and this return precedes the floor release, so a
            # drain flagged earlier would otherwise never be cleared (its pool
            # would stay force-shrunk while the operator is fed again). The same
            # applies to the adaptive ceiling: leaving it on a pool that is no
            # longer a drain candidate would cap it forever, precisely when the
            # operator has to run at full speed.
            self._release(op, "no_longer_candidate")
            self._clear_adaptive_ceiling(
                op, "the operator is no longer a drain candidate"
            )
            return False

        # Sampled on every iteration, whether or not the operator is currently
        # being withheld: both ends of the hysteresis are decided on the same
        # rolling share, so the window has to keep moving while draining. Left
        # frozen, it would keep reporting the state that engaged the drain and
        # relief could never be observed.
        stalled_fraction = self._stalled_fraction(op)

        # Step the pools' adaptive ceiling, at most once per window, before any of
        # the release paths below: an operator parked at its ceiling is released
        # every iteration, and the ceiling still has to keep stepping down while the
        # stall persists.
        self._update_adaptive_ceiling(op, stalled_fraction)

        if self._downstream_stalled(op):
            # Safety valve: withholding must never be what starves the pipeline.
            # The operator has to be able to produce again, so the adaptive ceiling
            # is dropped as well. Checked before the floor release: at the floor the
            # pool is deliberately parked, and a starving downstream still has to
            # be able to lift the ceiling and grow it again.
            self._release(op, "downstream_dry (an eligible downstream has run dry)")
            self._clear_adaptive_ceiling(op, "an eligible downstream has run dry")
            return False

        if self._consumer_is_starving(op):
            # Safety valve: the guard unblocks a sink precisely so a waiting
            # consumer gets data, so this is a state to feed, not to drain. This is
            # also checked before the floor release for the same liveness reason.
            self._release(
                op, "consumer_blocked (an external consumer is waiting for output)"
            )
            self._clear_adaptive_ceiling(
                op, "an external consumer is waiting for output"
            )
            return False

        if self._at_floor(op):
            # Nothing left to reclaim. The ceiling is kept -- it is what stops the
            # pool from being re-grown straight back into the stall.
            #
            # The window, however, is only kept when there *is* a ceiling. A pool
            # sitting at `min_size` cannot be telling us anything by being stalled
            # (there is nothing left to reclaim), so evidence gathered there must
            # not be what re-engages the drain the moment the pool grows above it:
            # that is a grow/shrink cycle at the floor, which the original release
            # deliberately avoided by dropping the window. With a ceiling, keeping
            # the window is what lets the ceiling keep stepping down.
            has_ceiling = any(
                p.dynamic_max_size() is not None
                for p in op.get_autoscaling_actor_pools()
            )
            self._release(
                op,
                "pool_at_target (nothing left to reclaim)"
                if has_ceiling
                else "pool_at_min (nothing left to reclaim)",
                # Drop the window when there is *no* ceiling: at `min_size` the
                # floor release is where the old behaviour re-earned its evidence.
                drop_window=not has_ceiling,
            )
            return False

        if op in self._draining:
            if stalled_fraction is not None and stalled_fraction <= self._resume_ratio:
                self._release(
                    op,
                    f"backpressure_relieved ({stalled_fraction:.0%} of the last "
                    f"{self._window_s}s stalled, at or below the "
                    f"{self._resume_ratio:.0%} resume threshold)",
                )
                return False
            # Still at or above the resume threshold: keep withholding. In the
            # band between the two thresholds this is the hysteresis -- the state
            # is kept rather than flipped by a flickering sample.
            return True

        if stalled_fraction is None or stalled_fraction < self._stall_ratio:
            return False

        if self._in_flight_tasks(op) == 0:
            # The pool is already idle; the autoscaler will shrink it on its own.
            return False

        self._draining.add(op)
        op.notify_actor_pool_drain(True)
        return True

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        was_draining = op in self._draining
        should_drain = self._should_drain(op)

        if should_drain and not was_draining:
            logger.info(
                "Draining actor pool of %s: output not read for >= %.0f%% of the "
                "last %ss while %s task(s) are in flight, so more inputs cannot "
                "raise its throughput and its actors cannot be reclaimed. "
                "Withholding new inputs until the output is read again or the "
                "pool reaches its drain floor (min_size, or the adaptive ceiling "
                "when one is in effect).",
                op,
                self._stall_ratio * 100,
                self._window_s,
                self._in_flight_tasks(op),
            )

        return not should_drain
