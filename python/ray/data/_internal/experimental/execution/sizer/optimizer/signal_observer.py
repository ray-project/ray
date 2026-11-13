"""The optimizer's memory of how each operator has been behaving."""

from __future__ import annotations

from typing import TYPE_CHECKING, AbstractSet, Callable, Dict, Sequence, Tuple

from ray.data._internal.execution.execution_flags import (
    SIZER_SHORTFALL_TICKS,
    SIZER_SIGNAL_WINDOW_SAMPLES,
)
from ray.data._internal.experimental.execution.sizer.models import OpShortfall
from ray.data._internal.experimental.execution.sizer.signals import (
    BoolStreak,
    WindowedSignal,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

# Answers "is this op's output backpressured?" -> (backpressured, blocked, running).
BackpressureProbe = Callable[
    ["ExperimentalActorPoolMapOperator"], Tuple[bool, int, int]
]


class SignalObserver:
    """Rules decide on windows, not instants: an op qualifies as a bottleneck
    or a donor only if its signals held for a full window, which keeps one
    noisy sample from moving actors around. ``observe`` samples once per
    scheduling-loop step, ``record_sizing_results`` folds in what each sizing
    pass granted, and ``signal_dict`` hands the whole picture to the rules as
    plain data."""

    def __init__(self, is_output_backpressured: BackpressureProbe):
        self._is_output_backpressured = is_output_backpressured
        self._busy_tasks_signal: Dict[
            "ExperimentalActorPoolMapOperator", WindowedSignal
        ] = {}
        self._productive_tasks_signal: Dict[
            "ExperimentalActorPoolMapOperator", WindowedSignal
        ] = {}
        self._backpressured_streak: Dict[
            "ExperimentalActorPoolMapOperator", BoolStreak
        ] = {}
        self._not_backpressured_streak: Dict[
            "ExperimentalActorPoolMapOperator", BoolStreak
        ] = {}
        self._shortfall_streak: Dict[
            "ExperimentalActorPoolMapOperator", BoolStreak
        ] = {}
        self._shortfalls: Dict["ExperimentalActorPoolMapOperator", OpShortfall] = {}
        # Wants a rule could not place, merged into the next sizing pass.
        self._silent_shortfalls: Dict[
            "ExperimentalActorPoolMapOperator", OpShortfall
        ] = {}

    def observe(self, ops: Sequence["ExperimentalActorPoolMapOperator"]) -> None:
        """Sample every op; called on every scheduling-loop step."""
        for op in ops:
            in_flight = op.actor_pool.num_tasks_in_flight()
            backpressured, _blocked_actors, _running = self._is_output_backpressured(op)
            self._window(self._busy_tasks_signal, op).record(in_flight)
            # Running tasks, not tasks in flight: a busy actor can also hold its
            # next task, queued on the actor behind the running one. That task
            # is not work yet, and counting it would protect up to
            # max_tasks_in_flight times the actors doing work. The op's input
            # backlog is not in this signal; a backlog keeps actors running,
            # which protects them.
            self._window(self._productive_tasks_signal, op).record(
                op.productive_task_slots()
            )
            self._streak(
                self._backpressured_streak, op, SIZER_SIGNAL_WINDOW_SAMPLES
            ).record(backpressured)
            self._streak(
                self._not_backpressured_streak, op, SIZER_SIGNAL_WINDOW_SAMPLES
            ).record(not backpressured)

    def record_sizing_results(
        self,
        ordered_ops: Sequence["ExperimentalActorPoolMapOperator"],
        ungranted_ops: AbstractSet["ExperimentalActorPoolMapOperator"],
        shortfalls: Dict["ExperimentalActorPoolMapOperator", OpShortfall],
        backpressured_with_demand: AbstractSet[
            "ExperimentalActorPoolMapOperator"
        ] = frozenset(),
    ) -> None:
        """Fold one sizing pass into the shortfall streaks.

        ``backpressured_with_demand`` ops neither grow nor reset their streak:
        only an ungranted ask builds it, but an op switching between asking
        and being backpressured keeps the progress it has made.
        """
        silent = self._silent_shortfalls
        self._silent_shortfalls = {}
        for op in ordered_ops:
            streak = self._streak(self._shortfall_streak, op, SIZER_SHORTFALL_TICKS)
            if op in ungranted_ops or op in silent:
                streak.record(True)
            elif op not in backpressured_with_demand:
                streak.record(False)
        # Sizer-recorded shortfalls win; silent ones fill the gaps.
        self._shortfalls = {**silent, **shortfalls}

    def merge_silent_shortfalls(
        self, silent: Dict["ExperimentalActorPoolMapOperator", OpShortfall]
    ) -> None:
        """Carry unplaced rule wants into the next sizing pass."""
        self._silent_shortfalls.update(silent)

    def reset(self, op: "ExperimentalActorPoolMapOperator") -> None:
        """Forget ``op``'s windows, so rules wait for a full window of fresh
        samples before judging it again."""
        for signals in (
            self._busy_tasks_signal,
            self._productive_tasks_signal,
            self._backpressured_streak,
            self._not_backpressured_streak,
        ):
            signals.pop(op, None)

    def signal_dict(self) -> Dict[str, object]:
        """The observed state, as OptimizationContext fields."""
        return dict(
            shortfall_streak=self._shortfall_streak,
            shortfalls=self._shortfalls,
            busy_tasks_signal=self._busy_tasks_signal,
            productive_tasks_signal=self._productive_tasks_signal,
            backpressured_streak=self._backpressured_streak,
            not_backpressured_streak=self._not_backpressured_streak,
        )

    @staticmethod
    def _window(
        windows: Dict["ExperimentalActorPoolMapOperator", WindowedSignal],
        op: "ExperimentalActorPoolMapOperator",
    ) -> WindowedSignal:
        if op not in windows:
            windows[op] = WindowedSignal(SIZER_SIGNAL_WINDOW_SAMPLES)
        return windows[op]

    @staticmethod
    def _streak(
        streaks: Dict["ExperimentalActorPoolMapOperator", BoolStreak],
        op: "ExperimentalActorPoolMapOperator",
        length: int,
    ) -> BoolStreak:
        if op not in streaks:
            streaks[op] = BoolStreak(length)
        return streaks[op]
