"""Metrics the sizer emits at run time."""

from ray.util.metrics import Counter, Histogram

# Sizer ticks are milliseconds-scale (a slow tick is tens of ms), far below the
# generic seconds-scale buckets.
SIZER_TICK_DURATION_BUCKETS_S = [
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
]


class SizerMetrics:
    """Per-operator actor churn and per-phase tick duration.

    Module-level singletons: Ray metrics are process-global, so every dataset
    in the process shares the objects and separates its series by the
    ``dataset`` tag.
    """

    actor_delta = Counter(
        "data_sizer_actor_delta",
        description=(
            "Actors the sizer added (direction=up) or removed (direction=down),"
            " by phase (per_op_how_many = what how_many asked for;"
            " per_op_where = what placement resolved)."
        ),
        tag_keys=("dataset", "operator", "phase", "direction"),
    )
    tick_duration_s = Histogram(
        "data_sizer_tick_duration_s",
        description=(
            "OperatorSizer per-phase wall time in seconds, per tick. build:"
            " the tick's cluster snapshot, summed over the steps since the"
            " last tick | prepare: status updates, room held for ops awaiting"
            " input, the bottleneck snapshot and the phase | how_many: one rule"
            " and count per op, and the ledger's cut on upscales |"
            " per_op_where: placement | apply: every hand-off to the pools"
            " this tick | tick: one whole sizing decision (everything but"
            " build)."
        ),
        boundaries=SIZER_TICK_DURATION_BUCKETS_S,
        tag_keys=("dataset", "phase"),
    )

    def __init__(self, dataset_id: str):
        self._dataset_id = dataset_id

    def record_actor_delta(self, *, operator: str, phase: str, delta: int) -> None:
        """One phase's signed actor delta for one operator."""
        if delta == 0:
            return
        self.actor_delta.inc(
            abs(delta),
            tags={
                "dataset": self._dataset_id,
                "operator": operator,
                "phase": phase,
                "direction": "up" if delta > 0 else "down",
            },
        )

    def record_tick_duration(self, phase: str, seconds: float) -> None:
        self.tick_duration_s.observe(
            seconds, tags={"dataset": self._dataset_id, "phase": phase}
        )
