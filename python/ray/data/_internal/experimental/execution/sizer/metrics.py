"""Metrics emmited by sizer (and optimizer) during its run time"""

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
            " by phase (per_op_how_many = what sizing asked for;"
            " per_op_where = what placement resolved)."
        ),
        tag_keys=("dataset", "operator", "phase", "direction"),
    )
    tick_duration_s = Histogram(
        "data_sizer_tick_duration_s",
        description=(
            "OperatorSizer per-phase tick wall time in seconds. prepare:"
            " signal sampling plus the tick's cluster snapshot |"
            " cross_op_optimize: the pipeline optimizer rules |"
            " per_op_how_many: per-operator sizing | per_op_where: placement |"
            " apply: handing requests to the pools | e2e: one whole"
            " perform_sizing_decision call."
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


class OptimizerMetrics:
    actions = Counter(
        "data_pipeline_optimizer_actions",
        description=(
            "Optimizer actions by stage. capacity_transfer: fired ->"
            " landed | timeout. silent_bottleneck: granted."
        ),
        tag_keys=("dataset", "optimization", "recipient", "result"),
    )
    capacity_transfers = Counter(
        "data_pipeline_optimizer_capacity_transfers",
        description=(
            "Actors moved by capacity transfers, as donor -> recipient"
            " edges (incremented by the actor count of each transfer)."
        ),
        tag_keys=("dataset", "donor", "recipient"),
    )
    refusals = Counter(
        "data_pipeline_optimizer_refusals",
        description=(
            "Why an optimization refused to act: warmup | cluster_settling |"
            " no_donor_surplus | no_free_capacity | capacity_already_free |"
            " victims_insufficient | thrashing."
        ),
        tag_keys=("dataset", "optimization", "reason"),
    )

    def __init__(self, dataset_id: str):
        self._dataset_id = dataset_id

    def record_outcome(self, optimization: str, operator: str, outcome: str) -> None:
        """One step of an optimization acting on ``operator`` (the op that
        grows). ``outcome`` is fired / landed / lost / timeout for
        capacity_transfer, granted for silent_bottleneck."""
        self.actions.inc(
            1,
            tags={
                "dataset": self._dataset_id,
                "optimization": optimization,
                "recipient": operator,
                "result": outcome,
            },
        )

    def record_capacity_transfer(
        self, donor: str, recipient: str, num_actors: int
    ) -> None:
        self.capacity_transfers.inc(
            num_actors,
            tags={
                "dataset": self._dataset_id,
                "donor": donor,
                "recipient": recipient,
            },
        )

    def record_refusal(self, optimization: str, reason: str) -> None:
        self.refusals.inc(
            1,
            tags={
                "dataset": self._dataset_id,
                "optimization": optimization,
                "reason": reason,
            },
        )
