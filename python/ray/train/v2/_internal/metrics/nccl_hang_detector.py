from enum import IntEnum
from typing import Dict

from ray.train.v2._internal.metrics.base import (
    RUN_ID_TAG_KEY,
    RUN_NAME_TAG_KEY,
    ValueMetric,
)


class NCCLHangDetectorState(IntEnum):
    """What the NCCL hang detector believes about the run, as a gauge value.

    Ordered by severity so the worst communicator's state is the run's state.
    """

    HEALTHY = 0
    SUSPECTED = 1
    CONFIRMED = 2


class NCCLHangDetectorMetrics:
    """Factory for the metrics recorded by the NCCL hang detector callback.

    Both metrics are recorded on every successful RAS poll so a healthy run has
    a flat series rather than none, and describe the run's most stalled
    communicator.
    """

    # ===== Metric Names =====
    STATE = "train_nccl_hang_detector_state"
    STALL_DURATION_S = "train_nccl_hang_stall_duration_s"

    @classmethod
    def get_nccl_hang_detector_metrics(
        cls, run_name: str, run_id: str
    ) -> Dict[str, ValueMetric]:
        base_tags = {RUN_NAME_TAG_KEY: run_name, RUN_ID_TAG_KEY: run_id}
        return {
            cls.STATE: ValueMetric(
                name=cls.STATE,
                description=(
                    "State of the NCCL hang detector: 0 for healthy, 1 for a "
                    "suspected hang and 2 for a confirmed hang."
                ),
                base_tags=base_tags,
            ),
            cls.STALL_DURATION_S: ValueMetric(
                name=cls.STALL_DURATION_S,
                description=(
                    "Seconds the most stalled NCCL communicator has made no "
                    "progress with a collective mismatch."
                ),
                base_tags=base_tags,
            ),
        }
