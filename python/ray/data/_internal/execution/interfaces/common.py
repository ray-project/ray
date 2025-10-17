# Node id string returned by `ray.get_runtime_context().get_node_id()`.
import bisect
import json
from typing import Dict, List, Optional
from ray.util.metrics import Histogram


NodeIdStr = str

# Used for time-based histograms (e.g. task completion time, block completion time)
histogram_buckets_s = [
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    15.0,
    20.0,
    25.0,
    50.0,
    75.0,
    100.0,
    150.0,
    500.0,
    1000.0,
    2500.0,
    5000.0,
]

KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB

# Used for size-based histograms (e.g. block size in bytes)
histogram_buckets_bytes = [
    KiB,
    8 * KiB,
    64 * KiB,
    128 * KiB,
    256 * KiB,
    512 * KiB,
    MiB,
    8 * MiB,
    64 * MiB,
    128 * MiB,
    256 * MiB,
    512 * MiB,
    GiB,
    4 * GiB,
    16 * GiB,
    64 * GiB,
    128 * GiB,
    256 * GiB,
    512 * GiB,
    1024 * GiB,
    4096 * GiB,
]

# Used for row count-based histograms (e.g. block size in rows)
histogram_bucket_rows = [
    1,
    5,
    10,
    25,
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10000,
    25000,
    50000,
    100000,
    250000,
    500000,
    1000000,
    2500000,
    5000000,
    10000000,
]


class RuntimeMetricsHistogram:
    """
    Class that tracks a histogram of values.

    Contains helper methods to record the values and apply those values to a `ray.util.metrics.Histogram` metric.
    """

    def __init__(self, boundaries: List[float]):
        self.boundaries = boundaries
        # Initialize bucket counts to 0 (+1 additional bucket to represent the +Inf bucket)
        self._bucket_counts = [0 for _ in range(len(boundaries) + 1)]

    def observe(self, value: float, num_observations: int = 1):
        self._bucket_counts[self._find_bucket_index(value)] += num_observations

    def apply_to_metric(
        self,
        metric: Histogram,
        tags: Dict[str, str],
    ):
        """
        This method calculates the difference between the current bucket counts and the previous bucket counts,
        and applies those observations to the metric.

        This method stores the previous_bucket_counts in the metric as `last_applied_bucket_counts_for_tags`.
        """
        if getattr(metric, "last_applied_bucket_counts_for_tags", None) is None:
            metric.last_applied_bucket_counts_for_tags = {}
        tags_key = json.dumps(tags, sort_keys=True)
        previous_bucket_counts = metric.last_applied_bucket_counts_for_tags.get(
            tags_key
        )

        for i in range(len(self._bucket_counts)):
            # Pick a value between the boundaries so the sample falls into the right bucket.
            # We need to calculate the mid point because choosing the exact boundary value
            # seems to have unreliable behavior on which bucket it ends up in.
            boundary_upper_bound = (
                self.boundaries[i]
                if i < len(self._bucket_counts) - 1
                else self.boundaries[-1] + 100
            )
            boundary_lower_bound = self.boundaries[i - 1] if i > 0 else 0
            bucket_value = (boundary_upper_bound + boundary_lower_bound) / 2

            # Calculate how many observations to add to the metric
            diff = (
                self._bucket_counts[i] - previous_bucket_counts[i]
                if previous_bucket_counts is not None
                else self._bucket_counts[i]
            )
            if diff > 0:
                for _ in range(diff):
                    metric.observe(bucket_value, tags)

        metric.last_applied_bucket_counts_for_tags[
            tags_key
        ] = self._bucket_counts.copy()

    def __repr__(self):
        return f"{self._bucket_counts}"

    def _find_bucket_index(self, value: float):
        return bisect.bisect_left(self.boundaries, value)
