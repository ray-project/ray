import json
from collections import deque
from numbers import Number
from typing import Tuple, Optional

from ray.tune.utils.serialization import TuneFunctionEncoder, TuneFunctionDecoder


class _TrainingRunMetadata:
    """Serializable struct for holding runtime trial metadata.

    Runtime metadata is data that changes and is updated on runtime. This includes
    e.g. the last result, the currently available checkpoints, and the number
    of errors encountered for a trial.
    """

    def __init__(self, n_steps: Tuple[int] = (5, 10)):
        # General metadata
        self.start_time = None

        # Errors
        self.num_failures = 0
        self.num_failures_after_restore = 0

        self.error_filename = None
        self.pickled_error_filename = None

        # Results and metrics
        self.last_result = {}
        self.last_result_time = -float("inf")

        # stores in memory max/min/avg/last-n-avg/last result for each
        # metric by trial
        self.metric_analysis = {}
        self._n_steps = n_steps
        self.metric_n_steps = {}

        # Checkpoints
        self.checkpoint_manager = None

        self._cached_json = None

    def invalidate_cache(self):
        self._cached_json = None

    def update_metric(self, metric: str, value: Number, step: Optional[int] = 1):
        if metric not in self.metric_analysis:
            self.metric_analysis[metric] = {
                "max": value,
                "min": value,
                "avg": value,
                "last": value,
            }
            self.metric_n_steps[metric] = {}
            for n in self._n_steps:
                key = "last-{:d}-avg".format(n)
                self.metric_analysis[metric][key] = value
                # Store n as string for correct restore.
                self.metric_n_steps[metric][str(n)] = deque([value], maxlen=n)
        else:
            step = step or 1
            self.metric_analysis[metric]["max"] = max(
                value, self.metric_analysis[metric]["max"]
            )
            self.metric_analysis[metric]["min"] = min(
                value, self.metric_analysis[metric]["min"]
            )
            self.metric_analysis[metric]["avg"] = (
                1 / step * (value + (step - 1) * self.metric_analysis[metric]["avg"])
            )
            self.metric_analysis[metric]["last"] = value

            for n in self._n_steps:
                key = "last-{:d}-avg".format(n)
                self.metric_n_steps[metric][str(n)].append(value)
                self.metric_analysis[metric][key] = sum(
                    self.metric_n_steps[metric][str(n)]
                ) / len(self.metric_n_steps[metric][str(n)])
        self.invalidate_cache()

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key not in {"_cached_json"}:
            self.invalidate_cache()

    def get_json_state(self) -> str:
        if self._cached_json is None:
            data = self.__dict__
            data.pop("_cached_json", None)
            self._cached_json = json.dumps(data, indent=2, cls=TuneFunctionEncoder)

        return self._cached_json

    @classmethod
    def from_json_state(cls, json_state: str) -> "_TrainingRunMetadata":
        state = json.loads(json_state, cls=TuneFunctionDecoder)

        run_metadata = cls()
        run_metadata.__dict__.update(state)

        return run_metadata
