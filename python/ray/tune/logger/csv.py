import csv
import logging
import os

from typing import TYPE_CHECKING, Dict, TextIO

from ray.tune.logger.logger import Logger, LoggerCallback
from ray.tune.result import EXPR_PROGRESS_FILE
from ray.tune.utils import flatten_dict
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.tune.experiment.trial import Trial  # noqa: F401

logger = logging.getLogger(__name__)


@PublicAPI
class CSVLogger(Logger):
    """Logs results to progress.csv under the trial directory.

    Automatically flattens nested dicts in the result dict before writing
    to csv:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}

    """

    def _init(self):
        self._initialized = False

    def _maybe_init(self):
        """CSV outputted with Headers as first set of results."""
        if not self._initialized:
            progress_file = os.path.join(self.logdir, EXPR_PROGRESS_FILE)
            self._continuing = (
                os.path.exists(progress_file) and os.path.getsize(progress_file) > 0
            )
            self._file = open(progress_file, "a")
            self._csv_out = None
            self._initialized = True

    def on_result(self, result: Dict):
        self._maybe_init()

        tmp = result.copy()
        if "config" in tmp:
            del tmp["config"]
        result = flatten_dict(tmp, delimiter="/")
        if self._csv_out is None:
            self._csv_out = csv.DictWriter(self._file, result.keys())
            if not self._continuing:
                self._csv_out.writeheader()
        self._csv_out.writerow(
            {k: v for k, v in result.items() if k in self._csv_out.fieldnames}
        )
        self._file.flush()

    def flush(self):
        if self._initialized and not self._file.closed:
            self._file.flush()

    def close(self):
        if self._initialized:
            self._file.close()


@PublicAPI
class CSVLoggerCallback(LoggerCallback):
    """Logs results to progress.csv under the trial directory.

    Automatically flattens nested dicts in the result dict before writing
    to csv:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}

    """

    def __init__(self):
        self._trial_continue: Dict["Trial", bool] = {}
        self._trial_files: Dict["Trial", TextIO] = {}
        self._trial_csv: Dict["Trial", csv.DictWriter] = {}

    def _setup_trial(self, trial: "Trial"):
        if trial in self._trial_files:
            self._trial_files[trial].close()

        # Make sure logdir exists
        trial.init_logdir()
        local_file = os.path.join(trial.logdir, EXPR_PROGRESS_FILE)
        self._trial_continue[trial] = (
            os.path.exists(local_file) and os.path.getsize(local_file) > 0
        )
        self._trial_files[trial] = open(local_file, "at")
        self._trial_csv[trial] = None

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        if trial not in self._trial_files:
            self._setup_trial(trial)

        tmp = result.copy()
        tmp.pop("config", None)
        result = flatten_dict(tmp, delimiter="/")

        if not self._trial_csv[trial]:
            self._trial_csv[trial] = csv.DictWriter(
                self._trial_files[trial], result.keys()
            )
            if not self._trial_continue[trial]:
                self._trial_csv[trial].writeheader()

        self._trial_csv[trial].writerow(
            {k: v for k, v in result.items() if k in self._trial_csv[trial].fieldnames}
        )
        self._trial_files[trial].flush()

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        if trial not in self._trial_files:
            return

        del self._trial_csv[trial]
        self._trial_files[trial].close()
        del self._trial_files[trial]
