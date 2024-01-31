import json
import logging
import numpy as np
import os

from typing import TYPE_CHECKING, Dict, TextIO

from ray.air.constants import (
    EXPR_PARAM_FILE,
    EXPR_PARAM_PICKLE_FILE,
    EXPR_RESULT_FILE,
)
import ray.cloudpickle as cloudpickle
from ray.tune.logger.logger import _LOGGER_DEPRECATION_WARNING, Logger, LoggerCallback
from ray.tune.utils.util import SafeFallbackEncoder
from ray.util.annotations import Deprecated, PublicAPI

if TYPE_CHECKING:
    from ray.tune.experiment.trial import Trial  # noqa: F401

logger = logging.getLogger(__name__)

tf = None
VALID_SUMMARY_TYPES = [int, float, np.float32, np.float64, np.int32, np.int64]


@Deprecated(
    message=_LOGGER_DEPRECATION_WARNING.format(
        old="JsonLogger", new="ray.tune.json.JsonLoggerCallback"
    ),
    warning=True,
)
@PublicAPI
class JsonLogger(Logger):
    """Logs trial results in json format.

    Also writes to a results file and param.json file when results or
    configurations are updated. Experiments must be executed with the
    JsonLogger to be compatible with the ExperimentAnalysis tool.
    """

    def _init(self):
        self.update_config(self.config)
        local_file = os.path.join(self.logdir, EXPR_RESULT_FILE)
        self.local_out = open(local_file, "a")

    def on_result(self, result: Dict):
        json.dump(result, self, cls=SafeFallbackEncoder)
        self.write("\n")
        self.local_out.flush()

    def write(self, b):
        self.local_out.write(b)

    def flush(self):
        if not self.local_out.closed:
            self.local_out.flush()

    def close(self):
        self.local_out.close()

    def update_config(self, config: Dict):
        self.config = config
        config_out = os.path.join(self.logdir, EXPR_PARAM_FILE)
        with open(config_out, "w") as f:
            json.dump(self.config, f, indent=2, sort_keys=True, cls=SafeFallbackEncoder)
        config_pkl = os.path.join(self.logdir, EXPR_PARAM_PICKLE_FILE)
        with open(config_pkl, "wb") as f:
            cloudpickle.dump(self.config, f)


@PublicAPI
class JsonLoggerCallback(LoggerCallback):
    """Logs trial results in json format.

    Also writes to a results file and param.json file when results or
    configurations are updated. Experiments must be executed with the
    JsonLoggerCallback to be compatible with the ExperimentAnalysis tool.
    """

    _SAVED_FILE_TEMPLATES = [EXPR_RESULT_FILE, EXPR_PARAM_FILE, EXPR_PARAM_PICKLE_FILE]

    def __init__(self):
        self._trial_configs: Dict["Trial", Dict] = {}
        self._trial_files: Dict["Trial", TextIO] = {}

    def log_trial_start(self, trial: "Trial"):
        if trial in self._trial_files:
            self._trial_files[trial].close()

        # Update config
        self.update_config(trial, trial.config)

        # Make sure logdir exists
        trial.init_local_path()
        local_file = os.path.join(trial.local_path, EXPR_RESULT_FILE)

        # Resume the file from remote storage.
        self._restore_from_remote(EXPR_RESULT_FILE, trial)

        self._trial_files[trial] = open(local_file, "at")

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        if trial not in self._trial_files:
            self.log_trial_start(trial)
        json.dump(result, self._trial_files[trial], cls=SafeFallbackEncoder)
        self._trial_files[trial].write("\n")
        self._trial_files[trial].flush()

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        if trial not in self._trial_files:
            return

        self._trial_files[trial].close()
        del self._trial_files[trial]

    def update_config(self, trial: "Trial", config: Dict):
        self._trial_configs[trial] = config

        config_out = os.path.join(trial.local_path, EXPR_PARAM_FILE)
        with open(config_out, "w") as f:
            json.dump(
                self._trial_configs[trial],
                f,
                indent=2,
                sort_keys=True,
                cls=SafeFallbackEncoder,
            )

        config_pkl = os.path.join(trial.local_path, EXPR_PARAM_PICKLE_FILE)
        with open(config_pkl, "wb") as f:
            cloudpickle.dump(self._trial_configs[trial], f)
