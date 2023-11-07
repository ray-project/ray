import json
import logging
import numpy as np
import os
from pathlib import Path
import shutil
import tempfile

from typing import TYPE_CHECKING, Dict, List, TextIO

from ray.air.constants import (
    EXPR_PARAM_FILE,
    EXPR_PARAM_PICKLE_FILE,
    EXPR_RESULT_FILE,
)
import ray.cloudpickle as cloudpickle
from ray.train._internal.syncer import _BackgroundProcessLauncher
from ray.train._internal.storage import _upload_to_fs_path
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

        self._tempdir = Path(tempfile.mkdtemp())

        self._process_launcher = _BackgroundProcessLauncher(period=60)

    def log_trial_start(self, trial: "Trial"):
        if trial in self._trial_files:
            self._trial_files[trial].close()

        # Make sure logdir exists
        trial_local_path = self._tempdir.joinpath(trial.storage.trial_dir_name)
        trial_local_path.mkdir(exist_ok=True)
        local_file = trial_local_path.joinpath(EXPR_RESULT_FILE)

        # Update config
        self.update_config(trial, trial.config, trial_local_path)

        # Resume the file from remote storage.
        if not local_file.exists():
            self._restore_from_remote(
                EXPR_RESULT_FILE, trial, trial_local_path.as_posix()
            )

        self._trial_files[trial] = open(local_file, "at")

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        if trial not in self._trial_files:
            self.log_trial_start(trial)
        json.dump(result, self._trial_files[trial], cls=SafeFallbackEncoder)
        self._trial_files[trial].write("\n")
        self._trial_files[trial].flush()

        self._process_launcher.launch_if_needed(
            _upload_to_fs_path,
            dict(
                local_path=self._tempdir.as_posix(),
                fs=trial.storage.storage_filesystem,
                fs_path=trial.storage.experiment_fs_path,
            ),
        )

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        if trial not in self._trial_files:
            return

        self._trial_files[trial].close()
        del self._trial_files[trial]

    def on_experiment_end(self, trials: List["Trial"], **info):
        self._process_launcher.launch(
            _upload_to_fs_path,
            dict(
                local_path=self._tempdir.as_posix(),
                fs=trials[0].storage.storage_filesystem,
                fs_path=trials[0].storage.experiment_fs_path,
            ),
        )
        self._process_launcher.wait()
        shutil.rmtree(self._tempdir.as_posix())

    def update_config(self, trial: "Trial", config: Dict, trial_local_path):
        self._trial_configs[trial] = config

        config_out = os.path.join(trial_local_path, EXPR_PARAM_FILE)
        with open(config_out, "w") as f:
            json.dump(
                self._trial_configs[trial],
                f,
                indent=2,
                sort_keys=True,
                cls=SafeFallbackEncoder,
            )

        config_pkl = os.path.join(trial_local_path, EXPR_PARAM_PICKLE_FILE)
        with open(config_pkl, "wb") as f:
            cloudpickle.dump(self._trial_configs[trial], f)
