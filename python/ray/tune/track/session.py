import logging
import shutil
import os
from datetime import datetime

from ray.tune.logger import UnifiedLogger
from ray.tune.result import DEFAULT_RESULTS_DIR, TRAINING_ITERATION
from ray.tune.trainable import CHECKPOINT_DIR_FORMAT, TrainableUtil
from ray.tune.trial import Trial

logger = logging.getLogger(__name__)

# TODO(ujvl): Refactor these classes.


class TrackSession:
    """Manages results for a single session.

    Represents a single Trial in an experiment.

    Attributes:
        trial_name (str): Custom trial name.
        experiment_dir (str): Directory where results for all trials
            are stored. Each session is stored into a unique directory
            inside experiment_dir.
        upload_dir (str): Directory to sync results to.
        trial_config (dict): Parameters that will be logged to disk.
    """

    def __init__(self,
                 trial_name="",
                 experiment_dir=None,
                 upload_dir=None,
                 trial_config=None,
                 init_logger=True):
        self._experiment_dir = None
        self._logdir = None
        self._upload_dir = None
        self._iteration = 0
        self.trial_config = None
        self.trial_id = Trial.generate_id()
        if trial_name:
            self.trial_id = trial_name + "_" + self.trial_id
        if init_logger:
            self._initialize_logging(trial_name, experiment_dir, upload_dir,
                                     trial_config)

    def _initialize_logging(self,
                            trial_name="",
                            experiment_dir=None,
                            upload_dir=None,
                            trial_config=None):
        if upload_dir:
            raise NotImplementedError("`upload_dir` is not yet implemented.")

        # TODO(rliaw): In other parts of the code, this is `local_dir`.
        if experiment_dir is None:
            experiment_dir = os.path.join(DEFAULT_RESULTS_DIR, "default")

        self._experiment_dir = os.path.expanduser(experiment_dir)

        # TODO(rliaw): Refactor `logdir` to `trial_dir`.
        self._logdir = Trial.create_logdir(trial_name, self._experiment_dir)
        self._upload_dir = upload_dir
        self.trial_config = trial_config or {}

        # misc metadata to save as well
        self.trial_config["trial_id"] = self.trial_id
        self._logger = UnifiedLogger(self.trial_config, self._logdir)

    def log(self, **metrics):
        """Logs all named arguments specified in `metrics`.

        This will log trial metrics locally, and they will be synchronized
        with the driver periodically through ray.

        Arguments:
            metrics: named arguments with corresponding values to log.
        """
        # TODO: Implement a batching mechanism for multiple calls to `log`
        #     within the same iteration.
        metrics_dict = metrics.copy()
        metrics_dict.update({"trial_id": self.trial_id})

        # TODO: Move Trainable autopopulation to a util function
        metrics_dict.setdefault(TRAINING_ITERATION, self._iteration)
        self._logger.on_result(metrics_dict)
        self._iteration += 1

    def close(self):
        self.trial_config["trial_completed"] = True
        self.trial_config["end_time"] = datetime.now().isoformat()
        # TODO(rliaw): Have Tune support updated configs
        self._logger.update_config(self.trial_config)
        self._logger.flush()
        self._logger.close()

    @property
    def logdir(self):
        """Trial logdir (subdir of given experiment directory)"""
        return self._logdir


class TuneSession(TrackSession):
    def __init__(self, _tune_reporter):
        """Initializes a Tune track session.

        Args:
            _tune_reporter (StatusReporter): Status reporter.
        """
        super(TuneSession, self).__init__(init_logger=False)
        self._logger = _tune_reporter
        self._logdir = self._logger.logdir
        self._restoring_from = None

    def log(self, **metrics):
        if self.is_pending_restore:
            raise ValueError("Trial is pending restore. `restore` must be "
                             "called before `log`.")
        if self._restoring_from:
            # Restoration is considered complete once `log` is called.
            if isinstance(self._restoring_from, dict):
                checkpoint_path = self._restoring_from["tune_checkpoint_path"]
            else:
                checkpoint_path = self._restoring_from
            checkpoint_dir = TrainableUtil.find_checkpoint_dir(checkpoint_path)
            shutil.rmtree(checkpoint_dir)
            self._restoring_from = None

        super(TuneSession, self).log(**metrics)

    def get_next_iter_checkpoint_dir(self):
        """Returns the next iteration's checkpoint directory."""
        checkpoint_dir = CHECKPOINT_DIR_FORMAT.format(
            root_dir=self.logdir, iteration=self._iteration + 1)
        return checkpoint_dir

    def restore(self):
        """Restores session state.

        `is_pending_restore` must return True before this is called.

        Returns:
            Checkpoint for training function to restore from.
        """
        if not self.is_pending_restore:
            raise ValueError("Trial is not pending restoration.")

        self._restoring_from = self._logger.pop_checkpoint()

        # Restore session state.
        if isinstance(self._restoring_from, dict):
            checkpoint_path = self._restoring_from["tune_checkpoint_path"]
        else:
            checkpoint_path = self._restoring_from
        metadata = TrainableUtil.read_metadata(checkpoint_path)
        self._iteration = metadata["iteration"]
        return self._restoring_from

    @property
    def is_pending_restore(self):
        """Whether the session and training function must be restored."""
        return self._logger.is_pending_restore()
