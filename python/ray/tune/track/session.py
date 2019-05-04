import os
import uuid
from datetime import datetime

from ray.tune.result import DEFAULT_RESULTS_DIR, TRAINING_ITERATION
from ray.tune.logger import UnifiedLogger, Logger


class _ReporterHook(Logger):
    def __init__(self, reporter):
        self.reporter = reporter

    def on_result(self, metrics):
        return self.reporter(**metrics)


class _TrackedState():
    def __init__(self):
        self.start_time = datetime.now().isoformat()


class TrackSession(object):
    """
    TrackSession attempts to infer the local log_dir and remote upload_dir
    automatically.

    In order of precedence, log_dir is determined by:
    (1) the path passed into the argument of the TrackSession constructor
    (2) autodetect.dfl_local_dir()

    The upload directory may be None (in which case no upload is performed),
    or an S3 directory or a GCS directory.

    Arguments:
        log_dir (str): base log directory in which the results for all trials
                       are stored. if not specified, uses autodetect.dfl_local_dir()
        upload_dir (str):
    """

    def __init__(self,
                 log_dir=None,
                 upload_dir=None,
                 sync_period=None,
                 trial_prefix="",
                 param_map=None):
        if log_dir is None:
            log_dir = DEFAULT_RESULTS_DIR
        # TODO should probably check if this exists and whether
        # we'll be clobbering anything in either the artifact dir
        # or the metadata dir, idk what the probability is that a
        # uuid truncation will get duplicated. Then also maybe
        # the same thing for the remote dir.

        base_dir = os.path.expanduser(log_dir)
        self.base_dir = base_dir
        self.trial_id = str(uuid.uuid1().hex[:10])
        if trial_prefix:
            self.trial_id = "_".join([trial_prefix, self.trial_id])

        self.artifact_dir = os.path.join(base_dir, self.trial_id)
        os.makedirs(self.artifact_dir, exist_ok=True)

        self._sync_period = sync_period

        self.upload_dir = upload_dir
        self.param_map = param_map or {}

        # misc metadata to save as well
        self.param_map["trial_id"] = self.trial_id
        self.param_map[TRAINING_ITERATION] = -1
        self.param_map["trial_completed"] = False

    def start(self, reporter=None):
        for path in [self.base_dir, self.artifact_dir]:
            if not os.path.exists(path):
                os.makedirs(path)

        self._hooks = []
        if not reporter:
            self._logger = UnifiedLogger(
                self.param_map,
                self.artifact_dir,
                self.upload_dir)
            self._hooks += [self._logger]
        else:
            self._hooks += [_ReporterHook(reporter)]

    def metric(self, iteration=None, **metrics):
        """
        Logs all named arguments specified in **metrics.
        This will log trial metrics locally, and they will be synchronized
        with the driver periodically through ray.

        Arguments:
            iteration (int): current iteration of the trial.
            **metrics: named arguments with corresponding values to log.
        """
        metrics_dict = metrics.copy()
        metrics_dict.update({"trial_id": self.trial_id})

        if iteration is not None:
            max_iter = max(iteration, self.param_map[TRAINING_ITERATION])
        else:
            max_iter = self.param_map[TRAINING_ITERATION]

        self.param_map[TRAINING_ITERATION] = max_iter
        metrics_dict[TRAINING_ITERATION] = max_iter

        for hook in self._hooks:
            hook.on_result(metrics_dict)

    def _get_fname(self, result_name, iteration=None):
        fname = os.path.join(self.artifact_dir, result_name)
        if iteration is None:
            iteration = self.param_map[TRAINING_ITERATION]
        base, file_extension = os.path.splittext(fname)
        result = base + "_" + str(iteration) + file_extension
        return result

    def trial_dir(self):
        """returns the local file path to the trial's artifact directory"""
        return self.artifact_dir

    def close(self):
        self.param_map["trial_completed"] = True
        self.param_map["end_time"] = datetime.now().isoformat()
        self._logger.update_config(self.param_map)

        for hook in self._hooks:
            hook.close()
