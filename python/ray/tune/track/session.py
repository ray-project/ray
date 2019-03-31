import os
import uuid
from datetime import datetime
from .autodetect import (
    git_repo, dfl_local_dir, git_hash, invocation, git_pretty)
from .constants import METADATA_FOLDER, RESULT_SUFFIX


from ray.tune.logger import UnifiedLogger, Logger


class _ReporterHook(Logger):
    def __init__(self, reporter):
        self.reporter = reporter

    def on_result(self, metrics):
        return self.reporter(**metrics)


class TrackSession(object):
    """
    TrackSession attempts to infer the local log_dir and remote upload_dir
    automatically.

    In order of precedence, log_dir is determined by:
    (1) the path passed into the argument of the TrackSession constructor
    (2) autodetect.dfl_local_dir()

    The upload directory may be None (in which case no upload is performed),
    or an S3 directory or a GCS directory.

    init_logging will automatically set up a logger at the debug level,
    along with handlers to print logs to stdout and to a persistent store.

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
                 param_map=None,
                 init_logging=True):
        if log_dir is None:
            log_dir = dfl_local_dir()
        # TODO should probably check if this exists and whether
        # we'll be clobbering anything in either the artifact dir
        # or the metadata dir, idk what the probability is that a
        # uuid truncation will get duplicated. Then also maybe
        # the same thing for the remote dir.

        base_dir = os.path.expanduser(log_dir)
        self.base_dir = base_dir
        self.data_dir = os.path.join(base_dir, METADATA_FOLDER)
        self.trial_id = str(uuid.uuid1().hex[:10])
        if trial_prefix:
            self.trial_id = "_".join([trial_prefix, self.trial_id])

        self._sync_period = sync_period
        self.artifact_dir = os.path.join(base_dir, self.trial_id)
        os.makedirs(self.artifact_dir, exist_ok=True)
        self.upload_dir = upload_dir
        self.param_map = param_map or {}

        # misc metadata to save as well
        self.param_map["trial_id"] = self.trial_id
        git_repo_or_none = git_repo()
        self.param_map["git_repo"] = git_repo_or_none or "unknown"
        self.param_map["git_hash"] = git_hash()
        self.param_map["git_pretty"] = git_pretty()
        self.param_map["invocation"] = invocation()
        self.param_map["start_time"] = datetime.now().isoformat()
        self.param_map["max_iteration"] = -1
        self.param_map["trial_completed"] = False

    def start(self, reporter=None):
        for path in [self.base_dir, self.data_dir, self.artifact_dir]:
            if not os.path.exists(path):
                os.makedirs(path)

        self._hooks = []
        if not reporter:
            self._hooks += [UnifiedLogger(
                self.param_map,
                self.data_dir,
                self.upload_dir,
                filename_prefix=self.trial_id + "_")]
        else:
            self._hooks += [_ReporterHook(reporter)]

    def metric(self, iteration=None, **kwargs):
        """
        Logs all named arguments specified in **kwargs.
        This will log trial metrics locally, and they will be synchronized
        with the driver periodically through ray.

        Arguments:
            iteration (int): current iteration of the trial.
            **kwargs: named arguments with corresponding values to log.
        """
        new_args = kwargs.copy()
        new_args.update({"iteration": iteration})
        new_args.update({"trial_id": self.trial_id})
        if iteration is not None:
            self.param_map["max_iteration"] = max(
                self.param_map["max_iteration"], iteration)
        for hook in self._hooks:
            hook.on_result(new_args)

    def _get_fname(self, result_name, iteration=None):
        fname = os.path.join(self.artifact_dir, result_name)
        if iteration is None:
            iteration = self.param_map["max_iteration"]
        base, file_extension = os.path.splittext(fname)
        result = base + "_" + str(iteration) + file_extension
        return result

    def save(self, result, result_name, save_fn, iteration=None, **kwargs):
        """
        Persists a result to disk as an artifact. These results will be
        synchronized with the driver periodically through ray.

        Arguments:
            result (object): the python object to persist to disk.
            result_fname (str): base filename for the object, e.g. "model.ckpt"
            save_fn (function): function to save out the object. called as
                                "save_fn(obj, fname, **kwargs)"
            iteration (int): the current iteration of the trial. If not
                             specified, overrides the previously saved file.
                             otherwise, creates a new object for each iter.
        """
        fname = self._get_fname(result_name, iteration=iteration)
        return save_fn(result, fname, **kwargs)

    def load(self, result_name, load_fn, iteration=None, **kwargs):
        """
        Loads the persisted object of the given type for the corresponding
        iteration.

        Arguments:
            result_name (str): base filename for the object as supplied to
                               TrackSession.save
            load_fn (function): function to load the object from disk. called
                               as "load_fn(fname, **kwargs)"
           iteration (int): iteration of trial to load from. If not specified,
                            track will load the most recent file.

        """
        fname = self._get_fname(result_name, iteration=iteration)
        return load_fn(fname, **kwargs)

    def trial_dir(self):
        """returns the local file path to the trial's artifact directory"""
        return self.artifact_dir

    def close(self):
        self.param_map["trial_completed"] = True
        self.param_map["end_time"] = datetime.now().isoformat()
        self._logger.update_config(self.param_map)

        for hook in self._hooks:
            hook.close()

    def get_result_filename(self):
        return os.path.join(self.data_dir, self.trial_id + "_" + RESULT_SUFFIX)
