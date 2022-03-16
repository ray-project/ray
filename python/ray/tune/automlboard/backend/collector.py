import logging
import os
import time

from threading import Thread

from ray.tune.automlboard.common.exception import CollectorError
from ray.tune.automlboard.common.utils import (
    parse_json,
    parse_multiple_json,
    timestamp2date,
)
from ray.tune.automlboard.models.models import JobRecord, TrialRecord, ResultRecord
from ray.tune.result import (
    DEFAULT_RESULTS_DIR,
    JOB_META_FILE,
    EXPR_PARAM_FILE,
    EXPR_RESULT_FILE,
    EXPR_META_FILE,
)


class CollectorService:
    """Server implementation to monitor the log directory.

    The service will save the information of job and
    trials information in db.
    """

    def __init__(
        self,
        log_dir=DEFAULT_RESULTS_DIR,
        reload_interval=30,
        standalone=True,
        log_level="INFO",
    ):
        """Initialize the collector service.

        Args:
            log_dir (str): Directory of the logs about trials' information.
            reload_interval (int): Sleep time period after each polling round.
            standalone (boolean): The service will not stop and if True.
            log_level (str): Level of logging.
        """
        self.logger = self.init_logger(log_level)
        self.standalone = standalone
        self.collector = Collector(
            reload_interval=reload_interval, logdir=log_dir, logger=self.logger
        )

    def run(self):
        """Start the collector worker thread.

        If running in standalone mode, the current thread will wait
        until the collector thread ends.
        """
        self.collector.start()
        if self.standalone:
            self.collector.join()

    def stop(self):
        """Stop the collector worker thread."""
        self.collector.stop()

    @classmethod
    def init_logger(cls, log_level):
        """Initialize logger settings."""
        logger = logging.getLogger("AutoMLBoard")
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(levelname)s %(asctime)s] %(filename)s: %(lineno)d  %(message)s"
        )
        handler.setFormatter(formatter)
        logger.setLevel(log_level)
        logger.addHandler(handler)
        return logger


class Collector(Thread):
    """Worker thread for collector service."""

    def __init__(self, reload_interval, logdir, logger):
        """Initialize collector worker thread.

        Args
            reload_interval (int): Time period to sleep after each round
                                   of polling.
            logdir (str): Directory path to save the status information of
                          jobs and trials.
            logger (Logger): Logger for collector thread.
        """
        super(Collector, self).__init__()
        self._is_finished = False
        self._reload_interval = reload_interval
        self._logdir = logdir
        self._monitored_jobs = set()
        self._monitored_trials = set()
        self._result_offsets = {}
        self.logger = logger
        self.daemon = True

    def run(self):
        """Run the main event loop for collector thread.

        In each round the collector traverse the results log directory
        and reload trial information from the status files.
        """
        self._initialize()

        self._do_collect()
        while not self._is_finished:
            time.sleep(self._reload_interval)
            self._do_collect()

        self.logger.info("Collector stopped.")

    def stop(self):
        """Stop the main polling loop."""
        self._is_finished = True

    def _initialize(self):
        """Initialize collector worker thread, Log path will be checked first.

        Records in DB backend will be cleared.
        """
        if not os.path.exists(self._logdir):
            raise CollectorError("Log directory %s not exists" % self._logdir)

        self.logger.info(
            "Collector started, taking %s as parent directory"
            "for all job logs." % self._logdir
        )

        # clear old records
        JobRecord.objects.filter().delete()
        TrialRecord.objects.filter().delete()
        ResultRecord.objects.filter().delete()

    def _do_collect(self):
        sub_dirs = os.listdir(self._logdir)
        job_names = filter(
            lambda d: os.path.isdir(os.path.join(self._logdir, d)), sub_dirs
        )
        for job_name in job_names:
            self.sync_job_info(job_name)

    def sync_job_info(self, job_name):
        """Load information of the job with the given job name.

        1. Traverse each experiment sub-directory and sync information
           for each trial.
        2. Create or update the job information, together with the job
           meta file.

        Args:
            job_name (str) name of the Tune experiment
        """
        job_path = os.path.join(self._logdir, job_name)

        if job_name not in self._monitored_jobs:
            self._create_job_info(job_path)
            self._monitored_jobs.add(job_name)
        else:
            self._update_job_info(job_path)

        expr_dirs = filter(
            lambda d: os.path.isdir(os.path.join(job_path, d)), os.listdir(job_path)
        )

        for expr_dir_name in expr_dirs:
            self.sync_trial_info(job_path, expr_dir_name)

        self._update_job_info(job_path)

    def sync_trial_info(self, job_path, expr_dir_name):
        """Load information of the trial from the given experiment directory.

        Create or update the trial information, together with the trial
        meta file.

        Args:
            job_path(str)
            expr_dir_name(str)

        """
        expr_name = expr_dir_name[-8:]
        expr_path = os.path.join(job_path, expr_dir_name)

        if expr_name not in self._monitored_trials:
            self._create_trial_info(expr_path)
            self._monitored_trials.add(expr_name)
        else:
            self._update_trial_info(expr_path)

    def _create_job_info(self, job_dir):
        """Create information for given job.

        Meta file will be loaded if exists, and the job information will
        be saved in db backend.

        Args:
            job_dir (str): Directory path of the job.
        """
        meta = self._build_job_meta(job_dir)

        self.logger.debug("Create job: %s" % meta)

        job_record = JobRecord.from_json(meta)
        job_record.save()

    @classmethod
    def _update_job_info(cls, job_dir):
        """Update information for given job.

        Meta file will be loaded if exists, and the job information in
        in db backend will be updated.

        Args:
            job_dir (str): Directory path of the job.

        Return:
            Updated dict of job meta info
        """
        meta_file = os.path.join(job_dir, JOB_META_FILE)
        meta = parse_json(meta_file)

        if meta:
            logging.debug("Update job info for %s" % meta["job_id"])
            JobRecord.objects.filter(job_id=meta["job_id"]).update(
                end_time=timestamp2date(meta["end_time"])
            )

    def _create_trial_info(self, expr_dir):
        """Create information for given trial.

        Meta file will be loaded if exists, and the trial information
        will be saved in db backend.

        Args:
            expr_dir (str): Directory path of the experiment.
        """
        meta = self._build_trial_meta(expr_dir)

        self.logger.debug("Create trial for %s" % meta)

        trial_record = TrialRecord.from_json(meta)
        trial_record.save()

    def _update_trial_info(self, expr_dir):
        """Update information for given trial.

        Meta file will be loaded if exists, and the trial information
        in db backend will be updated.

        Args:
            expr_dir(str)
        """
        trial_id = expr_dir[-8:]

        meta_file = os.path.join(expr_dir, EXPR_META_FILE)
        meta = parse_json(meta_file)

        result_file = os.path.join(expr_dir, EXPR_RESULT_FILE)
        offset = self._result_offsets.get(trial_id, 0)
        results, new_offset = parse_multiple_json(result_file, offset)
        self._add_results(results, trial_id)
        self._result_offsets[trial_id] = new_offset

        if meta:
            TrialRecord.objects.filter(trial_id=trial_id).update(
                trial_status=meta["status"],
                end_time=timestamp2date(meta.get("end_time", None)),
            )
        elif len(results) > 0:
            metrics = {
                "episode_reward": results[-1].get("episode_reward_mean", None),
                "accuracy": results[-1].get("mean_accuracy", None),
                "loss": results[-1].get("loss", None),
            }
            if results[-1].get("done"):
                TrialRecord.objects.filter(trial_id=trial_id).update(
                    trial_status="TERMINATED",
                    end_time=results[-1].get("date", None),
                    metrics=str(metrics),
                )
            else:
                TrialRecord.objects.filter(trial_id=trial_id).update(
                    metrics=str(metrics)
                )

    @classmethod
    def _build_job_meta(cls, job_dir):
        """Build meta file for job.

        Args:
            job_dir (str): Directory path of the job.

        Return:
            A dict of job meta info.
        """
        meta_file = os.path.join(job_dir, JOB_META_FILE)
        meta = parse_json(meta_file)

        if not meta:
            job_name = job_dir.split("/")[-1]
            user = os.environ.get("USER", None)
            meta = {
                "job_id": job_name,
                "job_name": job_name,
                "user": user,
                "type": "ray",
                "start_time": os.path.getctime(job_dir),
                "end_time": None,
                "best_trial_id": None,
            }

        if meta.get("start_time", None):
            meta["start_time"] = timestamp2date(meta["start_time"])

        return meta

    @classmethod
    def _build_trial_meta(cls, expr_dir):
        """Build meta file for trial.

        Args:
            expr_dir (str): Directory path of the experiment.

        Return:
            A dict of trial meta info.
        """
        meta_file = os.path.join(expr_dir, EXPR_META_FILE)
        meta = parse_json(meta_file)

        if not meta:
            job_id = expr_dir.split("/")[-2]
            trial_id = expr_dir[-8:]
            params = parse_json(os.path.join(expr_dir, EXPR_PARAM_FILE))
            meta = {
                "trial_id": trial_id,
                "job_id": job_id,
                "status": "RUNNING",
                "type": "TUNE",
                "start_time": os.path.getctime(expr_dir),
                "end_time": None,
                "progress_offset": 0,
                "result_offset": 0,
                "params": params,
            }

        if not meta.get("start_time", None):
            meta["start_time"] = os.path.getctime(expr_dir)

        if isinstance(meta["start_time"], float):
            meta["start_time"] = timestamp2date(meta["start_time"])

        if meta.get("end_time", None):
            meta["end_time"] = timestamp2date(meta["end_time"])

        meta["params"] = parse_json(os.path.join(expr_dir, EXPR_PARAM_FILE))

        return meta

    def _add_results(self, results, trial_id):
        """Add a list of results into db.

        Args:
            results (list): A list of json results.
            trial_id (str): Id of the trial.
        """
        for result in results:
            self.logger.debug("Appending result: %s" % result)
            result["trial_id"] = trial_id
            result_record = ResultRecord.from_json(result)
            result_record.save()
