import logging
import os
import sys
import time

from threading import Thread

from common.exception import CollectorError
from common.utils import dump_json, parse_json, parse_multiple_json, timestamp2date

from models.models import JobRecord, TrialRecord, ResultRecord


JOB_META_FILE = "job_meta.json"

EXPR_PARARM_FILE = "params.json"
EXPR_PROGRESS_FILE = "progress.csv"
EXPR_RESULT_FILE = "result.json"
EXPR_META_FILE = "expr_meta.json"


class CollectorService(object):
    """
    Server implementation to monitor the status directory and save the information of job
    and trials information in db.
    """

    DEFAULT_LOGDIR = "./ray_results"

    def __init__(self, log_dir=DEFAULT_LOGDIR, sleep_interval=30,
                 standalone=True, log_level="INFO"):
        """
        Initialization of the collector service.

        Args
            log_dir: directory of the logs about trials' information
            sleep_interval: sleep time period after each polling round
            standalone: the service will not stop and if True
            log_level: level of logging
        """
        self.init_logger(log_level)
        self.standalone = standalone
        self.collector = Collector(sleep_interval=sleep_interval, logdir=log_dir)

    def run(self):
        self.collector.start()
        if self.standalone:
            self.collector.join()

    def stop(self):
        self.collector.stop()

    @classmethod
    def init_logger(cls, log_level):
        logger = logging.getLogger("auomlboard")
        logger.setLevel(log_level)
        logging.getLogger().setLevel(logging.getLevelName(log_level))
        logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)

        logging.getLogger().handlers = []
        logging.basicConfig(stream=sys.stdout,
                            format='[%(asctime)s %(levelname)s] %(filename)s:%(funcName)s:%(lineno)d  %(message)s',
                            level=log_level)
        return log_level


class Collector(Thread):
    """
    Worker thread for collector service.
    """

    def __init__(self, sleep_interval, logdir):
        """
        Initialize collector worker thread.

        Args
            sleep_interval: time period to sleep after each round of polling.
            logdir: directory path to save the status information of jobs and trials.
        """
        super(Collector, self).__init__()
        self._is_finished = False
        self._sleep_interval = sleep_interval
        self._logdir = logdir

    def run(self):
        """
        Main event loop for collector thread. In each round the collector traverse the results log
        directory and reload trial information from the status files. Once a job finished, the directory will
        be marked as FINISHED so that this directory can be skipped during next polling round.
        """
        if not os.path.exists(self._logdir):
            raise CollectorError("status directory %s not exists" % self._logdir)

        logging.info("collector started to run.")

        while not self._is_finished:
            time.sleep(self._sleep_interval)
            job_dirs = os.listdir(self._logdir)
            for job_dir in job_dirs:
                self.sync_job_info(job_dir)

        logging.info("collector stopped.")

    def stop(self):
        self._is_finished = True

    def sync_job_info(self, job_name):
        """
        1. traverse each experiment sub-directory and sync information for each trial.
        2. create or update the job information, together with the job meta file.

        Args:
            job_name(str)
        """
        job_path = os.path.join(self._logdir, job_name)

        expr_dirs = filter(lambda d: os.path.isdir(os.path.join(job_path, d)), os.listdir(job_path))

        for expr_dir in expr_dirs:
            logging.debug("scanning info for experiment directory %s" % expr_dir)
            expr_path = os.path.join(job_path, expr_dir)
            self.sync_trial_info(expr_path)

        meta_file = os.path.join(job_path, JOB_META_FILE)
        meta = parse_json(meta_file)

        if not meta:
            self._create_job_info(job_path, expr_dirs)
        else:
            self._update_job_info(job_path, meta)

    @classmethod
    def sync_trial_info(cls, expr_dir):
        """
        create or update the trial information, together with the trial meta file.

        Args:
            expr_dir(str)
        """
        meta_file = os.path.join(expr_dir, EXPR_META_FILE)
        meta = parse_json(meta_file)

        if not meta:
            cls._create_trial_info(expr_dir)
        else:
            cls._update_trial_info(expr_dir, meta)

    @classmethod
    def _create_job_info(cls, job_dir, expr_dirs):
        """
        create information for given job, including the meta file and the information in db.

        Args:
            job_dir(str)
            expr_dirs(list) list of directories for all experiments of the job
        """
        meta = cls._build_job_meta(job_dir, len(expr_dirs))
        logging.info("create job: %s" % meta)
        job = JobRecord(job_id=meta["job_name"],
                        name=meta["job_name"],
                        user=meta["user"],
                        type=meta["type"],
                        start_time=meta["start_time"],
                        success_trials=meta["success_trials"],
                        failed_trials=meta["failed_trials"],
                        running_trials=meta["running_trials"],
                        total_trials=meta["total_trials"],
                        best_trial_id="None",
                        progress=meta["progress"])
        job.save()

    @classmethod
    def _update_job_info(cls, job_dir, meta):
        """
        update information for given job, including the meta file and the information in db.

        Args:
            job_dir(str)
            meta(dict)

        Return:
            updated dict of job meta info
        """
        if meta["end_time"]:
            # skip finished jobs
            return
        meta["progress"] = cls._get_job_progress(meta["success_trials"], meta["total_trials"])

        # TODO: update job info here
        logging.debug("update job info for %s" % meta)
        meta_file = os.path.join(job_dir, JOB_META_FILE)
        dump_json(meta, meta_file)

    @classmethod
    def _create_trial_info(cls, expr_dir):
        """
         create information for given trial, including the meta file and the information in db.

         Args:
             expr_dir(str)
         """
        meta = cls._build_trial_meta(expr_dir)
        logging.debug("create trial for %s" % meta)
        trial = TrialRecord(trial_id=meta['trial_id'],
                            job_id=meta["job_id"],
                            trial_status=meta["status"],
                            start_time=meta["start_time"],
                            params=meta["params"])
        trial.save()

    @classmethod
    def _update_trial_info(cls, expr_dir, meta):
        """
        update information for given trial, including the meta file and the information in db.

        Args:
            expr_dir(str)
            meta(dict)
        """
        if meta["end_time"]:
            return

        logging.debug("update trial information for %s" % meta)
        results, new_offset = parse_multiple_json(os.path.join(expr_dir, EXPR_RESULT_FILE),
                                                  meta["result_offset"])
        cls._add_results(results, meta)

        meta["result_offset"] = new_offset
        if results and results[-1]["done"]:
            meta["status"] = "TERMINAED"
            meta["end_time"] = results[-1]["date"]
            TrialRecord.objects \
                .filter(trial_id=meta['trial_id']) \
                .update(trial_status=meta["status"], end_time=meta["end_time"])

        meta_file = os.path.join(expr_dir, EXPR_META_FILE)
        dump_json(meta, meta_file)

    @classmethod
    def _build_job_meta(cls, job_dir, total_trials):
        """
        build meta file for job.

        Args:
            job_dir(str)
            total_trials(integer)

        Return:
            a dict of job meta info
        """
        job_name = job_dir.split('/')[-1]
        user = os.environ.get("USER", None)
        meta = {
            "job_id": job_name,
            "job_name": job_name,
            "user": user,
            "type": "RAY TUNE",
            "start_time": timestamp2date(os.path.getctime(job_dir)),
            "end_time": None,
            "success_trials": 0,
            "running_trials": 0,
            "failed_trials": 0,
            "total_trials": total_trials,
            "best_trial_id": None,
        }
        meta["progress"] = cls._get_job_progress(meta["success_trials"], meta["total_trials"])
        meta_file = os.path.join(job_dir, JOB_META_FILE)
        dump_json(meta, meta_file)
        return meta

    @classmethod
    def _build_trial_meta(cls, expr_dir):
        """
        build meta file for trial.

        Args:
            expr_dir(str)

        Return:
            a dict of trial meta info
        """
        user = os.environ.get("USER", None)
        job_id = expr_dir.split('/')[-2]
        trial_id = expr_dir[-8:]
        params = parse_json(os.path.join(expr_dir, EXPR_PARARM_FILE))
        meta = {
            "trial_id": trial_id,
            "job_id": job_id,
            "status": "RUNNING",
            "type": "RAYTUNE",
            "start_time": timestamp2date(os.path.getctime(expr_dir)),
            "end_time": None,
            "progress_offset": 0,
            "result_offset": 0,
            "params": params}
        meta_file = os.path.join(expr_dir, EXPR_META_FILE)
        dump_json(meta, meta_file)
        return meta

    @classmethod
    def _get_job_progress(cls, success_trials, total_trials):
        """
        get the job's progress for the current round.
        """
        if total_trials != 0:
            progress = int((float(success_trials) / total_trials) * 100)
        else:
            progress = 0
        return progress

    @classmethod
    def _add_results(cls, results, meta):
        """
        Add a list of results into db.

        Args:
            results(list)
            meta(dict)
        """
        for result in results:
            logging.debug("appending result: %s" % result)
            result = ResultRecord(trial_id=meta["trial_id"],
                                  timesteps_total=result["timesteps_total"],
                                  done=result["done"],
                                  info=result["info"],
                                  episode_reward_mean=result["episode_reward_mean"],
                                  episodes_total=result["episodes_total"],
                                  mean_accuracy=result["mean_accuracy"],
                                  mean_validation_accuracy=result["mean_validation_accuracy"],
                                  mean_loss=result["mean_loss"],
                                  experiment_id=result["experiment_id"],
                                  trainning_iteration=result["training_iteration"],
                                  timesteps_this_iter=result["timesteps_this_iter"],
                                  time_this_iter_s=result["time_this_iter_s"],
                                  time_total_s=result["time_total_s"],
                                  date=result["date"],
                                  timestamp=result["timestamp"],
                                  hostname=result["hostname"],
                                  node_ip=result["node_ip"],
                                  config=result["config"])
            result.save()
