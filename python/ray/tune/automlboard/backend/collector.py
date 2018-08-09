from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys
import time

from threading import Thread

from common.exception import CollectorError
from common.utils import dump_json, parse_json
from common.utils import parse_multiple_json, timestamp2date

from models.models import JobRecord, TrialRecord, ResultRecord

JOB_META_FILE = "job_status.json"

EXPR_PARARM_FILE = "params.json"
EXPR_PROGRESS_FILE = "progress.csv"
EXPR_RESULT_FILE = "result.json"
EXPR_META_FILE = "trial_status.json"


class CollectorService(object):
    """
    Server implementation to monitor the log directory.

    The service will save the information of job and
    trials information in db.
    """

    DEFAULT_LOGDIR = "./ray_results"

    def __init__(self,
                 log_dir=DEFAULT_LOGDIR,
                 reload_interval=30,
                 standalone=True,
                 log_level="INFO"):
        """
        Initialize the collector service.

        Args
            log_dir: directory of the logs about trials' information
            reload_interval: sleep time period after each polling round
            standalone: the service will not stop and if True
            log_level: level of logging
        """
        self.init_logger(log_level)
        self.standalone = standalone
        self.collector = Collector(
            reload_interval=reload_interval,
            logdir=log_dir)

    def run(self):
        """
        Start the collector worker thread.

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
        logger = logging.getLogger("auomlboard")
        logger.setLevel(log_level)
        logging.getLogger().setLevel(logging.getLevelName(log_level))
        logging.getLogger('requests.packages.urllib3.connectionpool') \
            .setLevel(logging.WARNING)

        logging.getLogger().handlers = []
        log_format = '[%(asctime)s %(levelname)s] %(filename)s:' \
                     '%(funcName)s:%(lineno)d  %(message)s'
        logging.basicConfig(
            stream=sys.stdout, format=log_format, level=log_level)
        return log_level


class Collector(Thread):
    """Worker thread for collector service."""

    def __init__(self, reload_interval, logdir):
        """
        Initialize collector worker thread.

        Args
            reload_interval: time period to sleep after each round
                             of polling.
            logdir: directory path to save the status information of
                    jobs and trials.
        """
        super(Collector, self).__init__()
        self._is_finished = False
        self._reload_interval = reload_interval
        self._logdir = logdir
        self._monitored_jobs = set()
        self._monitored_trials = set()
        self._result_offsets = {}

    def run(self):
        """
        Run the main event loop for collector thread.

        In each round the collector traverse the results log directory
        and reload trial information from the status files.
        """
        self._initialize()

        self._do_collect()
        while not self._is_finished:
            time.sleep(self._reload_interval)
            self._do_collect()

        logging.info("collector stopped.")

    def stop(self):
        """Stop the main polling loop."""
        self._is_finished = True

    def _initialize(self):
        """
        Initialize collector worker thread, Log path will be checked first.

        Records in DB backend will be cleared.
        """
        if not os.path.exists(self._logdir):
            raise CollectorError(
                "log directory %s not exists" % self._logdir)

        logging.info(
            "collector started to run, taking %s "
            "as parent directory for all job logs." % self._logdir)

        # clear old records
        JobRecord.objects.filter().delete()
        TrialRecord.objects.filter().delete()
        ResultRecord.objects.filter().delete()

    def _do_collect(self):
        sub_dirs = os.listdir(self._logdir)
        job_names = filter(
            lambda d: os.path.isdir(os.path.join(self._logdir, d)),
            sub_dirs)
        for job_name in job_names:
            self.sync_job_info(job_name)

    def sync_job_info(self, job_name):
        """
        Load information of the job with the given job name.

        1. traverse each experiment sub-directory and sync information
           for each trial.
        2. create or update the job information, together with the job
           meta file.

        Args:
            job_name(str)

        """
        job_path = os.path.join(self._logdir, job_name)

        expr_dirs = filter(lambda d: os.path.isdir(os.path.join(job_path, d)),
                           os.listdir(job_path))

        for expr_dir_name in expr_dirs:
            self.sync_trial_info(job_path, expr_dir_name)

        if job_name not in self._monitored_jobs:
            self._create_job_info(job_path)
            self._monitored_jobs.add(job_name)
        else:
            self._update_job_info(job_path)

    def sync_trial_info(self, job_path, expr_dir_name):
        """
        Load information of the trial from the given experiment directory.

        create or update the trial information, together with the trial
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
        """
        Create information for given job.

        Including the meta file and the information in db.

        Args:
            job_dir(str)

        """
        meta_file = os.path.join(job_dir, JOB_META_FILE)
        meta = parse_json(meta_file)

        if not meta:
            meta = self._build_job_meta(job_dir)

        logging.info("create job: %s" % meta)

        job_record = JobRecord.from_json(meta)
        job_record.save()

    @classmethod
    def _update_job_info(cls, job_dir):
        """
        Update information for given job.

        Including the meta file and the information in db.

        Args:
            job_dir(str)

        Return:
            updated dict of job meta info

        """
        meta_file = os.path.join(job_dir, JOB_META_FILE)
        meta = parse_json(meta_file)

        if meta:
            logging.debug("update job info for %s" % meta["job_id"])
            JobRecord.objects \
                .filter(job_id=meta["job_id"]) \
                .update(end_time=meta["end_time"])

    def _create_trial_info(self, expr_dir):
        """
        Create information for given trial.

        Including the meta file and the information in db.

        Args:
            expr_dir(str)

        """
        meta_file = os.path.join(expr_dir, EXPR_META_FILE)
        meta = parse_json(meta_file)

        if not meta:
            meta = self._build_trial_meta(expr_dir)

        logging.debug("create trial for %s" % meta)

        trial_record = TrialRecord.from_json(meta)
        trial_record.save()

    def _update_trial_info(self, expr_dir):
        """
        Update information for given trial.

        Including the meta file and the information in db.

        Args:
            expr_dir(str)

        """
        trial_id = expr_dir[-8:]

        result_file = os.path.join(expr_dir, EXPR_RESULT_FILE)
        offset = self._result_offsets.get(trial_id, 0)
        results, new_offset = parse_multiple_json(result_file, offset)
        self._add_results(results, trial_id)
        self._result_offsets[trial_id] = new_offset

        if results and results[-1]["done"]:
            logging.debug("update trial information for %s" % trial_id)
            TrialRecord.objects \
                .filter(trial_id=trial_id) \
                .update(trial_status="TERMINATED",
                        end_time=results[-1]["date"])

    @classmethod
    def _build_job_meta(cls, job_dir):
        """
        Build meta file for job.

        Args:
            job_dir(str)

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
            "best_trial_id": None,
        }
        return meta

    @classmethod
    def _build_trial_meta(cls, expr_dir):
        """
        Build meta file for trial.

        Args:
            expr_dir(str)

        Return:
            a dict of trial meta info

        """
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
            "params": params
        }
        meta_file = os.path.join(expr_dir, EXPR_META_FILE)
        dump_json(meta, meta_file)
        return meta

    @classmethod
    def _get_job_progress(cls, success_trials, total_trials):
        """Get the job's progress for the current round."""
        if total_trials != 0:
            progress = int((float(success_trials) / total_trials) * 100)
        else:
            progress = 0
        return progress

    @classmethod
    def _add_results(cls, results, trial_id):
        """
        Add a list of results into db.

        Args:
            results(list)
            trial_id(str)

        """
        for result in results:
            logging.debug("appending result: %s" % result)
            result["trial_id"] = trial_id
            result_record = ResultRecord.from_json(result)
            result_record.save()
