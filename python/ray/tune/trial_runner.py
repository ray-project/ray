from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import click
import collections
from datetime import datetime
import json
import logging
import os
import re
import time
import traceback

import ray.cloudpickle as cloudpickle
from ray.tune import TuneError
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.result import (TIME_THIS_ITER_S, RESULT_DUPLICATE,
                             SHOULD_CHECKPOINT)
from ray.tune.syncer import get_syncer
from ray.tune.trial import Trial, Checkpoint
from ray.tune.sample import function
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.util import warn_if_slow
from ray.utils import binary_to_hex, hex_to_binary
from ray.tune.web_server import TuneServer

MAX_DEBUG_TRIALS = 20

logger = logging.getLogger(__name__)


def _naturalize(string):
    """Provides a natural representation for string for nice sorting."""
    splits = re.split("([0-9]+)", string)
    return [int(text) if text.isdigit() else text.lower() for text in splits]


def _find_newest_ckpt(ckpt_dir):
    """Returns path to most recently modified checkpoint."""
    full_paths = [
        os.path.join(ckpt_dir, fname) for fname in os.listdir(ckpt_dir)
        if fname.startswith("experiment_state") and fname.endswith(".json")
    ]
    return max(full_paths)


class _TuneFunctionEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, function):
            return self._to_cloudpickle(obj)
        try:
            return super(_TuneFunctionEncoder, self).default(obj)
        except Exception:
            logger.debug("Unable to encode. Falling back to cloudpickle.")
            return self._to_cloudpickle(obj)

    def _to_cloudpickle(self, obj):
        return {
            "_type": "CLOUDPICKLE_FALLBACK",
            "value": binary_to_hex(cloudpickle.dumps(obj))
        }


class _TuneFunctionDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(
            self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if obj.get("_type") == "CLOUDPICKLE_FALLBACK":
            return self._from_cloudpickle(obj)
        return obj

    def _from_cloudpickle(self, obj):
        return cloudpickle.loads(hex_to_binary(obj["value"]))


class TrialRunner(object):
    """A TrialRunner implements the event loop for scheduling trials on Ray.

    Example:
        runner = TrialRunner()
        runner.add_trial(Trial(...))
        runner.add_trial(Trial(...))
        while not runner.is_finished():
            runner.step()
            print(runner.debug_string())

    The main job of TrialRunner is scheduling trials to efficiently use cluster
    resources, without overloading the cluster.

    While Ray itself provides resource management for tasks and actors, this is
    not sufficient when scheduling trials that may instantiate multiple actors.
    This is because if insufficient resources are available, concurrent trials
    could deadlock waiting for new resources to become available. Furthermore,
    oversubscribing the cluster could degrade training performance, leading to
    misleading benchmark results.
    """

    CKPT_FILE_TMPL = "experiment_state-{}.json"
    VALID_RESUME_TYPES = [True, "LOCAL", "REMOTE", "PROMPT"]

    def __init__(self,
                 search_alg=None,
                 scheduler=None,
                 launch_web_server=False,
                 local_checkpoint_dir=None,
                 remote_checkpoint_dir=None,
                 sync_to_cloud=None,
                 resume=False,
                 server_port=TuneServer.DEFAULT_PORT,
                 verbose=True,
                 checkpoint_period=10,
                 trial_executor=None):
        """Initializes a new TrialRunner.

        Args:
            search_alg (SearchAlgorithm): SearchAlgorithm for generating
                Trial objects.
            scheduler (TrialScheduler): Defaults to FIFOScheduler.
            launch_web_server (bool): Flag for starting TuneServer
            local_checkpoint_dir (str): Path where
                global checkpoints are stored and restored from.
            remote_checkpoint_dir (str): Remote path where
                global checkpoints are stored and restored from. Used
                if `resume` == REMOTE.
            resume (str|False): see `tune.py:run`.
            sync_to_cloud (func|str): see `tune.py:run`.
            server_port (int): Port number for launching TuneServer.
            verbose (bool): Flag for verbosity. If False, trial results
                will not be output.
            trial_executor (TrialExecutor): Defaults to RayTrialExecutor.
        """
        self._search_alg = search_alg or BasicVariantGenerator()
        self._scheduler_alg = scheduler or FIFOScheduler()
        self.trial_executor = trial_executor or RayTrialExecutor()

        # For debugging, it may be useful to halt trials after some time has
        # elapsed. TODO(ekl) consider exposing this in the API.
        self._global_time_limit = float(
            os.environ.get("TRIALRUNNER_WALLTIME_LIMIT", float("inf")))
        self._total_time = 0
        self._iteration = 0
        self._verbose = verbose

        self._server = None
        self._server_port = server_port
        if launch_web_server:
            self._server = TuneServer(self, self._server_port)

        self._trials = []
        self._stop_queue = []
        self._local_checkpoint_dir = local_checkpoint_dir

        if self._local_checkpoint_dir and not os.path.exists(
                self._local_checkpoint_dir):
            os.makedirs(self._local_checkpoint_dir)

        self._remote_checkpoint_dir = remote_checkpoint_dir
        self._syncer = get_syncer(local_checkpoint_dir, remote_checkpoint_dir,
                                  sync_to_cloud)

        self._resumed = False

        if self._validate_resume(resume_type=resume):
            try:
                self.resume()
                logger.info("Resuming trial.")
                self._resumed = True
            except Exception:
                logger.exception(
                    "Runner restore failed. Restarting experiment.")
        else:
            logger.info("Starting a new experiment.")

        self._start_time = time.time()
        self._last_checkpoint_time = -float("inf")
        self._checkpoint_period = checkpoint_period
        self._session_str = datetime.fromtimestamp(
            self._start_time).strftime("%Y-%m-%d_%H-%M-%S")
        self.checkpoint_file = None
        if self._local_checkpoint_dir:
            self.checkpoint_file = os.path.join(
                self._local_checkpoint_dir,
                TrialRunner.CKPT_FILE_TMPL.format(self._session_str))

    def _validate_resume(self, resume_type):
        """Checks whether to resume experiment.

        Args:
            resume_type: One of True, "REMOTE", "LOCAL", "PROMPT".
        """
        if not resume_type:
            return False
        assert resume_type in self.VALID_RESUME_TYPES, (
            "resume_type {} is not one of {}".format(resume_type,
                                                     self.VALID_RESUME_TYPES))
        # Not clear if we need this assertion, since we should always have a
        # local checkpoint dir.
        assert self._local_checkpoint_dir or self._remote_checkpoint_dir
        if resume_type in [True, "LOCAL", "PROMPT"]:
            if not self.checkpoint_exists(self._local_checkpoint_dir):
                raise ValueError("Called resume when no checkpoint exists "
                                 "in local directory.")
            elif resume_type == "PROMPT":
                if click.confirm("Resume from local directory?"):
                    return True

        if resume_type in ["REMOTE", "PROMPT"]:
            if resume_type == "PROMPT" and not click.confirm(
                    "Try downloading from remote directory?"):
                return False
            if not self._remote_checkpoint_dir:
                raise ValueError(
                    "Called resume from remote without remote directory.")

            # Try syncing down the upload directory.
            logger.info("Downloading from {}".format(
                self._remote_checkpoint_dir))
            self._syncer.sync_down_if_needed()

            if not self.checkpoint_exists(self._local_checkpoint_dir):
                raise ValueError("Called resume when no checkpoint exists "
                                 "in remote or local directory.")
        return True

    @classmethod
    def checkpoint_exists(cls, directory):
        if not os.path.exists(directory):
            return False
        return any(
            (fname.startswith("experiment_state") and fname.endswith(".json"))
            for fname in os.listdir(directory))

    def add_experiment(self, experiment):
        if not self._resumed:
            self._search_alg.add_configurations([experiment])
        else:
            logger.info("TrialRunner resumed, ignoring new add_experiment.")

    def checkpoint(self):
        """Saves execution state to `self._local_checkpoint_dir`.

        Overwrites the current session checkpoint, which starts when self
        is instantiated. Throttle depends on self._checkpoint_period.
        """
        if not self._local_checkpoint_dir:
            return
        if time.time() - self._last_checkpoint_time < self._checkpoint_period:
            return
        self._last_checkpoint_time = time.time()
        runner_state = {
            "checkpoints": list(
                self.trial_executor.get_checkpoints().values()),
            "runner_data": self.__getstate__(),
            "stats": {
                "start_time": self._start_time,
                "timestamp": self._last_checkpoint_time
            }
        }
        tmp_file_name = os.path.join(self._local_checkpoint_dir,
                                     ".tmp_checkpoint")
        with open(tmp_file_name, "w") as f:
            json.dump(runner_state, f, indent=2, cls=_TuneFunctionEncoder)

        os.rename(tmp_file_name, self.checkpoint_file)
        self._syncer.sync_up_if_needed()
        return self._local_checkpoint_dir

    def resume(self):
        """Resumes all checkpointed trials from previous run.

        Requires user to manually re-register their objects. Also stops
        all ongoing trials.
        """

        newest_ckpt_path = _find_newest_ckpt(self._local_checkpoint_dir)
        with open(newest_ckpt_path, "r") as f:
            runner_state = json.load(f, cls=_TuneFunctionDecoder)
            self.checkpoint_file = newest_ckpt_path

        logger.warning("".join([
            "Attempting to resume experiment from {}. ".format(
                self._local_checkpoint_dir), "This feature is experimental, "
            "and may not work with all search algorithms. ",
            "This will ignore any new changes to the specification."
        ]))

        self.__setstate__(runner_state["runner_data"])

        trials = []
        for trial_cp in runner_state["checkpoints"]:
            new_trial = Trial(trial_cp["trainable_name"])
            new_trial.__setstate__(trial_cp)
            trials += [new_trial]
        for trial in sorted(
                trials, key=lambda t: t.last_update_time, reverse=True):
            self.add_trial(trial)

    def is_finished(self):
        """Returns whether all trials have finished running."""

        if self._total_time > self._global_time_limit:
            logger.warning("Exceeded global time limit {} / {}".format(
                self._total_time, self._global_time_limit))
            return True

        trials_done = all(trial.is_finished() for trial in self._trials)
        return trials_done and self._search_alg.is_finished()

    def step(self):
        """Runs one step of the trial event loop.

        Callers should typically run this method repeatedly in a loop. They
        may inspect or modify the runner's state in between calls to step().
        """
        if self.is_finished():
            raise TuneError("Called step when all trials finished?")
        with warn_if_slow("on_step_begin"):
            self.trial_executor.on_step_begin()
        next_trial = self._get_next_trial()  # blocking
        if next_trial is not None:
            with warn_if_slow("start_trial"):
                self.trial_executor.start_trial(next_trial)
        elif self.trial_executor.get_running_trials():
            self._process_events()  # blocking
        else:
            for trial in self._trials:
                if trial.status == Trial.PENDING:
                    if not self.has_resources(trial.resources):
                        raise TuneError(
                            ("Insufficient cluster resources to launch trial: "
                             "trial requested {} but the cluster has only {}. "
                             "Pass `queue_trials=True` in "
                             "ray.tune.run() or on the command "
                             "line to queue trials until the cluster scales "
                             "up. {}").format(
                                 trial.resources.summary_string(),
                                 self.trial_executor.resource_string(),
                                 trial._get_trainable_cls().resource_help(
                                     trial.config)))
                elif trial.status == Trial.PAUSED:
                    raise TuneError(
                        "There are paused trials, but no more pending "
                        "trials with sufficient resources.")

        try:
            with warn_if_slow("experiment_checkpoint"):
                self.checkpoint()
        except Exception:
            logger.exception("Trial Runner checkpointing failed.")
        self._iteration += 1

        if self._server:
            with warn_if_slow("server"):
                self._process_requests()

            if self.is_finished():
                self._server.shutdown()
        with warn_if_slow("on_step_end"):
            self.trial_executor.on_step_end()

    def get_trial(self, tid):
        trial = [t for t in self._trials if t.trial_id == tid]
        return trial[0] if trial else None

    def get_trials(self):
        """Returns the list of trials managed by this TrialRunner.

        Note that the caller usually should not mutate trial state directly.
        """

        return self._trials

    def add_trial(self, trial):
        """Adds a new trial to this TrialRunner.

        Trials may be added at any time.

        Args:
            trial (Trial): Trial to queue.
        """
        trial.set_verbose(self._verbose)
        self._trials.append(trial)
        with warn_if_slow("scheduler.on_trial_add"):
            self._scheduler_alg.on_trial_add(self, trial)
        self.trial_executor.try_checkpoint_metadata(trial)

    def debug_string(self, max_debug=MAX_DEBUG_TRIALS):
        """Returns a human readable message for printing to the console."""
        messages = self._debug_messages()
        states = collections.defaultdict(set)
        limit_per_state = collections.Counter()
        for t in self._trials:
            states[t.status].add(t)

        # Show at most max_debug total, but divide the limit fairly
        while max_debug > 0:
            start_num = max_debug
            for s in states:
                if limit_per_state[s] >= len(states[s]):
                    continue
                max_debug -= 1
                limit_per_state[s] += 1
            if max_debug == start_num:
                break

        for local_dir in sorted({t.local_dir for t in self._trials}):
            messages.append("Result logdir: {}".format(local_dir))

        num_trials_per_state = {
            state: len(trials)
            for state, trials in states.items()
        }
        total_number_of_trials = sum(num_trials_per_state.values())
        if total_number_of_trials > 0:
            messages.append("Number of trials: {} ({})"
                            "".format(total_number_of_trials,
                                      num_trials_per_state))

        for state, trials in sorted(states.items()):
            limit = limit_per_state[state]
            messages.append("{} trials:".format(state))
            sorted_trials = sorted(
                trials, key=lambda t: _naturalize(t.experiment_tag))
            if len(trials) > limit:
                tail_length = limit // 2
                first = sorted_trials[:tail_length]
                for t in first:
                    messages.append(" - {}:\t{}".format(
                        t, t.progress_string()))
                messages.append(
                    "  ... {} not shown".format(len(trials) - tail_length * 2))
                last = sorted_trials[-tail_length:]
                for t in last:
                    messages.append(" - {}:\t{}".format(
                        t, t.progress_string()))
            else:
                for t in sorted_trials:
                    messages.append(" - {}:\t{}".format(
                        t, t.progress_string()))

        return "\n".join(messages) + "\n"

    def _debug_messages(self):
        messages = ["== Status =="]
        messages.append(self._scheduler_alg.debug_string())
        messages.append(self.trial_executor.debug_string())
        messages.append(self._memory_debug_string())
        return messages

    def _memory_debug_string(self):
        try:
            import psutil
            total_gb = psutil.virtual_memory().total / 1e9
            used_gb = total_gb - psutil.virtual_memory().available / 1e9
            if used_gb > total_gb * 0.9:
                warn = (": ***LOW MEMORY*** less than 10% of the memory on "
                        "this node is available for use. This can cause "
                        "unexpected crashes. Consider "
                        "reducing the memory used by your application "
                        "or reducing the Ray object store size by setting "
                        "`object_store_memory` when calling `ray.init`.")
            else:
                warn = ""
            return "Memory usage on this node: {}/{} GB{}".format(
                round(used_gb, 1), round(total_gb, 1), warn)
        except ImportError:
            return ("Unknown memory usage. Please run `pip install psutil` "
                    "(or ray[debug]) to resolve)")

    def has_resources(self, resources):
        """Returns whether this runner has at least the specified resources."""
        return self.trial_executor.has_resources(resources)

    def _get_next_trial(self):
        """Replenishes queue.

        Blocks if all trials queued have finished, but search algorithm is
        still not finished.
        """
        trials_done = all(trial.is_finished() for trial in self._trials)
        wait_for_trial = trials_done and not self._search_alg.is_finished()
        self._update_trial_queue(blocking=wait_for_trial)
        with warn_if_slow("choose_trial_to_run"):
            trial = self._scheduler_alg.choose_trial_to_run(self)
        return trial

    def _process_events(self):
        trial = self.trial_executor.get_next_available_trial()  # blocking
        with warn_if_slow("process_trial"):
            self._process_trial(trial)

    def _process_trial(self, trial):
        try:
            result = self.trial_executor.fetch_result(trial)

            is_duplicate = RESULT_DUPLICATE in result
            # TrialScheduler and SearchAlgorithm still receive a
            # notification because there may be special handling for
            # the `on_trial_complete` hook.
            if is_duplicate:
                logger.debug("Trial finished without logging 'done'.")
                result = trial.last_result
                result.update(done=True)

            self._total_time += result[TIME_THIS_ITER_S]

            if trial.should_stop(result):
                # Hook into scheduler
                self._scheduler_alg.on_trial_complete(self, trial, result)
                self._search_alg.on_trial_complete(
                    trial.trial_id, result=result)
                decision = TrialScheduler.STOP
            else:
                with warn_if_slow("scheduler.on_trial_result"):
                    decision = self._scheduler_alg.on_trial_result(
                        self, trial, result)
                with warn_if_slow("search_alg.on_trial_result"):
                    self._search_alg.on_trial_result(trial.trial_id, result)
                if decision == TrialScheduler.STOP:
                    with warn_if_slow("search_alg.on_trial_complete"):
                        self._search_alg.on_trial_complete(
                            trial.trial_id, early_terminated=True)

            if not is_duplicate:
                trial.update_last_result(
                    result, terminate=(decision == TrialScheduler.STOP))

            # Checkpoints to disk. This should be checked even if
            # the scheduler decision is STOP or PAUSE. Note that
            # PAUSE only checkpoints to memory and does not update
            # the global checkpoint state.
            self._checkpoint_trial_if_needed(
                trial, force=result.get(SHOULD_CHECKPOINT, False))

            if decision == TrialScheduler.CONTINUE:
                self.trial_executor.continue_training(trial)
            elif decision == TrialScheduler.PAUSE:
                self.trial_executor.pause_trial(trial)
            elif decision == TrialScheduler.STOP:
                self.trial_executor.export_trial_if_needed(trial)
                self.trial_executor.stop_trial(trial)
            else:
                assert False, "Invalid scheduling decision: {}".format(
                    decision)
        except Exception:
            logger.exception("Error processing event.")
            error_msg = traceback.format_exc()
            if trial.status == Trial.RUNNING:
                if trial.should_recover():
                    self._try_recover(trial, error_msg)
                else:
                    self._scheduler_alg.on_trial_error(self, trial)
                    self._search_alg.on_trial_complete(
                        trial.trial_id, error=True)
                    self.trial_executor.stop_trial(
                        trial, error=True, error_msg=error_msg)

    def _checkpoint_trial_if_needed(self, trial, force=False):
        """Checkpoints trial based off trial.last_result."""
        if trial.should_checkpoint() or force:
            # Save trial runtime if possible
            if hasattr(trial, "runner") and trial.runner:
                self.trial_executor.save(trial, storage=Checkpoint.DISK)
            self.trial_executor.try_checkpoint_metadata(trial)

    def _try_recover(self, trial, error_msg):
        """Tries to recover trial.

        Notifies SearchAlgorithm and Scheduler if failure to recover.

        Args:
            trial (Trial): Trial to recover.
            error_msg (str): Error message from prior to invoking this method.
        """
        try:
            self.trial_executor.stop_trial(
                trial,
                error=error_msg is not None,
                error_msg=error_msg,
                stop_logger=False)
            trial.result_logger.flush()
            if self.trial_executor.has_resources(trial.resources):
                logger.info("Attempting to recover"
                            " trial state from last checkpoint.")
                self.trial_executor.start_trial(trial)
                if trial.status == Trial.ERROR:
                    raise RuntimeError("Trial did not start correctly.")
            else:
                logger.debug("Notifying Scheduler and requeueing trial.")
                self._requeue_trial(trial)
        except Exception:
            logger.exception("Error recovering trial from checkpoint, abort.")
            self._scheduler_alg.on_trial_error(self, trial)
            self._search_alg.on_trial_complete(trial.trial_id, error=True)

    def _requeue_trial(self, trial):
        """Notification to TrialScheduler and requeue trial.

        This does not notify the SearchAlgorithm because the function
        evaluation is still in progress.

        """
        self._scheduler_alg.on_trial_error(self, trial)
        self.trial_executor.set_status(trial, Trial.PENDING)

        # TODO(rliaw): Right now, this pushes the trial to the end of queue
        # because restoration can be expensive. However, this is not
        # ideal since it just hides the issue - a better fix would
        # be to use an actor table to detect the IP of the Trainable
        # and rsync the files there.
        # See https://github.com/ray-project/ray/issues/5168
        self._trials.pop(self._trials.index(trial))
        self._trials.append(trial)

        with warn_if_slow("scheduler.on_trial_add"):
            self._scheduler_alg.on_trial_add(self, trial)

    def _update_trial_queue(self, blocking=False, timeout=600):
        """Adds next trials to queue if possible.

        Note that the timeout is currently unexposed to the user.

        Args:
            blocking (bool): Blocks until either a trial is available
                or is_finished (timeout or search algorithm finishes).
            timeout (int): Seconds before blocking times out.
        """
        trials = self._search_alg.next_trials()
        if blocking and not trials:
            start = time.time()
            # Checking `is_finished` instead of _search_alg.is_finished
            # is fine because blocking only occurs if all trials are
            # finished and search_algorithm is not yet finished
            while (not trials and not self.is_finished()
                   and time.time() - start < timeout):
                logger.info("Blocking for next trial...")
                trials = self._search_alg.next_trials()
                time.sleep(1)

        for trial in trials:
            self.add_trial(trial)

    def request_stop_trial(self, trial):
        self._stop_queue.append(trial)

    def _process_requests(self):
        while self._stop_queue:
            t = self._stop_queue.pop()
            self.stop_trial(t)

    def stop_trial(self, trial):
        """Stops trial.

        Trials may be stopped at any time. If trial is in state PENDING
        or PAUSED, calls `on_trial_remove`  for scheduler and
        `on_trial_complete(..., early_terminated=True) for search_alg.
        Otherwise waits for result for the trial and calls
        `on_trial_complete` for scheduler and search_alg if RUNNING.
        """
        error = False
        error_msg = None

        if trial.status in [Trial.ERROR, Trial.TERMINATED]:
            return
        elif trial.status in [Trial.PENDING, Trial.PAUSED]:
            self._scheduler_alg.on_trial_remove(self, trial)
            self._search_alg.on_trial_complete(
                trial.trial_id, early_terminated=True)
        elif trial.status is Trial.RUNNING:
            try:
                result = self.trial_executor.fetch_result(trial)
                trial.update_last_result(result, terminate=True)
                self._scheduler_alg.on_trial_complete(self, trial, result)
                self._search_alg.on_trial_complete(
                    trial.trial_id, result=result)
            except Exception:
                error_msg = traceback.format_exc()
                logger.exception("Error processing event.")
                self._scheduler_alg.on_trial_error(self, trial)
                self._search_alg.on_trial_complete(trial.trial_id, error=True)
                error = True

        self.trial_executor.stop_trial(trial, error=error, error_msg=error_msg)

    def __getstate__(self):
        """Gets state for trial.

        Note that this is not used as a pickling override as
        does not have all fields.
        """
        state = self.__dict__.copy()
        for k in [
                "_trials",
                "_stop_queue",
                "_server",
                "_search_alg",
                "_scheduler_alg",
                "trial_executor",
                "_syncer",
        ]:
            del state[k]
        state["launch_web_server"] = bool(self._server)
        return state

    def __setstate__(self, state):
        launch_web_server = state.pop("launch_web_server")

        # Use session_str from previous checkpoint if does not exist
        session_str = state.pop("_session_str")
        self.__dict__.setdefault("_session_str", session_str)
        # Use start_time from previous checkpoint if does not exist
        start_time = state.pop("_start_time")
        self.__dict__.setdefault("_start_time", start_time)

        self.__dict__.update(state)
        if launch_web_server:
            self._server = TuneServer(self, self._server_port)
