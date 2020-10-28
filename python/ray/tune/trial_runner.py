from typing import Dict, List

import click
from datetime import datetime
import json
import logging
import os
import time
import traceback
import types

import ray.cloudpickle as cloudpickle
from ray.services import get_node_ip_address
from ray.tune import TuneError
from ray.tune.stopper import NoopStopper
from ray.tune.progress_reporter import trial_progress_str
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.result import (TIME_THIS_ITER_S, RESULT_DUPLICATE,
                             SHOULD_CHECKPOINT)
from ray.tune.syncer import get_cloud_syncer
from ray.tune.trial import Checkpoint, Trial
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.utils import warn_if_slow, flatten_dict, env_integer
from ray.tune.web_server import TuneServer
from ray.utils import binary_to_hex, hex_to_binary
from ray.util.debug import log_once

MAX_DEBUG_TRIALS = 20

logger = logging.getLogger(__name__)


def _find_newest_ckpt(ckpt_dir):
    """Returns path to most recently modified checkpoint."""
    full_paths = [
        os.path.join(ckpt_dir, fname) for fname in os.listdir(ckpt_dir)
        if fname.startswith("experiment_state") and fname.endswith(".json")
    ]
    return max(full_paths)


class _TuneFunctionEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, types.FunctionType):
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


class Callback:
    """Tune base callback that can be extended and passed to a ``TrialRunner``

    Tune callbacks are called from within the ``TrialRunner`` class. There are
    several hooks that can be used, all of which are found in the submethod
    definitions of this base class.

    The parameters passed to the ``**info`` dict vary between hooks. The
    parameters passed are described in the docstrings of the methods.

    This example will print a metric each time a result is received:

    .. code-block:: python

        from ray import tune
        from ray.tune import Callback


        class MyCallback(Callback):
            def on_trial_result(self, iteration, trials, trial, result,
                                **info):
                print(f"Got result: {result['metric']}")


        def train(config):
            for i in range(10):
                tune.report(metric=i)


        tune.run(
            train,
            callbacks=[MyCallback()])

    """

    def on_step_begin(self, iteration: int, trials: List[Trial], **info):
        """Called at the start of each tuning loop step.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_step_end(self, iteration: int, trials: List[Trial], **info):
        """Called at the end of each tuning loop step.

        The iteration counter is increased before this hook is called.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_start(self, iteration: int, trials: List[Trial], trial: Trial,
                       **info):
        """Called after starting a trial instance.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has been started.
            **info: Kwargs dict for forward compatibility.

        """
        pass

    def on_trial_restore(self, iteration: int, trials: List[Trial],
                         trial: Trial, **info):
        """Called after restoring a trial instance.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has been restored.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_save(self, iteration: int, trials: List[Trial], trial: Trial,
                      **info):
        """Called after receiving a checkpoint from a trial.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just saved a checkpoint.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_result(self, iteration: int, trials: List[Trial],
                        trial: Trial, result: Dict, **info):
        """Called after receiving a result from a trial.

        The search algorithm and scheduler are notified before this
        hook is called.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just sent a result.
            result (Dict): Result that the trial sent.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_complete(self, iteration: int, trials: List[Trial],
                          trial: Trial, **info):
        """Called after a trial instance completed.

        The search algorithm and scheduler are notified before this
        hook is called.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has been completed.
            **info: Kwargs dict for forward compatibility.
        """
        pass

    def on_trial_fail(self, iteration: int, trials: List[Trial], trial: Trial,
                      **info):
        """Called after a trial instance failed (errored).

        The search algorithm and scheduler are notified before this
        hook is called.

        Arguments:
            iteration (int): Number of iterations of the tuning loop.
            trials (List[Trial]): List of trials.
            trial (Trial): Trial that just has errored.
            **info: Kwargs dict for forward compatibility.
        """
        pass


class _CallbackList:
    """Call multiple callbacks at once."""

    def __init__(self, callbacks: List[Callback]):
        self._callbacks = callbacks

    def on_step_begin(self, **info):
        for callback in self._callbacks:
            callback.on_step_begin(**info)

    def on_step_end(self, **info):
        for callback in self._callbacks:
            callback.on_step_end(**info)

    def on_trial_start(self, **info):
        for callback in self._callbacks:
            callback.on_trial_start(**info)

    def on_trial_restore(self, **info):
        for callback in self._callbacks:
            callback.on_trial_restore(**info)

    def on_trial_save(self, **info):
        for callback in self._callbacks:
            callback.on_trial_save(**info)

    def on_trial_result(self, **info):
        for callback in self._callbacks:
            callback.on_trial_result(**info)

    def on_trial_complete(self, **info):
        for callback in self._callbacks:
            callback.on_trial_complete(**info)

    def on_trial_fail(self, **info):
        for callback in self._callbacks:
            callback.on_trial_fail(**info)


class TrialRunner:
    """A TrialRunner implements the event loop for scheduling trials on Ray.

    .. code-block: python

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

    Args:
        search_alg (SearchAlgorithm): SearchAlgorithm for generating
            Trial objects.
        scheduler (TrialScheduler): Defaults to FIFOScheduler.
        local_checkpoint_dir (str): Path where
            global checkpoints are stored and restored from.
        remote_checkpoint_dir (str): Remote path where
            global checkpoints are stored and restored from. Used
            if `resume` == REMOTE.
        stopper: Custom class for stopping whole experiments. See
            ``Stopper``.
        resume (str|False): see `tune.py:run`.
        sync_to_cloud (func|str): See `tune.py:run`.
        server_port (int): Port number for launching TuneServer.
        fail_fast (bool | str): Finishes as soon as a trial fails if True.
            If fail_fast='raise' provided, Tune will automatically
            raise the exception received by the Trainable. fail_fast='raise'
            can easily leak resources and should be used with caution.
        verbose (bool): Flag for verbosity. If False, trial results
            will not be output.
        checkpoint_period (int): Trial runner checkpoint periodicity in
            seconds. Defaults to 10.
        trial_executor (TrialExecutor): Defaults to RayTrialExecutor.
        callbacks (list): List of callbacks that will be called at different
            times in the training loop. Must be instances of the
            ``ray.tune.trial_runner.Callback`` class.
    """

    CKPT_FILE_TMPL = "experiment_state-{}.json"
    VALID_RESUME_TYPES = [True, "LOCAL", "REMOTE", "PROMPT", "ERRORED_ONLY"]
    RAISE = "RAISE"

    def __init__(self,
                 search_alg=None,
                 scheduler=None,
                 local_checkpoint_dir=None,
                 remote_checkpoint_dir=None,
                 sync_to_cloud=None,
                 stopper=None,
                 resume=False,
                 server_port=None,
                 fail_fast=False,
                 verbose=True,
                 checkpoint_period=None,
                 trial_executor=None,
                 callbacks=None,
                 metric=None):
        self._search_alg = search_alg or BasicVariantGenerator()
        self._scheduler_alg = scheduler or FIFOScheduler()
        self.trial_executor = trial_executor or RayTrialExecutor()

        self._metric = metric

        if "TRIALRUNNER_WALLTIME_LIMIT" in os.environ:
            raise ValueError(
                "The TRIALRUNNER_WALLTIME_LIMIT environment variable is "
                "deprecated. "
                "Use `tune.run(time_budget_s=limit)` instead.")

        self._total_time = 0
        self._iteration = 0
        self._has_errored = False
        self._fail_fast = fail_fast
        if isinstance(self._fail_fast, str):
            self._fail_fast = self._fail_fast.upper()
            if self._fail_fast == TrialRunner.RAISE:
                logger.warning(
                    "fail_fast='raise' detected. Be careful when using this "
                    "mode as resources (such as Ray processes, "
                    "file descriptors, and temporary files) may not be "
                    "cleaned up properly. To use "
                    "a safer mode, use fail_fast=True.")
            else:
                raise ValueError("fail_fast must be one of {bool, RAISE}. "
                                 f"Got {self._fail_fast}.")
        self._verbose = verbose

        self._server = None
        self._server_port = server_port
        if server_port is not None:
            self._server = TuneServer(self, self._server_port)

        self._trials = []
        self._cached_trial_decisions = {}
        self._stop_queue = []
        self._should_stop_experiment = False  # used by TuneServer
        self._local_checkpoint_dir = local_checkpoint_dir

        if self._local_checkpoint_dir:
            os.makedirs(self._local_checkpoint_dir, exist_ok=True)

        self._remote_checkpoint_dir = remote_checkpoint_dir
        self._syncer = get_cloud_syncer(local_checkpoint_dir,
                                        remote_checkpoint_dir, sync_to_cloud)
        self._stopper = stopper or NoopStopper()
        self._resumed = False

        if self._validate_resume(resume_type=resume):
            errored_only = False
            if isinstance(resume, str):
                errored_only = resume.upper() == "ERRORED_ONLY"
            try:
                self.resume(run_errored_only=errored_only)
                self._resumed = True
            except Exception as e:
                if self._verbose:
                    logger.error(str(e))
                logger.exception("Runner restore failed.")
                if self._fail_fast:
                    raise
                logger.info("Restarting experiment.")
        else:
            logger.debug("Starting a new experiment.")

        self._start_time = time.time()
        self._last_checkpoint_time = -float("inf")
        if checkpoint_period is None:
            checkpoint_period = env_integer("TUNE_GLOBAL_CHECKPOINT_S", 10)
        self._checkpoint_period = checkpoint_period
        self._session_str = datetime.fromtimestamp(
            self._start_time).strftime("%Y-%m-%d_%H-%M-%S")
        self.checkpoint_file = None
        if self._local_checkpoint_dir:
            self.checkpoint_file = os.path.join(
                self._local_checkpoint_dir,
                TrialRunner.CKPT_FILE_TMPL.format(self._session_str))

        self._callbacks = _CallbackList(callbacks or [])

    @property
    def resumed(self):
        return self._resumed

    @property
    def scheduler_alg(self):
        return self._scheduler_alg

    def _validate_resume(self, resume_type):
        """Checks whether to resume experiment.

        Args:
            resume_type: One of True, "REMOTE", "LOCAL",
                "PROMPT", "ERRORED_ONLY".
        """
        # TODO: Consider supporting ERRORED_ONLY+REMOTE?
        if not resume_type:
            return False
        assert resume_type in self.VALID_RESUME_TYPES, (
            "resume_type {} is not one of {}".format(resume_type,
                                                     self.VALID_RESUME_TYPES))
        # Not clear if we need this assertion, since we should always have a
        # local checkpoint dir.
        assert self._local_checkpoint_dir or self._remote_checkpoint_dir
        if resume_type in [True, "LOCAL", "PROMPT", "ERRORED_ONLY"]:
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
            logger.info("Downloading from %s", self._remote_checkpoint_dir)
            # TODO(ujvl): Note that this syncs down the entire directory,
            #  which may also contain trial checkpoints. We should selectively
            #  sync the necessary files instead.
            self._syncer.sync_down_if_needed()
            self._syncer.wait()

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

    def checkpoint(self, force=False):
        """Saves execution state to `self._local_checkpoint_dir`.

        Overwrites the current session checkpoint, which starts when self
        is instantiated. Throttle depends on self._checkpoint_period.

        Also automatically saves the search algorithm to the local
        checkpoint dir.

        Args:
            force (bool): Forces a checkpoint despite checkpoint_period.
        """
        if not self._local_checkpoint_dir:
            return
        now = time.time()
        if now - self._last_checkpoint_time < self._checkpoint_period and (
                not force):
            return
        self._last_checkpoint_time = now
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

        os.replace(tmp_file_name, self.checkpoint_file)
        self._search_alg.save_to_dir(
            self._local_checkpoint_dir, session_str=self._session_str)

        if force:
            self._syncer.sync_up()
        else:
            self._syncer.sync_up_if_needed()
        return self._local_checkpoint_dir

    def resume(self, run_errored_only=False):
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
                self._local_checkpoint_dir),
            "This will ignore any new changes to the specification."
        ]))

        self.__setstate__(runner_state["runner_data"])
        if self._search_alg.has_checkpoint(self._local_checkpoint_dir):
            self._search_alg.restore_from_dir(self._local_checkpoint_dir)

        trials = []
        for trial_cp in runner_state["checkpoints"]:
            new_trial = Trial(trial_cp["trainable_name"])
            new_trial.__setstate__(trial_cp)
            trials += [new_trial]
        for trial in sorted(
                trials, key=lambda t: t.last_update_time, reverse=True):
            if run_errored_only and trial.status == Trial.ERROR:
                new_trial = trial.reset()
                self.add_trial(new_trial)
            else:
                self.add_trial(trial)

    def is_finished(self):
        """Returns whether all trials have finished running."""
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
            self.trial_executor.on_step_begin(self)
        with warn_if_slow("callbacks.on_step_begin"):
            self._callbacks.on_step_begin(
                iteration=self._iteration, trials=self._trials)
        next_trial = self._get_next_trial()  # blocking
        if next_trial is not None:
            with warn_if_slow("start_trial"):
                self.trial_executor.start_trial(next_trial)
                self._callbacks.on_trial_start(
                    iteration=self._iteration,
                    trials=self._trials,
                    trial=next_trial)
        elif self.trial_executor.get_running_trials():
            self._process_events()  # blocking
        else:
            self.trial_executor.on_no_available_trials(self)

        self._stop_experiment_if_needed()

        try:
            with warn_if_slow("experiment_checkpoint"):
                self.checkpoint()
        except Exception as e:
            logger.warning(f"Trial Runner checkpointing failed: {str(e)}")
        self._iteration += 1

        if self._server:
            with warn_if_slow("server"):
                self._process_stop_requests()

            if self.is_finished():
                self._server.shutdown()
        with warn_if_slow("on_step_end"):
            self.trial_executor.on_step_end(self)
        with warn_if_slow("callbacks.on_step_end"):
            self._callbacks.on_step_end(
                iteration=self._iteration, trials=self._trials)

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

    def debug_string(self, delim="\n"):
        result_keys = [
            list(t.last_result) for t in self.get_trials() if t.last_result
        ]
        metrics = set().union(*result_keys)
        messages = [
            self._scheduler_alg.debug_string(),
            self.trial_executor.debug_string(),
            trial_progress_str(self.get_trials(), metrics),
        ]
        return delim.join(messages)

    def has_resources(self, resources):
        """Returns whether this runner has at least the specified resources."""
        return self.trial_executor.has_resources(resources)

    def _stop_experiment_if_needed(self):
        """Stops all trials."""
        fail_fast = self._fail_fast and self._has_errored
        if (self._stopper.stop_all() or fail_fast
                or self._should_stop_experiment):
            self._search_alg.set_finished()
            [
                self.trial_executor.stop_trial(t) for t in self._trials
                if t.status is not Trial.ERROR
            ]

    def _get_next_trial(self):
        """Replenishes queue.

        Blocks if all trials queued have finished, but search algorithm is
        still not finished.
        """
        trials_done = all(trial.is_finished() for trial in self._trials)
        wait_for_trial = trials_done and not self._search_alg.is_finished()
        # Only fetch a new trial if we have no pending trial
        if not any(trial.status == Trial.PENDING for trial in self._trials) \
           or wait_for_trial:
            self._update_trial_queue(blocking=wait_for_trial)
        with warn_if_slow("choose_trial_to_run"):
            trial = self._scheduler_alg.choose_trial_to_run(self)
            logger.debug("Running trial {}".format(trial))
        return trial

    def _process_events(self):
        failed_trial = self.trial_executor.get_next_failed_trial()
        if failed_trial:
            error_msg = (
                "{} (IP: {}) detected as stale. This is likely because the "
                "node was lost").format(failed_trial, failed_trial.node_ip)
            logger.info(error_msg)
            with warn_if_slow("process_failed_trial"):
                self._process_trial_failure(failed_trial, error_msg=error_msg)
        else:
            # TODO(ujvl): Consider combining get_next_available_trial and
            #  fetch_result functionality so that we don't timeout on fetch.
            trial = self.trial_executor.get_next_available_trial()  # blocking
            if trial.is_restoring:
                with warn_if_slow("process_trial_restore"):
                    self._process_trial_restore(trial)
                with warn_if_slow("callbacks.on_trial_restore"):
                    self._callbacks.on_trial_restore(
                        iteration=self._iteration,
                        trials=self._trials,
                        trial=trial)
            elif trial.is_saving:
                with warn_if_slow("process_trial_save") as profile:
                    self._process_trial_save(trial)
                with warn_if_slow("callbacks.on_trial_save"):
                    self._callbacks.on_trial_save(
                        iteration=self._iteration,
                        trials=self._trials,
                        trial=trial)
                if profile.too_slow and trial.sync_on_checkpoint:
                    # TODO(ujvl): Suggest using DurableTrainable once
                    #  API has converged.

                    msg = (
                        "Consider turning off forced head-worker trial "
                        "checkpoint syncs by setting sync_on_checkpoint=False"
                        ". Note that this may result in faulty trial "
                        "restoration if a failure occurs while the checkpoint "
                        "is being synced from the worker to the head node.")

                    if trial.location.hostname and (trial.location.hostname !=
                                                    get_node_ip_address()):
                        if log_once("tune_head_worker_checkpoint"):
                            logger.warning(msg)

            else:
                with warn_if_slow("process_trial"):
                    self._process_trial(trial)

    def _process_trial(self, trial):
        """Processes a trial result.

        Fetches the trial's latest result and makes a scheduling decision
        regarding its next action. If a checkpoint is taken, the decided
        action is cached and acted on only after the checkpoint is later
        processed (see `_process_trial_save`). Otherwise the decision is
        acted on immediately.

        Args:
            trial (Trial): Trial with a result ready to be processed.
        """
        try:
            result = self.trial_executor.fetch_result(trial)
            is_duplicate = RESULT_DUPLICATE in result
            force_checkpoint = result.get(SHOULD_CHECKPOINT, False)
            # TrialScheduler and SearchAlgorithm still receive a
            # notification because there may be special handling for
            # the `on_trial_complete` hook.
            if is_duplicate:
                logger.debug("Trial finished without logging 'done'.")
                result = trial.last_result
                result.update(done=True)

            self._validate_result_metrics(result)
            self._total_time += result.get(TIME_THIS_ITER_S, 0)

            flat_result = flatten_dict(result)
            if self._stopper(trial.trial_id,
                             result) or trial.should_stop(flat_result):
                # Hook into scheduler
                self._scheduler_alg.on_trial_complete(self, trial, flat_result)
                self._search_alg.on_trial_complete(
                    trial.trial_id, result=flat_result)
                self._callbacks.on_trial_complete(
                    iteration=self._iteration,
                    trials=self._trials,
                    trial=trial)
                decision = TrialScheduler.STOP
            else:
                with warn_if_slow("scheduler.on_trial_result"):
                    decision = self._scheduler_alg.on_trial_result(
                        self, trial, flat_result)
                with warn_if_slow("search_alg.on_trial_result"):
                    self._search_alg.on_trial_result(trial.trial_id,
                                                     flat_result)
                with warn_if_slow("callbacks.on_trial_result"):
                    self._callbacks.on_trial_result(
                        iteration=self._iteration,
                        trials=self._trials,
                        trial=trial,
                        result=result.copy())
                if decision == TrialScheduler.STOP:
                    with warn_if_slow("search_alg.on_trial_complete"):
                        self._search_alg.on_trial_complete(
                            trial.trial_id, result=flat_result)
                    with warn_if_slow("callbacks.on_trial_complete"):
                        self._callbacks.on_trial_complete(
                            iteration=self._iteration,
                            trials=self._trials,
                            trial=trial)

            if not is_duplicate:
                trial.update_last_result(
                    result, terminate=(decision == TrialScheduler.STOP))

            # Checkpoints to disk. This should be checked even if
            # the scheduler decision is STOP or PAUSE. Note that
            # PAUSE only checkpoints to memory and does not update
            # the global checkpoint state.
            self._checkpoint_trial_if_needed(trial, force=force_checkpoint)

            if trial.is_saving:
                # Cache decision to execute on after the save is processed.
                # This prevents changing the trial's state or kicking off
                # another training step prematurely.
                self._cached_trial_decisions[trial.trial_id] = decision
            else:
                self._execute_action(trial, decision)
        except Exception:
            logger.exception("Trial %s: Error processing event.", trial)
            if self._fail_fast == TrialRunner.RAISE:
                raise
            self._process_trial_failure(trial, traceback.format_exc())

    def _validate_result_metrics(self, result):
        """
        Check if any of the required metrics was not reported
        in the last result. If the only item is `done=True`, this
        means that no result was ever received and the trial just
        returned. This is also okay and will not raise an error.
        """
        if int(os.environ.get("TUNE_DISABLE_STRICT_METRIC_CHECKING",
                              0)) != 1 and (len(result) > 1
                                            or "done" not in result):
            base_metric = self._metric
            scheduler_metric = self._scheduler_alg.metric
            search_metrics = self._search_alg.metric
            if isinstance(search_metrics, str):
                search_metrics = [search_metrics]

            if base_metric and base_metric not in result:
                report_metric = base_metric
                location = "tune.run()"
            elif scheduler_metric and scheduler_metric not in result:
                report_metric = scheduler_metric
                location = type(self._scheduler_alg).__name__
            elif search_metrics and any(search_metric not in result
                                        for search_metric in search_metrics):
                report_metric = list(
                    filter(lambda search_metric: search_metric not in result,
                           search_metrics))
                if len(report_metric) == 1:
                    report_metric = report_metric[0]
                location = type(self._search_alg).__name__
            else:
                report_metric = None
                location = None

            if report_metric:
                raise ValueError(
                    "Trial returned a result which did not include the "
                    "specified metric(s) `{}` that `{}` expects. "
                    "Make sure your calls to `tune.report()` include the "
                    "metric, or set the "
                    "TUNE_DISABLE_STRICT_METRIC_CHECKING "
                    "environment variable to 1. Result: {}".format(
                        report_metric, location, result))

    def _process_trial_save(self, trial):
        """Processes a trial save.

        Acts on the decision cached during the last `_process_trial` call.

        Args:
            trial (Trial): Trial being saved.
        """
        logger.debug("Trial %s: Processing trial save.", trial)
        checkpoint_value = None

        try:
            checkpoint_value = self.trial_executor.fetch_result(trial)
        except Exception:
            logger.exception("Trial %s: Error processing result.", trial)
            if self._fail_fast == TrialRunner.RAISE:
                raise
            self._process_trial_failure(trial, traceback.format_exc())

        if checkpoint_value:
            try:
                trial.saving_to.value = checkpoint_value
                trial.on_checkpoint(trial.saving_to)
                self.trial_executor.try_checkpoint_metadata(trial)
            except Exception:
                logger.exception("Trial %s: Error handling checkpoint %s",
                                 trial, checkpoint_value)
                if self._fail_fast == TrialRunner.RAISE:
                    raise

        trial.saving_to = None
        decision = self._cached_trial_decisions.pop(trial.trial_id, None)
        if decision and checkpoint_value:
            self._execute_action(trial, decision)

    def _process_trial_restore(self, trial):
        """Processes a trial restore.

        Args:
            trial (Trial): Trial being restored.
        """
        logger.debug("Trial %s: Processing trial restore.", trial)
        try:
            self.trial_executor.fetch_result(trial)
            trial.on_restore()
            logger.debug("Trial %s: Restore processed successfully", trial)
            self.trial_executor.set_status(trial, Trial.RUNNING)
            self.trial_executor.continue_training(trial)
        except Exception:
            logger.exception("Trial %s: Error processing restore.", trial)
            if self._fail_fast == TrialRunner.RAISE:
                raise
            self._process_trial_failure(trial, traceback.format_exc())

    def _process_trial_failure(self, trial, error_msg):
        """Handle trial failure.

        Attempt trial recovery if possible, clean up state otherwise.

        Args:
            trial (Trial): Failed trial.
            error_msg (str): Error message prior to invoking this method.
        """
        self._has_errored = True
        if trial.status == Trial.RUNNING:
            if trial.should_recover():
                self._try_recover(trial, error_msg)
            else:
                self._scheduler_alg.on_trial_error(self, trial)
                self._search_alg.on_trial_complete(trial.trial_id, error=True)
                self._callbacks.on_trial_fail(
                    iteration=self._iteration,
                    trials=self._trials,
                    trial=trial)
                self.trial_executor.stop_trial(
                    trial, error=True, error_msg=error_msg)

    def _execute_action(self, trial, decision):
        """Executes action based on decision.

        Args:
            trial (Trial): Trial to act on.
            decision (str): Scheduling decision to undertake.
        """
        if decision == TrialScheduler.CONTINUE:
            self.trial_executor.continue_training(trial)
        elif decision == TrialScheduler.PAUSE:
            self.trial_executor.pause_trial(trial)
        elif decision == TrialScheduler.STOP:
            self.trial_executor.export_trial_if_needed(trial)
            self.trial_executor.stop_trial(trial)
        else:
            raise ValueError("Invalid decision: {}".format(decision))

    def _checkpoint_trial_if_needed(self, trial, force=False):
        """Checkpoints trial based off trial.last_result."""
        if trial.should_checkpoint() or force:
            # Save trial runtime if possible.
            if trial.runner:
                self.trial_executor.save(trial, storage=Checkpoint.PERSISTENT)

    def _try_recover(self, trial, error_msg):
        """Tries to recover trial.

        Notifies SearchAlgorithm and Scheduler if failure to recover.

        Args:
            trial (Trial): Trial to recover.
            error_msg (str): Error message from prior to invoking this method.
        """
        if trial.is_restoring:
            # Restore was unsuccessful, try again without checkpoint.
            trial.clear_checkpoint()
        self.trial_executor.stop_trial(
            trial,
            error=error_msg is not None,
            error_msg=error_msg,
            stop_logger=False)
        trial.result_logger.flush()
        if self.trial_executor.has_resources(trial.resources):
            logger.info(
                "Trial %s: Attempting to restore "
                "trial state from last checkpoint.", trial)
            self.trial_executor.start_trial(trial)
            if trial.status == Trial.ERROR:
                logger.exception(
                    "Trial %s: Error restoring trial from checkpoint, abort.",
                    trial)
                self._scheduler_alg.on_trial_error(self, trial)
                self._search_alg.on_trial_complete(trial.trial_id, error=True)
            else:
                logger.debug("Trial %s: Restore dispatched correctly.", trial)
        else:
            logger.debug("Trial %s: Notifying Scheduler and requeueing.",
                         trial)
            self._requeue_trial(trial)

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
        trial = self._search_alg.next_trial()
        if blocking and not trial:
            start = time.time()
            # Checking `is_finished` instead of _search_alg.is_finished
            # is fine because blocking only occurs if all trials are
            # finished and search_algorithm is not yet finished
            while (not trial and not self.is_finished()
                   and time.time() - start < timeout):
                logger.info("Blocking for next trial...")
                trial = self._search_alg.next_trial()
                time.sleep(1)

        if trial:
            self.add_trial(trial)

    def request_stop_trial(self, trial):
        self._stop_queue.append(trial)

    def request_stop_experiment(self):
        self._should_stop_experiment = True

    def _process_stop_requests(self):
        while self._stop_queue:
            t = self._stop_queue.pop()
            self.stop_trial(t)

    def stop_trial(self, trial):
        """Stops trial.

        Trials may be stopped at any time. If trial is in state PENDING
        or PAUSED, calls `on_trial_remove`  for scheduler and
        `on_trial_complete() for search_alg.
        Otherwise waits for result for the trial and calls
        `on_trial_complete` for scheduler and search_alg if RUNNING.
        """
        error = False
        error_msg = None

        if trial.status in [Trial.ERROR, Trial.TERMINATED]:
            return
        elif trial.status in [Trial.PENDING, Trial.PAUSED]:
            self._scheduler_alg.on_trial_remove(self, trial)
            self._search_alg.on_trial_complete(trial.trial_id)
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

    def cleanup_trials(self):
        self.trial_executor.cleanup()

    def __getstate__(self):
        """Gets state for trial.

        Note that this is not used as a pickling override as
        does not have all fields.
        """
        state = self.__dict__.copy()
        for k in [
                "_trials", "_stop_queue", "_server", "_search_alg",
                "_scheduler_alg", "trial_executor", "_syncer", "_callbacks"
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
