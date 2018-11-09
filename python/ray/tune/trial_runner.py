from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import logging
import os
import re
import time
import traceback

from ray.tune import TuneError
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.result import TIME_THIS_ITER_S
from ray.tune.trial import Trial
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.web_server import TuneServer

MAX_DEBUG_TRIALS = 20

logger = logging.getLogger(__name__)


def _naturalize(string):
    """Provides a natural representation for string for nice sorting."""
    splits = re.split("([0-9]+)", string)
    return [int(text) if text.isdigit() else text.lower() for text in splits]


class TrialRunner(object):
    """A TrialRunner implements the event loop for scheduling trials on Ray.

    Example:
        runner = TrialRunner(BasicVariantGenerator())
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

    def __init__(self,
                 search_alg,
                 scheduler=None,
                 launch_web_server=False,
                 server_port=TuneServer.DEFAULT_PORT,
                 verbose=True,
                 queue_trials=False,
                 trial_executor=None):
        """Initializes a new TrialRunner.

        Args:
            search_alg (SearchAlgorithm): SearchAlgorithm for generating
                Trial objects.
            scheduler (TrialScheduler): Defaults to FIFOScheduler.
            launch_web_server (bool): Flag for starting TuneServer
            server_port (int): Port number for launching TuneServer
            verbose (bool): Flag for verbosity. If False, trial results
                will not be output.
            queue_trials (bool): Whether to queue trials when the cluster does
                not currently have enough resources to launch one. This should
                be set to True when running on an autoscaling cluster to enable
                automatic scale-up.
            trial_executor (TrialExecutor): Defaults to RayTrialExecutor.
        """
        self._search_alg = search_alg
        self._scheduler_alg = scheduler or FIFOScheduler()
        self._trials = []
        self.trial_executor = trial_executor or \
            RayTrialExecutor(queue_trials=queue_trials)

        # For debugging, it may be useful to halt trials after some time has
        # elapsed. TODO(ekl) consider exposing this in the API.
        self._global_time_limit = float(
            os.environ.get("TRIALRUNNER_WALLTIME_LIMIT", float('inf')))
        self._total_time = 0
        self._server = None
        if launch_web_server:
            self._server = TuneServer(self, server_port)
        self._stop_queue = []
        self._verbose = verbose
        self._queue_trials = queue_trials

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
        self.trial_executor.on_step_begin()
        next_trial = self._get_next_trial()
        if next_trial is not None:
            self.trial_executor.start_trial(next_trial)
        elif self.trial_executor.get_running_trials():
            self._process_events()
        else:
            for trial in self._trials:
                if trial.status == Trial.PENDING:
                    if not self.has_resources(trial.resources):
                        raise TuneError(
                            ("Insufficient cluster resources to launch trial: "
                             "trial requested {} but the cluster summary: {} "
                             "Pass `queue_trials=True` in "
                             "ray.tune.run_experiments() or on the command "
                             "line to queue trials until the cluster scales "
                             "up. {}").format(
                                 trial.resources.summary_string(),
                                 self.trial_executor.debug_string(),
                                 trial._get_trainable_cls().resource_help(
                                     trial.config)))
                elif trial.status == Trial.PAUSED:
                    raise TuneError(
                        "There are paused trials, but no more pending "
                        "trials with sufficient resources.")

        if self._server:
            self._process_requests()

            if self.is_finished():
                self._server.shutdown()
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
        self._scheduler_alg.on_trial_add(self, trial)
        self._trials.append(trial)

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
        return messages

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
        trial = self._scheduler_alg.choose_trial_to_run(self)
        return trial

    def _process_events(self):
        trial = self.trial_executor.get_next_available_trial()
        try:
            result = self.trial_executor.fetch_result(trial)
            self._total_time += result[TIME_THIS_ITER_S]

            if trial.should_stop(result):
                # Hook into scheduler
                self._scheduler_alg.on_trial_complete(self, trial, result)
                self._search_alg.on_trial_complete(
                    trial.trial_id, result=result)
                decision = TrialScheduler.STOP

            else:
                decision = self._scheduler_alg.on_trial_result(
                    self, trial, result)
                self._search_alg.on_trial_result(trial.trial_id, result)
                if decision == TrialScheduler.STOP:
                    self._search_alg.on_trial_complete(
                        trial.trial_id, early_terminated=True)
            trial.update_last_result(
                result, terminate=(decision == TrialScheduler.STOP))

            if decision == TrialScheduler.CONTINUE:
                if trial.should_checkpoint(result):
                    # TODO(rliaw): This is a blocking call
                    self.trial_executor.save(trial)
                self.trial_executor.continue_training(trial)
            elif decision == TrialScheduler.PAUSE:
                self.trial_executor.pause_trial(trial)
            elif decision == TrialScheduler.STOP:
                # Checkpoint before ending the trial
                # if checkpoint_at_end experiment option is set to True
                if trial.should_checkpoint(result):
                    self.trial_executor.save(trial)
                self.trial_executor.stop_trial(trial)
            else:
                assert False, "Invalid scheduling decision: {}".format(
                    decision)
        except Exception:
            logger.exception("Error processing event.")
            error_msg = traceback.format_exc()
            if trial.status == Trial.RUNNING:
                if trial.has_checkpoint() and \
                        trial.num_failures < trial.max_failures:
                    self._try_recover(trial, error_msg)
                else:
                    self._scheduler_alg.on_trial_error(self, trial)
                    self._search_alg.on_trial_complete(
                        trial.trial_id, error=True)
                    self.trial_executor.stop_trial(trial, True, error_msg)

    def _try_recover(self, trial, error_msg):
        try:
            logger.info("Attempting to recover"
                        " trial state from last checkpoint.")
            self.trial_executor.restart_trial(trial, error_msg)
        except Exception:
            error_msg = traceback.format_exc()
            logger.warning("Error recovering trial from checkpoint, abort.")
            self.trial_executor.stop_trial(trial, True, error_msg=error_msg)

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
