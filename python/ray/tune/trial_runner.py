from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import os
import ray
import time
import traceback

from ray.tune import TuneError
from ray.tune.web_server import TuneServer
from ray.tune.trial import Trial, Resources
from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler


MAX_DEBUG_TRIALS = 20


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

    def __init__(self, scheduler=None, launch_web_server=False,
                 server_port=TuneServer.DEFAULT_PORT):
        """Initializes a new TrialRunner.

        Args:
            scheduler (TrialScheduler): Defaults to FIFOScheduler.
            launch_web_server (bool): Flag for starting TuneServer
            server_port (int): Port number for launching TuneServer"""

        self._scheduler_alg = scheduler or FIFOScheduler()
        self._trials = []
        self._running = {}
        self._avail_resources = Resources(cpu=0, gpu=0)
        self._committed_resources = Resources(cpu=0, gpu=0)
        self._resources_initialized = False

        # For debugging, it may be useful to halt trials after some time has
        # elapsed. TODO(ekl) consider exposing this in the API.
        self._global_time_limit = float(
            os.environ.get("TRIALRUNNER_WALLTIME_LIMIT", float('inf')))
        self._total_time = 0
        self._server = None
        if launch_web_server:
            self._server = TuneServer(self, server_port)
        self._stop_queue = []

    def is_finished(self):
        """Returns whether all trials have finished running."""

        if self._total_time > self._global_time_limit:
            print(
                "Exceeded global time limit {} / {}".format(
                    self._total_time, self._global_time_limit))
            return True

        for t in self._trials:
            if t.status in [Trial.PENDING, Trial.RUNNING, Trial.PAUSED]:
                return False
        return True

    def step(self):
        """Runs one step of the trial event loop.

        Callers should typically run this method repeatedly in a loop. They
        may inspect or modify the runner's state in between calls to step().
        """
        if self._can_launch_more():
            self._launch_trial()
        elif self._running:
            self._process_events()
        else:
            for trial in self._trials:
                if trial.status == Trial.PENDING:
                    if not self.has_resources(trial.resources):
                        raise TuneError((
                            "Insufficient cluster resources to launch trial: "
                            "trial requested {} but the cluster only has {} "
                            "available.").format(
                                trial.resources.summary_string(),
                                self._avail_resources.summary_string()))
                elif trial.status == Trial.PAUSED:
                    raise TuneError(
                        "There are paused trials, but no more pending "
                        "trials with sufficient resources.")
            raise TuneError("Called step when all trials finished?")

        if self._server:
            self._process_requests()

            if self.is_finished():
                self._server.shutdown()

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
        """
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

        for local_dir in sorted(set([t.local_dir for t in self._trials])):
            messages.append("Result logdir: {}".format(local_dir))
        for state, trials in sorted(states.items()):
            limit = limit_per_state[state]
            messages.append("{} trials:".format(state))
            for t in sorted(
                    trials, key=lambda t: t.experiment_tag)[:limit]:
                messages.append(" - {}:\t{}".format(t, t.progress_string()))
            if len(trials) > limit:
                messages.append("  ... {} more not shown".format(
                    len(trials) - limit))
        return "\n".join(messages) + "\n"

    def _debug_messages(self):
        messages = ["== Status =="]
        messages.append(self._scheduler_alg.debug_string())
        if self._resources_initialized:
            messages.append(
                "Resources used: {}/{} CPUs, {}/{} GPUs".format(
                    self._committed_resources.cpu,
                    self._avail_resources.cpu,
                    self._committed_resources.gpu,
                    self._avail_resources.gpu))
        return messages

    def has_resources(self, resources):
        """Returns whether this runner has at least the specified resources."""

        cpu_avail = self._avail_resources.cpu - self._committed_resources.cpu
        gpu_avail = self._avail_resources.gpu - self._committed_resources.gpu
        return resources.cpu <= cpu_avail and resources.gpu <= gpu_avail

    def _can_launch_more(self):
        self._update_avail_resources()
        trial = self._get_runnable()
        return trial is not None

    def _launch_trial(self, custom_trial=None):
        trial = custom_trial or self._get_runnable()
        self._commit_resources(trial.resources)
        try:
            trial.start()
            self._running[trial.train_remote()] = trial
        except Exception:
            error_msg = traceback.format_exc()
            print("Error starting runner, retrying:", error_msg)
            time.sleep(2)
            trial.stop(error=True, error_msg=error_msg)
            try:
                trial.start()
                self._running[trial.train_remote()] = trial
            except Exception:
                error_msg = traceback.format_exc()
                print("Error starting runner, abort:", error_msg)
                trial.stop(error=True, error_msg=error_msg)
                # note that we don't return the resources, since they may
                # have been lost

    def _process_events(self):
        [result_id], _ = ray.wait(list(self._running))
        trial = self._running.pop(result_id)
        try:
            result = ray.get(result_id)
            self._total_time += result.time_this_iter_s

            if trial.should_stop(result):
                self._scheduler_alg.on_trial_complete(self, trial, result)
                decision = TrialScheduler.STOP
            else:
                decision = self._scheduler_alg.on_trial_result(
                    self, trial, result)
            trial.update_last_result(
                result, terminate=(decision == TrialScheduler.STOP))

            if decision == TrialScheduler.CONTINUE:
                if trial.should_checkpoint():
                    # TODO(rliaw): This is a blocking call
                    trial.checkpoint()
                self._running[trial.train_remote()] = trial
            elif decision == TrialScheduler.PAUSE:
                self._pause_trial(trial)
            elif decision == TrialScheduler.STOP:
                self._stop_trial(trial)
            else:
                assert False, "Invalid scheduling decision: {}".format(
                    decision)
        except Exception:
            error_msg = traceback.format_exc()
            print("Error processing event:", error_msg)
            if trial.status == Trial.RUNNING:
                if trial.has_checkpoint() and \
                        trial.num_failures < trial.max_failures:
                    self._try_recover(trial, error_msg)
                else:
                    self._scheduler_alg.on_trial_error(self, trial)
                    self._stop_trial(trial, error=True, error_msg=error_msg)

    def _try_recover(self, trial, error_msg):
        try:
            print("Attempting to recover trial state from last checkpoint")
            trial.stop(error=True, error_msg=error_msg, stop_logger=False)
            trial.result_logger.flush()  # make sure checkpoint is synced
            trial.start()
            self._running[trial.train_remote()] = trial
        except Exception:
            error_msg = traceback.format_exc()
            print("Error recovering trial from checkpoint, abort:", error_msg)
            self._stop_trial(trial, error=True, error_msg=error_msg)

    def _get_runnable(self):
        return self._scheduler_alg.choose_trial_to_run(self)

    def _commit_resources(self, resources):
        self._committed_resources = Resources(
            self._committed_resources.cpu + resources.cpu,
            self._committed_resources.gpu + resources.gpu)

    def _return_resources(self, resources):
        self._committed_resources = Resources(
            self._committed_resources.cpu - resources.cpu,
            self._committed_resources.gpu - resources.gpu)
        assert self._committed_resources.cpu >= 0
        assert self._committed_resources.gpu >= 0

    def request_stop_trial(self, trial):
        self._stop_queue.append(trial)

    def _process_requests(self):
        while self._stop_queue:
            t = self._stop_queue.pop()
            self.stop_trial(t)

    def stop_trial(self, trial):
        """Stops trial.

        Trials may be stopped at any time. If trial is in state PENDING
        or PAUSED, calls `scheduler.on_trial_remove`. Otherwise waits for
        result for the trial and calls `scheduler.on_trial_complete`
        if RUNNING."""
        error = False
        error_msg = None

        if trial.status in [Trial.ERROR, Trial.TERMINATED]:
            return
        elif trial.status in [Trial.PENDING, Trial.PAUSED]:
            self._scheduler_alg.on_trial_remove(self, trial)
        elif trial.status is Trial.RUNNING:
            # NOTE: There should only be one...
            result_id = [rid for rid, t in self._running.items()
                         if t is trial][0]
            self._running.pop(result_id)
            try:
                result = ray.get(result_id)
                trial.update_last_result(result, terminate=True)
                self._scheduler_alg.on_trial_complete(self, trial, result)
            except Exception:
                error_msg = traceback.format_exc()
                print("Error processing event:", error_msg)
                self._scheduler_alg.on_trial_error(self, trial)
                error = True

        self._stop_trial(trial, error=error, error_msg=error_msg)

    def _stop_trial(self, trial, error=False, error_msg=None):
        """Only returns resources if resources allocated."""
        prior_status = trial.status
        trial.stop(error=error, error_msg=error_msg)
        if prior_status == Trial.RUNNING:
            self._return_resources(trial.resources)

    def _pause_trial(self, trial):
        """Only returns resources if resources allocated."""
        prior_status = trial.status
        trial.pause()
        if prior_status == Trial.RUNNING:
            self._return_resources(trial.resources)

    def _update_avail_resources(self):
        clients = ray.global_state.client_table()
        local_schedulers = [
            entry for client in clients.values() for entry in client
            if (entry['ClientType'] == 'local_scheduler' and not
                entry['Deleted'])
        ]
        num_cpus = sum(ls['CPU'] for ls in local_schedulers)
        num_gpus = sum(ls.get('GPU', 0) for ls in local_schedulers)
        self._avail_resources = Resources(int(num_cpus), int(num_gpus))
        self._resources_initialized = True
