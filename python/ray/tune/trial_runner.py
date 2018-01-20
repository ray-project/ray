from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import ray
import time
import traceback

from ray.tune import TuneError
from ray.tune.trial import Trial, Resources
from ray.tune.trial_scheduler import FIFOScheduler, TrialScheduler


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

    def __init__(self, scheduler=None):
        """Initializes a new TrialRunner."""

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
                        raise TuneError(
                            "Insufficient cluster resources to launch trial",
                            (trial.resources, self._avail_resources))
                elif trial.status == Trial.PAUSED:
                    raise TuneError(
                        "There are paused trials, but no more pending "
                        "trials with sufficient resources.")
            raise TuneError("Called step when all trials finished?")

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

    def debug_string(self):
        """Returns a human readable message for printing to the console."""

        messages = ["== Status =="]
        messages.append(self._scheduler_alg.debug_string())
        if self._resources_initialized:
            messages.append(
                "Resources used: {}/{} CPUs, {}/{} GPUs".format(
                    self._committed_resources.cpu,
                    self._avail_resources.cpu,
                    self._committed_resources.gpu,
                    self._avail_resources.gpu))
        for local_dir in sorted(set([t.local_dir for t in self._trials])):
            messages.append("Result logdir: {}".format(local_dir))
            for t in self._trials:
                if t.local_dir == local_dir:
                    messages.append(
                        " - {}:\t{}".format(t, t.progress_string()))
        return "\n".join(messages) + "\n"

    def has_resources(self, resources):
        """Returns whether this runner has at least the specified resources."""

        cpu_avail = self._avail_resources.cpu - self._committed_resources.cpu
        gpu_avail = self._avail_resources.gpu - self._committed_resources.gpu
        return resources.cpu <= cpu_avail and resources.gpu <= gpu_avail

    def _can_launch_more(self):
        self._update_avail_resources()
        trial = self._get_runnable()
        return trial is not None

    def _launch_trial(self):
        trial = self._get_runnable()
        self._commit_resources(trial.resources)
        try:
            trial.start()
            self._running[trial.train_remote()] = trial
        except Exception:
            print("Error starting runner, retrying:", traceback.format_exc())
            time.sleep(2)
            trial.stop(error=True)
            try:
                trial.start()
                self._running[trial.train_remote()] = trial
            except Exception:
                print("Error starting runner, abort:", traceback.format_exc())
                trial.stop(error=True)
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
            print("Error processing event:", traceback.format_exc())
            if trial.status == Trial.RUNNING:
                self._scheduler_alg.on_trial_error(self, trial)
                self._stop_trial(trial, error=True)

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

    def _stop_trial(self, trial, error=False):
        """Only returns resources if resources allocated."""
        prior_status = trial.status
        trial.stop(error=error)
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
