from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time
import traceback

from ray.tune.trial import Trial


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
    This is because if insufficient resources are available, concurrent agents
    could deadlock waiting for new resources to become available. Furthermore,
    oversubscribing the cluster could degrade training performance, leading to
    misleading benchmark results.
    """

    def __init__(self):
        """Initializes a new TrialRunner."""

        self._trials = []
        self._pending = {}
        self._avail_resources = {'cpu': 0, 'gpu': 0}
        self._committed_resources = {k: 0 for k in self._avail_resources}

    def is_finished(self):
        """Returns whether all trials have finished running."""

        for t in self._trials:
            if t.status in [Trial.PENDING, Trial.RUNNING]:
                return False
        return True

    def step(self):
        """Runs one step of the trial event loop.

        Callers should typically run this method repeatedly in a loop. They
        may inspect or modify the runner's state in between calls to step().
        """

        if self._can_launch_more():
            self._launch_trial()
        elif self._pending:
            self._process_events()
        else:
            for trial in self._trials:
                if trial.status == Trial.PENDING:
                    assert self._has_resources(trial.resources), \
                        ("Insufficient cluster resources to launch trial",
                         trial.resources)
            assert False, "Called step when all trials finished?"

    def get_trials(self):
        """Returns the list of trials managed by this TrialRunner.

        Note that the caller usually should not mutate trial state directly.
        """

        return self._trials

    def add_trial(self, trial):
        """Adds a new trial to this TrialRunner.

        Trials may be added at any time.
        """

        self._trials.append(trial)

    def debug_string(self):
        """Returns a human readable message for printing to the console."""

        messages = ["== Status =="]
        messages.append(
            "Available resources: {}".format(self._avail_resources))
        messages.append(
            "Committed resources: {}".format(self._committed_resources))
        for local_dir in sorted(set([t.local_dir for t in self._trials])):
            messages.append("Tensorboard logdir: {}".format(local_dir))
            for t in self._trials:
                if t.local_dir == local_dir:
                    messages.append(
                        " - {}:\t{}".format(t, t.progress_string()))
        return "\n".join(messages) + "\n"

    def _can_launch_more(self):
        self._update_avail_resources()
        trial = self._get_runnable()
        return trial is not None

    def _launch_trial(self):
        trial = self._get_runnable()
        self._commit_resources(trial.resources)
        try:
            trial.start()
            self._pending[trial.train_remote()] = trial
        except:
            print("Error starting agent, retrying:", traceback.format_exc())
            time.sleep(2)
            trial.stop(error=True)
            try:
                trial.start()
                self._pending[trial.train_remote()] = trial
            except:
                print("Error starting agent, abort:", traceback.format_exc())
                trial.stop(error=True)
                # note that we don't return the resources, since they may
                # have been lost

    def _process_events(self):
        [result_id], _ = ray.wait(self._pending.keys())
        trial = self._pending[result_id]
        del self._pending[result_id]
        try:
            result = ray.get(result_id)
            print("result", result)
            trial.last_result = result

            if trial.should_stop(result):
                self._return_resources(trial.resources)
                trial.stop()
            else:
                # TODO(rliaw): This implements checkpoint in a blocking manner
                if trial.should_checkpoint():
                    trial.checkpoint()
                self._pending[trial.train_remote()] = trial
        except:
            print("Error processing event:", traceback.format_exc())
            if trial.status == Trial.RUNNING:
                self._return_resources(trial.resources)
                trial.stop(error=True)

    def _get_runnable(self):
        for trial in self._trials:
            if (trial.status == Trial.PENDING and
                    self._has_resources(trial.resources)):
                return trial
        return None

    def _has_resources(self, resources):
        for k, v in resources.items():
            if self._avail_resources[k] - self._committed_resources[k] < v:
                return False
        return True

    def _commit_resources(self, resources):
        for k, v in resources.items():
            self._committed_resources[k] += v
            assert self._avail_resources[k] >= 0

    def _return_resources(self, resources):
        for k, v in resources.items():
            self._committed_resources[k] -= v
            assert self._committed_resources[k] >= 0

    def _update_avail_resources(self):
        clients = ray.global_state.client_table()
        local_schedulers = [
            entry for client in clients.values() for entry in client
            if (entry['ClientType'] == 'local_scheduler' and not
                entry['Deleted'])
        ]
        num_cpus = sum(ls['NumCPUs'] for ls in local_schedulers)
        num_gpus = sum(ls['NumGPUs'] for ls in local_schedulers)
        self._avail_resources = {
            'cpu': int(num_cpus),
            'gpu': int(num_gpus),
        }
