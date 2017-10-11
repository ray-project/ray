from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import time
import traceback

from ray.tune.trial import PENDING, RUNNING


class TrialRunner(object):

    def __init__(self):
        self._trials = []
        self._pending = {}
        self._avail_resources = {'cpu': 0, 'gpu': 0}
        self._committed_resources = {k: 0 for k in self._avail_resources}

    def is_finished(self):
        for t in self._trials:
            if t.status in [PENDING, RUNNING]:
                return False
        return True

    def get_trials(self):
        return self._trials

    def add_trial(self, trial):
        self._trials.append(trial)

    def step(self):
        if self._can_launch_more():
            self._launch_trial()
        elif self._pending:
            self._process_events()
        else:
            for trial in self._trials:
                if trial.status == PENDING:
                    assert self._has_resources(trial.resources), \
                        ("Insufficient cluster resources to launch trial",
                         trial.resources)
            assert False, "Called step when all trials finished?"

    def debug_string(self):
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
            time.sleep(5)
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
            trial.update_progress(result)

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
            if trial.status == RUNNING:
                self._return_resources(trial.resources)
                trial.stop(error=True)

    def _get_runnable(self):
        for trial in self._trials:
            if (trial.status == PENDING and
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
