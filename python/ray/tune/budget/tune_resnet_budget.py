#!/usr/bin/env python
#
# ==============================================================================

# Disable linter warnings to maintain consistency with tutorial.
# pylint: disable=invalid-name
# pylint: disable=g-bad-import-order

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import json
import numpy as np
import scipy.io as sio

import ray
from ray.tune import grid_search, run_experiments, register_trainable, \
    Trainable, TrainingResult
from budget import BudgetedScheduler


STEPBASE = 5


class TrainResOnCifar(Trainable):
    """Example ResNet on CIFAR10 trainable."""

    def _setup(self):
        self.timestep = STEPBASE
        self.accuracy = 0
        self.perf = []
        self.iterations = 0
        self.timesteps_total = 0
        self.info = []

        mat = sio.loadmat('/home/zhiyunlu/hyperparams/ray_ext/data/resnet_sept.mat')
        self.idx = self.config['index']
        self.y = mat['Y'][0][self.idx].ravel()
        self.lens = self.y.size
        self.x = mat['X'][self.idx]
        # print('\n*** TRAINER ***: RETURNING SETUP index ', self.idx, 'hyp ', self.x, '####')

        # assert self.lens > 1

    def _train(self):
        self.iterations += 1
        self.timesteps_total += self.timestep
        self.info = self.y[:self.timesteps_total]
        v = np.max(self.info)
        print('*** TRAINER ***: RETURNING TRAIN index %d, epoch %d, acc %g ####' %
              (self.idx, self.timesteps_total, v))
        tr = TrainingResult(mean_accuracy=v, info=self.info,
                            timesteps_this_iter=self.timestep, timesteps_total=self.timesteps_total,
                            time_this_iter_s=1,  # to trick the system to stop when budget runs out
                            config={'idx': self.idx, 'hyp': self.x})
        return tr

    def _save(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "w") as f:
            f.write(
                json.dumps({
                    "timestep": self.timestep,
                    "iterations": self.iterations,
                    "timesteps_total": self.timesteps_total,
                    "trajectory": self.info.tolist()
                }))
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path) as f:
            data = json.loads(f.read())
            self.timesteps_total = data["timesteps_total"]
            self.info = np.array(data["trajectory"])
            self.iterations = data["iterations"]


# !!! Example of using the ray.tune Python API !!!
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args, _ = parser.parse_known_args()
    np.random.seed(1)
    iter_budget = 5
    register_trainable("my_res_sept", TrainResOnCifar)
    num_cpus = 4
    res_spec = {
        'run': 'my_res_sept',
        'trial_resources': {'cpu': num_cpus, 'gpu': 0},
        'stop': {
            'mean_accuracy': 0.93,
            # 'time_total_s': iter_budget,
        },
        'config': {
            'index': grid_search(list(np.random.choice(96, size=5, replace=False))),
            # index range 0-95
        },
        'repeat': 1,
        'local_dir': '~/hyperparams/ray_ext/ray_results/',
    }

    ray.init(num_cpus=num_cpus)

    budget = BudgetedScheduler(
        time_attr="training_iteration", reward_attr="mean_accuracy", t_budget=iter_budget)

    run_experiments({'resnet_budget_test': res_spec}, scheduler=budget, verbose=False)
