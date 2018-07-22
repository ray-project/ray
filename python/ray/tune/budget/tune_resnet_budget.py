#!/usr/bin/env python
#
# Copyright 2015 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

# Disable linter warnings to maintain consistency with tutorial.
# pylint: disable=invalid-name
# pylint: disable=g-bad-import-order

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import time

import ray
from ray.tune import grid_search, run_experiments, register_trainable, \
    Trainable, TrainingResult
from budget import BudgetedScheduler

import os
import json
import numpy as np
import scipy.io as sio

stepbase = 5
class TrainResOnCifar(Trainable):
    """Example ResNet on CIFAR10 trainable."""

    def _setup(self):
        self.timestep = stepbase
        self.accuracy = 0
        self.perf = []
        self.iterations = 0
        self.timesteps_total = 0

        mat = sio.loadmat('/home/zhiyunlu/hyperparams/ray_ext/data/resnet_sept.mat')
        self.idx = self.config['index']
        self.y = mat['Y'][0][self.idx].ravel()
        self.lens = self.y.size
        self.x = mat['X'][self.idx]
        print('\n#### TRAINER ###: RETURNING SETUP index ', self.idx, 'hyp ', self.x, '####')

        assert (self.lens > 1)

    def _train(self):
        self.iterations += 1
        self.timesteps_total += self.timestep
        self.episodes_total = self.y[:self.timesteps_total]
        v = np.max(self.episodes_total)
        print('#### TRAINER ###: RETURNING TRAIN index %d, epoch %d, acc %g ####' % (self.idx, self.timesteps_total, v))
        tr = TrainingResult(mean_accuracy=v, episodes_total=self.episodes_total,
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
                    "episodes_total": self.episodes_total.tolist()
                }))
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path) as f:
            data = json.loads(f.read())
            self.timesteps_total = data["timesteps_total"]
            self.episodes_total = np.array(data["episodes_total"])
            self.iterations = data["iterations"]


# !!! Example of using the ray.tune Python API !!!
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args, _ = parser.parse_known_args()
    np.random.seed(1)
    iter_budget = 5
    register_trainable("my_res_sept", TrainResOnCifar)
    res_spec = {
        'run': 'my_res_sept',
        'trial_resources': {'cpu': 1, 'gpu': 0},
        'stop': {
            'mean_accuracy': 0.93,
            'time_total_s': iter_budget,
        },
        'config': {
            'index': grid_search(list(np.random.choice(96, size=5, replace=False))),  # index range 0-95
        },
        'repeat': 1,
        'local_dir': '~/hyperparams/ray_ext/ray_results/',
    }

    ray.init()

    budget = BudgetedScheduler(
        time_attr="training_iteration", reward_attr="mean_accuracy", t_budget=iter_budget)

    run_experiments({'resnet_budget_test': res_spec}, scheduler=budget, verbose=False)
