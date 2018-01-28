#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random

import ray
from ray.tune import run_experiments
from ray.tune.pbt import PopulationBasedTraining

if __name__ == "__main__":
    pbt = PopulationBasedTraining(
        time_attr="time_total_s", reward_attr="episode_reward_mean",
        perturbation_interval=180,
        hyperparam_mutations={
            "num_sgd_iter": lambda config: random.randint(1, 30),
            "sgd_batchsize": lambda config: random.randint(128, 16384),
            "timesteps_per_batch": lambda config: random.randint(1000, 160000),
        })

    ray.init()
    run_experiments({
        "pbt_walker2d_test": {
            "run": "PPO",
            "env": "Walker2d-v1",
            "repeat": 8,
            "resources": {"cpu": 1, "gpu": 1},
            "config": {
                "kl_coeff": 1.0,
                "num_workers": 4,
                "devices": ["/gpu:0"],
                "sgd_stepsize": .0001,
                "num_sgd_iter": 20,
                "sgd_batchsize": 2048,
                "timesteps_per_batch": 20000,
            },
        },
    }, scheduler=pbt, verbose=False)
