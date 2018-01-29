#!/usr/bin/env python

"""Example of using PBT with RLlib.

Note that this requires a cluster with at least 8 GPUs in order for all trials
to run concurrently, otherwise PBT will round-robin train the trials which
is less efficient."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random

import ray
from ray.tune import run_experiments
from ray.tune.pbt import PopulationBasedTraining

if __name__ == "__main__":

    # Postprocess the perturbed config to ensure it's still valid
    def explore(config):
        # ensure we collect enough timesteps to do sgd
        if config["timesteps_per_batch"] < config["sgd_batchsize"] * 2:
            config["timesteps_per_batch"] = config["sgd_batchsize"] * 2
        return config

    pbt = PopulationBasedTraining(
        time_attr="time_total_s", reward_attr="episode_reward_mean",
        perturbation_interval=120,
        resample_probability=0.25,
        # Specifies the resampling distributions of these hyperparams
        hyperparam_mutations={
            "sgd_stepsize": lambda config: random.uniform(.0001, .002),
            "num_sgd_iter": lambda config: random.randint(1, 30),
            "sgd_batchsize": lambda config: random.randint(128, 16384),
            "timesteps_per_batch":
                lambda config: random.randint(2000, 160000),
        },
        custom_explore_fn=explore)

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
                # Start off with several random variations
                "sgd_stepsize": lambda spec: random.uniform(.0001, .001),
                "num_sgd_iter": lambda spec: random.choice([10, 20, 30]),
                "sgd_batchsize": lambda spec: random.choice([128, 512, 2048]),
                "timesteps_per_batch":
                    lambda spec: random.choice([10000, 20000, 40000])
            },
        },
    }, scheduler=pbt)
