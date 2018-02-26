#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import run_experiments
from ray.tune.pbt import PopulationBasedTraining

if __name__ == "__main__":

    pbt = PopulationBasedTraining(
        time_attr="time_total_s", reward_attr="episode_reward_mean",
        perturbation_interval=300,
        resample_probability=0.25,
        hyperparam_mutations={
            "train_batch_size": [32, 64, 128, 256, 512],
            "target_network_update_freq": [5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000],
            "lr": [1e-4, 2e-4, 3e-4, 4e-4, 5e-4],
        })

    ray.init()
    run_experiments({
        "pbt_apex_pong": {
            "run": "DQN",
            "env": "Pong-v0",
            "repeat": 4,
            "resources": {"cpu": 32, "gpu": 1},
            "config": {
                "num_workers": 32,
                "lr": .0001,
                "n_step": 3,
                "gamma": 0.99,
                "learning_starts": 50000,
                "buffer_size": 2000000,
                "target_network_update_freq": 160000,
                "sample_batch_size": 50,
                "max_weight_sync_delay": 400,
                "train_batch_size": 512,
                "apex_optimizer": True,
                "timesteps_per_iteration": 25000,
                "per_worker_exploration": True,
                "worker_side_prioritization": True,
                "num_replay_buffer_shards": 4,
                "model": {"grayscale": True},
            },
        },
    }, scheduler=pbt)
