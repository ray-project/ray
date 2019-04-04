"""Integration test: (1) pendulum works, (2) single-agent multi-agent works."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.tests.test_multi_agent_env import make_multiagent
from ray.tune import run_experiments
from ray.tune.registry import register_env

if __name__ == "__main__":
    ray.init()
    MultiPendulum = make_multiagent("Pendulum-v0")
    register_env("multi_pend", lambda _: MultiPendulum(1))
    trials = run_experiments({
        "test": {
            "run": "PPO",
            "env": "multi_pend",
            "stop": {
                "timesteps_total": 500000,
                "episode_reward_mean": -200,
            },
            "config": {
                "train_batch_size": 2048,
                "vf_clip_param": 10.0,
                "num_workers": 0,
                "num_envs_per_worker": 10,
                "lambda": 0.1,
                "gamma": 0.95,
                "lr": 0.0003,
                "sgd_minibatch_size": 64,
                "num_sgd_iter": 10,
                "model": {
                    "fcnet_hiddens": [64, 64],
                },
                "batch_mode": "complete_episodes",
            },
        }
    })
    if trials[0].last_result["episode_reward_mean"] < -200:
        raise ValueError("Did not get to -200 reward", trials[0].last_result)
