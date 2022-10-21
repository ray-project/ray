"""Integration test: (1) pendulum works, (2) single-agent multi-agent works."""
import unittest

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env
from ray.rllib.examples.env.multi_agent import MultiAgentPendulum
from ray.rllib.utils.test_utils import framework_iterator


class TestMultiAgentPendulum(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_multi_agent_pendulum(self):
        register_env(
            "multi_agent_pendulum", lambda _: MultiAgentPendulum({"num_agents": 1})
        )

        stop = {
            "timesteps_total": 500000,
            "episode_reward_mean": -400.0,
        }

        # Test for both torch and tf.
        for fw in framework_iterator(frameworks=["torch", "tf"]):
            trials = run_experiments(
                {
                    "test": {
                        "run": "PPO",
                        "env": "multi_agent_pendulum",
                        "stop": stop,
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
                                "fcnet_hiddens": [128, 128],
                            },
                            "batch_mode": "complete_episodes",
                            "framework": fw,
                        },
                    }
                },
                verbose=2,
            )
            if (
                trials[0].last_result["episode_reward_mean"]
                <= stop["episode_reward_mean"]
            ):
                raise ValueError(
                    "Did not get to {} reward".format(stop["episode_reward_mean"]),
                    trials[0].last_result,
                )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
