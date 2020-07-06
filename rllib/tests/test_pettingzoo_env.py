"""Integration test: (1) pendulum works, (2) single-agent multi-agent works."""
import unittest

import ray
from ray.tune import run_experiments
from ray.tune.registry import register_env
from ray.rllib.env import PettingZooEnv
from ray.rllib.utils.test_utils import framework_iterator
from pettingzoo.mpe import simple_world_comm_v0


class TestMultiAgentPendulum(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_multi_agent_pendulum(self):
        register_env("prison", lambda _: PettingZooEnv(simple_world_comm_v0.env()))

        ray.tune(
            "A2C",
            stop={"episodes_total": 60},
            checkpoint_freq=100,
            config={
                "train_batch_size": 128,
                "num_workers": 0,
                "num_envs_per_worker": 10,
                "lambda": 0.1,
                "gamma": 0.95,
                "lr": 0.0003,
            }
        )


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
