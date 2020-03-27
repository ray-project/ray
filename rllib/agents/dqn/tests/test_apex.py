import numpy as np
import pytest
import unittest

import ray
import ray.rllib.agents.dqn.apex as apex


class TestApex(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_apex_epsilon_distribution(self):
        config = apex.APEX_DEFAULT_CONFIG.copy()
        config["num_workers"] = 3
        config["optimizer"]["num_replay_buffer_shards"] = 1
        trainer = apex.ApexTrainer(config, env="CartPole-v0")
        infos = trainer.workers.foreach_policy(
            lambda p, _: p.get_exploration_info())
        eps = [i["cur_epsilon"] for i in infos]
        assert np.allclose(eps, [1.0, 0.016190862, 0.00065536, 2.6527108e-05])


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
