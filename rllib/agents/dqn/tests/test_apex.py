import numpy as np
import pytest
import unittest

import ray
import ray.rllib.agents.dqn.apex as apex
from ray.rllib.utils.test_utils import framework_iterator


class TestApex(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_apex_compilation(self):
        """Test whether an APEX-DQNTrainer can be built on all frameworks."""
        config = apex.APEX_DEFAULT_CONFIG.copy()
        config["num_workers"] = 3
        config["prioritized_replay"] = True
        config["optimizer"]["num_replay_buffer_shards"] = 1
        num_iterations = 1

        for _ in framework_iterator(config, ("torch", "tf", "eager")):
            plain_config = config.copy()
            trainer = apex.ApexTrainer(config=plain_config, env="CartPole-v0")

            # Test per-worker epsilon distribution.
            infos = trainer.workers.foreach_policy(
                lambda p, _: p.get_exploration_info())
            eps = [i["cur_epsilon"] for i in infos]
            assert np.allclose(eps,
                               [1.0, 0.016190862, 0.00065536, 2.6527108e-05])

            for i in range(num_iterations):
                results = trainer.train()
                print(results)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
