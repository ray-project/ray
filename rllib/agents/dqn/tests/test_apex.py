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

    def test_apex_compilation_and_per_worker_epsilon_values(self):
        """Test whether an APEX-DQNTrainer can be built on all frameworks."""
        config = apex.APEX_DEFAULT_CONFIG.copy()
        config["num_workers"] = 3
        config["prioritized_replay"] = True
        config["timesteps_per_iteration"] = 100
        config["min_iter_time_s"] = 1
        config["optimizer"]["num_replay_buffer_shards"] = 1

        for _ in framework_iterator(config, ("torch", "tf", "eager")):
            plain_config = config.copy()
            trainer = apex.ApexTrainer(config=plain_config, env="CartPole-v0")

            # Test per-worker epsilon distribution.
            infos = trainer.workers.foreach_policy(
                lambda p, _: p.get_exploration_info())
            eps = [i["cur_epsilon"] for i in infos]
            assert np.allclose(eps, [0.0, 0.4, 0.016190862, 0.00065536])

            # TODO(ekl) fix iterator metrics bugs w/multiple trainers.
            #            for i in range(1):
            #                results = trainer.train()
            #                print(results)

            trainer.stop()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
