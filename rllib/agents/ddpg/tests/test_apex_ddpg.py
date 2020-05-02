import pytest
import unittest

import ray
import ray.rllib.agents.ddpg.apex as apex_ddpg
from ray.rllib.utils.test_utils import framework_iterator


class TestApexDDPG(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_apex_ddpg_compilation_and_per_worker_epsilon_values(self):
        """Test whether an APEX-DDPGTrainer can be built on all frameworks."""
        config = apex_ddpg.APEX_DDPG_DEFAULT_CONFIG.copy()
        config["num_workers"] = 3
        config["prioritized_replay"] = True
        config["timesteps_per_iteration"] = 100
        config["min_iter_time_s"] = 1
        config["learning_starts"] = 0
        config["optimizer"]["num_replay_buffer_shards"] = 1
        num_iterations = 1
        for _ in framework_iterator(config, ("torch", "tf")):
            plain_config = config.copy()
            trainer = apex_ddpg.ApexDDPGTrainer(
                config=plain_config, env="Pendulum-v0")

            for _ in range(num_iterations):
                print(trainer.train())

            trainer.stop()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
