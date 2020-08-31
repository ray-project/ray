import pytest
import unittest

import ray
import ray.rllib.agents.ddpg.apex as apex_ddpg
from ray.rllib.utils.test_utils import check, check_compute_single_action, \
    framework_iterator


class TestApexDDPG(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    def test_apex_ddpg_compilation_and_per_worker_epsilon_values(self):
        """Test whether an APEX-DDPGTrainer can be built on all frameworks."""
        config = apex_ddpg.APEX_DDPG_DEFAULT_CONFIG.copy()
        config["num_workers"] = 2
        config["prioritized_replay"] = True
        config["timesteps_per_iteration"] = 100
        config["min_iter_time_s"] = 1
        config["learning_starts"] = 0
        config["optimizer"]["num_replay_buffer_shards"] = 1
        num_iterations = 1
        for _ in framework_iterator(config):
            plain_config = config.copy()
            trainer = apex_ddpg.ApexDDPGTrainer(
                config=plain_config, env="Pendulum-v0")

            # Test per-worker scale distribution.
            infos = trainer.workers.foreach_policy(
                lambda p, _: p.get_exploration_info())
            scale = [i["cur_scale"] for i in infos]
            expected = [
                0.4**(1 + (i + 1) / float(config["num_workers"] - 1) * 7)
                for i in range(config["num_workers"])
            ]
            check(scale, [0.0] + expected)

            for _ in range(num_iterations):
                print(trainer.train())
            check_compute_single_action(trainer)

            # Test again per-worker scale distribution
            # (should not have changed).
            infos = trainer.workers.foreach_policy(
                lambda p, _: p.get_exploration_info())
            scale = [i["cur_scale"] for i in infos]
            check(scale, [0.0] + expected)

            trainer.stop()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
