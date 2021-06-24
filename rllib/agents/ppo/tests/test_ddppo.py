import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.policy.policy import LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


class TestDDPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ddppo_compilation(self):
        """Test whether a DDPPOTrainer can be built with both frameworks."""
        config = ppo.ddppo.DEFAULT_CONFIG.copy()
        config["num_gpus_per_worker"] = 0
        num_iterations = 2

        for _ in framework_iterator(config, "torch"):
            trainer = ppo.ddppo.DDPPOTrainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                trainer.train()
            check_compute_single_action(trainer)
            trainer.stop()

    def test_ddppo_schedule(self):
        """Test whether lr_schedule will anneal lr to 0"""
        config = ppo.ddppo.DEFAULT_CONFIG.copy()
        config["num_gpus_per_worker"] = 0
        config["lr_schedule"] = [[0, config["lr"]], [1000, 0.0]]
        num_iterations = 3

        for _ in framework_iterator(config, "torch"):
            trainer = ppo.ddppo.DDPPOTrainer(config=config, env="CartPole-v0")
            for _ in range(num_iterations):
                result = trainer.train()
                lr = result["info"]["learner"][DEFAULT_POLICY_ID][
                    LEARNER_STATS_KEY]["cur_lr"]
            trainer.stop()
            assert lr == 0.0, "lr should anneal to 0.0"


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
