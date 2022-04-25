import unittest
import pytest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


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

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.ddppo.DDPPOTrainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)
                # Make sure, weights on all workers are the same.
                weights = trainer.workers.foreach_worker(lambda w: w.get_weights())
                for w in weights[1:]:
                    check(w, weights[1])

            check_compute_single_action(trainer)
            trainer.stop()

    def test_ddppo_schedule(self):
        """Test whether lr_schedule will anneal lr to 0"""
        config = ppo.ddppo.DEFAULT_CONFIG.copy()
        config["num_gpus_per_worker"] = 0
        config["lr_schedule"] = [[0, config["lr"]], [1000, 0.0]]
        num_iterations = 10

        for _ in framework_iterator(config, "torch"):
            trainer = ppo.ddppo.DDPPOTrainer(config=config, env="CartPole-v0")
            for _ in range(num_iterations):
                result = trainer.train()
                if result["info"][LEARNER_INFO]:
                    lr = result["info"][LEARNER_INFO][DEFAULT_POLICY_ID][
                        LEARNER_STATS_KEY
                    ]["cur_lr"]
            trainer.stop()
            assert lr == 0.0, "lr should anneal to 0.0"

    def test_validate_config(self):
        """Test if DDPPO will raise errors after invalid configs are passed."""
        config = ppo.ddppo.DEFAULT_CONFIG.copy()
        config["kl_coeff"] = 1.0
        msg = "DDPPO doesn't support KL penalties like PPO-1"
        with pytest.raises(ValueError, match=msg):
            ppo.ddppo.DDPPOTrainer(config=config, env="CartPole-v0")
        config["kl_coeff"] = 0.0
        config["kl_target"] = 1.0
        with pytest.raises(ValueError, match=msg):
            ppo.ddppo.DDPPOTrainer(config=config, env="CartPole-v0")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
