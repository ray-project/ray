import unittest

import ray
import ray.rllib.agents.ars as ars
from ray.rllib.utils.test_utils import framework_iterator, check_compute_single_action


class TestARS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=3)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ars_compilation(self):
        """Test whether an ARSTrainer can be built on all frameworks."""
        config = ars.DEFAULT_CONFIG.copy()
        # Keep it simple.
        config["model"]["fcnet_hiddens"] = [10]
        config["model"]["fcnet_activation"] = None
        config["noise_size"] = 2500000
        # Test eval workers ("normal" WorkerSet, unlike ARS' list of
        # RolloutWorkers used for collecting train batches).
        config["evaluation_interval"] = 1
        config["evaluation_num_workers"] = 1

        num_iterations = 2

        for _ in framework_iterator(config):
            plain_config = config.copy()
            trainer = ars.ARSTrainer(config=plain_config, env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                print(results)

            check_compute_single_action(trainer)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
