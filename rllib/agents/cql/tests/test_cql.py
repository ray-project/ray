import unittest

import ray
import ray.rllib.agents.cql as cql
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator


class TestCQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_cql_compilation(self):
        """Test whether a MAMLTrainer can be built with all frameworks."""
        config = cql.CQL_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["twin_q"] = True
        config["clip_actions"] = False
        config["normalize_actions"] = True
        config["learning_starts"] = 0
        config["rollout_fragment_length"] = 10
        config["train_batch_size"] = 10
        num_iterations = 1

        # Test for tf framework (torch not implemented yet).
        for fw in framework_iterator(config, frameworks=("torch")):
            for env in [
                    "MountainCarContinuous-v0",
            ]:
                print("env={}".format(env))
                trainer = cql.CQLTrainer(config=config, env=env)
                for i in range(num_iterations):
                    trainer.train()
                check_compute_single_action(trainer)
                trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
