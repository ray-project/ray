import os
from pathlib import Path
import unittest

import ray
import ray.rllib.agents.marwil as marwil
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_compute_single_action, \
    framework_iterator

tf1, tf, tfv = try_import_tf()


class TestMARWIL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_marwil_compilation_and_learning_from_offline_file(self):
        """Test whether a MARWILTrainer can be built with all frameworks.

        And learns from a historic-data file.
        """
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print("data_file={} exists={}".format(
            data_file, os.path.isfile(data_file)))

        config = marwil.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["evaluation_num_workers"] = 1
        config["evaluation_interval"] = 1
        config["evaluation_config"] = {"input": "sampler"}
        config["input"] = [data_file]
        num_iterations = 300

        # Test for all frameworks.
        for _ in framework_iterator(config):
            trainer = marwil.MARWILTrainer(config=config, env="CartPole-v0")
            for i in range(num_iterations):
                eval_results = trainer.train()["evaluation"]
                print("iter={} R={}".format(
                    i, eval_results["episode_reward_mean"]))
                # Learn until some reward is reached on an actual live env.
                if eval_results["episode_reward_mean"] > 60.0:
                    print("learnt!")
                    break

            check_compute_single_action(
                trainer, include_prev_action_reward=True)

            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
