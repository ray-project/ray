import os
from pathlib import Path
import unittest

import ray
import ray.rllib.algorithms.marwil as marwil
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)

tf1, tf, tfv = try_import_tf()


class TestBC(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_bc_compilation_and_learning_from_offline_file(self):
        """Test whether a BCTrainer can be built with all frameworks.

        And learns from a historic-data file (while being evaluated on an
        actual env using evaluation_num_workers > 0).
        """
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        config = marwil.BC_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        config["evaluation_interval"] = 3
        config["evaluation_num_workers"] = 1
        config["evaluation_duration"] = 5
        config["evaluation_parallel_to_training"] = True
        # Evaluate on actual environment.
        config["evaluation_config"] = {"input": "sampler"}
        # Learn from offline data.
        config["input"] = [data_file]
        num_iterations = 350
        min_reward = 70.0

        # Test for all frameworks.
        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            trainer = marwil.BCTrainer(config=config, env="CartPole-v0")
            learnt = False
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)

                eval_results = results.get("evaluation")
                if eval_results:
                    print("iter={} R={}".format(i, eval_results["episode_reward_mean"]))
                    # Learn until good reward is reached in the actual env.
                    if eval_results["episode_reward_mean"] > min_reward:
                        print("learnt!")
                        learnt = True
                        break

            if not learnt:
                raise ValueError(
                    "BCTrainer did not reach {} reward from expert offline "
                    "data!".format(min_reward)
                )

            check_compute_single_action(trainer, include_prev_action_reward=True)

            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
